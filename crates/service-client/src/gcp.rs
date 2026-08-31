// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! GCP OIDC ID-token mint client for HTTP deployments hosted on Cloud Run and similar
//! Google-fronted endpoints.
//!
//! Credentials are cached in a process-wide registry. Construction runs on the ambient
//! [`TaskCenter`]'s default runtime so refresh tasks have process lifetime. Invocation tasks
//! inherit that task-local context at the invoker's `JoinSet` boundary.
//!
//! A deployment may instead select AWS-to-GCP workload identity federation; see the
//! [`federation`] submodule for that trust chain.

use std::sync::{Arc, Once, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use bytestring::ByteString;
use futures::FutureExt as _;
use futures::future::{BoxFuture, Shared};
use google_cloud_auth::credentials::Credentials as GoogleCredentials;
use moka::future::Cache;
use moka::ops::compute::{CompResult, Op};
use restate_core::{TaskCenter, TaskKind};
use restate_types::deployment::GoogleIdTokenAuth;
use thiserror::Error;
use tokio::sync::Semaphore;
use tokio::time::Instant;
use tracing::warn;

#[cfg(any(test, feature = "test_util"))]
use ahash::HashMap;
#[cfg(any(test, feature = "test_util"))]
use parking_lot::Mutex;

mod federation;

pub(crate) use federation::initialize_config as initialize_federation_config;

// Bounds one caller's wait across credential construction and initial token acquisition.
// Construction continues after a timeout and remains available to later callers.
const MINT_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);

const CACHE_TIME_TO_IDLE: Duration = Duration::from_secs(3600);

const CACHE_HOUSEKEEPING_INTERVAL: Duration = Duration::from_secs(300);

const SOURCE_PROBE_TIMEOUT: Duration = Duration::from_secs(1);

/// Caps concurrent blocking `google-cloud-auth`/ADC builds, so a burst of distinct new keys (or a
/// GCP outage causing every retry to rebuild) cannot exhaust tokio's blocking thread pool. This is
/// fixed rather than CPU-scaled because Tokio's blocking pool is independent of CPU concurrency.
const MAX_CONCURRENT_BLOCKING_BUILDS: usize = 4;

#[derive(Clone, Debug, Error)]
pub enum GcpAuthError {
    #[error(
        "failed to initialize GCP source credentials (audience '{audience}', impersonating '{service_account}'): {message}"
    )]
    CredentialSource {
        audience: String,
        service_account: String,
        message: String,
    },
    #[error("failed to build ID token credentials for audience '{audience}': {message}")]
    Build { audience: String, message: String },
    #[error(
        "the ambient Application Default Credentials identity cannot mint an ID token for audience '{audience}'. \
         User credentials (from `gcloud auth application-default login`) and Workload Identity Federation \
         (`external_account`) sources cannot mint ID tokens directly; set `--gcp-impersonate-service-account` to \
         mint the token via impersonation, or run Restate with a service-account key (`GOOGLE_APPLICATION_CREDENTIALS`) \
         or a GCE/GKE/Cloud Run metadata-server identity"
    )]
    AmbientUnsupported { audience: String },
    #[error(
        "failed to mint ID token (audience '{audience}', impersonating '{service_account}'): {message}"
    )]
    Mint {
        audience: String,
        service_account: String,
        message: String,
    },
    #[error(
        "token mint timed out after {duration:?} (audience '{audience}', impersonating '{service_account}')"
    )]
    Timeout {
        audience: String,
        service_account: String,
        duration: Duration,
    },
}

/// Identity dimensions that select credential construction and cache keys.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub(crate) enum IdTokenIdentity {
    Ambient,
    Impersonated {
        service_account: ByteString,
    },
    Federated {
        provider: ByteString,
        service_account: ByteString,
    },
}

impl IdTokenIdentity {
    fn impersonated(service_account: impl Into<ByteString>) -> Self {
        Self::Impersonated {
            service_account: service_account.into(),
        }
    }

    fn federated(provider: impl Into<ByteString>, service_account: impl Into<ByteString>) -> Self {
        Self::Federated {
            provider: provider.into(),
            service_account: service_account.into(),
        }
    }

    fn service_account_context(&self) -> &str {
        match self {
            IdTokenIdentity::Ambient => "(ambient)",
            IdTokenIdentity::Impersonated { service_account }
            | IdTokenIdentity::Federated {
                service_account, ..
            } => service_account,
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub(crate) struct IdTokenSpec {
    identity: IdTokenIdentity,
    audience: ByteString,
}

impl IdTokenSpec {
    pub(crate) fn from_deployment_auth(
        auth: GoogleIdTokenAuth,
    ) -> Result<Self, (ByteString, &'static str)> {
        let (audience, service_account, provider) = auth.into_parts();
        let identity = match (provider, service_account) {
            (Some(provider), Some(service_account)) => {
                IdTokenIdentity::federated(provider, service_account)
            }
            (None, Some(service_account)) => IdTokenIdentity::impersonated(service_account),
            (None, None) => IdTokenIdentity::Ambient,
            (Some(_), None) => {
                return Err((
                    audience,
                    "GCP workload identity federation requires impersonate_service_account to be \
                     set; re-register the deployment",
                ));
            }
        };
        Ok(Self { identity, audience })
    }

    pub(crate) fn audience(&self) -> &str {
        &self.audience
    }

    pub(crate) fn service_account_context(&self) -> &str {
        self.identity.service_account_context()
    }
}

/// Includes the source chain because `CredentialsError::Display` hides IAM 403 details.
fn display_error_chain(error: &(dyn std::error::Error + 'static)) -> String {
    let mut message = error.to_string();
    let mut cause = error.source();
    while let Some(err) = cause {
        message.push_str(": ");
        message.push_str(&err.to_string());
        cause = err.source();
    }
    message
}

#[async_trait]
trait IdTokenSource: Send + Sync {
    async fn id_token(&self) -> Result<String, google_cloud_auth::errors::CredentialsError>;
}

type Credential = Arc<dyn IdTokenSource>;
type CredentialBuild = Shared<BoxFuture<'static, Result<Credential, GcpAuthError>>>;

/// Cache-owned construction state. Retaining the shared future lets construction outlive any
/// individual caller, while the once-lock ensures only one TaskCenter build is spawned per entry.
struct CredentialEntry {
    build: OnceLock<CredentialBuild>,
}

impl CredentialEntry {
    fn new() -> Self {
        Self {
            build: OnceLock::new(),
        }
    }

    async fn get_or_start(
        &self,
        registry: &'static CredentialRegistry,
        spec: &IdTokenSpec,
    ) -> Result<Credential, GcpAuthError> {
        self.build
            .get_or_init(|| registry.start_build(spec.clone()))
            .clone()
            .await
    }

    fn completed_build_failed(&self) -> bool {
        // The TaskCenter-owned build can finish after its only waiter times out, leaving the
        // Shared wrapper unpolled. Poll once to harvest an already-completed task without waiting.
        self.build.get().is_some_and(|build| match build.peek() {
            Some(result) => result.is_err(),
            None => build
                .clone()
                .now_or_never()
                .is_some_and(|result| result.is_err()),
        })
    }
}

struct Live(google_cloud_auth::credentials::idtoken::IDTokenCredentials);

#[async_trait]
impl IdTokenSource for Live {
    async fn id_token(&self) -> Result<String, google_cloud_auth::errors::CredentialsError> {
        self.0.id_token().await
    }
}

/// Process-wide credentials and the shared sources they mint through.
struct CredentialRegistry {
    cache: Cache<IdTokenSpec, Arc<CredentialEntry>>,
    ambient_source: RecoverableCredentialSource,
    /// Weak-indexed sources leased by cached federated ID-token credentials.
    federated_access_token_sources: federation::FederatedAccessTokenSourceIndex,
    #[cfg(test)]
    test_hooks: TestHooks,
}

struct RecoverableCredentialSource {
    cell: tokio::sync::Mutex<Option<GoogleCredentials>>,
}

impl RecoverableCredentialSource {
    fn new() -> Self {
        Self {
            cell: tokio::sync::Mutex::new(None),
        }
    }

    async fn get_or_build(
        &self,
        build: impl Future<Output = Result<GoogleCredentials, String>>,
    ) -> Result<GoogleCredentials, String> {
        let mut guard = self.cell.lock().await;
        if let Some(value) = guard.as_ref() {
            return Ok(value.clone());
        }
        let value = build.await?;
        *guard = Some(value.clone());
        Ok(value)
    }

    // Recovery is idempotent. Do not queue another probe behind a build or recovery already
    // holding the slot; a later failed mint can retry if the in-flight operation does not heal it.
    async fn replace_if_dead(
        &self,
        build: impl Future<Output = Result<GoogleCredentials, String>>,
    ) -> Result<bool, String> {
        let Ok(mut guard) = self.cell.try_lock() else {
            return Ok(false);
        };
        let Some(current) = guard.as_ref() else {
            return Ok(false);
        };
        if !credential_source_is_dead(current).await {
            return Ok(false);
        }
        match build.await {
            Ok(value) => {
                *guard = Some(value);
                Ok(true)
            }
            Err(e) => {
                *guard = None;
                Err(e)
            }
        }
    }

    #[cfg(test)]
    async fn seed_for_test(&self, value: GoogleCredentials) {
        *self.cell.lock().await = Some(value);
    }
}

#[cfg(test)]
type ConstructOverride =
    Arc<dyn Fn(IdTokenSpec) -> BoxFuture<'static, Result<Credential, GcpAuthError>> + Send + Sync>;
#[cfg(test)]
type AmbientSourceOverride = Arc<dyn Fn() -> Result<GoogleCredentials, String> + Send + Sync>;

#[cfg(test)]
#[derive(Default)]
struct TestHooks {
    build_overrides: Mutex<HashMap<IdTokenSpec, ConstructOverride>>,
    ambient_source_override: Mutex<Option<AmbientSourceOverride>>,
}

static REGISTRY: OnceLock<CredentialRegistry> = OnceLock::new();
static HOUSEKEEPING_STARTED: Once = Once::new();

fn credential_registry() -> &'static CredentialRegistry {
    let registry = REGISTRY.get_or_init(CredentialRegistry::new);
    // Do not poison the Once if an embedding caller reaches the registry without TaskCenter
    // context. A later production caller can still start housekeeping.
    if TaskCenter::try_current().is_some() {
        HOUSEKEEPING_STARTED.call_once(|| registry.start_housekeeping());
    }
    registry
}

/// Mints an OIDC ID token for `spec`.
pub(crate) async fn mint(spec: &IdTokenSpec) -> Result<String, GcpAuthError> {
    let audience = spec.audience();
    let service_account = spec.service_account_context();

    #[cfg(any(test, feature = "test_util"))]
    if let Some(result) = test_override(spec, service_account) {
        return result;
    }

    let deadline = Instant::now() + MINT_ATTEMPT_TIMEOUT;
    let timeout_error = || GcpAuthError::Timeout {
        audience: audience.to_owned(),
        service_account: service_account.to_owned(),
        duration: MINT_ATTEMPT_TIMEOUT,
    };
    let registry = credential_registry();
    let entry = tokio::time::timeout_at(deadline, registry.get_entry(spec))
        .await
        .map_err(|_| timeout_error())?;
    let source = match tokio::time::timeout_at(deadline, entry.get_or_start(registry, spec)).await {
        Ok(Ok(source)) => source,
        Ok(Err(error)) => {
            let _ = registry.evict_if_unchanged(spec, &entry).await;
            return Err(error);
        }
        Err(_) => return Err(timeout_error()),
    };

    match tokio::time::timeout_at(deadline, source.id_token()).await {
        Ok(Ok(token)) => Ok(token),
        Ok(Err(error)) => {
            let message = display_error_chain(&error);
            // Transient errors may self-heal; evict permanent failures only if still current.
            if !error.is_transient() && registry.evict_if_unchanged(spec, &entry).await {
                // Federated and ambient sources recover independently.
                match &spec.identity {
                    IdTokenIdentity::Federated { provider, .. } => {
                        registry
                            .federated_access_token_sources
                            .recover_if_dead(provider)
                            .await;
                    }
                    IdTokenIdentity::Impersonated { .. } => {
                        registry.spawn_ambient_source_recovery(message.clone());
                    }
                    IdTokenIdentity::Ambient => {}
                }
            }
            Err(GcpAuthError::Mint {
                audience: audience.to_owned(),
                service_account: service_account.to_owned(),
                message,
            })
        }
        // A timeout does not prove the refresh task dead, so retain the entry. A continuously
        // used, wedged credential may keep timing out, but each caller remains bounded and no
        // duplicate refresh task is created.
        Err(_) => Err(timeout_error()),
    }
}

impl CredentialRegistry {
    fn new() -> Self {
        Self {
            // Keys come only from operator-controlled deployment or discovery configuration, not
            // invocation input. TTI removes abandoned attempts; a hard capacity would instead
            // turn excess cardinality into credential rebuild churn. See #5184.
            cache: Cache::builder().time_to_idle(CACHE_TIME_TO_IDLE).build(),
            ambient_source: RecoverableCredentialSource::new(),
            federated_access_token_sources: federation::FederatedAccessTokenSourceIndex::default(),
            #[cfg(test)]
            test_hooks: TestHooks::default(),
        }
    }

    fn start_housekeeping(&'static self) {
        // Cache operations also drive Moka's maintenance. This loop makes cleanup of completely
        // idle bookkeeping timely; if it fails, authentication remains correct, so the
        // Credentials task kind logs the failure rather than shutting down the node.
        let housekeeping = async move {
            let mut interval = tokio::time::interval(CACHE_HOUSEKEEPING_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                interval.tick().await;
                // Drop idle outer credentials before reaping their weakly indexed sources.
                self.cache.run_pending_tasks().await;
                self.federated_access_token_sources.reap_dead();
            }
        };
        let _ = TaskCenter::spawn(
            TaskKind::Credentials,
            "gcp-credential-housekeeping",
            housekeeping,
        );
    }

    async fn get_entry(&self, spec: &IdTokenSpec) -> Arc<CredentialEntry> {
        loop {
            let entry = self
                .cache
                .get_with_by_ref(spec, async { Arc::new(CredentialEntry::new()) })
                .await;
            // A caller may time out while construction continues and later fails. Do not make the
            // next caller consume that completed, stale error before it can start a fresh build.
            if !entry.completed_build_failed() {
                return entry;
            }
            let _ = self.evict_if_unchanged(spec, &entry).await;
        }
    }

    async fn evict_if_unchanged(
        &self,
        spec: &IdTokenSpec,
        expected: &Arc<CredentialEntry>,
    ) -> bool {
        let result = self
            .cache
            .entry_by_ref(spec)
            .and_compute_with(|entry| {
                let op = match &entry {
                    Some(entry) if Arc::ptr_eq(entry.value(), expected) => Op::Remove,
                    _ => Op::Nop,
                };
                std::future::ready(op)
            })
            .await;
        matches!(result, CompResult::Removed(_))
    }

    async fn ambient_source(&self) -> Result<GoogleCredentials, String> {
        self.ambient_source
            .get_or_build(self.build_ambient_source())
            .await
    }

    /// Replace only a source whose refresh task is proven dead; a target-scoped failure must not
    /// strand a healthy shared source.
    fn spawn_ambient_source_recovery(&'static self, triggering_error: String) {
        let _ = TaskCenter::current().spawn_unmanaged(
            TaskKind::Credentials,
            "gcp-ambient-credential-source-recovery",
            async move { self.recover_ambient_source_if_dead(&triggering_error).await },
        );
    }

    async fn recover_ambient_source_if_dead(&self, triggering_error: &str) {
        match self
            .ambient_source
            .replace_if_dead(self.build_ambient_source())
            .await
        {
            Ok(true) => {
                warn!(
                    triggering_error,
                    "replaced the shared ambient GCP credential source: its refresh task was proven dead"
                );
            }
            Ok(false) => {}
            Err(error) => {
                warn!(
                    error = %error,
                    triggering_error,
                    "failed to rebuild the ambient GCP credential source after its refresh task \
                     was proven dead; a future mint attempt will retry"
                );
            }
        }
    }

    async fn build_ambient_source(&self) -> Result<GoogleCredentials, String> {
        #[cfg(test)]
        {
            let override_fn = self.test_hooks.ambient_source_override.lock().clone();
            if let Some(f) = override_fn {
                return f();
            }
        }

        let build = || {
            google_cloud_auth::credentials::Builder::default()
                .build()
                .map_err(|e| e.to_string())
        };
        let task = TaskCenter::current()
            .spawn_unmanaged(
                TaskKind::Credentials,
                "gcp-ambient-credential-source-build",
                async move { spawn_bounded_blocking(build).await },
            )
            .map_err(|_| {
                "TaskCenter is shutting down while building the ambient GCP credential source"
                    .to_owned()
            })?;
        match task.await {
            Ok(Ok(result)) => result,
            Ok(Err(join_error)) => Err(format!(
                "ambient GCP credential source construction task panicked: {join_error}"
            )),
            Err(_shutdown) => {
                Err("ambient GCP credential source construction task failed".to_owned())
            }
        }
    }

    fn start_build(&'static self, spec: IdTokenSpec) -> CredentialBuild {
        let registry = self;
        // google-cloud-auth uses bare tokio::spawn for refresh tasks. Build on the TaskCenter's
        // default runtime so they do not inherit a short-lived partition runtime.
        let audience = spec.audience.clone();
        match TaskCenter::current().spawn_unmanaged(
            TaskKind::Credentials,
            "gcp-id-token-credential-build",
            async move {
                #[cfg(test)]
                if let Some(f) = {
                    registry
                        .test_hooks
                        .build_overrides
                        .lock()
                        .get(&spec)
                        .cloned()
                } {
                    return f(spec).await;
                }
                registry.build_credentials(spec).await
            },
        ) {
            Ok(task) => async move {
                task.await.unwrap_or_else(|_| {
                    Err(GcpAuthError::Build {
                        audience: audience.to_string(),
                        message: "GCP credential construction task failed".to_owned(),
                    })
                })
            }
            .boxed()
            .shared(),
            Err(_) => futures::future::ready(Err(GcpAuthError::Build {
                audience: audience.to_string(),
                message: "TaskCenter is shutting down".to_owned(),
            }))
            .boxed()
            .shared(),
        }
    }

    async fn build_credentials(&self, spec: IdTokenSpec) -> Result<Credential, GcpAuthError> {
        let IdTokenSpec { identity, audience } = spec;
        let panic_context = audience.to_string();
        match identity {
            IdTokenIdentity::Ambient => {
                run_blocking(panic_context, move || build_ambient_credentials(&audience)).await
            }
            IdTokenIdentity::Impersonated { service_account } => {
                let source = self.ambient_source().await.map_err(|message| {
                    GcpAuthError::CredentialSource {
                        audience: audience.to_string(),
                        service_account: service_account.to_string(),
                        message,
                    }
                })?;
                run_blocking(panic_context, move || {
                    build_impersonated_credentials(&audience, &service_account, source)
                })
                .await
            }
            // Federation is pure async I/O and deliberately bypasses the blocking permits.
            IdTokenIdentity::Federated {
                provider,
                service_account,
            } => {
                federation::build_federated_source(
                    &self.federated_access_token_sources,
                    provider.into(),
                    service_account.into(),
                    audience.into(),
                )
                .await
            }
        }
    }
}

// google-cloud-auth 1.16 publishes a permanent error before its refresh task exits and keeps
// retrying transient errors. Probe through its public headers() API; success, transient errors,
// and timeouts do not prove the source dead and must retain it.
async fn credential_source_is_dead(source: &GoogleCredentials) -> bool {
    matches!(
        tokio::time::timeout(SOURCE_PROBE_TIMEOUT, source.headers(http::Extensions::new()))
            .await,
        Ok(Err(e)) if !e.is_transient()
    )
}

/// Bounds only the blocking call, never a whole key construction, so waits cannot deadlock.
static BLOCKING_BUILD_PERMITS: Semaphore = Semaphore::const_new(MAX_CONCURRENT_BLOCKING_BUILDS);

async fn spawn_bounded_blocking<T: Send + 'static>(
    build: impl FnOnce() -> T + Send + 'static,
) -> Result<T, tokio::task::JoinError> {
    let permit = BLOCKING_BUILD_PERMITS
        .acquire()
        .await
        .expect("BLOCKING_BUILD_PERMITS is never closed");
    tokio::task::spawn_blocking(move || {
        // spawn_blocking work is not cancelled when its awaiting task is dropped, so the permit
        // must remain with the blocking work rather than the caller future.
        let _permit = permit;
        build()
    })
    .await
}

async fn run_blocking(
    audience: String,
    build: impl FnOnce() -> Result<Arc<dyn IdTokenSource>, GcpAuthError> + Send + 'static,
) -> Result<Arc<dyn IdTokenSource>, GcpAuthError> {
    spawn_bounded_blocking(build).await.unwrap_or_else(|e| {
        Err(GcpAuthError::Build {
            audience,
            message: format!("construction thread panicked: {e}"),
        })
    })
}

fn build_ambient_credentials(audience: &str) -> Result<Arc<dyn IdTokenSource>, GcpAuthError> {
    use google_cloud_auth::credentials::idtoken;

    let credentials = idtoken::Builder::new(audience).build().map_err(|e| {
        // authorized_user (gcloud) and external_account (Workload Identity Federation) ADC
        // sources cannot mint ID tokens directly.
        if e.is_not_supported() {
            GcpAuthError::AmbientUnsupported {
                audience: audience.to_owned(),
            }
        } else {
            GcpAuthError::Build {
                audience: audience.to_owned(),
                message: e.to_string(),
            }
        }
    })?;
    Ok(Arc::new(Live(credentials)) as Arc<dyn IdTokenSource>)
}

fn build_impersonated_credentials(
    audience: &str,
    service_account: &str,
    source: GoogleCredentials,
) -> Result<Arc<dyn IdTokenSource>, GcpAuthError> {
    use google_cloud_auth::credentials::idtoken;

    let credentials =
        idtoken::impersonated::Builder::from_source_credentials(audience, service_account, source)
            .build()
            .map_err(|e| GcpAuthError::Build {
                audience: audience.to_owned(),
                message: e.to_string(),
            })?;
    Ok(Arc::new(Live(credentials)) as Arc<dyn IdTokenSource>)
}

#[cfg(any(test, feature = "test_util"))]
enum TestOverride {
    Token(String),
    Failure(String),
}

#[cfg(any(test, feature = "test_util"))]
static TEST_OVERRIDES: std::sync::LazyLock<Mutex<HashMap<IdTokenSpec, Arc<TestOverride>>>> =
    std::sync::LazyLock::new(|| Mutex::new(HashMap::default()));

#[cfg(any(test, feature = "test_util"))]
pub struct TestOverrideGuard {
    spec: IdTokenSpec,
    installed: Arc<TestOverride>,
}

#[cfg(any(test, feature = "test_util"))]
impl Drop for TestOverrideGuard {
    fn drop(&mut self) {
        let mut overrides = TEST_OVERRIDES.lock();
        if matches!(overrides.get(&self.spec), Some(current) if Arc::ptr_eq(current, &self.installed))
        {
            overrides.remove(&self.spec);
        }
    }
}

#[cfg(any(test, feature = "test_util"))]
fn install_test_override(spec: IdTokenSpec, value: TestOverride) -> TestOverrideGuard {
    let installed = Arc::new(value);
    TEST_OVERRIDES
        .lock()
        .insert(spec.clone(), installed.clone());
    TestOverrideGuard { spec, installed }
}

#[cfg(any(test, feature = "test_util"))]
pub(crate) fn override_token_for_test(
    service_account: Option<&str>,
    audience: &str,
    token: String,
) -> TestOverrideGuard {
    install_test_override(
        IdTokenSpec {
            identity: service_account.map_or(IdTokenIdentity::Ambient, |service_account| {
                IdTokenIdentity::Impersonated {
                    service_account: service_account.into(),
                }
            }),
            audience: audience.into(),
        },
        TestOverride::Token(token),
    )
}

#[cfg(any(test, feature = "test_util"))]
pub(crate) fn override_failure_for_test(
    service_account: Option<&str>,
    audience: &str,
    message: &str,
) -> TestOverrideGuard {
    install_test_override(
        IdTokenSpec {
            identity: service_account.map_or(IdTokenIdentity::Ambient, |service_account| {
                IdTokenIdentity::Impersonated {
                    service_account: service_account.into(),
                }
            }),
            audience: audience.into(),
        },
        TestOverride::Failure(message.to_owned()),
    )
}

#[cfg(any(test, feature = "test_util"))]
fn test_override(
    spec: &IdTokenSpec,
    service_account: &str,
) -> Option<Result<String, GcpAuthError>> {
    match TEST_OVERRIDES.lock().get(spec)?.as_ref() {
        TestOverride::Token(token) => Some(Ok(token.clone())),
        TestOverride::Failure(message) => Some(Err(GcpAuthError::Mint {
            audience: spec.audience.to_string(),
            service_account: service_account.to_owned(),
            message: message.clone(),
        })),
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    fn token() -> String {
        "test-token".to_owned()
    }

    fn ok_source() -> Credential {
        MockSource::new(|_| MockOutcome::Token(token()))
    }

    fn permanently_failing_source() -> Credential {
        MockSource::new(|_| MockOutcome::Error(permanent_error("impersonation misconfigured")))
    }

    #[test]
    fn ambient_unsupported_error_is_actionable_and_leak_free() {
        let err = GcpAuthError::AmbientUnsupported {
            audience: "https://svc-abc-uc.a.run.app".into(),
        };
        let msg = err.to_string();
        // Actionable: names the audience and the fix.
        assert!(msg.contains("https://svc-abc-uc.a.run.app"), "{msg}");
        assert!(msg.contains("--gcp-impersonate-service-account"), "{msg}");
        // Leak-free: must not surface the google-cloud-auth internal API hint.
        assert!(!msg.contains("idtoken::user_account"), "{msg}");
        assert!(!msg.to_lowercase().contains("builder directly"), "{msg}");
    }

    struct MockSource {
        calls: AtomicUsize,
        behavior: Mutex<Box<dyn FnMut(usize) -> MockOutcome + Send>>,
    }

    enum MockOutcome {
        Token(String),
        Error(google_cloud_auth::errors::CredentialsError),
        Hang,
    }

    impl MockSource {
        fn new(behavior: impl FnMut(usize) -> MockOutcome + Send + 'static) -> Arc<Self> {
            Arc::new(Self {
                calls: AtomicUsize::new(0),
                behavior: Mutex::new(Box::new(behavior)),
            })
        }
    }

    #[async_trait]
    impl IdTokenSource for MockSource {
        async fn id_token(&self) -> Result<String, google_cloud_auth::errors::CredentialsError> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            let outcome = (self.behavior.lock())(call);
            match outcome {
                MockOutcome::Token(token) => Ok(token),
                MockOutcome::Error(error) => Err(error),
                MockOutcome::Hang => std::future::pending().await,
            }
        }
    }

    fn ready_entry(source: Credential) -> Arc<CredentialEntry> {
        let entry = Arc::new(CredentialEntry::new());
        assert!(
            entry
                .build
                .set(futures::future::ready(Ok(source)).boxed().shared())
                .is_ok()
        );
        entry
    }

    impl IdTokenSpec {
        fn ambient(audience: &str) -> Self {
            IdTokenSpec {
                identity: IdTokenIdentity::Ambient,
                audience: audience.into(),
            }
        }

        fn impersonated(audience: &str, service_account: &str) -> Self {
            IdTokenSpec {
                identity: IdTokenIdentity::impersonated(service_account),
                audience: audience.into(),
            }
        }

        fn federated(audience: &str, provider: &str, service_account: &str) -> Self {
            IdTokenSpec {
                identity: IdTokenIdentity::federated(provider, service_account),
                audience: audience.into(),
            }
        }
    }

    async fn mint_for_test(
        identity: IdTokenIdentity,
        audience: &str,
    ) -> Result<String, GcpAuthError> {
        let spec = IdTokenSpec {
            identity,
            audience: audience.into(),
        };
        mint(&spec).await
    }

    fn transient_error(message: &str) -> google_cloud_auth::errors::CredentialsError {
        google_cloud_auth::errors::CredentialsError::from_msg(true, message)
    }

    fn permanent_error(message: &str) -> google_cloud_auth::errors::CredentialsError {
        google_cloud_auth::errors::CredentialsError::from_msg(false, message)
    }

    fn add_build_override(
        cache_key: IdTokenSpec,
        f: impl Fn(&IdTokenSpec) -> Result<Credential, GcpAuthError> + Send + Sync + 'static,
    ) {
        let f = Arc::new(f);
        credential_registry()
            .test_hooks
            .build_overrides
            .lock()
            .insert(
                cache_key,
                Arc::new(move |spec| {
                    let result = f(&spec);
                    futures::future::ready(result).boxed()
                }),
            );
    }

    fn add_async_build_override<F, Fut>(cache_key: IdTokenSpec, f: F)
    where
        F: Fn(IdTokenSpec) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Credential, GcpAuthError>> + Send + 'static,
    {
        credential_registry()
            .test_hooks
            .build_overrides
            .lock()
            .insert(cache_key, Arc::new(move |spec| f(spec).boxed()));
    }

    fn add_ambient_source_override(
        f: impl Fn() -> Result<google_cloud_auth::credentials::Credentials, String>
        + Send
        + Sync
        + 'static,
    ) {
        *credential_registry()
            .test_hooks
            .ambient_source_override
            .lock() = Some(Arc::new(f));
    }

    #[derive(Clone, Copy, Debug)]
    enum ProbeOutcome {
        Healthy,
        Dead,
        Transient,
        Hang,
    }

    struct FakeCredentialsProvider(Mutex<Box<dyn FnMut() -> ProbeOutcome + Send>>);

    impl std::fmt::Debug for FakeCredentialsProvider {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("FakeCredentialsProvider")
        }
    }

    impl FakeCredentialsProvider {
        fn always(outcome: impl Fn() -> ProbeOutcome + Send + 'static) -> Self {
            Self(Mutex::new(Box::new(outcome)))
        }
    }

    impl google_cloud_auth::credentials::CredentialsProvider for FakeCredentialsProvider {
        async fn headers(
            &self,
            _extensions: http::Extensions,
        ) -> std::result::Result<
            google_cloud_auth::credentials::CacheableResource<http::HeaderMap>,
            google_cloud_auth::errors::CredentialsError,
        > {
            let outcome = (self.0.lock())();
            match outcome {
                ProbeOutcome::Healthy => {
                    Ok(google_cloud_auth::credentials::CacheableResource::New {
                        entity_tag: google_cloud_auth::credentials::EntityTag::new(),
                        data: http::HeaderMap::new(),
                    })
                }
                ProbeOutcome::Dead => Err(permanent_error("source refresh task permanently dead")),
                ProbeOutcome::Transient => Err(transient_error(
                    "source refresh task transiently unavailable",
                )),
                ProbeOutcome::Hang => std::future::pending().await,
            }
        }

        async fn universe_domain(&self) -> Option<String> {
            None
        }
    }

    fn credential_source(outcome: ProbeOutcome) -> google_cloud_auth::credentials::Credentials {
        google_cloud_auth::credentials::Credentials::from(FakeCredentialsProvider::always(
            move || outcome,
        ))
    }

    #[restate_core::test]
    async fn single_flight_builds_once_under_concurrent_misses() {
        let audience = "https://single-flight.example.com";
        let builds = Arc::new(AtomicUsize::new(0));

        add_build_override(IdTokenSpec::ambient(audience), {
            let builds = builds.clone();
            move |_| {
                builds.fetch_add(1, Ordering::SeqCst);
                Ok(ok_source())
            }
        });

        let results = futures::future::join_all(
            (0..64).map(|_| mint_for_test(IdTokenIdentity::Ambient, audience)),
        )
        .await;

        assert!(results.iter().all(|r| r.is_ok()), "{results:?}");
        assert_eq!(builds.load(Ordering::SeqCst), 1);
    }

    #[restate_core::test]
    async fn failed_shared_build_is_evicted_and_retried() {
        let audience = "https://build-retry.example.com";
        let builds = Arc::new(AtomicUsize::new(0));

        add_build_override(IdTokenSpec::ambient(audience), {
            let builds = builds.clone();
            move |_| match builds.fetch_add(1, Ordering::SeqCst) {
                0 => Err(GcpAuthError::Build {
                    audience: audience.to_owned(),
                    message: "first build failed".to_owned(),
                }),
                _ => Ok(ok_source()),
            }
        });

        let first = mint_for_test(IdTokenIdentity::Ambient, audience).await;
        assert!(matches!(first, Err(GcpAuthError::Build { .. })));
        let second = mint_for_test(IdTokenIdentity::Ambient, audience).await;
        assert!(second.is_ok(), "{second:?}");
        assert_eq!(builds.load(Ordering::SeqCst), 2);
    }

    #[restate_core::test(start_paused = true)]
    async fn timed_out_caller_leaves_shared_construction_running() {
        let audience = "https://slow-construction.example.com";
        let cache_key = IdTokenSpec::ambient(audience);
        let builds = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(tokio::sync::Notify::new());

        add_async_build_override(cache_key.clone(), {
            let builds = builds.clone();
            let release = release.clone();
            move |_| {
                let builds = builds.clone();
                let release = release.clone();
                async move {
                    builds.fetch_add(1, Ordering::SeqCst);
                    release.notified().await;
                    Ok(ok_source())
                }
            }
        });

        let first = mint_for_test(IdTokenIdentity::Ambient, audience).await;
        assert!(matches!(first, Err(GcpAuthError::Timeout { .. })));
        assert_eq!(builds.load(Ordering::SeqCst), 1);
        let entry = credential_registry()
            .cache
            .get(&cache_key)
            .await
            .expect("timed-out construction remains cached");

        // notify_one stores a permit if construction was descheduled between incrementing the
        // counter above and registering its waiter.
        release.notify_one();
        let second = mint_for_test(IdTokenIdentity::Ambient, audience).await;
        assert!(second.is_ok(), "{second:?}");
        assert_eq!(builds.load(Ordering::SeqCst), 1);
        let cached = credential_registry().cache.get(&cache_key).await;
        assert!(matches!(cached, Some(current) if Arc::ptr_eq(&current, &entry)));
    }

    #[restate_core::test(start_paused = true)]
    async fn completed_failure_after_caller_timeout_is_rebuilt_for_the_next_caller() {
        let audience = "https://stale-build-failure.example.com";
        let builds = Arc::new(AtomicUsize::new(0));
        let release_first_build = Arc::new(tokio::sync::Notify::new());
        let first_build_finished = Arc::new(tokio::sync::Notify::new());

        add_async_build_override(IdTokenSpec::ambient(audience), {
            let builds = builds.clone();
            let release_first_build = release_first_build.clone();
            let first_build_finished = first_build_finished.clone();
            move |_| {
                let release_first_build = release_first_build.clone();
                let first_build_finished = first_build_finished.clone();
                let build = builds.fetch_add(1, Ordering::SeqCst);
                async move {
                    if build == 0 {
                        release_first_build.notified().await;
                        first_build_finished.notify_one();
                        Err(GcpAuthError::Build {
                            audience: audience.to_owned(),
                            message: "first build failed after its caller timed out".to_owned(),
                        })
                    } else {
                        Ok(ok_source())
                    }
                }
            }
        });

        let first = mint_for_test(None, audience).await;
        assert!(matches!(first, Err(GcpAuthError::Timeout { .. })));

        release_first_build.notify_one();
        first_build_finished.notified().await;
        let failed_entry = credential_registry()
            .cache
            .get(&IdTokenSpec::ambient(audience))
            .await
            .expect("timed-out construction remains cached");
        tokio::time::timeout(Duration::from_secs(1), async {
            while !failed_entry.completed_build_failed() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("the TaskCenter-owned build publishes its completed failure");

        let second = mint_for_test(None, audience).await;
        assert!(second.is_ok(), "{second:?}");
        assert_eq!(builds.load(Ordering::SeqCst), 2);
    }

    #[restate_core::test(start_paused = true)]
    async fn construction_and_token_mint_share_one_deadline() {
        let audience = "https://shared-deadline.example.com";

        add_async_build_override(IdTokenSpec::ambient(audience), move |_| async move {
            tokio::time::sleep(Duration::from_secs(4)).await;
            Ok(MockSource::new(|_| MockOutcome::Hang) as Credential)
        });

        let started = Instant::now();
        let outcome = mint_for_test(IdTokenIdentity::Ambient, audience).await;
        assert!(matches!(outcome, Err(GcpAuthError::Timeout { .. })));
        assert_eq!(started.elapsed(), MINT_ATTEMPT_TIMEOUT);
    }

    #[restate_core::test]
    async fn concurrent_ambient_source_resolutions_share_one_build() {
        let build_count = Arc::new(AtomicUsize::new(0));
        add_ambient_source_override({
            let build_count = build_count.clone();
            move || {
                build_count.fetch_add(1, Ordering::SeqCst);
                Ok(credential_source(ProbeOutcome::Healthy))
            }
        });

        let registry = credential_registry();
        let results = futures::future::join_all((0..8).map(|_| registry.ambient_source())).await;

        assert!(results.iter().all(|r| r.is_ok()), "{results:?}");
        assert_eq!(build_count.load(Ordering::SeqCst), 1);
    }

    #[restate_core::test(start_paused = true)]
    async fn credential_source_is_dead_only_for_a_proven_permanent_error() {
        let cases = [
            (ProbeOutcome::Healthy, false),
            (ProbeOutcome::Transient, false),
            (ProbeOutcome::Dead, true),
            (ProbeOutcome::Hang, false),
        ];
        for (outcome, expected_dead) in cases {
            let source = credential_source(outcome);
            assert_eq!(
                credential_source_is_dead(&source).await,
                expected_dead,
                "{outcome:?}"
            );
        }
    }

    #[restate_core::test(start_paused = true)]
    async fn source_recovery_does_not_queue_behind_an_in_progress_operation() {
        let source = RecoverableCredentialSource::new();
        let _operation = source.cell.lock().await;

        let replaced = tokio::time::timeout(
            Duration::from_secs(1),
            source.replace_if_dead(async { unreachable!("a busy source is not rebuilt") }),
        )
        .await
        .expect("redundant recovery returns without waiting")
        .expect("skipping recovery is not an error");
        assert!(!replaced);
    }

    #[restate_core::test]
    async fn dead_ambient_source_is_replaced_after_permanent_impersonation_failure() {
        credential_registry()
            .ambient_source
            .seed_for_test(credential_source(ProbeOutcome::Dead))
            .await;

        let build_count = Arc::new(AtomicUsize::new(0));
        let (recovery_started_tx, mut recovery_started_rx) = tokio::sync::mpsc::unbounded_channel();
        add_ambient_source_override({
            let build_count = build_count.clone();
            move || {
                build_count.fetch_add(1, Ordering::SeqCst);
                recovery_started_tx
                    .send(())
                    .expect("the test still awaits recovery");
                Ok(credential_source(ProbeOutcome::Healthy))
            }
        });

        let audience = "https://ambient-recovery.example.com";
        let service_account = "sa@example.iam.gserviceaccount.com";
        add_build_override(IdTokenSpec::impersonated(audience, service_account), |_| {
            Ok(permanently_failing_source())
        });

        let outcome = mint_for_test(IdTokenIdentity::impersonated(service_account), audience).await;
        assert!(
            matches!(outcome, Err(GcpAuthError::Mint { .. })),
            "{outcome:?}"
        );
        tokio::time::timeout(Duration::from_secs(5), recovery_started_rx.recv())
            .await
            .expect("source recovery starts promptly")
            .expect("permanent failure schedules source recovery without awaiting it");

        assert!(credential_registry().ambient_source().await.is_ok());
        assert_eq!(
            build_count.load(Ordering::SeqCst),
            1,
            "the dead source must be replaced exactly once"
        );
    }

    #[restate_core::test(start_paused = true)]
    async fn permanent_failure_does_not_await_source_recovery() {
        credential_registry()
            .ambient_source
            .seed_for_test(credential_source(ProbeOutcome::Hang))
            .await;

        let audience = "https://detached-ambient-recovery.example.com";
        let service_account = "sa@example.iam.gserviceaccount.com";
        add_build_override(IdTokenSpec::impersonated(audience, service_account), |_| {
            Ok(permanently_failing_source())
        });

        let started = Instant::now();
        let outcome = mint_for_test(Some(service_account), audience).await;
        assert!(matches!(outcome, Err(GcpAuthError::Mint { .. })));
        assert!(
            started.elapsed() < AMBIENT_SOURCE_PROBE_TIMEOUT,
            "mint must not await the shared-source recovery probe"
        );
    }

    #[restate_core::test]
    async fn healthy_ambient_source_is_not_replaced_by_repeated_impersonation_failures() {
        let (probe_finished_tx, mut probe_finished_rx) = tokio::sync::mpsc::unbounded_channel();
        credential_registry()
            .ambient_source
            .seed_for_test(google_cloud_auth::credentials::Credentials::from(
                FakeCredentialsProvider::always(move || {
                    probe_finished_tx
                        .send(())
                        .expect("the test still awaits the source probe");
                    ProbeOutcome::Healthy
                }),
            ))
            .await;

        let build_count = Arc::new(AtomicUsize::new(0));
        add_ambient_source_override({
            let build_count = build_count.clone();
            move || {
                build_count.fetch_add(1, Ordering::SeqCst);
                Ok(credential_source(ProbeOutcome::Healthy))
            }
        });

        let audience = "https://ambient-stable.example.com";
        let service_account = "sa@example.iam.gserviceaccount.com";
        add_build_override(IdTokenSpec::impersonated(audience, service_account), |_| {
            Ok(permanently_failing_source())
        });

        for _ in 0..5 {
            let outcome =
                mint_for_test(IdTokenIdentity::impersonated(service_account), audience).await;
            assert!(
                matches!(outcome, Err(GcpAuthError::Mint { .. })),
                "{outcome:?}"
            );
        }
        tokio::time::timeout(Duration::from_secs(5), probe_finished_rx.recv())
            .await
            .expect("source probe completes promptly")
            .expect("at least one permanent failure probes the shared source");

        assert_eq!(
            build_count.load(Ordering::SeqCst),
            0,
            "a healthy source must never be replaced by an impersonation-only failure"
        );
    }

    #[derive(Clone, Copy, Debug)]
    enum MintFailure {
        Transient,
        Timeout,
        Permanent,
    }

    #[restate_core::test(start_paused = true)]
    async fn mint_failure_policy_controls_cache_eviction() {
        for (name, failure, retained) in [
            ("transient", MintFailure::Transient, true),
            ("timeout", MintFailure::Timeout, true),
            ("permanent", MintFailure::Permanent, false),
        ] {
            let audience = format!("https://{name}.example.com");
            let cache_key = IdTokenSpec::ambient(&audience);
            let source: Credential = match failure {
                MintFailure::Transient => MockSource::new(|call| {
                    if call == 0 {
                        MockOutcome::Error(transient_error("temporarily unavailable"))
                    } else {
                        MockOutcome::Token(token())
                    }
                }),
                MintFailure::Timeout => MockSource::new(|_| MockOutcome::Hang),
                MintFailure::Permanent => {
                    MockSource::new(|_| MockOutcome::Error(permanent_error("misconfigured")))
                }
            };
            let entry = ready_entry(source);
            credential_registry()
                .cache
                .insert(cache_key.clone(), entry.clone())
                .await;

            let outcome = mint_for_test(IdTokenIdentity::Ambient, &audience).await;
            match failure {
                MintFailure::Timeout => {
                    assert!(matches!(outcome, Err(GcpAuthError::Timeout { .. })))
                }
                MintFailure::Transient | MintFailure::Permanent => {
                    assert!(matches!(outcome, Err(GcpAuthError::Mint { .. })))
                }
            }

            let cached = credential_registry().cache.get(&cache_key).await;
            assert_eq!(
                cached.is_some(),
                retained,
                "unexpected cache policy for {name}"
            );
            if retained {
                assert!(matches!(cached, Some(current) if Arc::ptr_eq(&current, &entry)));
            }
            if matches!(failure, MintFailure::Transient) {
                let healed = mint_for_test(IdTokenIdentity::Ambient, &audience).await;
                assert!(healed.is_ok(), "{healed:?}");
            }
        }
    }

    #[restate_core::test]
    async fn aba_race_stale_caller_cannot_evict_or_recover_source() {
        let audience = "https://aba.example.com";
        let service_account = "sa@example.iam.gserviceaccount.com";
        let cache_key = IdTokenSpec::impersonated(audience, service_account);

        credential_registry()
            .ambient_source
            .seed_for_test(credential_source(ProbeOutcome::Dead))
            .await;
        let source_rebuilds = Arc::new(AtomicUsize::new(0));
        add_ambient_source_override({
            let source_rebuilds = source_rebuilds.clone();
            move || {
                source_rebuilds.fetch_add(1, Ordering::SeqCst);
                Ok(credential_source(ProbeOutcome::Healthy))
            }
        });

        let new_source = ok_source();
        let new_entry = ready_entry(new_source);

        struct SwapThenFail {
            spec: IdTokenSpec,
            replacement: Arc<CredentialEntry>,
        }

        #[async_trait]
        impl IdTokenSource for SwapThenFail {
            async fn id_token(
                &self,
            ) -> Result<String, google_cloud_auth::errors::CredentialsError> {
                credential_registry()
                    .cache
                    .insert(self.spec.clone(), self.replacement.clone())
                    .await;
                Err(permanent_error("old, now gone"))
            }
        }

        let old_source: Arc<dyn IdTokenSource> = Arc::new(SwapThenFail {
            spec: cache_key.clone(),
            replacement: new_entry.clone(),
        });
        let old_entry = ready_entry(old_source);
        credential_registry()
            .cache
            .insert(cache_key.clone(), old_entry)
            .await;

        let outcome = mint_for_test(IdTokenIdentity::impersonated(service_account), audience).await;
        assert!(
            matches!(outcome, Err(GcpAuthError::Mint { .. })),
            "{outcome:?}"
        );

        let cached = credential_registry().cache.get(&cache_key).await;
        assert!(
            matches!(cached, Some(s) if Arc::ptr_eq(&s, &new_entry)),
            "evict from a stale caller must not remove the freshly rebuilt healthy entry"
        );
        assert_eq!(
            source_rebuilds.load(Ordering::SeqCst),
            0,
            "a stale caller must not recover the shared source"
        );
    }

    #[test]
    fn credential_construction_runs_on_task_centers_default_runtime_not_the_callers() {
        use restate_core::TaskCenterFutureExt as _;

        let default_runtime = tokio::runtime::Runtime::new().expect("default runtime builds");
        let task_center = restate_core::TaskCenterBuilder::default()
            .default_runtime_handle(default_runtime.handle().clone())
            .build()
            .expect("task center builds")
            .into_handle();

        let audience = "https://runtime-affinity.example.com";
        let (probe_started_tx, probe_started_rx) = std::sync::mpsc::channel();
        let (probe_finished_tx, probe_finished_rx) = std::sync::mpsc::channel();
        let (allow_probe_to_finish_tx, allow_probe_to_finish_rx) = tokio::sync::oneshot::channel();
        let allow_probe_to_finish_rx = Mutex::new(Some(allow_probe_to_finish_rx));

        let registry = task_center.run_sync(|| {
            add_build_override(IdTokenSpec::ambient(audience), move |_| {
                let probe_started_tx = probe_started_tx.clone();
                let probe_finished_tx = probe_finished_tx.clone();
                let allow_probe_to_finish_rx = allow_probe_to_finish_rx
                    .lock()
                    .take()
                    .expect("credential builds once");
                // Simulate the library refresh task spawned during credential construction.
                tokio::spawn(async move {
                    probe_started_tx
                        .send(())
                        .expect("the test still awaits the probe");
                    let _ = allow_probe_to_finish_rx.await;
                    probe_finished_tx
                        .send(())
                        .expect("the test still awaits the probe");
                });
                Ok(ok_source())
            });
            credential_registry()
        });

        // The probe must survive dropping the runtime that called mint().
        {
            let caller_runtime = tokio::runtime::Runtime::new().expect("caller runtime builds");
            let spec = IdTokenSpec::ambient(audience);
            let result = caller_runtime.block_on(
                async {
                    let entry = registry.get_entry(&spec).await;
                    entry.get_or_start(registry, &spec).await
                }
                .in_tc(&task_center),
            );
            if let Err(error) = &result {
                panic!("{error}");
            }
            // Ensure the child exists before dropping the runtime that might own it.
            probe_started_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("credential construction spawns a probe child task");
        }

        // A child owned by the caller runtime was cancelled above; one owned by the TaskCenter can
        // still finish and report back.
        allow_probe_to_finish_tx
            .send(())
            .expect("a task spawned during construction must survive the caller runtime's drop");
        probe_finished_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("a task spawned during construction must survive the caller runtime's drop");
    }

    #[test]
    fn credentials_error_403_permission_denied_survives_display_error_chain() {
        let body = br#"{"error":{"code":403,"message":"The caller does not have permission","status":"PERMISSION_DENIED"}}"#;
        let gax_error = google_cloud_gax::error::Error::http(
            403,
            http::HeaderMap::new(),
            bytes::Bytes::from_static(body),
        );
        let credentials_error = google_cloud_auth::errors::CredentialsError::new(
            false,
            "failed to fetch ID token via impersonation",
            gax_error,
        );

        let message = display_error_chain(&credentials_error);
        assert!(message.contains("403"), "{message}");
        assert!(message.contains("PERMISSION_DENIED"), "{message}");
    }

    fn healthy_source() -> google_cloud_auth::credentials::Credentials {
        google_cloud_auth::credentials::Credentials::from(FakeCredentialsProvider::always(|| {
            ProbeOutcome::Healthy
        }))
    }

    fn dead_source() -> google_cloud_auth::credentials::Credentials {
        google_cloud_auth::credentials::Credentials::from(FakeCredentialsProvider::always(|| {
            ProbeOutcome::Dead
        }))
    }

    fn counted_access_token_source_override(provider: &str) -> Arc<AtomicUsize> {
        let builds = Arc::new(AtomicUsize::new(0));
        federation::test_hooks::install_access_token_source_override(provider, {
            let builds = builds.clone();
            move || {
                builds.fetch_add(1, Ordering::SeqCst);
                Ok(healthy_source())
            }
        });
        builds
    }

    async fn build_federated_source_for_spec(
        sources: &federation::FederatedAccessTokenSourceIndex,
        spec: IdTokenSpec,
    ) -> Result<Credential, GcpAuthError> {
        let IdTokenSpec { identity, audience } = spec;
        let IdTokenIdentity::Federated {
            provider,
            service_account,
        } = identity
        else {
            panic!("test helper requires a federated spec");
        };
        federation::build_federated_source(
            sources,
            provider.into(),
            service_account.into(),
            audience.into(),
        )
        .await
    }

    async fn federated_recovery_fixture(
        provider: &str,
        audience: &str,
        service_account: &str,
        seed: google_cloud_auth::credentials::Credentials,
    ) -> Arc<AtomicUsize> {
        let registry = credential_registry();
        let access_token_source = registry
            .federated_access_token_sources
            .get_or_create(provider);
        access_token_source.credentials.seed_for_test(seed).await;

        let builds = counted_access_token_source_override(provider);
        add_build_override(
            IdTokenSpec::federated(audience, provider, service_account),
            {
                let access_token_source = access_token_source.clone();
                move |_| {
                    Ok(Arc::new(LeasedFailingSource {
                        _access_token_source: access_token_source.clone(),
                    }) as Credential)
                }
            },
        );
        builds
    }

    struct LeasedFailingSource {
        _access_token_source: Arc<federation::FederatedAccessTokenSource>,
    }

    #[async_trait]
    impl IdTokenSource for LeasedFailingSource {
        async fn id_token(&self) -> Result<String, google_cloud_auth::errors::CredentialsError> {
            Err(permanent_error("impersonation misconfigured"))
        }
    }

    #[restate_core::test]
    async fn outer_credentials_for_one_provider_share_and_release_one_access_token_source() {
        let provider = "//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/p/providers/shared-lease";
        let service_account = "sa@example.iam.gserviceaccount.com";
        counted_access_token_source_override(provider);

        let registry = credential_registry();
        let spec_a = IdTokenSpec::federated(
            "https://shared-lease-a.example.com",
            provider,
            service_account,
        );
        let spec_b = IdTokenSpec::federated(
            "https://shared-lease-b.example.com",
            provider,
            service_account,
        );
        let outer_a = build_federated_source_for_spec(
            &registry.federated_access_token_sources,
            spec_a.clone(),
        )
        .await
        .expect("outer construction succeeds");
        let outer_b = build_federated_source_for_spec(
            &registry.federated_access_token_sources,
            spec_b.clone(),
        )
        .await
        .expect("outer construction succeeds");

        let weak = registry
            .federated_access_token_sources
            .weak_for_test(provider)
            .expect("the builds above must have created an entry for this provider");

        registry
            .cache
            .insert(spec_a.clone(), ready_entry(outer_a))
            .await;
        registry
            .cache
            .insert(spec_b.clone(), ready_entry(outer_b))
            .await;
        assert_eq!(
            weak.strong_count(),
            2,
            "both cached outer credentials must hold their own lease on the shared source"
        );

        registry.cache.invalidate(&spec_a).await;
        registry.cache.run_pending_tasks().await;
        assert_eq!(
            weak.strong_count(),
            1,
            "evicting one outer credential must drop exactly its own lease"
        );
        assert_eq!(
            registry.federated_access_token_sources.reap_dead(),
            1,
            "the shared source must stay indexed while spec_b's outer credential references it"
        );

        registry.cache.invalidate(&spec_b).await;
        registry.cache.run_pending_tasks().await;
        assert_eq!(
            weak.strong_count(),
            0,
            "evicting the last outer credential must drop the last lease"
        );
    }

    #[restate_core::test]
    async fn reap_removes_a_federated_access_token_source_after_its_last_outer_credential_expires()
    {
        let provider = "//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/p/providers/reap-test";
        let service_account = "sa@example.iam.gserviceaccount.com";
        let builds = counted_access_token_source_override(provider);

        let registry = credential_registry();
        let spec =
            IdTokenSpec::federated("https://reap-test.example.com", provider, service_account);
        let outer =
            build_federated_source_for_spec(&registry.federated_access_token_sources, spec.clone())
                .await
                .expect("outer construction succeeds");
        registry
            .cache
            .insert(spec.clone(), ready_entry(outer))
            .await;
        assert_eq!(builds.load(Ordering::SeqCst), 1);

        registry.cache.invalidate(&spec).await;
        registry.cache.run_pending_tasks().await;
        assert_eq!(
            registry.federated_access_token_sources.reap_dead(),
            0,
            "the live count must reflect removal once the last outer credential expires"
        );
        assert!(
            registry
                .federated_access_token_sources
                .weak_for_test(provider)
                .is_none(),
            "the map key itself must be gone after reaping, not merely a dead tombstone"
        );

        let second_spec = IdTokenSpec::federated(
            "https://reap-test-second.example.com",
            provider,
            service_account,
        );
        build_federated_source_for_spec(&registry.federated_access_token_sources, second_spec)
            .await
            .expect("outer construction succeeds");
        assert_eq!(
            builds.load(Ordering::SeqCst),
            2,
            "a provider whose access-token source was fully reaped must build exactly one fresh \
             source on its next reference"
        );
    }

    #[restate_core::test]
    async fn failed_construction_leaves_no_permanently_retained_source() {
        let provider = "//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/p/providers/construction-fails";
        let service_account = "sa@example.iam.gserviceaccount.com";
        federation::test_hooks::install_access_token_source_override(provider, || {
            Err("simulated STS exchange failure".to_owned())
        });

        let registry = credential_registry();
        let spec = IdTokenSpec::federated(
            "https://construction-fails.example.com",
            provider,
            service_account,
        );
        let result =
            build_federated_source_for_spec(&registry.federated_access_token_sources, spec).await;
        assert_matches!(result.err(), Some(GcpAuthError::CredentialSource { .. }));

        assert_eq!(
            registry.federated_access_token_sources.reap_dead(),
            0,
            "a failed build's dead weak entry must be pruned by housekeeping, not retained"
        );

        let builds = counted_access_token_source_override(provider);
        let retry_spec = IdTokenSpec::federated(
            "https://construction-fails-retry.example.com",
            provider,
            service_account,
        );
        build_federated_source_for_spec(&registry.federated_access_token_sources, retry_spec)
            .await
            .expect("a fresh lookup must be able to replace the dead tombstone directly");
        assert_eq!(builds.load(Ordering::SeqCst), 1);
    }

    /// Housekeeping may reap between eviction and recovery. The outer credential that failed still
    /// holds its lease at that point, so the reap must leave the source indexed and recoverable.
    #[restate_core::test]
    async fn reap_during_the_recovery_window_does_not_defeat_recovery() {
        let provider = "//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/p/providers/recovery-vs-housekeeping";
        let registry = credential_registry();
        let access_token_source = registry
            .federated_access_token_sources
            .get_or_create(provider);
        access_token_source
            .credentials
            .seed_for_test(dead_source())
            .await;
        let builds = counted_access_token_source_override(provider);

        // Stands in for the failed outer credential mint() still holds while it recovers.
        let lease = federation::test_hooks::leased_credential(access_token_source);
        assert_eq!(
            registry.federated_access_token_sources.reap_dead(),
            1,
            "a reap must not drop a source that a live outer credential still leases"
        );

        registry
            .federated_access_token_sources
            .recover_if_dead(provider)
            .await;
        assert_eq!(
            builds.load(Ordering::SeqCst),
            1,
            "recovery must replace the dead source it proved dead, not be defeated by the reap"
        );
        drop(lease);
    }

    #[restate_core::test]
    async fn dead_federated_source_is_replaced_after_permanent_mint_failure() {
        let provider = "//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/p/providers/recovery";
        let service_account = "sa@example.iam.gserviceaccount.com";
        let audience = "https://federated-recovery.example.com";
        let builds =
            federated_recovery_fixture(provider, audience, service_account, dead_source()).await;

        let outcome = mint_for_test(
            IdTokenIdentity::federated(provider, service_account),
            audience,
        )
        .await;
        assert_matches!(outcome, Err(GcpAuthError::Mint { .. }));
        assert_eq!(
            builds.load(Ordering::SeqCst),
            1,
            "the dead federated source must be replaced exactly once"
        );

        let reused = credential_registry()
            .federated_access_token_sources
            .get_or_create(provider)
            .credentials
            .get_or_build(async { unreachable!("the slot must already hold the recovered source") })
            .await;
        assert!(reused.is_ok());
        assert_eq!(builds.load(Ordering::SeqCst), 1);
    }

    #[restate_core::test]
    async fn healthy_federated_source_is_not_replaced_by_repeated_mint_failures() {
        let provider = "//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/p/providers/stable";
        let service_account = "sa@example.iam.gserviceaccount.com";
        let audience = "https://federated-stable.example.com";
        let builds =
            federated_recovery_fixture(provider, audience, service_account, healthy_source()).await;

        for _ in 0..5 {
            let outcome = mint_for_test(
                IdTokenIdentity::federated(provider, service_account),
                audience,
            )
            .await;
            assert_matches!(outcome, Err(GcpAuthError::Mint { .. }));
        }

        assert_eq!(
            builds.load(Ordering::SeqCst),
            0,
            "a healthy federated source must never be replaced by an impersonation-only failure"
        );
    }

    #[restate_core::test]
    async fn recovery_is_scoped_to_the_provider_whose_mint_failed() {
        let provider_a = "//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/p/providers/aaaa";
        let provider_b = "//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/p/providers/bbbb";
        let service_account = "sa@example.iam.gserviceaccount.com";
        let audience_a = "https://federated-independent-a.example.com";
        let audience_b = "https://federated-independent-b.example.com";
        let builds_a =
            federated_recovery_fixture(provider_a, audience_a, service_account, dead_source())
                .await;
        let builds_b =
            federated_recovery_fixture(provider_b, audience_b, service_account, healthy_source())
                .await;

        let outcome_b = mint_for_test(
            IdTokenIdentity::federated(provider_b, service_account),
            audience_b,
        )
        .await;
        assert_matches!(outcome_b, Err(GcpAuthError::Mint { .. }));
        assert_eq!(
            builds_b.load(Ordering::SeqCst),
            0,
            "provider_b's healthy source must not be replaced"
        );
        assert_eq!(
            builds_a.load(Ordering::SeqCst),
            0,
            "a mint against provider_b must never rebuild provider_a's source"
        );

        let outcome_a = mint_for_test(
            IdTokenIdentity::federated(provider_a, service_account),
            audience_a,
        )
        .await;
        assert_matches!(outcome_a, Err(GcpAuthError::Mint { .. }));
        assert_eq!(
            builds_a.load(Ordering::SeqCst),
            1,
            "provider_a's dead source must be replaced exactly once"
        );
        assert_eq!(
            builds_b.load(Ordering::SeqCst),
            0,
            "recovering provider_a's source must never touch provider_b's"
        );
    }

    #[restate_core::test]
    async fn federated_provider_is_a_distinct_cache_key_dimension() {
        let audience = "https://wif-cache-key.example.com";
        let service_account = "sa@proj.iam.gserviceaccount.com";
        let provider =
            "//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/p/providers/r";

        let impersonated_key = IdTokenSpec::impersonated(audience, service_account);
        let federated_key = IdTokenSpec::federated(audience, provider, service_account);
        assert_ne!(impersonated_key, federated_key);

        let impersonated_builds = Arc::new(AtomicUsize::new(0));
        let federated_builds = Arc::new(AtomicUsize::new(0));

        add_build_override(impersonated_key, {
            let impersonated_builds = impersonated_builds.clone();
            move |_| {
                impersonated_builds.fetch_add(1, Ordering::SeqCst);
                Ok(MockSource::new(|_| MockOutcome::Token(token())) as Credential)
            }
        });
        add_build_override(federated_key, {
            let federated_builds = federated_builds.clone();
            move |_| {
                federated_builds.fetch_add(1, Ordering::SeqCst);
                Ok(MockSource::new(|_| MockOutcome::Token(token())) as Credential)
            }
        });

        for _ in 0..2 {
            mint_for_test(IdTokenIdentity::impersonated(service_account), audience)
                .await
                .expect("impersonated key mints");
            mint_for_test(
                IdTokenIdentity::federated(provider, service_account),
                audience,
            )
            .await
            .expect("federated key mints");
        }

        assert_eq!(
            impersonated_builds.load(Ordering::SeqCst),
            1,
            "the impersonated key must build exactly once, independently of the federated key"
        );
        assert_eq!(
            federated_builds.load(Ordering::SeqCst),
            1,
            "the federated key must build exactly once, independently of the impersonated key"
        );
    }

    #[restate_core::test]
    async fn federation_requested_without_server_config_fails_closed_and_is_not_cached() {
        let provider =
            "//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/p/providers/r";
        let audience = "https://wif-no-config.example.com";
        let service_account = "sa@proj.iam.gserviceaccount.com";

        let err = mint_for_test(
            IdTokenIdentity::federated(provider, service_account),
            audience,
        )
        .await
        .expect_err("must fail without a [gcp-federation] configuration");
        assert!(
            err.to_string().contains("GCP source credentials"),
            "federated construction failures must not be mislabeled as ADC: {err}"
        );
        assert_matches!(err, GcpAuthError::CredentialSource { .. });

        let key = IdTokenSpec::federated(audience, provider, service_account);
        assert!(
            credential_registry().cache.get(&key).await.is_none(),
            "a construction failure must never populate the cache"
        );

        let err2 = mint_for_test(
            IdTokenIdentity::federated(provider, service_account),
            audience,
        )
        .await
        .expect_err("still fails without configuration");
        assert_matches!(err2, GcpAuthError::CredentialSource { .. });
    }

    /// Federated source recovery must run on the TaskCenter default runtime for the same reason
    /// construction does: `google-cloud-auth` spawns its refresh task with a bare `tokio::spawn`,
    /// so it must not land on a caller runtime that can be dropped.
    #[test]
    fn federated_source_recovery_runs_on_task_centers_default_runtime() {
        use restate_core::TaskCenterFutureExt as _;

        let default_runtime = tokio::runtime::Runtime::new().expect("default runtime builds");
        let task_center = restate_core::TaskCenterBuilder::default()
            .default_runtime_handle(default_runtime.handle().clone())
            .build()
            .expect("task center builds")
            .into_handle();

        let provider = "//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/p/providers/recovery-runtime";
        let registry = task_center.run_sync(credential_registry);
        let access_token_source = registry
            .federated_access_token_sources
            .get_or_create(provider);
        task_center.block_on(access_token_source.credentials.seed_for_test(dead_source()));

        // The probe child task reports on channels, so the assertions never wait on a timer.
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (survived_tx, survived_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let release_rx = Mutex::new(Some(release_rx));
        federation::test_hooks::install_access_token_source_override(provider, move || {
            let started_tx = started_tx.clone();
            let survived_tx = survived_tx.clone();
            let release_rx = release_rx.lock().take().expect("recovery builds once");
            tokio::spawn(async move {
                started_tx
                    .send(())
                    .expect("the test still awaits the probe");
                let _ = release_rx.await;
                survived_tx
                    .send(())
                    .expect("the test still awaits the probe");
            });
            Ok(healthy_source())
        });

        {
            let caller_runtime = tokio::runtime::Runtime::new().expect("caller runtime builds");
            caller_runtime.block_on(
                registry
                    .federated_access_token_sources
                    .recover_if_dead(provider)
                    .in_tc(&task_center),
            );
            started_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("recovery spawns a probe child task");
        }

        // The caller runtime is now dropped. Only a probe on the TaskCenter default runtime can
        // still observe the release and report back.
        release_tx.send(()).expect("the probe is still running");
        survived_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("a task spawned during recovery must survive the caller runtime's drop");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn blocking_builds_are_bounded_and_never_deadlock() {
        use std::sync::{Condvar, Mutex as StdMutex};

        let release = Arc::new((StdMutex::new(false), Condvar::new()));
        let (started_tx, mut started_rx) =
            tokio::sync::mpsc::channel(4 * MAX_CONCURRENT_BLOCKING_BUILDS);

        // Test the process-global semaphore's production choke point directly. This suite uses
        // nextest process isolation; construction overrides in the other tests bypass it.
        let tasks: Vec<_> = (0..4 * MAX_CONCURRENT_BLOCKING_BUILDS)
            .map(|_| {
                let release = release.clone();
                let started_tx = started_tx.clone();
                tokio::spawn(run_blocking("test".to_owned(), move || {
                    started_tx.blocking_send(()).expect("receiver stays open");
                    let (lock, wake) = &*release;
                    let mut released = lock.lock().expect("release lock is not poisoned");
                    while !*released {
                        released = wake.wait(released).expect("release lock is not poisoned");
                    }
                    Ok(ok_source())
                }))
            })
            .collect();

        for _ in 0..MAX_CONCURRENT_BLOCKING_BUILDS {
            started_rx.recv().await.expect("blocking build starts");
        }
        assert_eq!(BLOCKING_BUILD_PERMITS.available_permits(), 0);
        assert!(
            matches!(
                started_rx.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            ),
            "no additional blocking build may start while all permits are held"
        );

        let (lock, wake) = &*release;
        *lock.lock().expect("release lock is not poisoned") = true;
        wake.notify_all();
        tokio::time::timeout(Duration::from_secs(2), async {
            for task in tasks {
                let result = task.await.expect("task doesn't panic");
                if let Err(error) = &result {
                    panic!("{error}");
                }
            }
        })
        .await
        .expect("blocking builds complete without deadlocking");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn cancelled_callers_do_not_release_blocking_build_permits() {
        use std::sync::{Condvar, Mutex as StdMutex};

        let release = Arc::new((StdMutex::new(false), Condvar::new()));
        let (started_tx, mut started_rx) =
            tokio::sync::mpsc::channel(MAX_CONCURRENT_BLOCKING_BUILDS);

        let builds: Vec<_> = (0..MAX_CONCURRENT_BLOCKING_BUILDS)
            .map(|_| {
                let release = release.clone();
                let started_tx = started_tx.clone();
                tokio::spawn(spawn_bounded_blocking(move || {
                    started_tx.blocking_send(()).expect("receiver stays open");
                    let (lock, wake) = &*release;
                    let mut released = lock.lock().expect("release lock is not poisoned");
                    while !*released {
                        released = wake.wait(released).expect("release lock is not poisoned");
                    }
                }))
            })
            .collect();
        for _ in 0..MAX_CONCURRENT_BLOCKING_BUILDS {
            started_rx.recv().await.expect("blocking build starts");
        }
        for build in builds {
            build.abort();
        }

        assert_eq!(
            BLOCKING_BUILD_PERMITS.available_permits(),
            0,
            "cancelled callers must retain permits until their blocking work finishes"
        );
        let replacement = tokio::spawn(spawn_bounded_blocking(|| {}));

        let (lock, wake) = &*release;
        *lock.lock().expect("release lock is not poisoned") = true;
        wake.notify_all();
        tokio::time::timeout(Duration::from_secs(5), replacement)
            .await
            .expect("replacement starts after blocking work exits")
            .expect("replacement task does not panic")
            .expect("blocking build does not panic");
    }
}
