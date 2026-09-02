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
    pub(crate) fn from_deployment_auth(auth: GoogleIdTokenAuth) -> Result<Self, GcpAuthError> {
        let (audience, service_account, provider) = auth.into_parts();
        let identity = match (provider, service_account) {
            (Some(provider), Some(service_account)) => {
                IdTokenIdentity::federated(provider, service_account)
            }
            (None, Some(service_account)) => IdTokenIdentity::impersonated(service_account),
            (None, None) => IdTokenIdentity::Ambient,
            (Some(_), None) => {
                return Err(GcpAuthError::Build {
                    audience: audience.to_string(),
                    message: "GCP workload identity federation requires \
                              impersonate_service_account to be set; re-register the deployment"
                        .to_owned(),
                });
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
                            .spawn_recovery(provider.to_string(), message.clone());
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
mod tests;
