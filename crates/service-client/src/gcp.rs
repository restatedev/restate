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

use std::sync::{Arc, Once, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use futures::FutureExt as _;
use futures::future::{BoxFuture, Shared};
use google_cloud_auth::credentials::Credentials as GoogleCredentials;
use moka::future::Cache;
use moka::ops::compute::{CompResult, Op};
use restate_core::{TaskCenter, TaskKind};
use thiserror::Error;
use tokio::sync::Semaphore;
use tokio::time::Instant;
use tracing::warn;

#[cfg(any(test, feature = "test_util"))]
use ahash::HashMap;
#[cfg(any(test, feature = "test_util"))]
use parking_lot::Mutex;

// Bounds one caller's wait across credential construction and initial token acquisition.
// Construction continues after a timeout and remains available to later callers.
const MINT_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);

const CACHE_TIME_TO_IDLE: Duration = Duration::from_secs(3600);

const CACHE_HOUSEKEEPING_INTERVAL: Duration = Duration::from_secs(300);

const AMBIENT_SOURCE_PROBE_TIMEOUT: Duration = Duration::from_secs(1);

/// Caps concurrent blocking `google-cloud-auth`/ADC builds, so a burst of distinct new keys (or a
/// GCP outage causing every retry to rebuild) cannot exhaust tokio's blocking thread pool. This is
/// fixed rather than CPU-scaled because Tokio's blocking pool is independent of CPU concurrency.
const MAX_CONCURRENT_BLOCKING_BUILDS: usize = 4;

#[derive(Clone, Debug, Error)]
pub enum GcpAuthError {
    #[error(
        "failed to load Application Default Credentials (audience '{audience}', impersonating '{impersonate}'): {message}"
    )]
    Adc {
        audience: String,
        impersonate: String,
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
        "failed to mint ID token (audience '{audience}', impersonating '{impersonate}'): {message}"
    )]
    Mint {
        audience: String,
        impersonate: String,
        message: String,
    },
    #[error(
        "token mint timed out after {duration:?} (audience '{audience}', impersonating '{impersonate}')"
    )]
    Timeout {
        audience: String,
        impersonate: String,
        duration: Duration,
    },
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct IdTokenSpec {
    impersonate: Option<String>,
    audience: String,
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
        spec: IdTokenSpec,
    ) -> Result<Credential, GcpAuthError> {
        self.build
            .get_or_init(|| registry.start_build(spec))
            .clone()
            .await
    }
}

struct Live(google_cloud_auth::credentials::idtoken::IDTokenCredentials);

#[async_trait]
impl IdTokenSource for Live {
    async fn id_token(&self) -> Result<String, google_cloud_auth::errors::CredentialsError> {
        self.0.id_token().await
    }
}

/// Process-wide credentials and their shared ambient source.
struct CredentialRegistry {
    cache: Cache<IdTokenSpec, Arc<CredentialEntry>>,
    ambient_source: RecoverableCredentialSource,
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

    // bounded by the probe's own timeout; swapped under lock so no need for external ABA checks.
    // Returns whether a replacement actually happened, so callers can log accordingly.
    async fn replace_if_dead(
        &self,
        build: impl Future<Output = Result<GoogleCredentials, String>>,
    ) -> Result<bool, String> {
        let mut guard = self.cell.lock().await;
        let Some(current) = guard.as_ref() else {
            return Ok(false);
        };
        if !ambient_source_is_dead(current).await {
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
    HOUSEKEEPING_STARTED.call_once(|| registry.start_housekeeping());
    registry
}

impl CredentialRegistry {
    fn new() -> Self {
        Self {
            // Keys come only from operator-controlled deployment or discovery configuration, not
            // invocation input. TTI removes abandoned attempts; a hard capacity would instead
            // turn excess cardinality into credential rebuild churn. See #5184.
            cache: Cache::builder().time_to_idle(CACHE_TIME_TO_IDLE).build(),
            ambient_source: RecoverableCredentialSource::new(),
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
                self.cache.run_pending_tasks().await;
            }
        };
        let _ = TaskCenter::spawn(
            TaskKind::Credentials,
            "gcp-credential-housekeeping",
            housekeeping,
        );
    }

    async fn get_entry(&self, spec: &IdTokenSpec) -> Arc<CredentialEntry> {
        self.cache
            .get_with_by_ref(spec, async { Arc::new(CredentialEntry::new()) })
            .await
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
    async fn recover_ambient_source_if_dead(&self) {
        match self
            .ambient_source
            .replace_if_dead(self.build_ambient_source())
            .await
        {
            Ok(true) => {
                warn!(
                    "replaced the shared ambient GCP credential source: its refresh task was proven dead"
                );
            }
            Ok(false) => {}
            Err(error) => {
                warn!(
                    error = %error,
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
                        audience,
                        message: "GCP credential construction task failed".to_owned(),
                    })
                })
            }
            .boxed()
            .shared(),
            Err(_) => futures::future::ready(Err(GcpAuthError::Build {
                audience,
                message: "TaskCenter is shutting down".to_owned(),
            }))
            .boxed()
            .shared(),
        }
    }

    async fn build_credentials(&self, spec: IdTokenSpec) -> Result<Credential, GcpAuthError> {
        let IdTokenSpec {
            impersonate,
            audience,
        } = spec;
        let panic_context = audience.clone();
        match impersonate {
            None => run_blocking(panic_context, move || build_ambient_credentials(&audience)).await,
            Some(sa) => {
                let source = self
                    .ambient_source()
                    .await
                    .map_err(|message| GcpAuthError::Adc {
                        audience: audience.clone(),
                        impersonate: sa.clone(),
                        message,
                    })?;
                run_blocking(panic_context, move || {
                    build_impersonated_credentials(&audience, &sa, source)
                })
                .await
            }
        }
    }
}

// google-cloud-auth 1.16 publishes a permanent error before its refresh task exits and keeps
// retrying transient errors. Probe through its public headers() API; success, transient errors,
// and timeouts do not prove the source dead and must retain it.
async fn ambient_source_is_dead(source: &GoogleCredentials) -> bool {
    matches!(
        tokio::time::timeout(AMBIENT_SOURCE_PROBE_TIMEOUT, source.headers(http::Extensions::new()))
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

/// Cheap handle to the process-wide credential registry.
#[derive(Clone)]
pub struct GcpTokenClient {
    #[cfg(any(test, feature = "test_util"))]
    test_overrides: Arc<TestOverrides>,
}

#[cfg(any(test, feature = "test_util"))]
struct TestOverrides {
    forced_failure: Mutex<Option<String>>,
    tokens: Mutex<HashMap<IdTokenSpec, String>>,
}

impl GcpTokenClient {
    pub fn new() -> Self {
        Self {
            #[cfg(any(test, feature = "test_util"))]
            test_overrides: Arc::new(TestOverrides {
                forced_failure: Mutex::new(None),
                tokens: Mutex::default(),
            }),
        }
    }

    /// Mint an OIDC ID token for the given audience. If `impersonate_service_account` is set, the
    /// token is minted via the IAM Credentials `generateIdToken` API for that service account;
    /// otherwise it is minted from ambient ADC identity.
    pub async fn mint(
        &self,
        impersonate_service_account: Option<&str>,
        audience: &str,
    ) -> Result<String, GcpAuthError> {
        let spec = IdTokenSpec {
            impersonate: impersonate_service_account.map(str::to_owned),
            audience: audience.to_owned(),
        };
        let impersonate = impersonate_service_account
            .unwrap_or("(ambient)")
            .to_owned();

        #[cfg(any(test, feature = "test_util"))]
        if let Some(result) = self.test_override(&spec, &impersonate) {
            return result;
        }

        let deadline = Instant::now() + MINT_ATTEMPT_TIMEOUT;
        let timeout_error = || GcpAuthError::Timeout {
            audience: audience.to_owned(),
            impersonate: impersonate.clone(),
            duration: MINT_ATTEMPT_TIMEOUT,
        };
        let registry = credential_registry();
        let entry = tokio::time::timeout_at(deadline, registry.get_entry(&spec))
            .await
            .map_err(|_| timeout_error())?;
        let source =
            match tokio::time::timeout_at(deadline, entry.get_or_start(registry, spec.clone()))
                .await
            {
                Ok(Ok(source)) => source,
                Ok(Err(error)) => {
                    let _ = registry.evict_if_unchanged(&spec, &entry).await;
                    return Err(error);
                }
                Err(_) => return Err(timeout_error()),
            };

        match tokio::time::timeout_at(deadline, source.id_token()).await {
            Ok(Ok(token)) => Ok(token),
            Ok(Err(error)) => {
                // Transient errors may self-heal; evict permanent failures only if still current.
                if !error.is_transient() {
                    let evicted = registry.evict_if_unchanged(&spec, &entry).await;
                    if evicted && spec.impersonate.is_some() {
                        registry.recover_ambient_source_if_dead().await;
                    }
                }
                Err(GcpAuthError::Mint {
                    audience: audience.to_owned(),
                    impersonate: impersonate.clone(),
                    message: error.to_string(),
                })
            }
            Err(_) => Err(timeout_error()),
        }
    }

    #[cfg(any(test, feature = "test_util"))]
    fn test_override(
        &self,
        spec: &IdTokenSpec,
        impersonate: &str,
    ) -> Option<Result<String, GcpAuthError>> {
        if let Some(message) = self.test_overrides.forced_failure.lock().clone() {
            return Some(Err(GcpAuthError::Mint {
                audience: spec.audience.clone(),
                impersonate: impersonate.to_owned(),
                message,
            }));
        }
        self.test_overrides.tokens.lock().get(spec).cloned().map(Ok)
    }

    #[cfg(any(test, feature = "test_util"))]
    pub fn force_mint_failure_for_test(&self, message: &str) {
        *self.test_overrides.forced_failure.lock() = Some(message.to_owned());
    }

    #[cfg(any(test, feature = "test_util"))]
    pub fn seed_for_test(&self, impersonate: Option<&str>, audience: &str, token: String) {
        let spec = IdTokenSpec {
            impersonate: impersonate.map(str::to_owned),
            audience: audience.to_owned(),
        };
        self.test_overrides.tokens.lock().insert(spec, token);
    }
}

impl Default for GcpTokenClient {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use super::*;

    fn token() -> String {
        "test-token".to_owned()
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
                impersonate: None,
                audience: audience.to_owned(),
            }
        }

        fn impersonated(audience: &str, service_account: &str) -> Self {
            IdTokenSpec {
                impersonate: Some(service_account.to_owned()),
                audience: audience.to_owned(),
            }
        }
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

    #[restate_core::test]
    async fn single_flight_builds_once_under_concurrent_misses() {
        let client = GcpTokenClient::new();
        let audience = "https://single-flight.example.com";
        let builds = Arc::new(AtomicUsize::new(0));

        add_build_override(IdTokenSpec::ambient(audience), {
            let builds = builds.clone();
            move |_| {
                builds.fetch_add(1, Ordering::SeqCst);
                Ok(MockSource::new(|_| MockOutcome::Token(token())) as Arc<dyn IdTokenSource>)
            }
        });

        let results = futures::future::join_all((0..64).map(|_| client.mint(None, audience))).await;

        assert!(results.iter().all(|r| r.is_ok()), "{results:?}");
        assert_eq!(builds.load(Ordering::SeqCst), 1);
    }

    #[restate_core::test]
    async fn failed_shared_build_is_evicted_and_retried() {
        let client = GcpTokenClient::new();
        let audience = "https://build-retry.example.com";
        let builds = Arc::new(AtomicUsize::new(0));

        add_build_override(IdTokenSpec::ambient(audience), {
            let builds = builds.clone();
            move |_| match builds.fetch_add(1, Ordering::SeqCst) {
                0 => Err(GcpAuthError::Build {
                    audience: audience.to_owned(),
                    message: "first build failed".to_owned(),
                }),
                _ => Ok(MockSource::new(|_| MockOutcome::Token(token())) as Credential),
            }
        });

        let first = client.mint(None, audience).await;
        assert!(matches!(first, Err(GcpAuthError::Build { .. })));
        let second = client.mint(None, audience).await;
        assert!(second.is_ok(), "{second:?}");
        assert_eq!(builds.load(Ordering::SeqCst), 2);
    }

    #[restate_core::test(start_paused = true)]
    async fn timed_out_caller_leaves_shared_construction_running() {
        let client = GcpTokenClient::new();
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
                    Ok(MockSource::new(|_| MockOutcome::Token(token())) as Credential)
                }
            }
        });

        let first = client.mint(None, audience).await;
        assert!(matches!(first, Err(GcpAuthError::Timeout { .. })));
        assert_eq!(builds.load(Ordering::SeqCst), 1);
        let entry = credential_registry()
            .cache
            .get(&cache_key)
            .await
            .expect("timed-out construction remains cached");

        release.notify_waiters();
        let second = client.mint(None, audience).await;
        assert!(second.is_ok(), "{second:?}");
        assert_eq!(builds.load(Ordering::SeqCst), 1);
        let cached = credential_registry().cache.get(&cache_key).await;
        assert!(matches!(cached, Some(current) if Arc::ptr_eq(&current, &entry)));
    }

    #[restate_core::test(start_paused = true)]
    async fn construction_and_token_mint_share_one_deadline() {
        let client = GcpTokenClient::new();
        let audience = "https://shared-deadline.example.com";

        add_async_build_override(IdTokenSpec::ambient(audience), move |_| async move {
            tokio::time::sleep(Duration::from_secs(4)).await;
            Ok(MockSource::new(|_| MockOutcome::Hang) as Credential)
        });

        let started = Instant::now();
        let outcome = client.mint(None, audience).await;
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
                Ok(google_cloud_auth::credentials::Credentials::from(
                    FakeCredentialsProvider::always(|| ProbeOutcome::Healthy),
                ))
            }
        });

        let registry = credential_registry();
        let results = futures::future::join_all((0..8).map(|_| registry.ambient_source())).await;

        assert!(results.iter().all(|r| r.is_ok()), "{results:?}");
        assert_eq!(build_count.load(Ordering::SeqCst), 1);
    }

    #[restate_core::test]
    async fn ambient_source_is_dead_only_for_a_proven_permanent_error() {
        let cases = [
            (ProbeOutcome::Healthy, false),
            (ProbeOutcome::Transient, false),
            (ProbeOutcome::Dead, true),
            (ProbeOutcome::Hang, false),
        ];
        for (outcome, expected_dead) in cases {
            let source = google_cloud_auth::credentials::Credentials::from(
                FakeCredentialsProvider::always(move || outcome),
            );
            assert_eq!(
                ambient_source_is_dead(&source).await,
                expected_dead,
                "{outcome:?}"
            );
        }
    }

    #[restate_core::test]
    async fn dead_ambient_source_is_replaced_after_permanent_impersonation_failure() {
        credential_registry()
            .ambient_source
            .seed_for_test(google_cloud_auth::credentials::Credentials::from(
                FakeCredentialsProvider::always(|| ProbeOutcome::Dead),
            ))
            .await;

        let build_count = Arc::new(AtomicUsize::new(0));
        add_ambient_source_override({
            let build_count = build_count.clone();
            move || {
                build_count.fetch_add(1, Ordering::SeqCst);
                Ok(google_cloud_auth::credentials::Credentials::from(
                    FakeCredentialsProvider::always(|| ProbeOutcome::Healthy),
                ))
            }
        });

        let client = GcpTokenClient::new();
        let audience = "https://ambient-recovery.example.com";
        let service_account = "sa@example.iam.gserviceaccount.com";
        add_build_override(IdTokenSpec::impersonated(audience, service_account), |_| {
            Ok(MockSource::new(|_| {
                MockOutcome::Error(permanent_error("impersonation misconfigured"))
            }) as Arc<dyn IdTokenSource>)
        });

        let outcome = client.mint(Some(service_account), audience).await;
        assert!(
            matches!(outcome, Err(GcpAuthError::Mint { .. })),
            "{outcome:?}"
        );

        assert_eq!(
            build_count.load(Ordering::SeqCst),
            1,
            "the dead source must be replaced exactly once"
        );

        assert!(credential_registry().ambient_source().await.is_ok());
        assert_eq!(build_count.load(Ordering::SeqCst), 1);
    }

    #[restate_core::test]
    async fn healthy_ambient_source_is_not_replaced_by_repeated_impersonation_failures() {
        credential_registry()
            .ambient_source
            .seed_for_test(google_cloud_auth::credentials::Credentials::from(
                FakeCredentialsProvider::always(|| ProbeOutcome::Healthy),
            ))
            .await;

        let build_count = Arc::new(AtomicUsize::new(0));
        add_ambient_source_override({
            let build_count = build_count.clone();
            move || {
                build_count.fetch_add(1, Ordering::SeqCst);
                Ok(google_cloud_auth::credentials::Credentials::from(
                    FakeCredentialsProvider::always(|| ProbeOutcome::Healthy),
                ))
            }
        });

        let client = GcpTokenClient::new();
        let audience = "https://ambient-stable.example.com";
        let service_account = "sa@example.iam.gserviceaccount.com";
        add_build_override(IdTokenSpec::impersonated(audience, service_account), |_| {
            Ok(MockSource::new(|_| {
                MockOutcome::Error(permanent_error("impersonation misconfigured"))
            }) as Arc<dyn IdTokenSource>)
        });

        for _ in 0..5 {
            let outcome = client.mint(Some(service_account), audience).await;
            assert!(
                matches!(outcome, Err(GcpAuthError::Mint { .. })),
                "{outcome:?}"
            );
        }

        assert_eq!(
            build_count.load(Ordering::SeqCst),
            0,
            "a healthy source must never be replaced by an impersonation-only failure"
        );
    }

    #[restate_core::test]
    async fn transient_error_keeps_entry_and_self_heals() {
        let client = GcpTokenClient::new();
        let audience = "https://transient.example.com";
        let cache_key = IdTokenSpec::ambient(audience);
        let source = MockSource::new(|call| {
            if call == 0 {
                MockOutcome::Error(transient_error("temporarily unavailable"))
            } else {
                MockOutcome::Token(token())
            }
        });
        let dyn_source: Arc<dyn IdTokenSource> = source.clone();
        let cached_entry = ready_entry(dyn_source);
        credential_registry()
            .cache
            .insert(cache_key.clone(), cached_entry.clone())
            .await;

        let first = client.mint(None, audience).await;
        assert!(matches!(first, Err(GcpAuthError::Mint { .. })), "{first:?}");

        let still_cached = credential_registry().cache.get(&cache_key).await;
        assert!(matches!(still_cached, Some(s) if Arc::ptr_eq(&s, &cached_entry)));

        let second = client.mint(None, audience).await;
        assert!(second.is_ok(), "{second:?}");
    }

    #[restate_core::test(start_paused = true)]
    async fn timeout_keeps_entry() {
        let client = GcpTokenClient::new();
        let audience = "https://timeout.example.com";
        let cache_key = IdTokenSpec::ambient(audience);
        let source: Arc<dyn IdTokenSource> = MockSource::new(|_| MockOutcome::Hang);
        let entry = ready_entry(source);
        credential_registry()
            .cache
            .insert(cache_key.clone(), entry.clone())
            .await;

        let outcome = client.mint(None, audience).await;
        assert!(matches!(outcome, Err(GcpAuthError::Timeout { .. })));
        let still_cached = credential_registry().cache.get(&cache_key).await;
        assert!(matches!(still_cached, Some(s) if Arc::ptr_eq(&s, &entry)));
    }

    #[restate_core::test]
    async fn permanent_error_evicts_conditionally() {
        let client = GcpTokenClient::new();
        let audience = "https://permanent.example.com";
        let cache_key = IdTokenSpec::ambient(audience);
        let source: Arc<dyn IdTokenSource> =
            MockSource::new(|_| MockOutcome::Error(permanent_error("misconfigured")));
        credential_registry()
            .cache
            .insert(cache_key.clone(), ready_entry(source))
            .await;

        let outcome = client.mint(None, audience).await;
        assert!(
            matches!(outcome, Err(GcpAuthError::Mint { .. })),
            "{outcome:?}"
        );

        assert!(credential_registry().cache.get(&cache_key).await.is_none());
    }

    #[restate_core::test]
    async fn aba_race_stale_caller_cannot_evict_or_recover_source() {
        let client = GcpTokenClient::new();
        let audience = "https://aba.example.com";
        let service_account = "sa@example.iam.gserviceaccount.com";
        let cache_key = IdTokenSpec::impersonated(audience, service_account);

        credential_registry()
            .ambient_source
            .seed_for_test(google_cloud_auth::credentials::Credentials::from(
                FakeCredentialsProvider::always(|| ProbeOutcome::Dead),
            ))
            .await;
        let source_rebuilds = Arc::new(AtomicUsize::new(0));
        add_ambient_source_override({
            let source_rebuilds = source_rebuilds.clone();
            move || {
                source_rebuilds.fetch_add(1, Ordering::SeqCst);
                Ok(google_cloud_auth::credentials::Credentials::from(
                    FakeCredentialsProvider::always(|| ProbeOutcome::Healthy),
                ))
            }
        });

        let new_source: Arc<dyn IdTokenSource> = MockSource::new(|_| MockOutcome::Token(token()));
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

        let outcome = client.mint(Some(service_account), audience).await;
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
        let running = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let probe_completed = Arc::new(std::sync::atomic::AtomicBool::new(false));

        let registry = task_center.run_sync(|| {
            let running = running.clone();
            let probe_completed = probe_completed.clone();
            add_build_override(IdTokenSpec::ambient(audience), move |_| {
                let running = running.clone();
                let probe_completed = probe_completed.clone();
                tokio::spawn(async move {
                    while running.load(Ordering::SeqCst) {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                    probe_completed.store(true, Ordering::SeqCst);
                });
                Ok(MockSource::new(|_| MockOutcome::Token(token())) as Arc<dyn IdTokenSource>)
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
                    entry.get_or_start(registry, spec).await
                }
                .in_tc(&task_center),
            );
            if let Err(error) = &result {
                panic!("{error}");
            }
        }

        std::thread::sleep(Duration::from_millis(20));
        running.store(false, Ordering::SeqCst);
        std::thread::sleep(Duration::from_millis(100));
        assert!(
            probe_completed.load(Ordering::SeqCst),
            "a task spawned during construction must survive the caller runtime's drop"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn blocking_builds_are_bounded_and_never_deadlock() {
        let concurrent = Arc::new(AtomicUsize::new(0));
        let high_water_mark = Arc::new(AtomicUsize::new(0));

        // Test the semaphore's production choke point directly; construction overrides bypass it.
        let tasks: Vec<_> = (0..4 * MAX_CONCURRENT_BLOCKING_BUILDS)
            .map(|_| {
                let concurrent = concurrent.clone();
                let high_water_mark = high_water_mark.clone();
                tokio::spawn(run_blocking("test".to_owned(), move || {
                    let now = concurrent.fetch_add(1, Ordering::SeqCst) + 1;
                    high_water_mark.fetch_max(now, Ordering::SeqCst);
                    std::thread::sleep(Duration::from_millis(20));
                    concurrent.fetch_sub(1, Ordering::SeqCst);
                    Ok(MockSource::new(|_| MockOutcome::Token(token())) as Arc<dyn IdTokenSource>)
                }))
            })
            .collect();

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

        let mark = high_water_mark.load(Ordering::SeqCst);
        assert!(
            mark <= MAX_CONCURRENT_BLOCKING_BUILDS,
            "at most {MAX_CONCURRENT_BLOCKING_BUILDS} blocking builds may run concurrently, saw {mark}"
        );
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

        let replacement_started = Arc::new(AtomicBool::new(false));
        let replacement = {
            let replacement_started = replacement_started.clone();
            tokio::spawn(spawn_bounded_blocking(move || {
                replacement_started.store(true, Ordering::SeqCst);
            }))
        };

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            !replacement_started.load(Ordering::SeqCst),
            "cancelled callers must retain permits until their blocking work finishes"
        );

        let (lock, wake) = &*release;
        *lock.lock().expect("release lock is not poisoned") = true;
        wake.notify_all();
        tokio::time::timeout(Duration::from_secs(1), replacement)
            .await
            .expect("replacement starts after blocking work exits")
            .expect("replacement task does not panic")
            .expect("blocking build does not panic");
    }
}
