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
//! Credentials are cached in a registry bound to the ambient [`TaskCenter`]. Construction runs on
//! its default runtime so refresh tasks have process lifetime. Invocation tasks inherit that
//! task-local context at the invoker's `JoinSet` boundary.

use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use metrics::{counter, gauge, histogram};
use moka::future::Cache;
use moka::ops::compute::Op;
use parking_lot::RwLock;
use restate_core::{Handle, TaskCenter, TaskKind};
use thiserror::Error;
use tokio::sync::Semaphore;
use tracing::warn;

use crate::metric_definitions::{
    GCP_CREDENTIAL_BUILD_DURATION, GCP_CREDENTIAL_BUILDS, GCP_CREDENTIALS_ACTIVE, GCP_TOKEN_MINTS,
    MINT_OUTCOME_BUILD_ERROR, MINT_OUTCOME_PERMANENT_ERROR, MINT_OUTCOME_SUCCESS,
    MINT_OUTCOME_TIMEOUT, MINT_OUTCOME_TRANSIENT_ERROR, RESULT_ERROR, RESULT_SUCCESS,
};

#[cfg(any(test, feature = "test_util"))]
use ahash::HashMap;
#[cfg(any(test, feature = "test_util"))]
use parking_lot::Mutex;

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

struct Live(google_cloud_auth::credentials::idtoken::IDTokenCredentials);

#[async_trait]
impl IdTokenSource for Live {
    async fn id_token(&self) -> Result<String, google_cloud_auth::errors::CredentialsError> {
        self.0.id_token().await
    }
}

/// Credentials and their shared ambient source, bound to one TaskCenter.
struct CredentialRegistry {
    self_weak: Weak<CredentialRegistry>,
    cache: Cache<IdTokenSpec, Arc<dyn IdTokenSource>>,
    ambient_source: RecoverableCell<google_cloud_auth::credentials::Credentials>,
    #[cfg(any(test, feature = "test_util"))]
    test_hooks: TestHooks,
}

struct RecoverableCell<T> {
    cell: tokio::sync::Mutex<Option<T>>,
}

impl<T: Clone> RecoverableCell<T> {
    fn new() -> Self {
        Self {
            cell: tokio::sync::Mutex::new(None),
        }
    }

    async fn get_or_build<E>(&self, build: impl Future<Output = Result<T, E>>) -> Result<T, E> {
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
    async fn replace_if_failed<E>(
        &self,
        should_replace: impl AsyncFnOnce(&T) -> bool,
        build: impl Future<Output = Result<T, E>>,
    ) -> Result<bool, E> {
        let mut guard = self.cell.lock().await;
        let Some(current) = guard.as_ref() else {
            return Ok(false);
        };
        if !should_replace(current).await {
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
    async fn seed_for_test(&self, value: T) {
        *self.cell.lock().await = Some(value);
    }
}

#[cfg(any(test, feature = "test_util"))]
type ConstructOverride =
    Arc<dyn Fn(&IdTokenSpec) -> Result<Arc<dyn IdTokenSource>, GcpAuthError> + Send + Sync>;
#[cfg(any(test, feature = "test_util"))]
type AmbientSourceOverride =
    Arc<dyn Fn() -> Result<google_cloud_auth::credentials::Credentials, String> + Send + Sync>;

#[cfg(any(test, feature = "test_util"))]
#[derive(Default)]
struct TestHooks {
    build_overrides: Mutex<HashMap<IdTokenSpec, ConstructOverride>>,
    ambient_source_override: Mutex<Option<AmbientSourceOverride>>,
}

struct RegistrySlot {
    registry: Arc<CredentialRegistry>,
    task_center: Handle,
}

static REGISTRY: RwLock<Option<RegistrySlot>> = RwLock::new(None);

/// Clears only its own registry slot when the housekeeping task is cancelled. Pointer equality
/// prevents a stale guard from removing a successor registry.
struct ClearRegistrySlotOnDrop {
    registry: Weak<CredentialRegistry>,
}

impl Drop for ClearRegistrySlotOnDrop {
    fn drop(&mut self) {
        let Some(registry) = self.registry.upgrade() else {
            return;
        };
        let mut slot = REGISTRY.write();
        if matches!(slot.as_ref(), Some(current) if Arc::ptr_eq(&current.registry, &registry)) {
            *slot = None;
        }
    }
}

/// Returns the registry for the ambient TaskCenter, replacing a previous generation. A registry
/// built after its TaskCenter starts shutting down is deliberately not installed.
fn credential_registry() -> Arc<CredentialRegistry> {
    let current = TaskCenter::current();

    if let Some(slot) = REGISTRY.read().as_ref()
        && slot.task_center.ptr_eq(&current)
    {
        return slot.registry.clone();
    }

    let mut guard = REGISTRY.write();
    if let Some(slot) = guard.as_ref()
        && slot.task_center.ptr_eq(&current)
    {
        return slot.registry.clone();
    }

    let registry = CredentialRegistry::init(&current);
    // If shutdown won the race, return this unregistered registry. Its attempted build will fail
    // normally, and it cannot pin the dead generation.
    if !current.is_shutdown_requested() {
        *guard = Some(RegistrySlot {
            registry: registry.clone(),
            task_center: current,
        });
    }
    registry
}

impl CredentialRegistry {
    fn init(task_center: &Handle) -> Arc<Self> {
        Arc::new_cyclic(|self_weak| {
            crate::metric_definitions::describe_metrics();

            let cache = Cache::builder().time_to_idle(CACHE_TIME_TO_IDLE).build();

            let housekeeping_cache = cache.clone();
            let clear_slot_on_drop = ClearRegistrySlotOnDrop {
                registry: self_weak.clone(),
            };
            let housekeeping = async move {
                let _clear_slot_on_drop = clear_slot_on_drop;
                let mut interval = tokio::time::interval(CACHE_HOUSEKEEPING_INTERVAL);
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    interval.tick().await;
                    housekeeping_cache.run_pending_tasks().await;
                    gauge!(GCP_CREDENTIALS_ACTIVE).set(housekeeping_cache.entry_count() as f64);
                }
            };

            let _ = task_center.spawn(
                TaskKind::Credentials,
                "gcp-credential-housekeeping",
                housekeeping,
            );

            Self {
                self_weak: self_weak.clone(),
                cache,
                ambient_source: RecoverableCell::new(),
                #[cfg(any(test, feature = "test_util"))]
                test_hooks: TestHooks::default(),
            }
        })
    }

    fn arc(&self) -> Arc<Self> {
        self.self_weak
            .upgrade()
            .expect("a CredentialRegistry is always held by the Arc it was constructed in")
    }

    async fn get_or_build(
        &self,
        spec: &IdTokenSpec,
    ) -> Result<Arc<dyn IdTokenSource>, GcpAuthError> {
        let result = self
            .cache
            .try_get_with_by_ref(spec, self.build_on_tc_task(spec.clone()))
            .await
            .map_err(|error| (*error).clone());
        if result.is_ok() {
            gauge!(GCP_CREDENTIALS_ACTIVE).set(self.cache.entry_count() as f64);
        }
        result
    }

    async fn evict_if_unchanged(&self, spec: &IdTokenSpec, expected: &Arc<dyn IdTokenSource>) {
        self.cache
            .entry_by_ref(spec)
            .and_compute_with(|entry| {
                let op = match &entry {
                    Some(entry) if Arc::ptr_eq(entry.value(), expected) => Op::Remove,
                    _ => Op::Nop,
                };
                std::future::ready(op)
            })
            .await;
    }

    async fn ambient_source(&self) -> Result<google_cloud_auth::credentials::Credentials, String> {
        self.ambient_source
            .get_or_build(self.build_ambient_source())
            .await
    }

    /// Replace only a source whose refresh task is proven dead; a target-scoped failure must not
    /// strand a healthy shared source.
    async fn recover_ambient_source_if_dead(&self) {
        match self
            .ambient_source
            .replace_if_failed(ambient_source_is_dead, self.build_ambient_source())
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

    async fn build_ambient_source(
        &self,
    ) -> Result<google_cloud_auth::credentials::Credentials, String> {
        #[cfg(any(test, feature = "test_util"))]
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
            .spawn_unmanaged(TaskKind::Credentials, "gcp-credential-build", async move {
                spawn_bounded_blocking(build).await
            })
            .map_err(|_| "TaskCenter is shutting down".to_owned())?;
        match task.await {
            Ok(Ok(result)) => result,
            Ok(Err(join_error)) => Err(format!("construction task panicked: {join_error}")),
            Err(_shutdown) => Err("GCP credential construction task failed".to_owned()),
        }
    }

    async fn build_on_tc_task(
        &self,
        spec: IdTokenSpec,
    ) -> Result<Arc<dyn IdTokenSource>, GcpAuthError> {
        let registry = self.arc();
        // Credential refresh tasks spawned by build() must use the TaskCenter default runtime.
        let audience = spec.audience.clone();
        let task = TaskCenter::current()
            .spawn_unmanaged(TaskKind::Credentials, "gcp-credential-build", async move {
                #[cfg(any(test, feature = "test_util"))]
                if let Some(f) = registry
                    .test_hooks
                    .build_overrides
                    .lock()
                    .get(&spec)
                    .cloned()
                {
                    return f(&spec);
                }
                let start = Instant::now();
                let result = registry.build_credentials(spec).await;
                histogram!(GCP_CREDENTIAL_BUILD_DURATION).record(start.elapsed().as_secs_f64());
                result
            })
            .map_err(|_| GcpAuthError::Build {
                audience: audience.clone(),
                message: "TaskCenter is shutting down".to_owned(),
            })?;
        let result = task.await.unwrap_or_else(|_| {
            Err(GcpAuthError::Build {
                audience,
                message: "GCP credential construction task failed".to_owned(),
            })
        });
        counter!(
            GCP_CREDENTIAL_BUILDS,
            "result" => if result.is_ok() { RESULT_SUCCESS } else { RESULT_ERROR }
        )
        .increment(1);
        result
    }

    async fn build_credentials(
        &self,
        spec: IdTokenSpec,
    ) -> Result<Arc<dyn IdTokenSource>, GcpAuthError> {
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

/// A permanent probe error proves the source refresh task exited; all other outcomes may still be
/// live and must be retained.
async fn ambient_source_is_dead(source: &google_cloud_auth::credentials::Credentials) -> bool {
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
    let _permit = BLOCKING_BUILD_PERMITS
        .acquire()
        .await
        .expect("BLOCKING_BUILD_PERMITS is never closed");
    tokio::task::spawn_blocking(build).await
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
    source: google_cloud_auth::credentials::Credentials,
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

/// Cheap handle to the TaskCenter-scoped credential registry.
#[derive(Clone)]
pub struct GcpTokenClient {
    #[cfg(any(test, feature = "test_util"))]
    inner: Arc<Inner>,
}

#[cfg(any(test, feature = "test_util"))]
struct Inner {
    test_force_failure: Mutex<Option<String>>,
    test_sources: Mutex<HashMap<IdTokenSpec, Arc<dyn IdTokenSource>>>,
}

impl GcpTokenClient {
    pub fn new() -> Self {
        Self {
            #[cfg(any(test, feature = "test_util"))]
            inner: Arc::new(Inner {
                test_force_failure: Mutex::new(None),
                test_sources: Mutex::default(),
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

        // Recovery must act on the registry generation that built this source.
        let (source, registry) = match self.test_intercept(&spec, &impersonate) {
            Some(Ok(source)) => (source, None),
            Some(Err(error)) => return Err(error),
            None => {
                let registry = credential_registry();
                match registry.get_or_build(&spec).await {
                    Ok(source) => (source, Some(registry)),
                    Err(error) => {
                        // A failed single-flight build fails every waiting caller.
                        counter!(GCP_TOKEN_MINTS, "outcome" => MINT_OUTCOME_BUILD_ERROR)
                            .increment(1);
                        return Err(error);
                    }
                }
            }
        };

        match tokio::time::timeout(MINT_ATTEMPT_TIMEOUT, source.id_token()).await {
            Ok(Ok(token)) => {
                counter!(GCP_TOKEN_MINTS, "outcome" => MINT_OUTCOME_SUCCESS).increment(1);
                Ok(token)
            }
            Ok(Err(error)) => {
                counter!(
                    GCP_TOKEN_MINTS,
                    "outcome" => if error.is_transient() {
                        MINT_OUTCOME_TRANSIENT_ERROR
                    } else {
                        MINT_OUTCOME_PERMANENT_ERROR
                    }
                )
                .increment(1);
                // Transient errors may self-heal; evict permanent failures only if still current.
                if let Some(registry) = &registry
                    && !error.is_transient()
                {
                    registry.evict_if_unchanged(&spec, &source).await;
                    if spec.impersonate.is_some() {
                        registry.recover_ambient_source_if_dead().await;
                    }
                }
                Err(GcpAuthError::Mint {
                    audience: audience.to_owned(),
                    impersonate,
                    message: error.to_string(),
                })
            }
            Err(_) => {
                counter!(GCP_TOKEN_MINTS, "outcome" => MINT_OUTCOME_TIMEOUT).increment(1);
                Err(GcpAuthError::Timeout {
                    audience: audience.to_owned(),
                    impersonate,
                    duration: MINT_ATTEMPT_TIMEOUT,
                })
            }
        }
    }

    #[cfg(any(test, feature = "test_util"))]
    fn test_intercept(
        &self,
        spec: &IdTokenSpec,
        impersonate: &str,
    ) -> Option<Result<Arc<dyn IdTokenSource>, GcpAuthError>> {
        if let Some(message) = self.inner.test_force_failure.lock().clone() {
            return Some(Err(GcpAuthError::Mint {
                audience: spec.audience.clone(),
                impersonate: impersonate.to_owned(),
                message,
            }));
        }
        self.inner.test_sources.lock().get(spec).cloned().map(Ok)
    }

    #[cfg(not(any(test, feature = "test_util")))]
    #[inline(always)]
    fn test_intercept(
        &self,
        _spec: &IdTokenSpec,
        _impersonate: &str,
    ) -> Option<Result<Arc<dyn IdTokenSource>, GcpAuthError>> {
        None
    }

    #[cfg(any(test, feature = "test_util"))]
    pub fn force_mint_failure_for_test(&self, message: &str) {
        *self.inner.test_force_failure.lock() = Some(message.to_owned());
    }

    #[cfg(any(test, feature = "test_util"))]
    pub fn seed_for_test(&self, impersonate: Option<&str>, audience: &str, token: String) {
        struct MockToken(String);

        #[async_trait]
        impl IdTokenSource for MockToken {
            async fn id_token(
                &self,
            ) -> Result<String, google_cloud_auth::errors::CredentialsError> {
                Ok(self.0.clone())
            }
        }

        let spec = IdTokenSpec {
            impersonate: impersonate.map(str::to_owned),
            audience: audience.to_owned(),
        };
        self.inner
            .test_sources
            .lock()
            .insert(spec, Arc::new(MockToken(token)));
    }
}

impl Default for GcpTokenClient {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

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
            }
        }
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
        f: impl Fn(&IdTokenSpec) -> Result<Arc<dyn IdTokenSource>, GcpAuthError> + Send + Sync + 'static,
    ) {
        credential_registry()
            .test_hooks
            .build_overrides
            .lock()
            .insert(cache_key, Arc::new(f));
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
        credential_registry()
            .cache
            .insert(cache_key.clone(), dyn_source.clone())
            .await;

        let first = client.mint(None, audience).await;
        assert!(matches!(first, Err(GcpAuthError::Mint { .. })), "{first:?}");

        let still_cached = credential_registry().cache.get(&cache_key).await;
        assert!(matches!(still_cached, Some(s) if Arc::ptr_eq(&s, &dyn_source)));

        let second = client.mint(None, audience).await;
        assert!(second.is_ok(), "{second:?}");
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
            .insert(cache_key.clone(), source.clone())
            .await;

        let outcome = client.mint(None, audience).await;
        assert!(
            matches!(outcome, Err(GcpAuthError::Mint { .. })),
            "{outcome:?}"
        );

        assert!(credential_registry().cache.get(&cache_key).await.is_none());
    }

    #[restate_core::test]
    async fn aba_race_stale_caller_evict_is_a_no_op() {
        let client = GcpTokenClient::new();
        let audience = "https://aba.example.com";
        let cache_key = IdTokenSpec::ambient(audience);
        let new_source: Arc<dyn IdTokenSource> = MockSource::new(|_| MockOutcome::Token(token()));

        struct SwapThenFail {
            spec: IdTokenSpec,
            replacement: Arc<dyn IdTokenSource>,
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
            replacement: new_source.clone(),
        });
        credential_registry()
            .cache
            .insert(cache_key.clone(), old_source.clone())
            .await;

        let outcome = client.mint(None, audience).await;
        assert!(
            matches!(outcome, Err(GcpAuthError::Mint { .. })),
            "{outcome:?}"
        );

        let cached = credential_registry().cache.get(&cache_key).await;
        assert!(
            matches!(cached, Some(s) if Arc::ptr_eq(&s, &new_source)),
            "evict from a stale caller must not remove the freshly rebuilt healthy entry"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn registry_is_dropped_at_shutdown_and_a_new_task_center_gets_a_fresh_one() {
        use restate_core::{TaskCenterBuilder, TaskCenterFutureExt as _};

        let audience = "https://tc-lifecycle.example.com";
        let spec = IdTokenSpec::ambient(audience);
        let build_count = Arc::new(AtomicUsize::new(0));

        let install_override = || {
            add_build_override(spec.clone(), {
                let build_count = build_count.clone();
                move |_| {
                    build_count.fetch_add(1, Ordering::SeqCst);
                    Ok(MockSource::new(|_| MockOutcome::Token(token())) as Arc<dyn IdTokenSource>)
                }
            });
        };

        let tc_a = TaskCenterBuilder::default_for_tests()
            .build()
            .expect("task center builds")
            .into_handle();
        let weak = async {
            install_override();
            let registry = credential_registry();
            let result = registry.get_or_build(&spec).await;
            if let Err(error) = &result {
                panic!("{error}");
            }
            Arc::downgrade(&registry)
        }
        .in_tc(&tc_a)
        .await;
        assert_eq!(build_count.load(Ordering::SeqCst), 1);
        tc_a.shutdown_node("test done with TC-A", 0).await;

        // Cancellation is asynchronous; don't upgrade the Weak while polling because that would
        // keep the registry alive.
        let mut dropped = weak.strong_count() == 0;
        for _ in 0..50 {
            if dropped {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
            dropped = weak.strong_count() == 0;
        }
        assert!(
            dropped,
            "the registry must be dropped at TaskCenter shutdown, not at the next mint"
        );

        let tc_b = TaskCenterBuilder::default_for_tests()
            .build()
            .expect("task center builds")
            .into_handle();
        async {
            install_override();
            let registry = credential_registry();
            let result = registry.get_or_build(&spec).await;
            if let Err(error) = &result {
                panic!("{error}");
            }
        }
        .in_tc(&tc_b)
        .await;
        assert_eq!(
            build_count.load(Ordering::SeqCst),
            2,
            "a new task center must get a fresh registry -- the old one's cache must not persist"
        );
        tc_b.shutdown_node("test done with TC-B", 0).await;
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
            let result = caller_runtime
                .block_on(async { registry.get_or_build(&spec).await }.in_tc(&task_center));
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

        for task in tasks {
            let result = task.await.expect("task doesn't panic");
            if let Err(error) = &result {
                panic!("{error}");
            }
        }

        let mark = high_water_mark.load(Ordering::SeqCst);
        assert!(
            mark <= MAX_CONCURRENT_BLOCKING_BUILDS,
            "at most {MAX_CONCURRENT_BLOCKING_BUILDS} blocking builds may run concurrently, saw {mark}"
        );
    }
}
