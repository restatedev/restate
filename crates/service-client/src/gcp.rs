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
//! Building a `google-cloud-auth` credential starts a background refresh task that lives as long as
//! the credential does. The entire credential is cached in a process-global registry keyed by
//! `(audience, impersonate_service_account)`; impersonated keys additionally share a process-wide
//! ambient source credential (see [`CredentialRegistry::ambient_source`]). Construction must run as a
//! [`TaskKind::Credentials`] task on TaskCenter's default runtime, so a credential's refresh task
//! lands on a runtime with process lifetime rather than the runtime that happens to call `mint()`.

use std::sync::{Arc, LazyLock};
use std::time::Duration;

use async_trait::async_trait;
use moka::future::Cache;
use moka::ops::compute::Op;
use restate_core::{TaskCenter, TaskKind};
use thiserror::Error;

#[cfg(any(test, feature = "test_util"))]
use ahash::HashMap;
#[cfg(any(test, feature = "test_util"))]
use parking_lot::Mutex;

const MINT_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);

const CACHE_TIME_TO_IDLE: Duration = Duration::from_secs(3600);

/// moka evicts lazily; drive eviction when no mint attempts are happening
const CACHE_HOUSEKEEPING_INTERVAL: Duration = Duration::from_secs(300);

/// Bound on probing the shared ambient source's own cached state (see [`ambient_source_is_dead`])
const AMBIENT_SOURCE_PROBE_TIMEOUT: Duration = Duration::from_secs(1);

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

/// Internal seam that keeps the registry and the mint path testable without ADC or network: `Live`
/// wraps a real `IDTokenCredentials`, while tests inject mocks and a test-only `Seeded` source (see
/// [`GcpTokenClient::seed_for_test`]).
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

/// Process-global credential registry: a cache of credential objects and the shared ambient
/// credentials source. Created once per process; every [`GcpTokenClient`] is a handle to it.
struct CredentialRegistry {
    cache: Cache<IdTokenSpec, Arc<dyn IdTokenSource>>,
    ambient_source: RecoverableCell<google_cloud_auth::credentials::Credentials>,
    #[cfg(any(test, feature = "test_util"))]
    test_hooks: TestHooks,
}

/// A single cached, cheaply-cloneable value with single-flight, retry-on-error construction, and
/// supporting replacement in case the stored value is later found to be failed.
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

    // bounded by the probe's own timeout; swapped under lock so no need for external ABA checks
    async fn replace_if_failed<E>(
        &self,
        should_replace: impl AsyncFnOnce(&T) -> bool,
        build: impl Future<Output = Result<T, E>>,
    ) -> Result<(), E> {
        let mut guard = self.cell.lock().await;
        let Some(current) = guard.as_ref() else {
            return Ok(());
        };
        if !should_replace(current).await {
            return Ok(());
        }
        match build.await {
            Ok(value) => *guard = Some(value),
            Err(e) => {
                *guard = None;
                return Err(e);
            }
        }
        Ok(())
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

/// Lets unit tests drive the real moka-backed construction/eviction paths without ADC or network.
/// `build_overrides` is keyed by `IdTokenSpec` so distinct-key tests don't interfere.
/// `ambient_source_override` is a single process-wide slot matching the singleton it stands in for;
/// safe under `nextest` which runs each test in its own process.
#[cfg(any(test, feature = "test_util"))]
#[derive(Default)]
struct TestHooks {
    build_overrides: Mutex<HashMap<IdTokenSpec, ConstructOverride>>,
    ambient_source_override: Mutex<Option<AmbientSourceOverride>>,
}

static CREDENTIAL_REGISTRY: LazyLock<CredentialRegistry> = LazyLock::new(CredentialRegistry::init);

fn credential_registry() -> &'static CredentialRegistry {
    &CREDENTIAL_REGISTRY
}

impl CredentialRegistry {
    fn init() -> Self {
        let cache = Cache::builder().time_to_idle(CACHE_TIME_TO_IDLE).build();

        let housekeeping_cache = cache.clone();
        let housekeeping = async move {
            let mut interval = tokio::time::interval(CACHE_HOUSEKEEPING_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                interval.tick().await;
                housekeeping_cache.run_pending_tasks().await;
            }
        };

        // Managed (not unmanaged) so TaskCenter shutdown cancels and awaits it -- an unmanaged
        // infinite loop races test-process teardown and shows up as a nextest leak.
        let _ = TaskCenter::spawn(
            TaskKind::Credentials,
            "gcp-credential-housekeeping",
            housekeeping,
        );

        Self {
            cache,
            ambient_source: RecoverableCell::new(),
            #[cfg(any(test, feature = "test_util"))]
            test_hooks: TestHooks::default(),
        }
    }

    // concurrent misses for the same key coalesce, with waiters seeing the same error; failures are
    // not cached
    async fn get_or_build(
        &'static self,
        spec: &IdTokenSpec,
    ) -> Result<Arc<dyn IdTokenSource>, GcpAuthError> {
        #[cfg(any(test, feature = "test_util"))]
        let build_override_fn = self.test_hooks.build_overrides.lock().get(spec).cloned();

        self.cache
            .try_get_with_by_ref(spec, async {
                #[cfg(any(test, feature = "test_util"))]
                if let Some(f) = build_override_fn {
                    return f(spec);
                }
                self.build_on_tc_task(spec.clone()).await
            })
            .await
            .map_err(|error| (*error).clone())
    }

    // guards against a slow caller evicting a freshly rebuilt healthy credential
    async fn evict_if_unchanged(
        &'static self,
        spec: &IdTokenSpec,
        expected: &Arc<dyn IdTokenSource>,
    ) {
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

    async fn ambient_source(
        &'static self,
    ) -> Result<google_cloud_auth::credentials::Credentials, String> {
        self.ambient_source
            .get_or_build(self.build_ambient_source())
            .await
    }

    /// After a permanent *impersonated* mint failure -- which doesn't say whether the shared
    /// source died or just this key's target service account is misconfigured -- replace the
    /// source only if a probe proves its actor is gone; a misconfigured target with a healthy
    /// source (the common case) leaves it untouched, or every retry would strand a live actor.
    async fn recover_ambient_source_if_dead(&'static self) {
        let _ = self
            .ambient_source
            .replace_if_failed(ambient_source_is_dead, self.build_ambient_source())
            .await;
    }

    /// Builds the ambient source credential, consulting the test override when compiled for tests.
    async fn build_ambient_source(
        &'static self,
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
                tokio::task::spawn_blocking(build).await
            })
            .map_err(|_| "TaskCenter is shutting down".to_owned())?;
        match task.await {
            Ok(Ok(result)) => result,
            Ok(Err(join_error)) => Err(format!("construction task panicked: {join_error}")),
            Err(_shutdown) => Err("GCP credential construction task failed".to_owned()),
        }
    }

    /// Builds the outer credential for `spec` as a single [`TaskKind::Credentials`] task, so any
    /// refresh actor `build()` spawns internally lands on a runtime with process lifetime, not the
    /// runtime on which `mint()` gets called.
    async fn build_on_tc_task(
        &'static self,
        spec: IdTokenSpec,
    ) -> Result<Arc<dyn IdTokenSource>, GcpAuthError> {
        // The spawned task needs ownership of `spec`; one audience copy stays behind for the two
        // error paths, which consume it lazily.
        let audience = spec.audience.clone();
        let task = TaskCenter::current()
            .spawn_unmanaged(TaskKind::Credentials, "gcp-credential-build", async move {
                self.build_credentials(spec).await
            })
            .map_err(|_| GcpAuthError::Build {
                audience: audience.clone(),
                message: "TaskCenter is shutting down".to_owned(),
            })?;
        task.await.unwrap_or_else(|_| {
            Err(GcpAuthError::Build {
                audience,
                message: "GCP credential construction task failed".to_owned(),
            })
        })
    }

    /// Resolves the credential(s) needed for `key` and builds the outer ID-token credential,
    /// offloading the blocking `.build()` call. The impersonated arm first resolves the
    /// process-wide ambient source: a permanent failure here can only ever strand the outer actor,
    /// since the shared source actor is independent of any single key and is recovered separately
    /// (see [`CredentialRegistry::recover_ambient_source_if_dead`]).
    async fn build_credentials(
        &'static self,
        spec: IdTokenSpec,
    ) -> Result<Arc<dyn IdTokenSource>, GcpAuthError> {
        let IdTokenSpec {
            impersonate,
            audience,
        } = spec;
        // The blocking closure takes ownership of the strings; one audience copy stays behind as
        // run_blocking's panic context (builder errors already carry the audience themselves).
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

/// A permanent error probing `source`'s own cached token state proves its refresh actor already
/// published that error and exited -- replacing it strands nothing live. Anything else (a token, a
/// transient error, or the probe timing out) means the actor might still be alive, so it must be
/// kept.
async fn ambient_source_is_dead(source: &google_cloud_auth::credentials::Credentials) -> bool {
    matches!(
        tokio::time::timeout(AMBIENT_SOURCE_PROBE_TIMEOUT, source.headers(http::Extensions::new()))
            .await,
        Ok(Err(e)) if !e.is_transient()
    )
}

/// Runs `build` on a blocking thread, mapping a panicked build thread to a `GcpAuthError` at this
/// one call site.
async fn run_blocking(
    audience: String,
    build: impl FnOnce() -> Result<Arc<dyn IdTokenSource>, GcpAuthError> + Send + 'static,
) -> Result<Arc<dyn IdTokenSource>, GcpAuthError> {
    tokio::task::spawn_blocking(build)
        .await
        .unwrap_or_else(|e| {
            Err(GcpAuthError::Build {
                audience,
                message: format!("construction thread panicked: {e}"),
            })
        })
}

/// Builds the outer credential for a non-impersonated token.
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

/// Builds the outer credential for an impersonated key, from the shared ambient `source`.
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

/// Token-mint client: a cheap handle to the process-global credential [`Registry`]. Every
/// `ServiceClient` clone shares the same registry, so distinct GCP identities each own at most one
/// credential (and its refresh actor) for the life of the process. Outside tests this carries no
/// state of its own: it is a stateless handle to `credential_registry()`.
#[derive(Clone)]
pub struct GcpTokenClient {
    #[cfg(any(test, feature = "test_util"))]
    inner: Arc<Inner>,
}

/// Instance-local test overlay. Kept separate from the global registry so tests that construct
/// multiple `ServiceClient`s in one process cannot cross-contaminate each other by seeding a
/// shared cache entry.
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

        // Test seeds/forced failures are never evicted: they live on this instance, not in the
        // shared registry cache.
        let (source, evictable) = match self.test_intercept(&spec, &impersonate) {
            Some(Ok(source)) => (source, false),
            Some(Err(error)) => return Err(error),
            None => (credential_registry().get_or_build(&spec).await?, true),
        };

        match tokio::time::timeout(MINT_ATTEMPT_TIMEOUT, source.id_token()).await {
            Ok(Ok(token)) => Ok(token),
            Ok(Err(error)) => {
                // Transient failures self-heal via the credential's own refresh loop, no need to
                // evict the entry; a permanent failure will not recover, so evict it -- but only if
                // the cache still holds the exact credential that produced the error.
                if evictable && !error.is_transient() {
                    credential_registry()
                        .evict_if_unchanged(&spec, &source)
                        .await;
                    if spec.impersonate.is_some() {
                        credential_registry().recover_ambient_source_if_dead().await;
                    }
                }
                Err(GcpAuthError::Mint {
                    audience: audience.to_owned(),
                    impersonate,
                    message: error.to_string(),
                })
            }
            Err(_) => Err(GcpAuthError::Timeout {
                audience: audience.to_owned(),
                impersonate,
                duration: MINT_ATTEMPT_TIMEOUT,
            }),
        }
    }

    /// Test seam consulted at the top of `mint()`: a forced failure or a seeded source, if any.
    /// One expression in `mint()` covers both builds, so production and test compilation cannot
    /// drift apart.
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

    /// Test-only: seed a token so subsequent `mint` calls with the same key return it directly.
    /// Seeding is local to the `GcpTokenClient` instance, not the shared registry, so tests that
    /// construct multiple `ServiceClient`s in one process do not interfere with each other.
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

    /// A `CredentialsProvider` a test can drive deterministically, with no SDK dependencies:
    /// `Credentials::from(...)` wraps this so its `headers()` impl below is exactly what
    /// `ambient_source_is_dead` observes.
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
                ProbeOutcome::Dead => Err(permanent_error("source actor permanently dead")),
                ProbeOutcome::Transient => {
                    Err(transient_error("source actor transiently unavailable"))
                }
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

    /// Pins P1's fix (restatedev/restate#5151): the impersonated arm's source ADC credential is a
    /// single process-wide actor, not one per key. Proving `ambient_source` single-flights and
    /// shares its result is equivalent to proving N concurrent impersonated constructions share one
    /// source build.
    ///
    /// Relies on `ambient_source` being uninitialized when this test starts, which holds under
    /// `nextest` (one process per test) as long as no other test in this file exercises
    /// impersonation for real; see `TestHooks`'s doc comment.
    #[restate_core::test]
    async fn impersonated_constructions_share_one_ambient_source_build() {
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

        let results =
            futures::future::join_all((0..8).map(|_| credential_registry().ambient_source())).await;

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

    /// A shared ambient source whose actor has permanently died is replaced exactly once, by the
    /// first permanent impersonated mint failure to probe it -- and the replacement is then reused
    /// without further rebuilds.
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

        // The replacement is healthy and reusable without a further rebuild.
        assert!(credential_registry().ambient_source().await.is_ok());
        assert_eq!(build_count.load(Ordering::SeqCst), 1);
    }

    /// A healthy shared ambient source is never replaced by a repeatedly-failing impersonation
    /// target: the failure is scoped to that one key, and the source is provably fine.
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

        // The entry must still be present and unchanged (no eviction on transient failure).
        let still_cached = credential_registry().cache.get(&cache_key).await;
        assert!(matches!(still_cached, Some(s) if Arc::ptr_eq(&s, &dyn_source)));

        // The mock "self-heals" on the next call, as a real credential's refresh loop would.
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

        /// Simulates a concurrent rebuild completing -- replacing this source in the registry
        /// cache with `replacement` -- before this permanently-failing source's own error is
        /// reported back to `mint()`. Driving the swap from inside `id_token()` exercises the
        /// exact race `evict_if_unchanged`'s compare-and-evict guards against, through the real
        /// `mint()` call path rather than by hand-simulating the two steps independently.
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
}
