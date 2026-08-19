// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! GCP OIDC ID-token mint client for HTTP deployments hosted on Cloud
//! Run and similar Google-fronted endpoints.
//!
//! `google-cloud-auth` credentials are actors: every `build()` immediately spawns a background
//! refresh task that lives for as long as anything holds a clone of the credential. Restate
//! therefore caches credential *objects* — never token strings — in a process-global registry
//! keyed by `(impersonate_service_account, audience)`, so each distinct GCP identity owns at most
//! one refresh task for the life of the process. The credential's own `TokenCache` proactively
//! refreshes ~4 minutes before expiry, so a steady-state `mint()` call reads a `watch` channel and
//! never awaits network I/O.
//!
//! An impersonated key is a two-actor stack: an outer ID-token credential wrapping a source ADC
//! credential used to authenticate the impersonation call. That source credential represents the
//! process's own ambient identity, not any one key, so it is held once, process-wide, and shared
//! by every impersonated key rather than rebuilt per key (see [`Registry::ambient_source`]). Only
//! the outer credential is subject to per-key eviction.
//!
//! All credential construction executes on a small dedicated tokio runtime (see
//! [`build_auth_runtime`]) so refresh tasks live on a runtime with process lifetime rather than a
//! partition invoker runtime that can be torn down out from under them.

use std::fmt;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use async_trait::async_trait;
use moka::future::Cache;
use moka::ops::compute::Op;
use thiserror::Error;
use tokio::runtime::{Handle, Runtime};
use tokio::sync::{OnceCell, Semaphore};

#[cfg(any(test, feature = "test_util"))]
use ahash::HashMap;
#[cfg(any(test, feature = "test_util"))]
use parking_lot::Mutex;

/// Per-attempt timeout for an individual token mint call. Safe to drop on timeout: the refresh
/// task is owned by the cached credential and shared across callers, so a dropped read never
/// cancels an in-flight fetch.
const MINT_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);

/// A token with less time than this remaining is rejected rather than attached to a request. The
/// credential itself serves a token until its literal expiry (relying on its own ~4 minute early
/// refresh to stay ahead); this is Restate's margin of safety on top of that for the time an
/// in-flight request needs to complete.
const MIN_TOKEN_VALIDITY: Duration = Duration::from_secs(60);

/// Cache sizing is operator-controlled hygiene, not a security bound: keys exist only for
/// registered deployments.
const CACHE_MAX_CAPACITY: u64 = 1024;

/// An actively-used credential stays cached indefinitely; an idle one (deregistered deployment)
/// ages out after this long without a mint.
const CACHE_TIME_TO_IDLE: Duration = Duration::from_secs(3600);

/// moka evicts lazily, during cache operations, so a fully idle cache never expires anything on
/// its own. This interval drives eviction even with zero mint traffic.
const HOUSEKEEPING_INTERVAL: Duration = Duration::from_secs(300);

/// Bounds concurrent credential *construction* across distinct keys. Per-key single-flight (via
/// the moka cache) already collapses concurrent misses for the same key; this additionally caps
/// the many-new-keys storm that a mass of first-time mints (e.g. after a restart) could cause,
/// since each construction may block on ADC discovery / DNS.
const MAX_CONCURRENT_CONSTRUCTIONS: usize = 4;

const AUTH_RUNTIME_WORKER_THREADS: usize = 2;
const AUTH_RUNTIME_MAX_BLOCKING_THREADS: usize = 4;

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
struct CacheKey {
    impersonate: Option<String>,
    audience: String,
}

/// Error from an [`IdTokenSource`], carrying the transient/permanent classification that governs
/// whether a failed source is evicted from the registry (see [`Registry::evict_if_unchanged`]).
#[derive(Clone, Debug)]
struct SourceError {
    transient: bool,
    message: String,
}

impl SourceError {
    fn is_transient(&self) -> bool {
        self.transient
    }
}

impl fmt::Display for SourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for SourceError {}

impl From<google_cloud_auth::errors::CredentialsError> for SourceError {
    fn from(error: google_cloud_auth::errors::CredentialsError) -> Self {
        Self {
            transient: error.is_transient(),
            message: error.to_string(),
        }
    }
}

/// Internal seam that keeps the registry and the mint path testable without ADC or network:
/// `Live` wraps a real `IDTokenCredentials`, while tests inject mocks and a test-only `Seeded`
/// source (see [`GcpTokenClient::seed_for_test`]).
#[async_trait]
trait IdTokenSource: Send + Sync {
    async fn id_token(&self) -> Result<String, SourceError>;
}

struct Live(google_cloud_auth::credentials::idtoken::IDTokenCredentials);

#[async_trait]
impl IdTokenSource for Live {
    async fn id_token(&self) -> Result<String, SourceError> {
        self.0.id_token().await.map_err(SourceError::from)
    }
}

/// Builds the dedicated auth runtime. All credential construction executes here (see
/// [`Registry::get_or_build`]) so the refresh task a credential's `build()` spawns lives on a
/// runtime with process lifetime, never a partition invoker runtime that can be torn down while
/// the cached credential and its watch receivers survive.
fn build_auth_runtime() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(AUTH_RUNTIME_WORKER_THREADS)
        .max_blocking_threads(AUTH_RUNTIME_MAX_BLOCKING_THREADS)
        .thread_name("gcp-auth")
        .enable_all()
        .build()
        .expect("gcp auth runtime builds")
}

/// Process-global credential registry: the moka cache of credential objects, the dedicated auth
/// runtime construction executes on, and the semaphore bounding concurrent construction. Created
/// once per process; every [`GcpTokenClient`] is a handle to it. Per-invoker registries would
/// multiply key cardinality by partition count and tie credential lifecycles to invoker
/// lifecycles, which is exactly the leak this module exists to avoid.
struct Registry {
    cache: Cache<CacheKey, Arc<dyn IdTokenSource>>,
    // Kept alive for the process lifetime; never read directly (`handle` below is the clone used
    // to spawn work on it).
    _runtime: Runtime,
    handle: Handle,
    construction_permits: Semaphore,
    /// The process's shared ambient ADC identity, used as the source credential for every
    /// impersonated key. See [`Registry::ambient_source`] and the module docs.
    ambient_source: OnceCell<google_cloud_auth::credentials::Credentials>,
    #[cfg(any(test, feature = "test_util"))]
    test_hooks: TestHooks,
}

#[cfg(any(test, feature = "test_util"))]
type ConstructOverride =
    Arc<dyn Fn(&CacheKey) -> Result<Arc<dyn IdTokenSource>, GcpAuthError> + Send + Sync>;
#[cfg(any(test, feature = "test_util"))]
type AmbientSourceOverride =
    Arc<dyn Fn() -> Result<google_cloud_auth::credentials::Credentials, String> + Send + Sync>;

/// Test-only hooks that let unit tests drive the real moka-backed construction and eviction paths
/// deterministically, without touching ADC or the network. Never consulted in production builds.
/// `construct_overrides` is keyed by `CacheKey` so tests using distinct cache keys cannot
/// interfere with each other even if `cargo test` runs them concurrently in one process.
/// `ambient_source_override` is a single process-wide slot, matching the singleton it stands in
/// for — `nextest` (the primary test runner for this crate) isolates each test in its own
/// process, which is what makes sharing that slot safe in practice.
#[cfg(any(test, feature = "test_util"))]
#[derive(Default)]
struct TestHooks {
    construct_overrides: Mutex<HashMap<CacheKey, ConstructOverride>>,
    ambient_source_override: Mutex<Option<AmbientSourceOverride>>,
    ambient_source_builds: std::sync::atomic::AtomicUsize,
}

static REGISTRY: LazyLock<Registry> = LazyLock::new(Registry::init);

fn registry() -> &'static Registry {
    &REGISTRY
}

impl Registry {
    fn init() -> Self {
        let runtime = build_auth_runtime();
        let handle = runtime.handle().clone();
        let cache = Cache::builder()
            .max_capacity(CACHE_MAX_CAPACITY)
            .time_to_idle(CACHE_TIME_TO_IDLE)
            .build();

        // Spawned exactly once, here, rather than per `GcpTokenClient` (which is created per
        // invoker and would otherwise add a tick per partition).
        let housekeeping_cache = cache.clone();
        handle.spawn(async move {
            let mut interval = tokio::time::interval(HOUSEKEEPING_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                interval.tick().await;
                housekeeping_cache.run_pending_tasks().await;
            }
        });

        Self {
            cache,
            _runtime: runtime,
            handle,
            construction_permits: Semaphore::new(MAX_CONCURRENT_CONSTRUCTIONS),
            ambient_source: OnceCell::new(),
            #[cfg(any(test, feature = "test_util"))]
            test_hooks: TestHooks::default(),
        }
    }

    /// Returns the cached credential for `key`, building it on the auth runtime if this is the
    /// first mint for this key (or the previous credential was evicted). Concurrent misses for
    /// the same key coalesce into one build via the moka cache's single-flight semantics; waiters
    /// share the error on failure, and errors are not cached.
    async fn get_or_build(
        &'static self,
        key: CacheKey,
    ) -> Result<Arc<dyn IdTokenSource>, GcpAuthError> {
        #[cfg(any(test, feature = "test_util"))]
        let override_fn = self
            .test_hooks
            .construct_overrides
            .lock()
            .get(&key)
            .cloned();

        let init_key = key.clone();
        self.cache
            .try_get_with(key, async move {
                let _permit = self
                    .construction_permits
                    .acquire()
                    .await
                    .expect("construction semaphore is never closed");

                #[cfg(any(test, feature = "test_util"))]
                if let Some(f) = override_fn {
                    return f(&init_key);
                }

                build_on_auth_runtime(self, init_key).await
            })
            .await
            .map_err(|error| (*error).clone())
    }

    /// Removes the cached entry for `key`, but only if it is still exactly `stale` — the
    /// credential that produced the permanent error being handled. This guards against the ABA
    /// race where a slow caller holding a since-replaced failed credential would otherwise evict
    /// a freshly rebuilt, healthy one.
    async fn evict_if_unchanged(&'static self, key: &CacheKey, stale: &Arc<dyn IdTokenSource>) {
        self.cache
            .entry(key.clone())
            .and_compute_with(|entry| {
                let op = match &entry {
                    Some(entry) if Arc::ptr_eq(entry.value(), stale) => Op::Remove,
                    _ => Op::Nop,
                };
                std::future::ready(op)
            })
            .await;
    }

    /// Returns the process's shared ambient source credential, building it on the auth runtime on
    /// first use. This credential represents the process's own ADC identity, not any particular
    /// mint key: every impersonated `(impersonate, audience)` key clones the same underlying actor
    /// rather than each spawning its own. A failed build is not cached — the next caller retries —
    /// but concurrent callers during a build share the one attempt, via `OnceCell`'s semaphore.
    async fn ambient_source(
        &'static self,
    ) -> Result<google_cloud_auth::credentials::Credentials, String> {
        #[cfg(any(test, feature = "test_util"))]
        let override_fn = self.test_hooks.ambient_source_override.lock().clone();

        self.ambient_source
            .get_or_try_init(|| async {
                let _permit = self
                    .construction_permits
                    .acquire()
                    .await
                    .expect("construction semaphore is never closed");

                #[cfg(any(test, feature = "test_util"))]
                {
                    self.test_hooks
                        .ambient_source_builds
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    if let Some(f) = override_fn {
                        return f();
                    }
                }

                build_ambient_source_on_auth_runtime(&self.handle).await
            })
            .await
            .cloned()
    }
}

/// Resolves the credential(s) needed for `key` and builds the outer ID-token credential on the
/// auth runtime. The impersonated arm first resolves the process-wide ambient source (see
/// [`Registry::ambient_source`]) before building its own outer credential, so a permanent failure
/// here can only ever strand the outer actor — the shared source actor is independent of any
/// single key and is evicted-then-rebuilt on its own failure/retry cycle.
async fn build_on_auth_runtime(
    registry: &'static Registry,
    key: CacheKey,
) -> Result<Arc<dyn IdTokenSource>, GcpAuthError> {
    let audience = key.audience.clone();

    let build: Box<dyn FnOnce() -> Result<Arc<dyn IdTokenSource>, GcpAuthError> + Send> =
        match key.impersonate.clone() {
            None => Box::new(move || build_ambient_credentials(key)),
            Some(sa) => {
                let source =
                    registry
                        .ambient_source()
                        .await
                        .map_err(|message| GcpAuthError::Adc {
                            audience: audience.clone(),
                            impersonate: sa.clone(),
                            message,
                        })?;
                Box::new(move || build_impersonated_credentials(key, sa, source))
            }
        };

    registry
        .handle
        .spawn_blocking(build)
        .await
        .unwrap_or_else(|join_error| {
            Err(GcpAuthError::Build {
                audience,
                message: format!("GCP auth runtime task failed: {join_error}"),
            })
        })
}

/// Runs inside `spawn_blocking` on the auth runtime: `build()` synchronously spawns the ambient
/// source credential's refresh task via `tokio::spawn` — which lands on the ambient runtime, i.e.
/// the auth runtime, because tokio propagates runtime context into `spawn_blocking` closures. ADC
/// discovery here is also blocking filesystem reads.
async fn build_ambient_source_on_auth_runtime(
    handle: &Handle,
) -> Result<google_cloud_auth::credentials::Credentials, String> {
    handle
        .spawn_blocking(|| {
            google_cloud_auth::credentials::Builder::default()
                .build()
                .map_err(|e| e.to_string())
        })
        .await
        .unwrap_or_else(|join_error| Err(format!("GCP auth runtime task failed: {join_error}")))
}

/// Builds the outer ID-token credential for an unimpersonated (ambient) key. Runs inside
/// `spawn_blocking` on the auth runtime — see [`build_on_auth_runtime`].
fn build_ambient_credentials(key: CacheKey) -> Result<Arc<dyn IdTokenSource>, GcpAuthError> {
    use google_cloud_auth::credentials::idtoken;

    let credentials = idtoken::Builder::new(key.audience.clone())
        .build()
        .map_err(|e| {
            // authorized_user (gcloud) and external_account (Workload Identity Federation) ADC
            // sources cannot mint ID tokens directly.
            if e.is_not_supported() {
                tracing::debug!(
                    audience = %key.audience,
                    error = %e,
                    "ambient ADC identity cannot mint an ID token; impersonation required"
                );
                GcpAuthError::AmbientUnsupported {
                    audience: key.audience.clone(),
                }
            } else {
                GcpAuthError::Build {
                    audience: key.audience.clone(),
                    message: e.to_string(),
                }
            }
        })?;
    Ok(Arc::new(Live(credentials)) as Arc<dyn IdTokenSource>)
}

/// Builds the outer ID-token credential for an impersonated key, from the process-wide shared
/// `source` credential. Runs inside `spawn_blocking` on the auth runtime — see
/// [`build_on_auth_runtime`].
fn build_impersonated_credentials(
    key: CacheKey,
    service_account: String,
    source: google_cloud_auth::credentials::Credentials,
) -> Result<Arc<dyn IdTokenSource>, GcpAuthError> {
    use google_cloud_auth::credentials::idtoken;

    let credentials = idtoken::impersonated::Builder::from_source_credentials(
        key.audience.clone(),
        service_account,
        source,
    )
    .build()
    .map_err(|e| GcpAuthError::Build {
        audience: key.audience.clone(),
        message: e.to_string(),
    })?;
    Ok(Arc::new(Live(credentials)) as Arc<dyn IdTokenSource>)
}

/// Outcome of fetching a token from an [`IdTokenSource`], before it is mapped to the public
/// [`GcpAuthError`]. Kept separate so the registry-backed mint path can inspect
/// [`SourceError::is_transient`] to decide on eviction without re-parsing the error message.
enum FetchError {
    Timeout,
    Source(SourceError),
    /// The source returned a token, but with less than [`MIN_TOKEN_VALIDITY`] remaining. Treated
    /// as transient: the credential is not at fault (it will refresh eventually) so the entry is
    /// never evicted for this reason.
    ShortValidity,
}

async fn mint_from_source(
    source: &dyn IdTokenSource,
    timeout: Duration,
) -> Result<String, FetchError> {
    let token = tokio::time::timeout(timeout, source.id_token())
        .await
        .map_err(|_| FetchError::Timeout)?
        .map_err(FetchError::Source)?;

    match parse_jwt_exp(&token) {
        Some(remaining) if remaining >= MIN_TOKEN_VALIDITY => Ok(token),
        _ => Err(FetchError::ShortValidity),
    }
}

fn to_auth_error(error: FetchError, audience: &str, impersonate: Option<&str>) -> GcpAuthError {
    let impersonate = impersonate.unwrap_or("(ambient)").to_owned();
    match error {
        FetchError::Timeout => GcpAuthError::Timeout {
            audience: audience.to_owned(),
            impersonate,
            duration: MINT_ATTEMPT_TIMEOUT,
        },
        FetchError::Source(source_error) => GcpAuthError::Mint {
            audience: audience.to_owned(),
            impersonate,
            message: source_error.to_string(),
        },
        FetchError::ShortValidity => GcpAuthError::Mint {
            audience: audience.to_owned(),
            impersonate,
            message: format!(
                "minted token has less than {}s of validity remaining",
                MIN_TOKEN_VALIDITY.as_secs()
            ),
        },
    }
}

/// Token-mint client: a cheap handle to the process-global credential [`Registry`]. Every
/// `ServiceClient` clone shares the same registry, so distinct GCP identities each own at most
/// one credential (and its refresh task) for the life of the process. Outside tests this carries
/// no state of its own: it is a stateless handle to `registry()`.
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
    test_sources: Mutex<HashMap<CacheKey, Arc<dyn IdTokenSource>>>,
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
        // Test-only short-circuit: when `force_mint_failure_for_test` has been called, every mint
        // returns an error. Used to verify that a mint failure does NOT trigger an
        // unauthenticated fallback request.
        #[cfg(any(test, feature = "test_util"))]
        if let Some(message) = self.inner.test_force_failure.lock().clone() {
            return Err(GcpAuthError::Mint {
                audience: audience.to_owned(),
                impersonate: impersonate_service_account
                    .unwrap_or("(ambient)")
                    .to_owned(),
                message,
            });
        }

        let key = CacheKey {
            impersonate: impersonate_service_account.map(str::to_owned),
            audience: audience.to_owned(),
        };

        #[cfg(any(test, feature = "test_util"))]
        let test_source = self.inner.test_sources.lock().get(&key).cloned();
        #[cfg(any(test, feature = "test_util"))]
        if let Some(source) = test_source {
            return mint_from_source(source.as_ref(), MINT_ATTEMPT_TIMEOUT)
                .await
                .map_err(|e| to_auth_error(e, audience, impersonate_service_account));
        }

        let source = registry().get_or_build(key.clone()).await?;
        match mint_from_source(source.as_ref(), MINT_ATTEMPT_TIMEOUT).await {
            Ok(token) => Ok(token),
            Err(error) => {
                // Transient failures need no handling from us: the credential's refresh loop
                // retries on its own cooldown and self-heals, so the entry stays cached. Permanent
                // failures can never recover, so evict — but only if the cache still holds the
                // exact credential that produced the error (see `evict_if_unchanged`).
                if let FetchError::Source(ref source_error) = error
                    && !source_error.is_transient()
                {
                    registry().evict_if_unchanged(&key, &source).await;
                }
                Err(to_auth_error(error, audience, impersonate_service_account))
            }
        }
    }

    /// Test-only: force every subsequent `mint` call to fail with the given message. Used to
    /// verify that a mint failure does NOT trigger an unauthenticated fallback request.
    #[cfg(any(test, feature = "test_util"))]
    pub fn force_mint_failure_for_test(&self, message: &str) {
        *self.inner.test_force_failure.lock() = Some(message.to_owned());
    }

    /// Test-only: seed a token so subsequent `mint` calls with the same key return it without
    /// contacting Google. Seeding is local to this `GcpTokenClient` instance, not the shared
    /// registry, so tests that construct multiple `ServiceClient`s in one process do not
    /// interfere with each other.
    #[cfg(any(test, feature = "test_util"))]
    pub fn seed_for_test(
        &self,
        impersonate: Option<&str>,
        audience: &str,
        token: String,
        _expires_in: Duration,
    ) {
        struct Seeded(String);

        #[async_trait]
        impl IdTokenSource for Seeded {
            async fn id_token(&self) -> Result<String, SourceError> {
                Ok(self.0.clone())
            }
        }

        let key = CacheKey {
            impersonate: impersonate.map(str::to_owned),
            audience: audience.to_owned(),
        };
        self.inner
            .test_sources
            .lock()
            .insert(key, Arc::new(Seeded(token)));
    }
}

impl Default for GcpTokenClient {
    fn default() -> Self {
        Self::new()
    }
}

/// Best-effort parse of a JWT's `exp` claim into a Duration-from-now. Returns None if the token is
/// malformed or already expired. Used only for the validity guard above — the credential itself
/// is the source of truth for when to refresh.
fn parse_jwt_exp(token: &str) -> Option<Duration> {
    use base64::Engine;

    let payload_b64 = token.split('.').nth(1)?;
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload_b64)
        .ok()?;
    let payload_json: serde_json::Value = serde_json::from_slice(&payload).ok()?;
    let exp = payload_json.get("exp")?.as_u64()?;

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_secs();

    if exp <= now {
        None
    } else {
        Some(Duration::from_secs(exp - now))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use base64::Engine as _;

    use super::*;

    fn synthesize_jwt(exp_offset: Duration) -> String {
        let exp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + exp_offset.as_secs();
        let header = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(br#"{"alg":"none","typ":"JWT"}"#);
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(format!(r#"{{"exp":{exp}}}"#).as_bytes());
        format!("{header}.{payload}.")
    }

    #[test]
    fn parse_jwt_exp_returns_some_for_valid_token() {
        let token = synthesize_jwt(Duration::from_secs(3600));
        let dur = parse_jwt_exp(&token).expect("expected Some");
        assert!(dur > Duration::from_secs(3500));
        assert!(dur <= Duration::from_secs(3600));
    }

    #[test]
    fn parse_jwt_exp_returns_none_for_expired() {
        let header = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(br#"{"alg":"none","typ":"JWT"}"#);
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(br#"{"exp":1}"#);
        let token = format!("{header}.{payload}.");
        assert!(parse_jwt_exp(&token).is_none());
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
        Error(SourceError),
        Pending,
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
        async fn id_token(&self) -> Result<String, SourceError> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            let outcome = (self.behavior.lock())(call);
            match outcome {
                MockOutcome::Token(token) => Ok(token),
                MockOutcome::Error(error) => Err(error),
                MockOutcome::Pending => std::future::pending().await,
            }
        }
    }

    fn transient_error(message: &str) -> SourceError {
        SourceError {
            transient: true,
            message: message.to_owned(),
        }
    }

    fn permanent_error(message: &str) -> SourceError {
        SourceError {
            transient: false,
            message: message.to_owned(),
        }
    }

    fn fresh_token() -> String {
        synthesize_jwt(Duration::from_secs(3600))
    }

    fn key(audience: &str) -> CacheKey {
        CacheKey {
            impersonate: None,
            audience: audience.to_owned(),
        }
    }

    /// Installs a per-key override that `get_or_build` consults instead of `build_on_auth_runtime`.
    /// Keying by `CacheKey` means tests using distinct cache keys cannot interfere with each other
    /// even if `cargo test` runs them concurrently in one process.
    fn install_construct_override(
        cache_key: CacheKey,
        f: impl Fn(&CacheKey) -> Result<Arc<dyn IdTokenSource>, GcpAuthError> + Send + Sync + 'static,
    ) {
        registry()
            .test_hooks
            .construct_overrides
            .lock()
            .insert(cache_key, Arc::new(f));
    }

    /// Installs the process-wide override that `Registry::ambient_source` consults instead of
    /// building real ADC credentials.
    fn install_ambient_source_override(
        f: impl Fn() -> Result<google_cloud_auth::credentials::Credentials, String>
        + Send
        + Sync
        + 'static,
    ) {
        *registry().test_hooks.ambient_source_override.lock() = Some(Arc::new(f));
    }

    #[tokio::test]
    async fn single_flight_builds_once_under_concurrent_misses() {
        let client = GcpTokenClient::new();
        let audience = "https://single-flight.example.com";
        let builds = Arc::new(AtomicUsize::new(0));

        install_construct_override(key(audience), {
            let builds = builds.clone();
            move |_| {
                builds.fetch_add(1, Ordering::SeqCst);
                Ok(MockSource::new(|_| MockOutcome::Token(fresh_token()))
                    as Arc<dyn IdTokenSource>)
            }
        });

        let results = futures::future::join_all((0..64).map(|_| client.mint(None, audience))).await;

        assert!(results.iter().all(|r| r.is_ok()), "{results:?}");
        assert_eq!(builds.load(Ordering::SeqCst), 1);
    }

    #[derive(Debug)]
    struct FakeCredentialsProvider;

    impl google_cloud_auth::credentials::CredentialsProvider for FakeCredentialsProvider {
        async fn headers(
            &self,
            _extensions: http::Extensions,
        ) -> std::result::Result<
            google_cloud_auth::credentials::CacheableResource<http::HeaderMap>,
            google_cloud_auth::errors::CredentialsError,
        > {
            Ok(google_cloud_auth::credentials::CacheableResource::New {
                entity_tag: google_cloud_auth::credentials::EntityTag::new(),
                data: http::HeaderMap::new(),
            })
        }

        async fn universe_domain(&self) -> Option<String> {
            None
        }
    }

    /// Pins P1's fix (restatedev/restate#5151): the impersonated arm's source ADC credential is a
    /// single process-wide actor, not one per key. `Registry::ambient_source` is exactly what
    /// `build_on_auth_runtime` calls before building any impersonated key's outer credential, so
    /// proving it single-flights and shares its result here is equivalent to proving that N
    /// concurrent impersonated constructions share one source build — without also leaving N real
    /// (if harmless) background refresh tasks running for their outer credentials, which
    /// `get_or_build` would otherwise spin up for the rest of the test process.
    ///
    /// Relies on `ambient_source` being uninitialized when this test starts, which holds under
    /// `nextest` (one process per test) as long as no other test in this file exercises
    /// impersonation for real; see `TestHooks`'s doc comment.
    #[tokio::test]
    async fn impersonated_constructions_share_one_ambient_source_build() {
        let build_count = Arc::new(AtomicUsize::new(0));
        install_ambient_source_override({
            let build_count = build_count.clone();
            move || {
                build_count.fetch_add(1, Ordering::SeqCst);
                Ok(google_cloud_auth::credentials::Credentials::from(
                    FakeCredentialsProvider,
                ))
            }
        });

        let results = futures::future::join_all((0..8).map(|_| registry().ambient_source())).await;

        assert!(results.iter().all(|r| r.is_ok()), "{results:?}");
        assert_eq!(build_count.load(Ordering::SeqCst), 1);
    }

    /// Real credentials can't be constructed in a unit test (no ADC, no network), so this
    /// exercises the same runtime-placement primitive `build_on_auth_runtime` relies on directly:
    /// a `tokio::spawn` performed inside a `spawn_blocking` closure lands on the runtime that
    /// owns the blocking pool (the auth runtime), not on the caller's runtime. A real
    /// credential's `build()` spawns its refresh task the same way, which is exactly why
    /// `Registry::get_or_build` routes construction through `handle.spawn_blocking` in the first
    /// place: the refresh task must survive a partition invoker runtime being torn down.
    #[test]
    fn auth_runtime_outlives_a_caller_runtime_that_constructed_on_it() {
        let handle = registry().handle.clone();
        let running = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let (tx, rx) = std::sync::mpsc::channel();

        {
            // Stands in for a partition invoker runtime that gets torn down after triggering a
            // credential build.
            let caller_runtime = tokio::runtime::Runtime::new().expect("caller runtime builds");
            let running = running.clone();
            caller_runtime.block_on(async move {
                handle
                    .spawn_blocking(move || {
                        tokio::spawn(async move {
                            while running.load(Ordering::SeqCst) {
                                tokio::time::sleep(Duration::from_millis(5)).await;
                            }
                            let _ = tx.send(());
                        });
                    })
                    .await
                    .expect("construction runs on the auth runtime");
            });
            // Dropping the caller runtime here. If the spawned task had landed on it instead of
            // the auth runtime, dropping it would abort the task and the channel send below would
            // never happen.
        }

        std::thread::sleep(Duration::from_millis(50));
        running.store(false, Ordering::SeqCst);
        rx.recv_timeout(Duration::from_secs(2))
            .expect("task spawned via the auth runtime survives the caller runtime's drop");
    }

    #[tokio::test]
    async fn transient_error_keeps_entry_and_self_heals() {
        let client = GcpTokenClient::new();
        let audience = "https://transient.example.com";
        let cache_key = key(audience);
        let source = MockSource::new(|call| {
            if call == 0 {
                MockOutcome::Error(transient_error("temporarily unavailable"))
            } else {
                MockOutcome::Token(fresh_token())
            }
        });
        let dyn_source: Arc<dyn IdTokenSource> = source.clone();
        registry()
            .cache
            .insert(cache_key.clone(), dyn_source.clone())
            .await;

        let first = client.mint(None, audience).await;
        assert!(matches!(first, Err(GcpAuthError::Mint { .. })), "{first:?}");

        // The entry must still be present and unchanged (no eviction on transient failure).
        let still_cached = registry().cache.get(&cache_key).await;
        assert!(matches!(still_cached, Some(s) if Arc::ptr_eq(&s, &dyn_source)));

        // The mock "self-heals" on the next call, as a real credential's refresh loop would.
        let second = client.mint(None, audience).await;
        assert!(second.is_ok(), "{second:?}");
    }

    #[tokio::test]
    async fn permanent_error_evicts_conditionally() {
        let client = GcpTokenClient::new();
        let audience = "https://permanent.example.com";
        let cache_key = key(audience);
        let source: Arc<dyn IdTokenSource> =
            MockSource::new(|_| MockOutcome::Error(permanent_error("misconfigured")));
        registry()
            .cache
            .insert(cache_key.clone(), source.clone())
            .await;

        let outcome = client.mint(None, audience).await;
        assert!(
            matches!(outcome, Err(GcpAuthError::Mint { .. })),
            "{outcome:?}"
        );

        assert!(registry().cache.get(&cache_key).await.is_none());
    }

    #[tokio::test]
    async fn aba_race_stale_caller_evict_is_a_no_op() {
        let client = GcpTokenClient::new();
        let audience = "https://aba.example.com";
        let cache_key = key(audience);
        let new_source: Arc<dyn IdTokenSource> =
            MockSource::new(|_| MockOutcome::Token(fresh_token()));

        /// Simulates a concurrent rebuild completing — replacing this source in the registry
        /// cache with `replacement` — before this permanently-failing source's own error is
        /// reported back to `mint()`. Driving the swap from inside `id_token()` exercises the
        /// exact race `evict_if_unchanged`'s compare-and-evict guards against, through the real
        /// `mint()` call path rather than by hand-simulating the two steps independently.
        struct SwapThenFail {
            key: CacheKey,
            replacement: Arc<dyn IdTokenSource>,
        }

        #[async_trait]
        impl IdTokenSource for SwapThenFail {
            async fn id_token(&self) -> Result<String, SourceError> {
                registry()
                    .cache
                    .insert(self.key.clone(), self.replacement.clone())
                    .await;
                Err(permanent_error("old, now gone"))
            }
        }

        let old_source: Arc<dyn IdTokenSource> = Arc::new(SwapThenFail {
            key: cache_key.clone(),
            replacement: new_source.clone(),
        });
        registry()
            .cache
            .insert(cache_key.clone(), old_source.clone())
            .await;

        let outcome = client.mint(None, audience).await;
        assert!(
            matches!(outcome, Err(GcpAuthError::Mint { .. })),
            "{outcome:?}"
        );

        let cached = registry().cache.get(&cache_key).await;
        assert!(
            matches!(cached, Some(s) if Arc::ptr_eq(&s, &new_source)),
            "evict from a stale caller must not remove the freshly rebuilt healthy entry"
        );
    }

    #[tokio::test]
    async fn validity_guard_rejects_tokens_with_short_remaining_validity() {
        let source: Arc<dyn IdTokenSource> =
            MockSource::new(|_| MockOutcome::Token(synthesize_jwt(Duration::from_secs(30))));
        let outcome = mint_from_source(source.as_ref(), Duration::from_secs(1)).await;
        assert!(matches!(outcome, Err(FetchError::ShortValidity)));
    }

    #[tokio::test]
    async fn timeout_leaves_the_shared_source_usable_by_other_callers() {
        let source = MockSource::new(|call| {
            if call == 0 {
                MockOutcome::Pending
            } else {
                MockOutcome::Token(fresh_token())
            }
        });

        let timed_out = mint_from_source(source.as_ref(), Duration::from_millis(50)).await;
        assert!(matches!(timed_out, Err(FetchError::Timeout)));

        // Dropping a timed-out read must not have torn down the mock's state; another caller
        // sharing the same source can still mint successfully.
        let recovered = mint_from_source(source.as_ref(), Duration::from_secs(1)).await;
        assert!(recovered.is_ok());
    }

    #[tokio::test]
    async fn seam_isolation_two_instances_do_not_share_overlay_state() {
        let seeded_client = GcpTokenClient::new();
        let failing_client = GcpTokenClient::new();
        let audience = "https://isolation.example.com";
        let token = fresh_token();

        seeded_client.seed_for_test(None, audience, token.clone(), Duration::from_secs(3600));
        failing_client.force_mint_failure_for_test("independent client fails independently");

        assert_eq!(
            seeded_client.mint(None, audience).await.expect("seeded"),
            token
        );
        assert!(failing_client.mint(None, audience).await.is_err());

        // Isolation is structural, not just a coincidence of behavior: the seed lives only on the
        // instance it was set on.
        assert!(
            seeded_client
                .inner
                .test_sources
                .lock()
                .contains_key(&key(audience))
        );
        assert!(failing_client.inner.test_sources.lock().is_empty());
    }
}
