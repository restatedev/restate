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
//! This module mirrors the cache-mode pattern of [`crate::lambda`]:
//! `None` mode on the admin/discovery path (no per-key client retention)
//! and `Unbounded` mode on the worker/invoker path (per-key caching for
//! amortized mint cost).

use std::sync::Arc;
use std::time::Duration;

use ahash::HashMap;
use arc_swap::ArcSwap;
use thiserror::Error;
use tokio::time::Instant;

#[cfg(any(test, feature = "test_util"))]
use parking_lot::Mutex;

/// Skew applied to token expiry timestamps before treating a cached token as stale. 60 seconds
/// gives plenty of room for in-flight requests to complete using a token close to expiry.
const CACHE_EVICTION_SKEW: Duration = Duration::from_secs(60);

/// Per-attempt timeout for an individual token mint call.
const MINT_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);

/// Cache mode for minted ID tokens, similar to Lambda `AssumeRoleCacheMode`.
#[derive(Debug, Clone, Copy)]
pub enum IdTokenCacheMode {
    None,
    /// Cache per (impersonation target, audience) tuple, unbounded. Used on worker/invoker dispatch
    /// where deployments are already admitted and high invocation rates make caching beneficial.
    Unbounded,
}

#[derive(Debug, Error)]
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

#[derive(Clone, Hash, PartialEq, Eq)]
struct CacheKey {
    impersonate: Option<String>,
    audience: String,
}

struct CachedToken {
    token: String,
    expires_at: Instant,
}

impl CachedToken {
    fn is_fresh(&self, now: Instant) -> bool {
        now + CACHE_EVICTION_SKEW < self.expires_at
    }
}

/// Token-mint client. Wraps the google-cloud-auth crate with per-call caching keyed by
/// `(impersonate_service_account, audience)`.
#[derive(Clone)]
pub struct GcpTokenClient {
    inner: Arc<Inner>,
}

struct Inner {
    cache: Option<ArcSwap<HashMap<CacheKey, Arc<CachedToken>>>>,

    #[cfg(any(test, feature = "test_util"))]
    test_force_failure: Mutex<Option<String>>,
}

impl GcpTokenClient {
    pub fn new(cache_mode: IdTokenCacheMode) -> Self {
        let cache = match cache_mode {
            IdTokenCacheMode::Unbounded => Some(ArcSwap::from_pointee(HashMap::default())),
            IdTokenCacheMode::None => None,
        };
        Self {
            inner: Arc::new(Inner {
                cache,
                #[cfg(any(test, feature = "test_util"))]
                test_force_failure: Mutex::new(None),
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
        // Test-only short-circuit: when `force_mint_failure_for_test`
        // has been called, every mint returns an error. Used to verify
        // that a mint failure does NOT trigger an unauthenticated
        // fallback request.
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

        if let Some(cache) = &self.inner.cache {
            let snapshot = cache.load();
            if let Some(cached) = snapshot.get(&key)
                && cached.is_fresh(Instant::now())
            {
                return Ok(cached.token.clone());
            }
        }

        let mint_fut = self.mint_via_sdk(impersonate_service_account, audience);
        let token = tokio::time::timeout(MINT_ATTEMPT_TIMEOUT, mint_fut)
            .await
            .map_err(|_| GcpAuthError::Timeout {
                audience: audience.to_owned(),
                impersonate: impersonate_service_account
                    .unwrap_or("(ambient)")
                    .to_owned(),
                duration: MINT_ATTEMPT_TIMEOUT,
            })??;

        if let Some(cache) = &self.inner.cache {
            // The google-cloud-auth API exposes only the token string, not its expiry. JWT parsing
            // of the `exp` claim is the reliable source.
            let expires_at = parse_jwt_exp(&token)
                .map(|exp| Instant::now() + exp)
                .unwrap_or_else(|| Instant::now() + Duration::from_secs(3600));

            // Use ArcSwap rcu to avoid races on parallel inserts.
            cache.rcu(|prev| {
                let mut next = (**prev).clone();
                next.insert(
                    key.clone(),
                    Arc::new(CachedToken {
                        token: token.clone(),
                        expires_at,
                    }),
                );
                next
            });
        }

        Ok(token)
    }

    async fn mint_via_sdk(
        &self,
        impersonate: Option<&str>,
        audience: &str,
    ) -> Result<String, GcpAuthError> {
        use google_cloud_auth::credentials::idtoken;

        // google-cloud-auth's Builder is internally thread-safe and ADC
        // discovery is a set of idempotent filesystem reads; no
        // application-level locking is needed around concurrent first
        // use. On cache miss two callers may mint in parallel for the
        // same key; that's accepted (see CACHE_EVICTION_SKEW notes).
        match impersonate {
            None => {
                let creds = idtoken::Builder::new(audience.to_owned())
                    .build()
                    .map_err(|e| {
                        // authorized_user (gcloud) and external_account (Workload Identity
                        // Federation) ADC sources cannot mint ID tokens directly
                        if e.is_not_supported() {
                            tracing::debug!(
                                audience = %audience,
                                error = %e,
                                "ambient ADC identity cannot mint an ID token; impersonation required"
                            );
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
                creds.id_token().await.map_err(|e| GcpAuthError::Mint {
                    audience: audience.to_owned(),
                    impersonate: String::from("(ambient)"),
                    message: e.to_string(),
                })
            }
            Some(sa) => {
                // Impersonation path: load ADC as the source credentials,
                // then build the impersonated ID token builder.
                let source = google_cloud_auth::credentials::Builder::default()
                    .build()
                    .map_err(|e| GcpAuthError::Adc {
                        audience: audience.to_owned(),
                        impersonate: sa.to_owned(),
                        message: e.to_string(),
                    })?;
                let creds = idtoken::impersonated::Builder::from_source_credentials(
                    audience.to_owned(),
                    sa.to_owned(),
                    source,
                )
                .build()
                .map_err(|e| GcpAuthError::Build {
                    audience: audience.to_owned(),
                    message: e.to_string(),
                })?;
                creds.id_token().await.map_err(|e| GcpAuthError::Mint {
                    audience: audience.to_owned(),
                    impersonate: sa.to_owned(),
                    message: e.to_string(),
                })
            }
        }
    }

    /// Test-only: force every subsequent `mint` call to fail with the given message. Used to verify
    /// the no-unauthenticated-fallback behaviour.
    #[cfg(any(test, feature = "test_util"))]
    pub fn force_mint_failure_for_test(&self, message: &str) {
        *self.inner.test_force_failure.lock() = Some(message.to_owned());
    }

    /// Test-only: seed a token into the cache so subsequent `mint` calls with the same key return
    /// it without contacting Google.
    #[cfg(any(test, feature = "test_util"))]
    pub fn seed_for_test(
        &self,
        impersonate: Option<&str>,
        audience: &str,
        token: String,
        expires_in: Duration,
    ) {
        let cache = self
            .inner
            .cache
            .as_ref()
            .expect("seed_for_test requires Unbounded cache mode");
        let key = CacheKey {
            impersonate: impersonate.map(str::to_owned),
            audience: audience.to_owned(),
        };
        let entry = Arc::new(CachedToken {
            token,
            expires_at: Instant::now() + expires_in,
        });
        cache.rcu(|prev| {
            let mut next = (**prev).clone();
            next.insert(key.clone(), entry.clone());
            next
        });
    }
}

/// Best-effort parse of a JWT's `exp` claim into a Duration-from-now. Returns None if the token is
/// malformed or already expired.
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
    use super::*;
    use base64::Engine as _;

    #[test]
    fn parse_jwt_exp_returns_some_for_valid_token() {
        // Synthesize a JWT with exp 1 hour in the future.
        let future = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600;
        let header = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(br#"{"alg":"none","typ":"JWT"}"#);
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(format!(r#"{{"exp":{future}}}"#).as_bytes());
        let token = format!("{header}.{payload}.");
        let dur = parse_jwt_exp(&token).expect("expected Some");
        assert!(dur > Duration::from_secs(3500));
        assert!(dur <= Duration::from_secs(3600));
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

    #[test]
    fn parse_jwt_exp_returns_none_for_expired() {
        let past = 1u64;
        let header = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(br#"{"alg":"none","typ":"JWT"}"#);
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(format!(r#"{{"exp":{past}}}"#).as_bytes());
        let token = format!("{header}.{payload}.");
        assert!(parse_jwt_exp(&token).is_none());
    }
}
