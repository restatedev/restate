// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! SASL/OAUTHBEARER token generation for Kafka consumers.
//!
//! Currently supported providers:
//! - `msk-iam`: AWS MSK IAM authentication using SIGv4 signed tokens.
//!
//! Configuration format (sasl.oauthbearer.config):
//! - `provider=msk-iam,region=us-east-1`
//! - `provider=msk-iam,region=us-east-1,profile=my-profile`
//!
//! This module is always compiled in. Whether Restate's own OAUTHBEARER token
//! callback is installed is decided at runtime, per subscription: only when the
//! config resolves to a Restate-managed provider (currently `msk-iam`) does the
//! consumer use the `MskOAuth` policy, which enables the custom callback via
//! `ClientContext::ENABLE_REFRESH_OAUTH_TOKEN = true`. Every other case (no
//! `provider`, or an OAUTHBEARER mechanism handled by librdkafka itself such as
//! Confluent OIDC) uses the plain context and leaves librdkafka's built-in
//! OAUTHBEARER handling untouched. See [`is_msk_iam_config`].
//!
//! The provider-based dispatch is designed to be extensible for other
//! OAUTHBEARER mechanisms in the future (e.g., Confluent Cloud, Azure Event Hubs).

use std::time::Duration;

use aws_config::{BehaviorVersion, Region};
use tracing::debug;
use tracing::warn;

/// The default AWS region used when `region` is not specified in the config.
const DEFAULT_REGION: &str = "us-east-1";

/// The only OAUTHBEARER provider currently supported.
const PROVIDER_MSK_IAM: &str = "msk-iam";

/// Maximum time to wait for MSK IAM token generation before giving up, so a hung
/// AWS/STS endpoint can never wedge consumer creation or node startup.
const MSK_TOKEN_TIMEOUT: Duration = Duration::from_secs(10);

/// Errors that can occur while generating a SASL/OAUTHBEARER token.
#[derive(Debug, thiserror::Error)]
pub(crate) enum OAuthError {
    #[error(
        "Missing 'provider' in sasl.oauthbearer.config. Configure with: \
         sasl.oauthbearer.config = \"provider={PROVIDER_MSK_IAM},region={DEFAULT_REGION}\""
    )]
    MissingProvider,
    #[error(
        "Unknown OAUTHBEARER provider '{0}'. Supported providers: {PROVIDER_MSK_IAM}. \
         Configure with: sasl.oauthbearer.config = \
         \"provider={PROVIDER_MSK_IAM},region={DEFAULT_REGION}\""
    )]
    UnknownProvider(String),
    #[error("MSK IAM token generation thread panicked")]
    ThreadPanicked,
    #[error("no credentials provider available for the configured AWS profile")]
    NoCredentialsProvider,
    #[error("failed to generate AWS MSK IAM token: {0}")]
    TokenGeneration(String),
    #[error("timed out generating AWS MSK IAM token")]
    TokenTimeout,
}

/// Configuration for OAUTHBEARER authentication parsed from sasl.oauthbearer.config
#[derive(Debug, Clone, PartialEq, Eq)]
struct OAuthBearerConfig {
    provider: Option<String>,
    region: String,
    profile: Option<String>,
}

/// Parse OAUTHBEARER configuration from config string.
/// Format: "provider=msk-iam,region=us-east-1,profile=my-profile"
///
/// Unknown keys are ignored for forward compatibility. The `region` field
/// defaults to [`DEFAULT_REGION`] when not specified to preserve backward
/// compatibility with any existing configurations. When a key appears more
/// than once, the last occurrence wins.
fn parse_oauthbearer_config(config: &str) -> OAuthBearerConfig {
    let mut provider = None;
    let mut region = DEFAULT_REGION.to_string();
    let mut profile = None;

    for part in config.split(',') {
        if let Some((key, value)) = part.split_once('=') {
            let value = value.trim().to_string();
            match key.trim() {
                "provider" => provider = Some(value),
                "region" => region = value,
                "profile" => profile = Some(value),
                _ => {} // Ignore unknown keys for forward compatibility
            }
        }
    }

    OAuthBearerConfig {
        provider,
        region,
        profile,
    }
}

/// The resolved provider to use for token generation.
///
/// Extracted as a pure, side-effect-free step so the dispatch/validation logic
/// can be unit-tested without a tokio runtime or any AWS calls.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ResolvedProvider {
    MskIam {
        region: String,
        profile: Option<String>,
    },
}

/// Validate the parsed config and resolve which provider to use.
///
/// Returns an [`OAuthError`] when the provider is missing or unsupported.
fn resolve_provider(config: OAuthBearerConfig) -> Result<ResolvedProvider, OAuthError> {
    match config.provider.as_deref() {
        Some(PROVIDER_MSK_IAM) => Ok(ResolvedProvider::MskIam {
            region: config.region,
            profile: config.profile,
        }),
        Some(other) => Err(OAuthError::UnknownProvider(other.to_string())),
        None => Err(OAuthError::MissingProvider),
    }
}

/// Returns `true` if the given `sasl.oauthbearer.config` selects a
/// Restate-managed OAUTHBEARER provider (currently only `msk-iam`).
///
/// This is the runtime switch used at consumer-creation time to decide between
/// the `MskOAuth` policy (which installs Restate's custom
/// token callback) and the plain policy (which leaves librdkafka's built-in
/// OAUTHBEARER handling in place, e.g. for Confluent OIDC).
///
/// It deliberately mirrors [`resolve_provider`] so the two never drift: a config
/// is "MSK IAM" exactly when [`resolve_provider`] would resolve it to
/// [`ResolvedProvider::MskIam`]. A missing or unknown provider returns `false`
/// here; the resulting error is surfaced later by [`generate_oauth_token`] only
/// if librdkafka actually asks for a token.
pub(crate) fn is_msk_iam_config(oauthbearer_config: Option<&str>) -> bool {
    let config = parse_oauthbearer_config(oauthbearer_config.unwrap_or(""));
    matches!(resolve_provider(config), Ok(ResolvedProvider::MskIam { .. }))
}

/// Generate an OAuth token for SASL/OAUTHBEARER authentication.
///
/// This is intended to be called from `ClientContext::generate_oauth_token`,
/// which librdkafka invokes only when `sasl.mechanisms=OAUTHBEARER` is configured.
/// Consumers using other SASL mechanisms (PLAIN, SCRAM, etc.) or no auth at all
/// are unaffected.
///
/// # Threading
///
/// This callback can be invoked from two contexts:
/// 1. During consumer creation - on the thread driving consumer setup
/// 2. During token refresh - from librdkafka's background polling thread
///
/// It spawns a fresh OS thread that builds its **own** current-thread tokio
/// runtime and drives the async AWS SDK call on it. Using a dedicated runtime
/// (rather than the shared node runtime's `Handle`) is essential: on the shared
/// multi-thread runtime, `block_on` needs a worker thread to advance the I/O
/// reactor, but during consumer creation the workers can all be blocked - so the
/// AWS HTTP I/O would never be driven and the node deadlocks (observed as a hang
/// right after "Generating OAUTHBEARER token" that wedges startup). A
/// current-thread runtime drives its own I/O on this OS thread, so it cannot be
/// starved. The call is bounded by [`MSK_TOKEN_TIMEOUT`] so a hung AWS/STS
/// endpoint returns an error (librdkafka just retries) instead of wedging the
/// node forever.
///
/// Token refresh occurs roughly every 10-15 minutes for MSK IAM, so the cost of a
/// short-lived OS thread + runtime per refresh is negligible.
pub(crate) fn generate_oauth_token(
    oauthbearer_config: Option<&str>,
) -> Result<rdkafka::client::OAuthToken, Box<dyn std::error::Error + 'static>> {
    let config_str = oauthbearer_config.unwrap_or("");
    let config = parse_oauthbearer_config(config_str);

    match resolve_provider(config)? {
        ResolvedProvider::MskIam { region, profile } => {
            debug!(
                provider = PROVIDER_MSK_IAM,
                region = %region,
                profile = ?profile,
                "Generating OAUTHBEARER token"
            );

            let thread_result = std::thread::spawn(
                move || -> Result<rdkafka::client::OAuthToken, OAuthError> {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .map_err(|e| OAuthError::TokenGeneration(e.to_string()))?;

                    rt.block_on(async {
                        match tokio::time::timeout(
                            MSK_TOKEN_TIMEOUT,
                            generate_msk_token_async(region, profile),
                        )
                        .await
                        {
                            Ok(result) => result,
                            Err(_elapsed) => Err(OAuthError::TokenTimeout),
                        }
                    })
                },
            )
            .join();

            match thread_result {
                Ok(result) => result.map_err(Into::into),
                Err(_) => Err(OAuthError::ThreadPanicked.into()),
            }
        }
    }
}

/// Async implementation of MSK IAM token generation.
async fn generate_msk_token_async(
    region: String,
    profile: Option<String>,
) -> Result<rdkafka::client::OAuthToken, OAuthError> {
    let aws_region = Region::new(region);

    let token_result = if let Some(profile) = profile {
        let sdk_config = aws_config::defaults(BehaviorVersion::latest())
            .profile_name(&profile)
            .region(aws_region.clone())
            .load()
            .await;

        match sdk_config.credentials_provider() {
            Some(credentials_provider) => {
                aws_msk_iam_sasl_signer::generate_auth_token_from_credentials_provider(
                    aws_region,
                    credentials_provider,
                )
                .await
            }
            None => return Err(OAuthError::NoCredentialsProvider),
        }
    } else {
        // Default credential chain: IRSA, instance profiles, env vars, etc.
        aws_msk_iam_sasl_signer::generate_auth_token(aws_region).await
    };

    match token_result {
        Ok((token, expiry)) => {
            debug!(expiry_ms = expiry, "MSK IAM token generated successfully");
            Ok(rdkafka::client::OAuthToken {
                token,
                // MSK does not require a principal_name claim; brokers that do
                // would need to surface it from the IAM context here.
                principal_name: String::new(),
                lifetime_ms: expiry,
            })
        }
        Err(e) => {
            warn!(error = %e, "Failed to generate AWS MSK IAM token");
            Err(OAuthError::TokenGeneration(e.to_string()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- parse_oauthbearer_config -------------------------------------------------

    #[test]
    fn parses_full_config() {
        let cfg = parse_oauthbearer_config("provider=msk-iam,region=us-west-2,profile=dev");
        assert_eq!(cfg.provider.as_deref(), Some("msk-iam"));
        assert_eq!(cfg.region, "us-west-2");
        assert_eq!(cfg.profile.as_deref(), Some("dev"));
    }

    #[test]
    fn defaults_region_when_omitted() {
        let cfg = parse_oauthbearer_config("provider=msk-iam");
        assert_eq!(cfg.provider.as_deref(), Some("msk-iam"));
        assert_eq!(cfg.region, DEFAULT_REGION);
        assert!(cfg.profile.is_none());
    }

    #[test]
    fn empty_string_yields_no_provider() {
        let cfg = parse_oauthbearer_config("");
        assert!(cfg.provider.is_none());
        assert_eq!(cfg.region, DEFAULT_REGION);
        assert!(cfg.profile.is_none());
    }

    #[test]
    fn ignores_unknown_keys() {
        let cfg = parse_oauthbearer_config("provider=msk-iam,region=eu-central-1,futurething=on");
        assert_eq!(cfg.provider.as_deref(), Some("msk-iam"));
        assert_eq!(cfg.region, "eu-central-1");
        assert!(cfg.profile.is_none());
    }

    #[test]
    fn tolerates_whitespace() {
        let cfg = parse_oauthbearer_config(" provider = msk-iam , region = ap-south-1 ");
        assert_eq!(cfg.provider.as_deref(), Some("msk-iam"));
        assert_eq!(cfg.region, "ap-south-1");
    }

    #[test]
    fn segments_without_equals_are_skipped() {
        let cfg = parse_oauthbearer_config("provider=msk-iam,garbage,region=us-east-2");
        assert_eq!(cfg.provider.as_deref(), Some("msk-iam"));
        assert_eq!(cfg.region, "us-east-2");
    }

    #[test]
    fn last_duplicate_key_wins() {
        let cfg = parse_oauthbearer_config("region=us-east-1,region=us-west-1,provider=msk-iam");
        assert_eq!(cfg.region, "us-west-1");
        assert_eq!(cfg.provider.as_deref(), Some("msk-iam"));
    }

    #[test]
    fn empty_value_is_preserved() {
        // A key with an empty value should still be recorded (not defaulted).
        let cfg = parse_oauthbearer_config("provider=msk-iam,region=,profile=");
        assert_eq!(cfg.region, "");
        assert_eq!(cfg.profile.as_deref(), Some(""));
    }

    #[test]
    fn provider_key_is_case_sensitive() {
        // Keys must match exactly; "Provider" is not "provider".
        let cfg = parse_oauthbearer_config("Provider=msk-iam");
        assert!(cfg.provider.is_none());
    }

    #[test]
    fn value_with_internal_equals_keeps_remainder() {
        // split_once on first '=' means the value may itself contain '='.
        let cfg = parse_oauthbearer_config("profile=a=b,provider=msk-iam");
        assert_eq!(cfg.profile.as_deref(), Some("a=b"));
        assert_eq!(cfg.provider.as_deref(), Some("msk-iam"));
    }

    // ---- resolve_provider ---------------------------------------------------------

    #[test]
    fn resolves_msk_iam_with_region_and_profile() {
        let cfg = parse_oauthbearer_config("provider=msk-iam,region=us-west-2,profile=dev");
        let resolved = resolve_provider(cfg).expect("should resolve msk-iam");
        assert_eq!(
            resolved,
            ResolvedProvider::MskIam {
                region: "us-west-2".to_string(),
                profile: Some("dev".to_string()),
            }
        );
    }

    #[test]
    fn resolves_msk_iam_with_default_region_and_no_profile() {
        let cfg = parse_oauthbearer_config("provider=msk-iam");
        let resolved = resolve_provider(cfg).expect("should resolve msk-iam");
        assert_eq!(
            resolved,
            ResolvedProvider::MskIam {
                region: DEFAULT_REGION.to_string(),
                profile: None,
            }
        );
    }

    #[test]
    fn resolve_missing_provider_errors_with_guidance() {
        let cfg = parse_oauthbearer_config("region=us-east-1");
        let err = resolve_provider(cfg).expect_err("expected missing-provider error");
        assert!(matches!(err, OAuthError::MissingProvider));
        assert!(err.to_string().contains("Missing 'provider'"));
        assert!(err.to_string().contains(PROVIDER_MSK_IAM));
    }

    #[test]
    fn resolve_unknown_provider_errors_with_offending_name() {
        let cfg = parse_oauthbearer_config("provider=azure-eh");
        let err = resolve_provider(cfg).expect_err("expected unknown-provider error");
        assert!(matches!(err, OAuthError::UnknownProvider(ref p) if p == "azure-eh"));
        assert!(err.to_string().contains("azure-eh"));
        assert!(err.to_string().contains(PROVIDER_MSK_IAM));
    }

    // ---- generate_oauth_token (error paths, no AWS calls) -------------------------

    #[test]
    fn missing_provider_returns_error() {
        let result = generate_oauth_token(Some("region=us-east-1"));

        let err = result.err().unwrap();
        assert!(err.to_string().contains("Missing 'provider'"), "got: {err}");
    }

    #[test]
    fn unknown_provider_returns_error() {
        let result = generate_oauth_token(Some("provider=azure-eh"));
        let err = result.err().unwrap();
        assert!(err.to_string().contains("azure-eh"), "got: {err}");
        assert!(err.to_string().contains("msk-iam"), "got: {err}");
    }

    #[test]
    fn none_config_returns_missing_provider_error() {
        // No config at all is equivalent to an empty config: provider is missing.
        let result = generate_oauth_token(None);
        let err = result.err().unwrap();
        assert!(err.to_string().contains("Missing 'provider'"), "got: {err}");
    }
}
