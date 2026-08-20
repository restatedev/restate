// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! AWS -> GCP workload identity federation, for minting a Google ID token from AWS-hosted
//! Restate without any Google identity of its own.
//!
//! The trust chain (restate-cloud#1188):
//!
//! ```text
//! EKS Pod Identity
//!   -> sts:AssumeRole(shared broker role, RoleSessionName set by the operator)
//!   -> SigV4-signed GetCallerIdentity envelope (AIP-4117 aws4_request)
//!   -> Google STS token exchange at the customer's workload identity provider
//!   -> IAM Credentials generateIdToken, impersonating the customer's invocation service account
//! ```
//!
//! The broker role assumption (the first hop) is shared by every federated deployment in the
//! process: it is operator configuration ([`GcpFederationOptions`]), not tenant-controlled, and
//! multiplying it per deployment would multiply `sts:AssumeRole` traffic for no isolation gain.
//! Everything from the SigV4 envelope onward is built fresh per deployment, scoped by that
//! deployment's own `workload_identity_provider` and `impersonate_service_account`.

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use aws_config::BehaviorVersion;
use aws_config::sts::AssumeRoleProvider;
use aws_credential_types::Credentials as AwsCredentials;
use aws_credential_types::provider::{ProvideCredentials, SharedCredentialsProvider};
use aws_sigv4::http_request::{SignableBody, SignableRequest, SigningSettings, sign};
use aws_sigv4::sign::v4;
use google_cloud_auth::credentials::external_account::ProgrammaticBuilder;
use google_cloud_auth::credentials::idtoken;
use google_cloud_auth::credentials::subject_token::{
    Builder as SubjectTokenBuilder, SubjectToken, SubjectTokenProvider,
};
use google_cloud_auth::errors::SubjectTokenProviderError;
use tokio::sync::{Mutex, OnceCell};

use restate_types::config::GcpFederationOptions;

use super::{CacheKey, GcpAuthError, IdTokenSource, Live};

/// AWS subject-token type Google STS expects for a SigV4-signed `GetCallerIdentity` envelope.
const AWS4_SUBJECT_TOKEN_TYPE: &str = "urn:ietf:params:aws:token-type:aws4_request";
const GOOGLE_STS_TOKEN_URL: &str = "https://sts.googleapis.com/v1/token";

/// Refresh the cached broker session this far ahead of its literal expiry, so a deployment
/// refreshing its own credential never blocks on a concurrent broker refresh.
const BROKER_REFRESH_MARGIN: Duration = Duration::from_secs(300);

/// The process-wide `[gcp-federation]` config, installed once from `ServiceClient` construction
/// (see [`install_config`]). Unset means the operator never configured the block: every federated
/// construction then fails with a permanent, actionable [`GcpAuthError::Build`] rather than
/// falling back to an unauthenticated request.
static FEDERATION_CONFIG: std::sync::OnceLock<GcpFederationOptions> = std::sync::OnceLock::new();

/// Install the process-wide `[gcp-federation]` config. Every `ServiceClient` built from the same
/// `ServiceClientOptions` calls this: `None` is always a no-op; the first `Some` installs; an
/// identical `Some` re-install is a no-op; a *differing* `Some` is a startup error, since a broker
/// built from the first config may already be in use by in-flight federated mints.
///
/// Validates `config` before installing it, so an operator learns about a typo in
/// `broker-role-arn` or `session-name` at server startup rather than on the first federated
/// invocation -- config errors here are always permanent, unlike the runtime failures
/// `GcpAuthError` classifies as transient/permanent.
pub(crate) fn install_config(config: Option<GcpFederationOptions>) -> Result<(), String> {
    let Some(config) = config else {
        return Ok(());
    };
    validate_broker_role_arn(&config.broker_role_arn)?;
    validate_session_name(&config.session_name)?;

    match FEDERATION_CONFIG.get() {
        None => {
            let _ = FEDERATION_CONFIG.set(config);
            Ok(())
        }
        Some(installed) if *installed == config => Ok(()),
        Some(_) => Err(
            "a differing [gcp-federation] configuration is already installed for this process"
                .to_owned(),
        ),
    }
}

/// Validates `arn` against the shape of an AWS IAM role ARN Restate can assume:
/// `arn:aws[-\w]*:iam::<12-digit account id>:role/<name-or-path>`. Rejects anything else with an
/// actionable message rather than deferring the typo to the first `sts:AssumeRole` failure.
fn validate_broker_role_arn(arn: &str) -> Result<(), String> {
    let invalid = || {
        format!(
            "broker-role-arn '{arn}' is not a valid AWS IAM role ARN; expected the form              arn:aws:iam::<12-digit account id>:role/<role-name-or-path>"
        )
    };
    let parts: Vec<&str> = arn.split(':').collect();
    let [scheme, partition, service, region, account, resource] = parts.as_slice() else {
        return Err(invalid());
    };
    let partition_suffix_ok = partition.strip_prefix("aws").is_some_and(|suffix| {
        suffix
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    });
    if *scheme != "arn"
        || !partition_suffix_ok
        || *service != "iam"
        || !region.is_empty()
        || account.len() != 12
        || !account.bytes().all(|b| b.is_ascii_digit())
        || !resource.starts_with("role/")
        || resource.len() <= "role/".len()
    {
        return Err(invalid());
    }
    Ok(())
}

/// Validates `name` against AWS STS's `RoleSessionName` constraints: 2-64 characters from
/// `[\w+=,.@-]`. AWS itself enforces this at `sts:AssumeRole` time; validating it at config-install
/// time surfaces a typo at server startup instead of on the first federated mint attempt.
fn validate_session_name(name: &str) -> Result<(), String> {
    let len = name.chars().count();
    let chars_ok = name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || "_+=,.@-".contains(c));
    if !(2..=64).contains(&len) || !chars_ok {
        return Err(format!(
            "session-name '{name}' is not a valid AWS STS RoleSessionName; expected 2-64              characters from [A-Za-z0-9_+=,.@-]"
        ));
    }
    Ok(())
}

/// The shared AWS broker identity: one role assumption for the whole process, reused by every
/// federated [`AwsSubjectTokenProvider`]. Lazily constructed on the first federated construction.
/// `provider` is type-erased behind `SharedCredentialsProvider` (rather than the concrete
/// `AssumeRoleProvider`) so tests can wrap a fixed [`AwsCredentials`] value directly instead of
/// driving the real AWS SDK config/STS machinery.
struct Broker {
    /// Resolved once, from the AWS SDK default chain, at broker construction.
    region: String,
    provider: SharedCredentialsProvider,
    cached: Mutex<Option<AwsCredentials>>,
}

impl fmt::Debug for Broker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Broker")
            .field("region", &self.region)
            .finish_non_exhaustive()
    }
}

impl Broker {
    async fn init(config: &GcpFederationOptions) -> Result<Self, String> {
        let sdk_config = aws_config::load_defaults(BehaviorVersion::latest()).await;
        let region = sdk_config
            .region()
            .map(|region| region.to_string())
            .ok_or_else(|| {
                "no AWS region resolved from the SDK default chain; the region is required to \
                 sign the GetCallerIdentity subject token"
                    .to_owned()
            })?;
        let provider = AssumeRoleProvider::builder(config.broker_role_arn.clone())
            .configure(&sdk_config)
            .session_name(config.session_name.clone())
            .build()
            .await;
        Ok(Self {
            region,
            provider: SharedCredentialsProvider::new(provider),
            cached: Mutex::new(None),
        })
    }

    /// Returns the current broker session credentials, refreshing them via `sts:AssumeRole` if
    /// the cached session is absent or within [`BROKER_REFRESH_MARGIN`] of expiry. Shared across
    /// every federated deployment's `subject_token()` calls, so a fleet of federated deployments
    /// refreshing around the same time coalesces into the one `AssumeRole` call each needs rather
    /// than one per deployment.
    async fn credentials(&self) -> Result<AwsCredentials, FederationError> {
        let mut guard = self.cached.lock().await;
        if let Some(creds) = guard.as_ref() {
            let fresh_enough = creds
                .expiry()
                .is_none_or(|expiry| expiry > SystemTime::now() + BROKER_REFRESH_MARGIN);
            if fresh_enough {
                return Ok(creds.clone());
            }
        }
        let fresh = self
            .provider
            .provide_credentials()
            .await
            .map_err(|e| federation_error_from_assume_role_failure(&e))?;
        *guard = Some(fresh.clone());
        Ok(fresh)
    }
}

/// Classifies an `sts:AssumeRole` failure surfaced by `AssumeRoleProvider::provide_credentials()`.
/// Authorization failures (trust policy or IAM denying `sts:AssumeRole`) are permanent: no amount
/// of retrying fixes them without an operator changing IAM, and classifying them as transient
/// would have the refresh loop retry an `AccessDenied` forever instead of publishing a permanent
/// error and exiting -- which is what lets [`super::SourceSlot`]'s probe-and-replace recovery
/// rebuild the source once IAM is fixed. Everything else (network/dispatch/timeout/chain
/// resolution) genuinely can resolve on retry and stays transient.
fn federation_error_from_assume_role_failure(
    error: &aws_credential_types::provider::error::CredentialsError,
) -> FederationError {
    let message = format!("assuming the GCP workload identity federation broker role: {error}");
    let access_denied = matches!(
        assume_role_error_code(error),
        Some("AccessDenied" | "AccessDeniedException")
    );
    if access_denied {
        FederationError::permanent(message)
    } else {
        FederationError::transient(message)
    }
}

/// Walks `error`'s source chain for the `SdkError<AssumeRoleError>` `AssumeRoleProvider` wraps its
/// STS response errors in, and returns the STS wire error code from it, if any. `AssumeRole`'s
/// `AccessDenied` has no modeled exception variant in `AssumeRoleError`, so it always surfaces
/// through the catch-all `Unhandled` variant; `ProvideErrorMetadata::code()` still recovers the
/// wire error code for unmodeled errors, which is why this classifies on that structured field
/// rather than matching the human-readable `Display` text or the service-error enum's variants.
fn assume_role_error_code<'a>(error: &'a (dyn std::error::Error + 'static)) -> Option<&'a str> {
    use aws_sdk_sts::error::{ProvideErrorMetadata, SdkError};
    use aws_sdk_sts::operation::assume_role::AssumeRoleError;

    let mut cause = Some(error);
    while let Some(err) = cause {
        if let Some(sdk_error) = err.downcast_ref::<SdkError<AssumeRoleError>>() {
            return sdk_error.code();
        }
        cause = err.source();
    }
    None
}

static BROKER: OnceCell<Arc<Broker>> = OnceCell::const_new();

/// Returns the shared broker, constructing it on first use. Construction failure (missing
/// `[gcp-federation]` config, or no AWS region resolvable) is not cached:
/// [`OnceCell::get_or_try_init`] leaves the cell empty on `Err`, so the next attempt retries.
async fn broker() -> Result<Arc<Broker>, String> {
    BROKER
        .get_or_try_init(|| async {
            let Some(config) = FEDERATION_CONFIG.get() else {
                return Err(
                    "this deployment requests GCP workload identity federation, but the server \
                     has no [gcp-federation] configuration; set broker-role-arn and \
                     session-name to enable it"
                        .to_owned(),
                );
            };
            Broker::init(config).await.map(Arc::new)
        })
        .await
        .cloned()
}

/// Error from a step of the federation chain, carrying the transient/permanent classification
/// [`google_cloud_auth`]'s external-account refresh loop uses to decide whether to keep retrying.
#[derive(Debug)]
struct FederationError {
    transient: bool,
    message: String,
}

impl FederationError {
    fn transient(message: String) -> Self {
        Self {
            transient: true,
            message,
        }
    }

    fn permanent(message: String) -> Self {
        Self {
            transient: false,
            message,
        }
    }
}

impl fmt::Display for FederationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for FederationError {}

impl SubjectTokenProviderError for FederationError {
    fn is_transient(&self) -> bool {
        self.transient
    }
}

/// Supplies the AIP-4117 AWS subject token to `google-cloud-auth`'s external-account credential
/// on each refresh. Holds no AWS credential machinery of its own beyond a clone of the shared
/// [`Broker`]: no `AWS_*` environment reads or writes, no process-global AWS SDK state.
#[derive(Debug)]
struct AwsSubjectTokenProvider {
    broker: Arc<Broker>,
    /// The full resource name of the customer's workload identity provider. Doubles as the
    /// `x-goog-cloud-target-resource` header value that binds the signed envelope to this pool
    /// (see `build_subject_token`) and as the STS `audience` parameter.
    provider_resource: String,
}

impl SubjectTokenProvider for AwsSubjectTokenProvider {
    type Error = FederationError;

    async fn subject_token(&self) -> Result<SubjectToken, Self::Error> {
        let credentials = self.broker.credentials().await?;
        let envelope = build_subject_token(
            &credentials,
            &self.broker.region,
            &self.provider_resource,
            SystemTime::now(),
        )
        .map_err(|e| {
            FederationError::permanent(format!("signing the GetCallerIdentity subject token: {e}"))
        })?;

        // google-cloud-auth's built-in AWS credential source (`external_account_sources::aws_sourced`)
        // form-urlencodes the AIP-4117 JSON envelope before returning it as the subject token; the
        // STS token-exchange request then form-encodes the whole request body for transport, so the
        // envelope arrives at Google encoded exactly twice: once by us, once by the transport layer.
        // We match that convention here so the two encoding layers are exactly as Google expects,
        // pinned by `federation_tests::subject_token_matches_aws_sourced_encoding_convention`.
        let subject_token: String =
            url::form_urlencoded::byte_serialize(envelope.as_bytes()).collect();
        Ok(SubjectTokenBuilder::new(subject_token).build())
    }
}

/// Sign a `GetCallerIdentity` call with `credentials` and render the AIP-4117 subject-token
/// envelope. `target_resource` (the workload identity provider resource name) travels as the
/// `x-goog-cloud-target-resource` header and is bound inside `SignedHeaders`: that binding is
/// what stops a signed envelope minted for one workload identity pool from being replayed against
/// another.
fn build_subject_token(
    credentials: &AwsCredentials,
    region: &str,
    target_resource: &str,
    signing_time: SystemTime,
) -> Result<String, String> {
    let url =
        format!("https://sts.{region}.amazonaws.com/?Action=GetCallerIdentity&Version=2011-06-15");
    let host = format!("sts.{region}.amazonaws.com");

    let identity = credentials.clone().into();
    let params = v4::SigningParams::builder()
        .identity(&identity)
        .region(region)
        .name("sts")
        .time(signing_time)
        .settings(SigningSettings::default())
        .build()
        .map_err(|e| format!("building sigv4 signing params: {e}"))?;

    // Headers presented for signing; the signer canonicalises, so order does not matter here.
    let headers = vec![
        ("host", host.as_str()),
        ("x-goog-cloud-target-resource", target_resource),
    ];

    let signable =
        SignableRequest::new("POST", &url, headers.into_iter(), SignableBody::Bytes(b""))
            .map_err(|e| format!("building signable request: {e}"))?;

    let (instructions, _signature) = sign(signable, &params.into())
        .map_err(|e| format!("signing GetCallerIdentity: {e}"))?
        .into_parts();

    let mut out = vec![
        SubjectTokenHeader {
            key: "host".to_owned(),
            value: host,
        },
        SubjectTokenHeader {
            key: "x-goog-cloud-target-resource".to_owned(),
            value: target_resource.to_owned(),
        },
    ];
    for (name, value) in instructions.headers() {
        out.push(SubjectTokenHeader {
            key: name.to_owned(),
            value: value.to_owned(),
        });
    }

    let envelope = SubjectTokenEnvelope {
        url,
        method: "POST",
        headers: out,
        body: String::new(),
    };
    serde_json::to_string(&envelope).map_err(|e| format!("serializing subject token: {e}"))
}

/// The AIP-4117 envelope Google STS expects for `aws4_request` subject tokens: a JSON description
/// of a signed `GetCallerIdentity` request.
#[derive(Debug, serde::Serialize)]
struct SubjectTokenEnvelope {
    url: String,
    method: &'static str,
    headers: Vec<SubjectTokenHeader>,
    body: String,
}

#[derive(Debug, serde::Serialize)]
struct SubjectTokenHeader {
    key: String,
    value: String,
}

/// Assembles the federation chain for `key` (whose `wif_provider` is `Some`) and returns the
/// resulting [`IdTokenSource`]: broker credentials -> shared external-account source credential
/// -> impersonation. Only the outer impersonated credential built here is per-key; the
/// external-account source is shared per WIF provider (see [`external_account_source`]) exactly
/// like the registry's ambient source is shared across impersonated keys.
pub(super) async fn build_federated_source(
    key: CacheKey,
) -> Result<Arc<dyn IdTokenSource>, GcpAuthError> {
    let CacheKey {
        wif_provider,
        impersonate,
        audience,
    } = key;
    let wif_provider = wif_provider.expect("build_federated_source called with wif_provider unset");
    let Some(impersonate) = impersonate else {
        // The schema registry requires `impersonate_service_account` whenever
        // `workload_identity_provider` is set (the external-account credential this chain
        // produces cannot mint an ID token ambiently), so this is unreachable via registration —
        // guarded here defensively for callers that bypass the registry.
        return Err(GcpAuthError::Build {
            audience,
            message: "GCP workload identity federation requires impersonate_service_account to \
                      be set"
                .to_owned(),
        });
    };

    let source = external_account_source(&wif_provider)
        .await
        .map_err(|message| GcpAuthError::Adc {
            audience: audience.clone(),
            impersonate: impersonate.clone(),
            message,
        })?;

    let credentials = idtoken::impersonated::Builder::from_source_credentials(
        audience.clone(),
        impersonate,
        source,
    )
    .build()
    .map_err(|e| GcpAuthError::Build {
        audience,
        message: e.to_string(),
    })?;

    Ok(Arc::new(Live(credentials)) as Arc<dyn IdTokenSource>)
}

/// One [`super::SourceSlot`] per WIF provider resource name, each holding the shared
/// external-account source credential every federated key targeting that provider clones. Keyed
/// by provider rather than a single process-wide slot (unlike the ambient source, of which there
/// is exactly one) because distinct providers are distinct external identities with no shared
/// actor to begin with; cardinality is registration-bounded, like the outer credential cache.
static EXTERNAL_ACCOUNT_SOURCES: std::sync::LazyLock<
    parking_lot::Mutex<
        std::collections::HashMap<
            String,
            Arc<super::SourceSlot<google_cloud_auth::credentials::Credentials>>,
        >,
    >,
> = std::sync::LazyLock::new(Default::default);

/// Returns the [`super::SourceSlot`] for `provider`, creating an empty one on first reference.
pub(super) fn external_account_source_slot(
    provider: &str,
) -> Arc<super::SourceSlot<google_cloud_auth::credentials::Credentials>> {
    let mut sources = EXTERNAL_ACCOUNT_SOURCES.lock();
    Arc::clone(
        sources
            .entry(provider.to_owned())
            .or_insert_with(|| Arc::new(super::SourceSlot::new())),
    )
}

/// Returns `provider`'s shared external-account source credential, building it on first use and
/// reusing it thereafter -- single-flighted by the provider's [`super::SourceSlot`], so N
/// concurrent cold federated keys for the same provider share one build. Recovery from a
/// permanent post-build failure is driven separately (see [`recover_federated_source_if_dead`]).
async fn external_account_source(
    provider: &str,
) -> Result<google_cloud_auth::credentials::Credentials, String> {
    external_account_source_slot(provider)
        .get_or_build(boxed_external_account_source_build(provider.to_owned()))
        .await
}

/// Probes `provider`'s cached external-account source credential and replaces it if -- and only
/// if -- the probe proves its background refresh actor has permanently died, mirroring
/// `Registry::recover_ambient_source_if_dead` exactly. Called from `mint()` after any permanent
/// mint failure on a federated key targeting `provider`.
pub(super) async fn recover_federated_source_if_dead(provider: &str) {
    let _ = external_account_source_slot(provider)
        .recover_if_dead(
            super::credentials_source_is_dead,
            boxed_external_account_source_build(provider.to_owned()),
        )
        .await;
}

/// Boxes [`build_external_account_source`]'s future. `SourceSlot::get_or_build`/`recover_if_dead`
/// are small generic utilities that hold their `build` future inline across an await point;
/// `build_external_account_source` resolves the shared broker internally
/// (`aws_config::load_defaults()`'s own future is large), so leaving it unboxed here would size
/// `mint()`'s own future -- which reaches this through both `external_account_source` and
/// recovery -- for that on every mint, federated or not.
fn boxed_external_account_source_build(
    provider_resource: String,
) -> std::pin::Pin<
    Box<dyn Future<Output = Result<google_cloud_auth::credentials::Credentials, String>> + Send>,
> {
    Box::pin(build_external_account_source(provider_resource))
}

/// Test-only override for [`external_account_source`]'s build step, keyed by provider so distinct
/// providers' tests cannot interfere with each other. Consulted before resolving a broker at all
/// (see [`build_external_account_source`]), so overriding tests never need `[gcp-federation]`
/// configuration or a `Broker` in place.
#[cfg(test)]
type FederatedSourceOverride =
    Arc<dyn Fn() -> Result<google_cloud_auth::credentials::Credentials, String> + Send + Sync>;
#[cfg(test)]
static FEDERATED_SOURCE_OVERRIDES: std::sync::LazyLock<
    parking_lot::Mutex<std::collections::HashMap<String, FederatedSourceOverride>>,
> = std::sync::LazyLock::new(Default::default);

/// Builds a fresh external-account source credential for `provider`: the test override when
/// compiled for tests, otherwise the real chain (resolve the shared broker, sign a SigV4 subject
/// token, exchange it at Google STS). Shared by `external_account_source` (first build) and
/// `recover_federated_source_if_dead` (rebuild after a proven-dead probe).
async fn build_external_account_source(
    provider_resource: String,
) -> Result<google_cloud_auth::credentials::Credentials, String> {
    #[cfg(test)]
    if let Some(f) = FEDERATED_SOURCE_OVERRIDES
        .lock()
        .get(&provider_resource)
        .cloned()
    {
        return f();
    }

    let broker = broker().await?;
    let subject_token_provider = Arc::new(AwsSubjectTokenProvider {
        broker,
        provider_resource: provider_resource.clone(),
    });
    ProgrammaticBuilder::new(subject_token_provider)
        .with_audience(provider_resource)
        .with_subject_token_type(AWS4_SUBJECT_TOKEN_TYPE)
        .with_token_url(GOOGLE_STS_TOKEN_URL)
        .build()
        .map_err(|e| format!("building GCP workload identity federation source credentials: {e}"))
}

/// Test-only: install `f` as the build step for `provider`'s shared external-account source (see
/// [`FEDERATED_SOURCE_OVERRIDES`]). `f` returns a `google_cloud_auth::credentials::Credentials`
/// directly -- a fully public type -- so callers outside this module (e.g. `gcp::tests`) can
/// install one without needing to see anything internal to federation.
#[cfg(test)]
pub(super) fn install_federated_source_override_for_test(
    provider: &str,
    f: impl Fn() -> Result<google_cloud_auth::credentials::Credentials, String> + Send + Sync + 'static,
) {
    FEDERATED_SOURCE_OVERRIDES
        .lock()
        .insert(provider.to_owned(), Arc::new(f));
}

#[cfg(test)]
mod federation_tests {
    use std::time::{Duration, SystemTime};

    use aws_credential_types::Credentials as AwsCredentials;
    use google_cloud_auth::errors::SubjectTokenProviderError;

    use super::build_subject_token;

    const PROVIDER: &str = "//iam.googleapis.com/projects/123456789/locations/global/workloadIdentityPools/restate-cloud/providers/aws-broker";

    fn fixed_credentials() -> AwsCredentials {
        AwsCredentials::new(
            "AKIDEXAMPLE",
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            Some("SESSIONTOKENEXAMPLE".to_owned()),
            None,
            "test",
        )
    }

    fn fixed_time() -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(1_755_000_000)
    }

    #[test]
    fn subject_token_has_the_shape_google_sts_expects() {
        let token =
            build_subject_token(&fixed_credentials(), "us-east-1", PROVIDER, fixed_time()).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&token).unwrap();

        assert_eq!(parsed["method"], "POST");
        assert_eq!(parsed["body"], "");
        assert_eq!(
            parsed["url"],
            "https://sts.us-east-1.amazonaws.com/?Action=GetCallerIdentity&Version=2011-06-15"
        );

        let headers: Vec<(String, String)> = parsed["headers"]
            .as_array()
            .unwrap()
            .iter()
            .map(|h| {
                (
                    h["key"].as_str().unwrap().to_lowercase(),
                    h["value"].as_str().unwrap().to_owned(),
                )
            })
            .collect();

        let names: Vec<&str> = headers.iter().map(|(k, _)| k.as_str()).collect();
        assert!(names.contains(&"authorization"), "{names:?}");
        assert!(names.contains(&"host"), "{names:?}");
        assert!(names.contains(&"x-amz-date"), "{names:?}");
        assert!(names.contains(&"x-amz-security-token"), "{names:?}");
        assert!(names.contains(&"x-goog-cloud-target-resource"), "{names:?}");
    }

    /// The provider resource name must be covered by the signature. If it were not, a signed
    /// envelope minted for one workload identity pool could be replayed against another -- which
    /// is the environment-isolation boundary restate-cloud#1188 requires.
    #[test]
    fn target_resource_is_a_signed_header() {
        let token =
            build_subject_token(&fixed_credentials(), "us-east-1", PROVIDER, fixed_time()).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&token).unwrap();

        let authorization = parsed["headers"]
            .as_array()
            .unwrap()
            .iter()
            .find(|h| {
                h["key"]
                    .as_str()
                    .unwrap()
                    .eq_ignore_ascii_case("authorization")
            })
            .expect("authorization header")["value"]
            .as_str()
            .unwrap()
            .to_owned();

        assert!(
            authorization.contains("x-goog-cloud-target-resource"),
            "target resource not in SignedHeaders: {authorization}"
        );
    }

    // -- Tests below exercise the real `AwsSubjectTokenProvider` + `ProgrammaticBuilder` chain
    // against a local mock Google STS server. They build a `Broker` directly (bypassing
    // `Broker::init`'s `aws_config::load_defaults()` and the process-global `BROKER`/
    // `FEDERATION_CONFIG` statics entirely) with a pre-cached, never-expiring credentials
    // fixture, so `Broker::credentials()` never calls the real `sts:AssumeRole` API -- no network
    // access to AWS is needed or attempted. This also means these tests never touch the shared
    // statics `broker()` reads, so they cannot interfere with `gcp::tests`'
    // `wif_requested_without_server_config_is_a_permanent_build_error`, which depends on
    // `FEDERATION_CONFIG` staying uninstalled for the lifetime of this test binary.

    use std::convert::Infallible;
    use std::sync::{Arc, Mutex};

    use bytes::Bytes;
    use google_cloud_auth::credentials::CacheableResource;
    use google_cloud_auth::credentials::external_account::ProgrammaticBuilder;
    use http_body_util::{BodyExt, Full};
    use hyper::body::Incoming;
    use hyper::server::conn::http1;
    use hyper::service::service_fn;
    use hyper::{Request, Response, StatusCode};
    use hyper_util::rt::TokioIo;
    use tokio::net::TcpListener;

    use super::{AWS4_SUBJECT_TOKEN_TYPE, AwsSubjectTokenProvider, Broker};

    /// Builds a `Broker` that never performs AWS network I/O: `provider` wraps a fixed credential
    /// directly (see `Broker`'s doc), and the cache is pre-seeded so `Broker::credentials()`
    /// always takes the cache-hit path.
    fn fixture_broker() -> Arc<Broker> {
        let credentials = fixed_credentials();
        Arc::new(Broker {
            region: "us-east-1".to_owned(),
            provider: aws_credential_types::provider::SharedCredentialsProvider::new(
                credentials.clone(),
            ),
            cached: tokio::sync::Mutex::new(Some(credentials)),
        })
    }

    fn aws_env_snapshot() -> std::collections::BTreeMap<String, String> {
        std::env::vars()
            .filter(|(k, _)| k.starts_with("AWS_"))
            .collect()
    }

    type CapturedBody = Arc<Mutex<Option<Bytes>>>;

    /// Stand up a tiny local Google STS stand-in: captures the raw POST body of the first request
    /// and responds with a well-formed token-exchange response.
    async fn mock_google_sts() -> (String, CapturedBody) {
        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind mock STS server");
        let addr = listener.local_addr().expect("local_addr");
        let captured: CapturedBody = Arc::new(Mutex::new(None));
        let captured_for_task = Arc::clone(&captured);

        tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(pair) => pair,
                    Err(_) => return,
                };
                let captured = Arc::clone(&captured_for_task);
                tokio::spawn(async move {
                    let io = TokioIo::new(stream);
                    let svc = service_fn(move |req: Request<Incoming>| {
                        let captured = Arc::clone(&captured);
                        async move {
                            let body = req.into_body().collect().await.unwrap().to_bytes();
                            *captured.lock().unwrap() = Some(body);
                            let response_body = serde_json::json!({
                                "access_token": "mock-federated-access-token",
                                "issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
                                "token_type": "Bearer",
                                "expires_in": 3600,
                            })
                            .to_string();
                            Ok::<_, Infallible>(
                                Response::builder()
                                    .status(StatusCode::OK)
                                    .header("content-type", "application/json")
                                    .body(Full::new(Bytes::from(response_body)))
                                    .expect("response build"),
                            )
                        }
                    });
                    let _ = http1::Builder::new().serve_connection(io, svc).await;
                });
            }
        });

        (format!("http://{addr}/token"), captured)
    }

    /// Exercises the real `AwsSubjectTokenProvider` + `ProgrammaticBuilder` path against a mock
    /// Google STS: the exchange request's `grant_type`/`subject_token_type`/`audience` fields, that
    /// the AIP-4117 JSON arrives at Google percent-encoded exactly once after undoing the STS
    /// exchange's own transport form-encoding, and that none of it touches any `AWS_*` environment
    /// variable. Stops at the STS hop: `idtoken::impersonated::Builder`'s impersonation URL has no
    /// public override in google-cloud-auth 1.15, so `generateIdToken` cannot be redirected to a
    /// local mock from outside the crate.
    #[tokio::test]
    async fn programmatic_builder_sts_exchange_matches_expected_wire_format() {
        let env_before = aws_env_snapshot();

        let broker = fixture_broker();
        let subject_token_provider = Arc::new(AwsSubjectTokenProvider {
            broker,
            provider_resource: PROVIDER.to_owned(),
        });

        let (token_url, captured) = mock_google_sts().await;
        let credentials = ProgrammaticBuilder::new(subject_token_provider)
            .with_audience(PROVIDER)
            .with_subject_token_type(AWS4_SUBJECT_TOKEN_TYPE)
            .with_token_url(token_url)
            .build()
            .expect("programmatic external-account credentials build");

        let result = credentials.headers(http::Extensions::new()).await;
        let CacheableResource::New { data: headers, .. } =
            result.expect("STS exchange succeeds against the mock server")
        else {
            panic!("expected fresh headers on first fetch");
        };
        assert!(
            headers.contains_key(http::header::AUTHORIZATION),
            "exchanged credentials should produce an Authorization header: {headers:?}"
        );

        let body = captured
            .lock()
            .unwrap()
            .clone()
            .expect("mock STS server received a request");
        let form: std::collections::HashMap<String, String> =
            url::form_urlencoded::parse(&body).into_owned().collect();

        assert_eq!(
            form.get("grant_type").map(String::as_str),
            Some("urn:ietf:params:oauth:grant-type:token-exchange")
        );
        assert_eq!(
            form.get("subject_token_type").map(String::as_str),
            Some(AWS4_SUBJECT_TOKEN_TYPE)
        );
        assert_eq!(form.get("audience").map(String::as_str), Some(PROVIDER));

        // The subject_token field on the wire has been through two encoding layers: ours (this
        // module's `byte_serialize` over the SigV4 envelope) and the STS exchange's own
        // form-encoding for transport. `url::form_urlencoded::parse` above already undid the
        // transport layer; recomputing our own encoding step independently and comparing proves
        // what arrived is encoded exactly once, not zero or two times.
        let subject_token_wire = form
            .get("subject_token")
            .expect("subject_token field present");
        let envelope = build_subject_token(
            &fixed_credentials(),
            "us-east-1",
            PROVIDER,
            SystemTime::now(),
        )
        .expect("rebuilding an envelope with the same shape succeeds");
        // Time-dependent (SigV4 signs the request time), so compare shape rather than equality:
        // both must be a once-encoded JSON envelope with the same structural fields.
        let decoded_wire: String = url::form_urlencoded::parse(subject_token_wire.as_bytes())
            .next()
            .map(|(k, _)| k.into_owned())
            .expect("subject token decodes to one key");
        let decoded_wire_json: serde_json::Value =
            serde_json::from_str(&decoded_wire).expect("decoded subject token is JSON");
        let envelope_json: serde_json::Value =
            serde_json::from_str(&envelope).expect("locally-built envelope is JSON");
        assert_eq!(decoded_wire_json["method"], envelope_json["method"]);
        assert_eq!(decoded_wire_json["url"], envelope_json["url"]);

        assert_eq!(
            env_before,
            aws_env_snapshot(),
            "constructing and using a federated subject-token provider must not mutate any \
             AWS_* environment variable"
        );
    }

    #[test]
    fn accepts_a_well_formed_broker_role_arn() {
        super::validate_broker_role_arn("arn:aws:iam::123456789012:role/RestateCloudGcpFederation")
            .expect("well-formed ARN accepted");
        super::validate_broker_role_arn("arn:aws-us-gov:iam::123456789012:role/path/to/role")
            .expect("non-default partition with a role path accepted");
    }

    #[test]
    fn rejects_malformed_broker_role_arns() {
        for arn in [
            "not-an-arn",
            "arn:aws:s3::123456789012:role/wrong-service",
            "arn:aws:iam:us-east-1:123456789012:role/has-a-region",
            "arn:aws:iam::12345:role/too-short-account",
            "arn:aws:iam::12345678901a:role/non-numeric-account",
            "arn:aws:iam::123456789012:user/not-a-role",
            "arn:aws:iam::123456789012:role/",
            "arn:aws!:iam::123456789012:role/bad-partition-chars",
        ] {
            super::validate_broker_role_arn(arn)
                .expect_err(&format!("expected '{arn}' to be rejected"));
        }
    }

    #[test]
    fn accepts_well_formed_session_names() {
        for name in [
            "ab",
            "env-xyz",
            "env_xyz.123@foo,bar+baz=qux",
            &"a".repeat(64),
        ] {
            super::validate_session_name(name).expect("well-formed session name accepted");
        }
    }

    #[test]
    fn rejects_malformed_session_names() {
        for name in [
            "",
            "a",
            &"a".repeat(65),
            "has a space",
            "has/slash",
            "has#hash",
        ] {
            super::validate_session_name(name)
                .expect_err(&format!("expected '{name}' to be rejected"));
        }
    }

    #[test]
    fn install_config_rejects_invalid_broker_role_arn() {
        let config = super::GcpFederationOptions {
            broker_role_arn: "not-an-arn".to_owned(),
            session_name: "valid-session".to_owned(),
        };
        super::install_config(Some(config)).expect_err("invalid broker-role-arn must be rejected");
    }

    #[test]
    fn install_config_rejects_invalid_session_name() {
        let config = super::GcpFederationOptions {
            broker_role_arn: "arn:aws:iam::123456789012:role/RestateCloudGcpFederation".to_owned(),
            session_name: "has a space".to_owned(),
        };
        super::install_config(Some(config)).expect_err("invalid session-name must be rejected");
    }

    fn assume_role_access_denied_error() -> aws_credential_types::provider::error::CredentialsError
    {
        use aws_sdk_sts::error::SdkError;
        use aws_sdk_sts::operation::assume_role::AssumeRoleError;
        use aws_smithy_runtime_api::client::orchestrator::HttpResponse;
        use aws_smithy_runtime_api::http::StatusCode;
        use aws_smithy_types::body::SdkBody;
        use aws_smithy_types::error::ErrorMetadata;

        let service_error = AssumeRoleError::generic(
            ErrorMetadata::builder()
                .code("AccessDenied")
                .message(
                    "User: arn:aws:sts::123456789012:assumed-role/... is not authorized to \
                     perform: sts:AssumeRole on resource: ...",
                )
                .build(),
        );
        let raw = HttpResponse::new(
            StatusCode::try_from(403).unwrap(),
            SdkBody::from("<AccessDenied/>"),
        );
        aws_credential_types::provider::error::CredentialsError::provider_error(
            SdkError::service_error(service_error, raw),
        )
    }

    fn assume_role_dispatch_timeout_error()
    -> aws_credential_types::provider::error::CredentialsError {
        use aws_sdk_sts::error::SdkError;
        use aws_sdk_sts::operation::assume_role::AssumeRoleError;

        let sdk_error: SdkError<AssumeRoleError> =
            SdkError::timeout_error("connect timed out reaching sts.us-east-1.amazonaws.com");
        aws_credential_types::provider::error::CredentialsError::provider_error(sdk_error)
    }

    #[test]
    fn assume_role_access_denied_classifies_permanent() {
        let raw = assume_role_access_denied_error();
        // The wire error code is recovered through the unmodeled `Unhandled` variant, not matched
        // on `Display` text -- pin that alongside the classification it feeds.
        assert_eq!(super::assume_role_error_code(&raw), Some("AccessDenied"));
        let error = super::federation_error_from_assume_role_failure(&raw);
        assert!(
            !error.is_transient(),
            "an AssumeRole AccessDenied must classify as permanent, got {error:?}"
        );
    }

    #[test]
    fn assume_role_dispatch_timeout_classifies_transient() {
        let raw = assume_role_dispatch_timeout_error();
        assert_eq!(super::assume_role_error_code(&raw), None);
        let error = super::federation_error_from_assume_role_failure(&raw);
        assert!(
            error.is_transient(),
            "an AssumeRole connector/timeout failure must classify as transient, got {error:?}"
        );
    }
}
