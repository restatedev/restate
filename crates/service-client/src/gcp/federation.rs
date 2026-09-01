// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! AWS-to-GCP workload identity federation for minting Google ID tokens without storing Google
//! credentials in Restate.
//!
//! The trust chain is:
//!
//! ```text
//! ambient AWS credentials
//!   -> sts:AssumeRole(operator-configured AWS federation role)
//!   -> SigV4-signed GetCallerIdentity envelope (AIP-4117 aws4_request)
//!   -> Google STS token exchange at the deployment's workload identity provider
//!   -> IAM Credentials generateIdToken as the deployment's service account
//! ```
//!
//! The assumed AWS role session is shared across the process. Everything after it is scoped by a
//! deployment's `workload_identity_provider` and `impersonate_service_account`.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, OnceLock, Weak};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
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
use tracing::warn;

use restate_core::{TaskCenter, TaskKind};
use restate_types::config::GcpFederationOptions;

use super::{Credential, GcpAuthError, IdTokenSource, RecoverableCredentialSource};

/// AWS subject-token type Google STS expects for a SigV4-signed `GetCallerIdentity` envelope.
const AWS4_SUBJECT_TOKEN_TYPE: &str = "urn:ietf:params:aws:token-type:aws4_request";
const GOOGLE_STS_TOKEN_URL: &str = "https://sts.googleapis.com/v1/token";

/// Refresh the assumed AWS role session before it expires so subject-token generation does not
/// race its expiry.
const AWS_ROLE_REFRESH_MARGIN: Duration = Duration::from_secs(300);

/// Operator configuration captured at node startup. An initialized `None` is intentional: later
/// config reloads cannot enable federation for an already-running process.
static FEDERATION_CONFIG: OnceLock<Option<GcpFederationOptions>> = OnceLock::new();

pub(crate) fn initialize_config(config: Option<GcpFederationOptions>) -> Result<(), String> {
    initialize_config_once(&FEDERATION_CONFIG, config)
}

fn initialize_config_once(
    config_slot: &OnceLock<Option<GcpFederationOptions>>,
    config: Option<GcpFederationOptions>,
) -> Result<(), String> {
    if let Some(installed) = config_slot.get() {
        if installed != &config {
            warn!(
                "ignoring GCP federation configuration that differs from the value captured at \
                 first node startup; restart the server to apply the new configuration"
            );
        }
        return Ok(());
    }
    if let Some(config) = &config {
        validate_aws_role_arn(&config.aws_role_arn)?;
        validate_aws_role_session_name(&config.aws_role_session_name)?;
    }
    let _ = config_slot.set(config);
    Ok(())
}

/// Fails invalid role ARNs at startup; the AWS SDK has no public ARN parser.
///
/// Follows the ARN grammar in
/// <https://docs.aws.amazon.com/IAM/latest/UserGuide/reference-arns.html> and the `RoleName` and
/// `Path` patterns in
/// <https://docs.aws.amazon.com/IAM/latest/APIReference/API_CreateRole.html>.
fn validate_aws_role_arn(arn: &str) -> Result<(), String> {
    let invalid = || {
        format!(
            "aws-role-arn '{arn}' is not a valid AWS IAM role ARN; expected \
             arn:aws:iam::<12-digit account id>:role/<role-name-or-path> or the aws-us-gov \
             equivalent"
        )
    };
    // An ARN has exactly six colon-separated fields, and the trailing resource field may itself
    // contain colons: a role path admits any printable ASCII, ':' included.
    let parts: Vec<&str> = arn.splitn(6, ':').collect();
    let [scheme, partition, service, region, account, resource] = parts.as_slice() else {
        return Err(invalid());
    };
    let Some(role_resource) = resource.strip_prefix("role/") else {
        return Err(invalid());
    };
    let Some(role_name) = role_resource.rsplit('/').next() else {
        return Err(invalid());
    };
    let role_name_ok = (1..=64).contains(&role_name.len())
        && role_name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "_+=,.@-".contains(c));
    let path_ok = role_resource
        .split('/')
        .all(|part| !part.is_empty() && part.chars().all(|c| c.is_ascii_graphic()));
    if *scheme != "arn"
        || !matches!(*partition, "aws" | "aws-us-gov")
        || *service != "iam"
        || !region.is_empty()
        || account.len() != 12
        || !account.bytes().all(|b| b.is_ascii_digit())
        || !role_name_ok
        || !path_ok
    {
        return Err(invalid());
    }
    Ok(())
}

/// Fails invalid `RoleSessionName` values at startup, following the `RoleSessionName` constraints
/// in <https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRole.html>.
fn validate_aws_role_session_name(name: &str) -> Result<(), String> {
    let len = name.chars().count();
    let chars_ok = name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || "_+=,.@-".contains(c));
    if !(2..=64).contains(&len) || !chars_ok {
        return Err(format!(
            "aws-role-session-name '{name}' is not a valid AWS STS RoleSessionName; expected \
             2-64 characters from [A-Za-z0-9_+=,.@-]"
        ));
    }
    Ok(())
}

/// Cached process-wide AWS federation-role session credentials.
struct AwsFederationCredentials {
    region: String,
    provider: SharedCredentialsProvider,
    cached: Mutex<Option<AwsCredentials>>,
}

impl fmt::Debug for AwsFederationCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AwsFederationCredentials")
            .field("region", &self.region)
            .finish_non_exhaustive()
    }
}

impl AwsFederationCredentials {
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
        let provider = AssumeRoleProvider::builder(config.aws_role_arn.clone())
            .configure(&sdk_config)
            .session_name(config.aws_role_session_name.clone())
            .build()
            .await;
        Ok(Self {
            region,
            provider: SharedCredentialsProvider::new(provider),
            cached: Mutex::new(None),
        })
    }

    async fn credentials(&self) -> Result<AwsCredentials, FederationError> {
        let mut guard = self.cached.lock().await;
        if let Some(creds) = guard.as_ref() {
            let fresh_enough = creds
                .expiry()
                .is_none_or(|expiry| expiry > SystemTime::now() + AWS_ROLE_REFRESH_MARGIN);
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

/// Configuration and authorization failures require operator action. Expired ambient credentials,
/// transport failures, and unknown errors may recover on retry.
fn federation_error_from_assume_role_failure(
    error: &aws_credential_types::provider::error::CredentialsError,
) -> FederationError {
    let message = format!("assuming the AWS federation role for GCP authentication: {error}");
    // These are stable STS API error codes, see AssumeRole and common error references:
    // https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRole.html#API_AssumeRole_Errors
    // https://docs.aws.amazon.com/STS/latest/APIReference/CommonErrors.html
    let permanent = matches!(
        assume_role_error_code(error),
        Some(
            "AccessDenied"
                | "AccessDeniedException"
                | "MalformedPolicyDocument"
                | "PackedPolicyTooLarge"
                | "RegionDisabledException"
                | "ValidationError"
        )
    );
    if permanent {
        FederationError::permanent(message)
    } else {
        FederationError::transient(message)
    }
}

/// `AssumeRole` exposes AccessDenied only as an unmodeled error; recover its wire code rather
/// than classifying its display text.
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

static AWS_FEDERATION_CREDENTIALS: OnceCell<Arc<AwsFederationCredentials>> = OnceCell::const_new();

async fn aws_federation_credentials() -> Result<Arc<AwsFederationCredentials>, String> {
    AWS_FEDERATION_CREDENTIALS
        .get_or_try_init(|| async {
            let Some(config) = FEDERATION_CONFIG.get().and_then(Option::as_ref) else {
                return Err(
                    "this deployment requests GCP workload identity federation, but the server \
                     has no [gcp-federation] configuration; set aws-role-arn and \
                     aws-role-session-name to enable it"
                        .to_owned(),
                );
            };
            AwsFederationCredentials::init(config).await.map(Arc::new)
        })
        .await
        .cloned()
}

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
/// on each refresh.
///
/// `google-cloud-auth`'s built-in AWS source resolves environment or IMDS credentials itself and
/// cannot accept Restate's shared assume-role provider. This adapter signs `GetCallerIdentity`
/// with that provider's current session credentials without mutating `AWS_*` variables or
/// process-global AWS SDK state.
#[derive(Debug)]
struct AwsSubjectTokenProvider {
    aws_federation_credentials: Arc<AwsFederationCredentials>,
    /// The full resource name of the customer's workload identity provider. Doubles as the
    /// `x-goog-cloud-target-resource` header value that binds the signed envelope to this pool
    /// (see `build_subject_token`) and as the STS `audience` parameter.
    provider_resource: String,
}

impl SubjectTokenProvider for AwsSubjectTokenProvider {
    type Error = FederationError;

    async fn subject_token(&self) -> Result<SubjectToken, Self::Error> {
        self.subject_token_inner().await
    }
}

impl AwsSubjectTokenProvider {
    async fn subject_token_inner(&self) -> Result<SubjectToken, FederationError> {
        let credentials = self.aws_federation_credentials.credentials().await?;
        let envelope = build_subject_token(
            &credentials,
            &self.aws_federation_credentials.region,
            &self.provider_resource,
            SystemTime::now(),
        )
        .map_err(|e| {
            FederationError::permanent(format!("signing the GetCallerIdentity subject token: {e}"))
        })?;

        // Match google-cloud-auth's AWS source: encode the envelope once here, before the STS
        // request form-encodes it again. The wire-format test pins this otherwise subtle contract.
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

/// Shared Google STS access-token source for one workload identity provider.
pub(super) struct FederatedAccessTokenSource {
    pub(super) credentials: RecoverableCredentialSource,
}

/// Weak-indexed sources keyed by workload identity provider. Cached outer credentials retain the
/// strong lease, so sources disappear when those credentials do rather than on an unrelated timer.
///
/// The provider resource name is a sufficient key while the process has exactly one configured
/// AWS federation identity. If deployments can later select among AWS identities, that identity
/// must become part of the key: the same provider reached through a different AWS identity is a
/// distinct access-token source.
#[derive(Default)]
pub(super) struct FederatedAccessTokenSourceIndex {
    entries: parking_lot::Mutex<HashMap<String, Weak<FederatedAccessTokenSource>>>,
}

impl FederatedAccessTokenSourceIndex {
    pub(super) fn get_or_create(&self, provider: &str) -> Arc<FederatedAccessTokenSource> {
        let mut entries = self.entries.lock();
        if let Some(existing) = entries.get(provider).and_then(Weak::upgrade) {
            return existing;
        }
        let fresh = Arc::new(FederatedAccessTokenSource {
            credentials: RecoverableCredentialSource::new(),
        });
        entries.insert(provider.to_owned(), Arc::downgrade(&fresh));
        fresh
    }

    /// Rebuild only a live source whose refresh task was proven permanently dead. Do not create
    /// an unleased replacement for an absent or already-dead weak entry.
    pub(super) async fn recover_if_dead(&self, provider: &str) {
        let Some(access_token_source) = self.entries.lock().get(provider).and_then(Weak::upgrade)
        else {
            return;
        };
        match access_token_source
            .credentials
            .replace_if_dead(build_federated_access_token_source_on_tc_task(
                provider.to_owned(),
            ))
            .await
        {
            Ok(true) => {
                warn!(
                    provider_resource = %provider,
                    "replaced a federated GCP access-token source: its refresh task was proven dead"
                );
            }
            Ok(false) => {}
            Err(error) => {
                warn!(
                    provider_resource = %provider,
                    error = %error,
                    "failed to rebuild a federated GCP access-token source after its refresh task \
                     was proven dead; a future mint attempt will retry"
                );
            }
        }
    }

    /// Drops entries without a live lease and returns the number that remain.
    pub(super) fn reap_dead(&self) -> usize {
        let mut entries = self.entries.lock();
        entries.retain(|_, weak| weak.strong_count() > 0);
        entries.len()
    }

    #[cfg(test)]
    pub(super) fn weak_for_test(&self, provider: &str) -> Option<Weak<FederatedAccessTokenSource>> {
        self.entries.lock().get(provider).cloned()
    }
}

/// Per-audience/service-account ID-token credentials, retaining their source's lease.
pub(super) struct FederatedIdTokenCredentials {
    credentials: google_cloud_auth::credentials::idtoken::IDTokenCredentials,
    _access_token_source: Arc<FederatedAccessTokenSource>,
}

#[async_trait]
impl IdTokenSource for FederatedIdTokenCredentials {
    async fn id_token(&self) -> Result<String, google_cloud_auth::errors::CredentialsError> {
        self.credentials.id_token().await
    }
}

pub(super) async fn build_federated_source(
    sources: &FederatedAccessTokenSourceIndex,
    provider: String,
    service_account: String,
    audience: String,
) -> Result<Credential, GcpAuthError> {
    let access_token_source = sources.get_or_create(&provider);
    let access_token_credentials = access_token_source
        .credentials
        .get_or_build(build_federated_access_token_source(provider.clone()))
        .await
        .map_err(|message| GcpAuthError::CredentialSource {
            audience: audience.clone(),
            service_account: service_account.clone(),
            message,
        })?;

    #[cfg(test)]
    if test_hooks::is_overridden(&provider) {
        // Source-lifecycle tests must not start the outer credential's real Google refresh task.
        return Ok(test_hooks::leased_credential(access_token_source));
    }

    let credentials = idtoken::impersonated::Builder::from_source_credentials(
        audience.clone(),
        service_account,
        access_token_credentials,
    )
    .build()
    .map_err(|e| GcpAuthError::Build {
        audience,
        message: e.to_string(),
    })?;

    Ok(Arc::new(FederatedIdTokenCredentials {
        credentials,
        _access_token_source: access_token_source,
    }) as Credential)
}

async fn build_federated_access_token_source_on_tc_task(
    provider_resource: String,
) -> Result<google_cloud_auth::credentials::Credentials, String> {
    let task = TaskCenter::current()
        .spawn_unmanaged(
            TaskKind::Credentials,
            "gcp-federated-source-recovery",
            build_federated_access_token_source(provider_resource),
        )
        .map_err(|_| "TaskCenter is shutting down".to_owned())?;
    task.await
        .map_err(|_| "GCP federated credential construction task failed".to_owned())?
}

async fn build_federated_access_token_source(
    provider_resource: String,
) -> Result<google_cloud_auth::credentials::Credentials, String> {
    #[cfg(test)]
    if let Some(f) = test_hooks::access_token_source_override(&provider_resource) {
        return f();
    }

    let aws_federation_credentials = aws_federation_credentials().await?;
    let subject_token_provider = Arc::new(AwsSubjectTokenProvider {
        aws_federation_credentials,
        provider_resource: provider_resource.clone(),
    });
    ProgrammaticBuilder::new(subject_token_provider)
        .with_audience(provider_resource)
        .with_subject_token_type(AWS4_SUBJECT_TOKEN_TYPE)
        .with_token_url(GOOGLE_STS_TOKEN_URL)
        .build()
        .map_err(|e| format!("building GCP workload identity federation source credentials: {e}"))
}

/// Offline substitutes for federation source construction.
#[cfg(test)]
pub(super) mod test_hooks {
    use std::collections::HashMap;
    use std::sync::{Arc, LazyLock};

    use parking_lot::Mutex;

    use super::{Credential, FederatedAccessTokenSource};

    pub(super) type AccessTokenSourceOverride =
        Arc<dyn Fn() -> Result<google_cloud_auth::credentials::Credentials, String> + Send + Sync>;

    static OVERRIDES: LazyLock<Mutex<HashMap<String, AccessTokenSourceOverride>>> =
        LazyLock::new(Default::default);

    pub(super) fn access_token_source_override(
        provider: &str,
    ) -> Option<AccessTokenSourceOverride> {
        OVERRIDES.lock().get(provider).cloned()
    }

    pub(super) fn is_overridden(provider: &str) -> bool {
        OVERRIDES.lock().contains_key(provider)
    }

    pub(in crate::gcp) fn install_access_token_source_override(
        provider: &str,
        f: impl Fn() -> Result<google_cloud_auth::credentials::Credentials, String>
        + Send
        + Sync
        + 'static,
    ) {
        OVERRIDES.lock().insert(provider.to_owned(), Arc::new(f));
    }

    pub(super) struct FederatedIdTokenCredentials {
        pub(super) _access_token_source: Arc<FederatedAccessTokenSource>,
    }

    #[async_trait::async_trait]
    impl super::IdTokenSource for FederatedIdTokenCredentials {
        async fn id_token(&self) -> Result<String, google_cloud_auth::errors::CredentialsError> {
            Ok("test-federated-id-token".to_owned())
        }
    }

    pub(in crate::gcp) fn leased_credential(
        access_token_source: Arc<FederatedAccessTokenSource>,
    ) -> Credential {
        Arc::new(FederatedIdTokenCredentials {
            _access_token_source: access_token_source,
        })
    }
}

#[cfg(test)]
mod federation_tests {
    use std::time::{Duration, SystemTime};

    use aws_credential_types::Credentials as AwsCredentials;
    use google_cloud_auth::errors::SubjectTokenProviderError;

    use super::build_subject_token;

    const PROVIDER: &str = "//iam.googleapis.com/projects/123456789/locations/global/workloadIdentityPools/restate-cloud/providers/aws-federation";

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
    /// is the environment-isolation boundary this authentication scheme relies on.
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

    // The wire test uses pre-cached AWS credentials and a local Google STS stand-in. It never
    // resolves ambient AWS credentials or touches the process-global federation configuration.

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

    use super::{AWS4_SUBJECT_TOKEN_TYPE, AwsFederationCredentials, AwsSubjectTokenProvider};

    fn fixture_aws_federation_credentials() -> Arc<AwsFederationCredentials> {
        let credentials = fixed_credentials();
        Arc::new(AwsFederationCredentials {
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

    /// Pins the subject-token wire contract at the `ProgrammaticBuilder` integration boundary.
    /// The test stops at Google STS because the IAM impersonation URL is not configurable.
    #[restate_core::test]
    async fn programmatic_builder_sts_exchange_matches_expected_wire_format() {
        let env_before = aws_env_snapshot();

        let aws_federation_credentials = fixture_aws_federation_credentials();
        let subject_token_provider = Arc::new(AwsSubjectTokenProvider {
            aws_federation_credentials,
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

        // Parsing the request body removes transport encoding. The field itself must still contain
        // our encoded AIP-4117 envelope.
        let subject_token_wire = form
            .get("subject_token")
            .expect("subject_token field present");
        assert!(
            serde_json::from_str::<serde_json::Value>(subject_token_wire).is_err(),
            "transport decoding must leave the subject-token envelope percent-encoded"
        );
        let decoded_wire: String = url::form_urlencoded::parse(subject_token_wire.as_bytes())
            .next()
            .map(|(k, _)| k.into_owned())
            .expect("subject token decodes to one key");
        let decoded_wire_json: serde_json::Value =
            serde_json::from_str(&decoded_wire).expect("decoded subject token is JSON");
        assert_eq!(decoded_wire_json["method"], "POST");
        assert_eq!(
            decoded_wire_json["url"],
            "https://sts.us-east-1.amazonaws.com/?Action=GetCallerIdentity&Version=2011-06-15"
        );
        assert!(
            decoded_wire_json["headers"]
                .as_array()
                .expect("headers are an array")
                .iter()
                .any(|header| {
                    header["key"]
                        .as_str()
                        .is_some_and(|key| key.eq_ignore_ascii_case("x-goog-cloud-target-resource"))
                        && header["value"] == PROVIDER
                })
        );

        assert_eq!(
            env_before,
            aws_env_snapshot(),
            "constructing and using a federated subject-token provider must not mutate any \
             AWS_* environment variable"
        );
    }

    #[test]
    fn accepts_a_well_formed_aws_role_arn() {
        super::validate_aws_role_arn("arn:aws:iam::123456789012:role/RestateCloudGcpFederation")
            .expect("well-formed ARN accepted");
        super::validate_aws_role_arn("arn:aws-us-gov:iam::123456789012:role/path/to/role")
            .expect("non-default partition with a role path accepted");
        // A role path admits every printable ASCII character, ':' included, so the resource is the
        // sixth field of the ARN rather than the last colon-separated token.
        super::validate_aws_role_arn("arn:aws:iam::123456789012:role/tenant:a/Federation")
            .expect("a colon inside the role path accepted");
    }

    #[test]
    fn rejects_malformed_aws_role_arns() {
        for arn in [
            "not-an-arn",
            "arn:aws:s3::123456789012:role/wrong-service",
            "arn:aws:iam:us-east-1:123456789012:role/has-a-region",
            "arn:aws:iam::12345:role/too-short-account",
            "arn:aws:iam::12345678901a:role/non-numeric-account",
            "arn:aws:iam::123456789012:user/not-a-role",
            "arn:aws:iam::123456789012:role/",
            "arn:aws:iam::123456789012:role/path/",
            "arn:aws:iam::123456789012:role/has a space",
            "arn:aws:iam::123456789012:role/has#hash",
            "arn:aws_:iam::123456789012:role/bad-partition-chars",
            "arn:aws-cn:iam::123456789012:role/unsupported-partition",
            "arn:aws-iso:iam::123456789012:role/unsupported-partition",
            "arn:aws!:iam::123456789012:role/bad-partition-chars",
        ] {
            super::validate_aws_role_arn(arn)
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
            super::validate_aws_role_session_name(name).expect("well-formed session name accepted");
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
            super::validate_aws_role_session_name(name)
                .expect_err(&format!("expected '{name}' to be rejected"));
        }
    }

    #[test]
    fn initialization_rejects_invalid_config() {
        for (aws_role_arn, aws_role_session_name) in [
            ("not-an-arn", "valid-session"),
            (
                "arn:aws:iam::123456789012:role/RestateCloudGcpFederation",
                "has a space",
            ),
        ] {
            let config = super::GcpFederationOptions {
                aws_role_arn: aws_role_arn.to_owned(),
                aws_role_session_name: aws_role_session_name.to_owned(),
            };
            super::initialize_config_once(&std::sync::OnceLock::new(), Some(config))
                .expect_err("invalid federation configuration must be rejected");
        }
    }

    fn fixture_config(aws_role_arn: &str) -> super::GcpFederationOptions {
        super::GcpFederationOptions {
            aws_role_arn: aws_role_arn.to_owned(),
            aws_role_session_name: "session".to_owned(),
        }
    }

    #[test]
    fn initialization_captures_the_first_value_including_absence() {
        let a = fixture_config("arn:aws:iam::123456789012:role/A");
        let absent_first = std::sync::OnceLock::new();
        super::initialize_config_once(&absent_first, None).expect("absence initializes config");
        super::initialize_config_once(&absent_first, Some(a.clone()))
            .expect("later configuration is ignored");
        assert_eq!(absent_first.get(), Some(&None));

        let configured_first = std::sync::OnceLock::new();
        super::initialize_config_once(&configured_first, Some(a.clone()))
            .expect("initial configuration is valid");
        super::initialize_config_once(&configured_first, None).expect("later absence is ignored");
        assert_eq!(configured_first.get(), Some(&Some(a)));
    }

    fn assume_role_service_error(
        code: &'static str,
    ) -> aws_credential_types::provider::error::CredentialsError {
        use aws_sdk_sts::error::SdkError;
        use aws_sdk_sts::operation::assume_role::AssumeRoleError;
        use aws_smithy_runtime_api::client::orchestrator::HttpResponse;
        use aws_smithy_runtime_api::http::StatusCode;
        use aws_smithy_types::body::SdkBody;
        use aws_smithy_types::error::ErrorMetadata;

        let service_error = AssumeRoleError::generic(
            ErrorMetadata::builder()
                .code(code)
                .message("simulated AssumeRole failure")
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
    fn assume_role_service_errors_are_classified() {
        for (code, expected_transient) in [
            ("AccessDenied", false),
            ("AccessDeniedException", false),
            ("MalformedPolicyDocument", false),
            ("PackedPolicyTooLarge", false),
            ("RegionDisabledException", false),
            ("ValidationError", false),
            ("ExpiredTokenException", true),
        ] {
            let raw = assume_role_service_error(code);
            assert_eq!(super::assume_role_error_code(&raw), Some(code));
            let error = super::federation_error_from_assume_role_failure(&raw);
            assert_eq!(
                error.is_transient(),
                expected_transient,
                "unexpected classification for {code}"
            );
        }
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

    #[test]
    fn federated_access_token_source_upgrades_while_live_and_replaces_once_dead() {
        let provider = "//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/p/providers/lookup-test";
        let sources: super::FederatedAccessTokenSourceIndex = Default::default();

        let first = sources.get_or_create(provider);
        let second = sources.get_or_create(provider);
        assert!(
            Arc::ptr_eq(&first, &second),
            "a live provider must always resolve to the same access-token source"
        );

        let first_weak = Arc::downgrade(&first);
        drop(first);
        drop(second);
        assert!(
            first_weak.upgrade().is_none(),
            "the original source must die after its last strong reference drops"
        );

        let third = sources.get_or_create(provider);
        let indexed = sources
            .weak_for_test(provider)
            .expect("the dead tombstone must be replaced")
            .upgrade()
            .expect("the replacement must be live");
        assert!(
            Arc::ptr_eq(&third, &indexed),
            "the index must point at the fresh source returned by the lookup"
        );
    }
}
