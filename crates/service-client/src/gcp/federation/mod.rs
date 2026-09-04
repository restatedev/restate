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

/// Bounds one AssumeRole request while the process-wide credential mutex is held.
const AWS_ASSUME_ROLE_TIMEOUT: Duration = Duration::from_secs(5);

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
        let fresh = tokio::time::timeout(
            AWS_ASSUME_ROLE_TIMEOUT,
            self.provider.provide_credentials(),
        )
        .await
        .map_err(|_| {
            FederationError::transient(format!(
                "assuming the AWS federation role for GCP authentication timed out after {:?}",
                AWS_ASSUME_ROLE_TIMEOUT
            ))
        })?
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
    pub(super) fn spawn_recovery(&'static self, provider: String, triggering_error: String) {
        let _ = TaskCenter::current().spawn_unmanaged(
            TaskKind::Credentials,
            "gcp-federated-access-token-source-recovery",
            async move { self.recover_if_dead(&provider, &triggering_error).await },
        );
    }

    pub(super) async fn recover_if_dead(&self, provider: &str, triggering_error: &str) {
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
                    triggering_error,
                    "replaced a federated GCP access-token source: its refresh task was proven dead"
                );
            }
            Ok(false) => {}
            Err(error) => {
                warn!(
                    provider_resource = %provider,
                    error = %error,
                    triggering_error,
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
mod tests;
