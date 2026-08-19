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
use aws_credential_types::provider::ProvideCredentials;
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
/// (see [`install_config`]). `None` means the operator never configured the block: every
/// federated construction then fails with a permanent, actionable [`GcpAuthError::Build`] rather
/// than falling back to an unauthenticated request.
static FEDERATION_CONFIG: std::sync::OnceLock<Option<GcpFederationOptions>> =
    std::sync::OnceLock::new();

/// Install the process-wide `[gcp-federation]` config. Every `ServiceClient` built from the same
/// `ServiceClientOptions` calls this, so installing an identical config (including `None`) is a
/// no-op; installing a config that differs from one already installed only warns and keeps the
/// first one, since a broker built from it may already be in use by in-flight federated mints.
pub(crate) fn install_config(config: Option<GcpFederationOptions>) {
    match FEDERATION_CONFIG.get() {
        None => {
            let _ = FEDERATION_CONFIG.set(config);
        }
        Some(installed) if *installed == config => {}
        Some(_) => tracing::warn!(
            "a [gcp-federation] configuration is already installed for this process; ignoring a \
             differing re-install attempt"
        ),
    }
}

/// The shared AWS broker identity: one role assumption for the whole process, reused by every
/// federated [`AwsSubjectTokenProvider`]. Lazily constructed on the first federated construction.
struct Broker {
    /// Resolved once, from the AWS SDK default chain, at broker construction.
    region: String,
    provider: AssumeRoleProvider,
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
            provider,
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
        let fresh = self.provider.provide_credentials().await.map_err(|e| {
            FederationError::transient(format!(
                "assuming the GCP workload identity federation broker role: {e}"
            ))
        })?;
        *guard = Some(fresh.clone());
        Ok(fresh)
    }
}

static BROKER: OnceCell<Arc<Broker>> = OnceCell::const_new();

/// Returns the shared broker, constructing it on first use. Construction failure (missing
/// `[gcp-federation]` config, or no AWS region resolvable) is not cached: [`OnceCell::get_or_try_init`]
/// leaves the cell empty on `Err`, so the next attempt retries, exactly like a fixed misconfiguration
/// being picked up immediately elsewhere in this module.
async fn broker(audience: &str) -> Result<Arc<Broker>, GcpAuthError> {
    let Some(config) = FEDERATION_CONFIG.get().cloned().flatten() else {
        return Err(GcpAuthError::Build {
            audience: audience.to_owned(),
            message: "this deployment requests GCP workload identity federation, but the server \
                      has no [gcp-federation] configuration; set broker-role-arn and \
                      session-name to enable it"
                .to_owned(),
        });
    };
    BROKER
        .get_or_try_init(|| async { Broker::init(&config).await.map(Arc::new) })
        .await
        .cloned()
        .map_err(|message| GcpAuthError::Build {
            audience: audience.to_owned(),
            message,
        })
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
/// resulting [`IdTokenSource`]: broker credentials -> SigV4 subject token -> Google STS exchange
/// -> impersonation. Runs on the auth runtime, under the same construction semaphore as the
/// ambient/impersonated paths (see `Registry::get_or_build`).
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

    let broker = broker(&audience).await?;
    let subject_token_provider = Arc::new(AwsSubjectTokenProvider {
        broker,
        provider_resource: wif_provider.clone(),
    });

    let source_credentials = ProgrammaticBuilder::new(subject_token_provider)
        .with_audience(wif_provider.clone())
        .with_subject_token_type(AWS4_SUBJECT_TOKEN_TYPE)
        .with_token_url(GOOGLE_STS_TOKEN_URL)
        .build()
        .map_err(|e| GcpAuthError::Build {
            audience: audience.clone(),
            message: format!("building GCP workload identity federation credentials: {e}"),
        })?;

    let credentials = idtoken::impersonated::Builder::from_source_credentials(
        audience.clone(),
        impersonate,
        source_credentials,
    )
    .build()
    .map_err(|e| GcpAuthError::Build {
        audience,
        message: e.to_string(),
    })?;

    Ok(Arc::new(Live(credentials)) as Arc<dyn IdTokenSource>)
}

#[cfg(test)]
mod federation_tests {
    use std::time::{Duration, SystemTime};

    use aws_credential_types::Credentials as AwsCredentials;

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

    /// Signing is deterministic for fixed credentials, region, target and time -- so a change to
    /// the envelope construction shows up as a test failure rather than an opaque 400 from Google
    /// STS.
    #[test]
    fn signing_is_deterministic() {
        let a = build_subject_token(&fixed_credentials(), "eu-central-1", PROVIDER, fixed_time())
            .unwrap();
        let b = build_subject_token(&fixed_credentials(), "eu-central-1", PROVIDER, fixed_time())
            .unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn a_different_provider_yields_a_different_signature() {
        let a =
            build_subject_token(&fixed_credentials(), "us-east-1", PROVIDER, fixed_time()).unwrap();
        let b = build_subject_token(
            &fixed_credentials(),
            "us-east-1",
            &PROVIDER.replace("aws-broker", "other-provider"),
            fixed_time(),
        )
        .unwrap();
        assert_ne!(a, b);
    }

    /// Pins the encoding convention documented on `AwsSubjectTokenProvider::subject_token`:
    /// `url::form_urlencoded::byte_serialize` is exactly what
    /// `external_account_sources::aws_sourced::AwsSourcedCredentials` applies to its own JSON
    /// envelope before returning it as a subject token. If a future google-cloud-auth version
    /// changes that convention, this test documents what we must match, even though it can only
    /// assert our own side of the encoding (the built-in source is a private crate module we
    /// cannot call directly).
    #[test]
    fn subject_token_matches_aws_sourced_encoding_convention() {
        let envelope =
            build_subject_token(&fixed_credentials(), "us-east-1", PROVIDER, fixed_time()).unwrap();
        let encoded: String = url::form_urlencoded::byte_serialize(envelope.as_bytes()).collect();

        // Encoded exactly once: byte_serialize percent-encodes every `&`/`=` in the JSON away
        // (alongside `:`, `"`, and everything else outside the unreserved set), so parsing the
        // encoded string as a form body yields exactly one key with an empty value -- and
        // decoding that key recovers the original JSON exactly.
        assert_ne!(encoded, envelope);
        let mut pairs = url::form_urlencoded::parse(encoded.as_bytes());
        let (key, value) = pairs.next().expect("at least one pair");
        assert!(
            pairs.next().is_none(),
            "expected exactly one pair: the envelope's own literal '&' bytes must have been \
             encoded away"
        );
        assert_eq!(
            value, "",
            "the envelope's own literal '=' bytes must have been encoded away"
        );
        assert_eq!(key, envelope);
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
    use tokio::sync::Mutex as AsyncMutex;

    use super::{AWS4_SUBJECT_TOKEN_TYPE, AwsSubjectTokenProvider, Broker};

    /// A `Broker` that never performs AWS network I/O: its cached credentials never expire, so
    /// `Broker::credentials()` always takes the cache-hit path. `AssumeRoleProvider::builder(..).build()`
    /// itself does no I/O (only `provide_credentials()` would), and an explicit region on the
    /// `SdkConfig` passed via `.configure(..)` means construction never touches
    /// `aws_config::load_defaults()`'s environment/IMDS probing either.
    async fn fixture_broker() -> Arc<Broker> {
        use aws_config::sts::AssumeRoleProvider;
        use aws_config::{BehaviorVersion, Region};

        // `aws_config::defaults(..).region(..).load()` fills in a time source, sleep
        // implementation, and HTTP client without any network I/O: region is explicitly
        // overridden here, and credential-provider resolution is lazy (only the later
        // `provide_credentials()` call would touch the network, which this fixture never makes --
        // its cached credentials never expire).
        let sdk_config = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .load()
            .await;
        let provider = AssumeRoleProvider::builder("arn:aws:iam::123456789012:role/unused-in-test")
            .configure(&sdk_config)
            .session_name("federation-wire-format-test")
            .build()
            .await;

        Arc::new(Broker {
            region: "us-east-1".to_owned(),
            provider,
            cached: AsyncMutex::new(Some(AwsCredentials::for_tests())),
        })
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

    /// Exercises the real `AwsSubjectTokenProvider` + `ProgrammaticBuilder` path (design item 9e):
    /// the exchange request's `grant_type`/`subject_token_type`/`audience` fields, and that the
    /// AIP-4117 JSON arrives at Google percent-encoded exactly once after undoing the STS
    /// exchange's own transport form-encoding -- matching the convention
    /// `external_account_sources::aws_sourced::AwsSourcedCredentials` uses (see
    /// `subject_token_matches_aws_sourced_encoding_convention` above for the pinned encoding
    /// itself). Stops at the STS hop: `idtoken::impersonated::Builder`'s impersonation URL has no
    /// public override in google-cloud-auth 1.15 (`ImpersonationUrl::endpoint` is `pub(crate)`
    /// there), so `generateIdToken` cannot be redirected to a local mock from outside the crate.
    #[tokio::test]
    async fn programmatic_builder_sts_exchange_matches_expected_wire_format() {
        let broker = fixture_broker().await;
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
    }

    /// AWS credential isolation (design item 9d): constructing and using a federated subject-token
    /// provider must never mutate any `AWS_*` environment variable. Exercises the same real chain
    /// as the wire-format test above (broker credentials fetch, SigV4 signing, STS exchange), so
    /// this is a property of the actual code path, not just a documentation claim.
    #[tokio::test]
    async fn federated_chain_never_mutates_aws_environment_variables() {
        fn aws_env_snapshot() -> std::collections::BTreeMap<String, String> {
            std::env::vars()
                .filter(|(k, _)| k.starts_with("AWS_"))
                .collect()
        }

        let before = aws_env_snapshot();

        let broker = fixture_broker().await;
        let subject_token_provider = AwsSubjectTokenProvider {
            broker,
            provider_resource: PROVIDER.to_owned(),
        };
        let (token_url, _captured) = mock_google_sts().await;
        let credentials = ProgrammaticBuilder::new(Arc::new(subject_token_provider))
            .with_audience(PROVIDER)
            .with_subject_token_type(AWS4_SUBJECT_TOKEN_TYPE)
            .with_token_url(token_url)
            .build()
            .expect("credentials build");
        let _ = credentials.headers(http::Extensions::new()).await;

        assert_eq!(
            before,
            aws_env_snapshot(),
            "constructing and using a federated subject-token provider must not mutate any \
             AWS_* environment variable"
        );
    }
}
