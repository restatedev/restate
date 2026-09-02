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
fn subject_token_has_the_shape_and_replay_binding_google_sts_expects() {
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

    // The provider resource name must be covered by the signature. Otherwise, an envelope
    // minted for one identity pool could be replayed against another.
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
        super::validate_aws_role_arn(arn).expect_err(&format!("expected '{arn}' to be rejected"));
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

fn assume_role_dispatch_timeout_error() -> aws_credential_types::provider::error::CredentialsError {
    use aws_sdk_sts::error::SdkError;
    use aws_sdk_sts::operation::assume_role::AssumeRoleError;

    let sdk_error: SdkError<AssumeRoleError> =
        SdkError::timeout_error("connect timed out reaching sts.us-east-1.amazonaws.com");
    aws_credential_types::provider::error::CredentialsError::provider_error(sdk_error)
}

#[test]
fn assume_role_failures_are_classified() {
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

    let raw = assume_role_dispatch_timeout_error();
    assert_eq!(super::assume_role_error_code(&raw), None);
    let error = super::federation_error_from_assume_role_failure(&raw);
    assert!(
        error.is_transient(),
        "an AssumeRole connector/timeout failure must classify as transient, got {error:?}"
    );
}

#[derive(Debug)]
struct HangingCredentialsProvider;

impl aws_credential_types::provider::ProvideCredentials for HangingCredentialsProvider {
    fn provide_credentials<'a>(
        &'a self,
    ) -> aws_credential_types::provider::future::ProvideCredentials<'a>
    where
        Self: 'a,
    {
        aws_credential_types::provider::future::ProvideCredentials::new(std::future::pending())
    }
}

#[tokio::test(start_paused = true)]
async fn assume_role_request_is_bounded() {
    let credentials = super::AwsFederationCredentials {
        region: "us-east-1".to_owned(),
        provider: aws_credential_types::provider::SharedCredentialsProvider::new(
            HangingCredentialsProvider,
        ),
        cached: tokio::sync::Mutex::new(None),
    };

    let started = tokio::time::Instant::now();
    let error = credentials.credentials().await.unwrap_err();
    assert!(error.is_transient());
    assert_eq!(started.elapsed(), super::AWS_ASSUME_ROLE_TIMEOUT);
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
