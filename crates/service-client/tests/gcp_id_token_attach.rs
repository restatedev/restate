// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Integration test for the GCP OIDC ID-token bearer-attach path.
//!
//! Spins up a local HTTP test server that captures the headers a
//! ServiceClient dispatch sends to it. Verifies that the minted token
//! always lands in `X-Serverless-Authorization` with the correct
//! audience claim; Cloud Run validates that header in precedence over
//! `Authorization` and strips it before forwarding to the container,
//! leaving any customer-supplied `Authorization` in `additional_headers`
//! to pass through to the workload.
//!
//! The token is pre-seeded into `GcpTokenClient`'s cache so the test
//! does not depend on ADC discovery or external network. The
//! google-cloud-auth crate is exercised in its own test suite.

use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use base64::Engine;
use bytes::Bytes;
use bytestring::ByteString;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use restate_service_client::{Endpoint, Method as ClientMethod, Parts, ServiceClient};
use restate_types::config::ServiceClientOptions;
use restate_types::deployment::{GoogleIdTokenAuth, HttpAuth};
use tokio::net::TcpListener;

type RecordedHeaders = Arc<Mutex<HashMap<String, String>>>;

/// Synthesize a JWT-shaped string (header.payload.signature) with a payload containing the given
/// `aud` claim. The token is not cryptographically signed because the bearer-attach path does not
/// verify it; Cloud Run would verify against Google's JWKS at the receiving end.
fn fake_jwt_with_audience(audience: &str) -> String {
    let header =
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(br#"{"alg":"none","typ":"JWT"}"#);
    let exp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 3600;
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(
        serde_json::json!({"aud": audience, "exp": exp})
            .to_string()
            .as_bytes(),
    );
    let signature = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"sig");
    format!("{header}.{payload}.{signature}")
}

fn extract_audience(bearer: &str) -> String {
    let token = bearer.strip_prefix("Bearer ").expect("bearer prefix");
    let payload_b64 = token.split('.').nth(1).expect("payload segment");
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload_b64)
        .expect("payload base64");
    let v: serde_json::Value = serde_json::from_slice(&payload).expect("payload json");
    v["aud"].as_str().expect("aud claim").to_owned()
}

/// Stand up a tiny HTTP server bound to 127.0.0.1:0 that records the headers of any incoming
/// request and returns 200 OK with an empty body. Returns the bound address and the headers handle.
async fn upstream_recorder() -> (SocketAddr, RecordedHeaders) {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("bind upstream test server");
    let addr = listener.local_addr().expect("local_addr");
    let recorded: RecordedHeaders = Arc::new(Mutex::new(HashMap::new()));
    let recorded_for_task = Arc::clone(&recorded);

    tokio::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(pair) => pair,
                Err(_) => return,
            };
            let recorded = Arc::clone(&recorded_for_task);
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let svc = service_fn(move |req: Request<Incoming>| {
                    let recorded = Arc::clone(&recorded);
                    async move {
                        let mut headers = recorded.lock().unwrap();
                        for (name, value) in req.headers() {
                            headers.insert(
                                name.as_str().to_lowercase(),
                                value.to_str().unwrap_or_default().to_owned(),
                            );
                        }
                        Ok::<_, Infallible>(
                            Response::builder()
                                .status(StatusCode::OK)
                                .header(
                                    "content-type",
                                    "application/vnd.restate.invocation.v4+json",
                                )
                                .body(Full::new(Bytes::new()))
                                .expect("response build"),
                        )
                    }
                });
                let _ = http1::Builder::new().serve_connection(io, svc).await;
            });
        }
    });

    (addr, recorded)
}

fn build_service_client() -> ServiceClient {
    ServiceClient::from_options(
        &ServiceClientOptions::default(),
        restate_service_client::AssumeRoleCacheMode::Unbounded,
    )
    .expect("ServiceClient construction")
}

async fn dispatch(
    client: &ServiceClient,
    endpoint: Endpoint,
    extra_headers: hyper::HeaderMap,
) -> Response<impl http_body::Body<Data = Bytes>> {
    let parts = Parts::new(
        ClientMethod::Post,
        endpoint,
        hyper::http::uri::PathAndQuery::from_static("/discover"),
        extra_headers,
    );
    let body: Full<Bytes> = Full::new(Bytes::new());
    let req = restate_service_client::Request::new(parts, body);
    client
        .call(req)
        .await
        .expect("dispatch succeeds against local upstream")
}

#[tokio::test]
async fn bearer_attached_with_persisted_audience() {
    // The persisted record always carries a concrete audience (the REST boundary derives it from
    // the deployment URI before persisting). The dispatch path reads that value verbatim.
    let (upstream_addr, recorded) = upstream_recorder().await;
    let upstream_uri: hyper::Uri = format!("http://{upstream_addr}/").parse().unwrap();
    let expected_audience = format!("http://{upstream_addr}");

    let client = build_service_client();
    let token = fake_jwt_with_audience(&expected_audience);
    client.gcp_for_test().seed_for_test(
        None,
        &expected_audience,
        token.clone(),
        Duration::from_secs(3600),
    );

    let auth = HttpAuth::GoogleIdToken(GoogleIdTokenAuth::new(
        ByteString::from(expected_audience.clone()),
        None,
    ));
    let response = dispatch(
        &client,
        Endpoint::Http(upstream_uri, None, Some(auth)),
        hyper::HeaderMap::new(),
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);

    let captured = recorded.lock().unwrap();
    let bearer = captured
        .get("x-serverless-authorization")
        .cloned()
        .expect("X-Serverless-Authorization header present");
    assert!(
        bearer.starts_with("Bearer "),
        "X-Serverless-Authorization header should start with 'Bearer ', got {bearer}"
    );
    assert_eq!(extract_audience(&bearer), expected_audience);
    assert!(
        !captured.contains_key("authorization"),
        "Authorization must not appear when the deployment does not supply one"
    );
}

#[tokio::test]
async fn customer_authorization_passes_through_alongside_minted_xsa() {
    // Cloud Run strips X-Serverless-Authorization before forwarding to the container; placing the
    // minted token there leaves Authorization free for the customer's own app-level auth.
    let (upstream_addr, recorded) = upstream_recorder().await;
    let upstream_uri: hyper::Uri = format!("http://{upstream_addr}/").parse().unwrap();
    let expected_audience = format!("http://{upstream_addr}");

    let client = build_service_client();
    let token = fake_jwt_with_audience(&expected_audience);
    client.gcp_for_test().seed_for_test(
        None,
        &expected_audience,
        token.clone(),
        Duration::from_secs(3600),
    );

    let mut extra = hyper::HeaderMap::new();
    extra.insert(
        hyper::header::AUTHORIZATION,
        hyper::http::HeaderValue::from_static("Bearer user-supplied-token"),
    );

    let auth = HttpAuth::GoogleIdToken(GoogleIdTokenAuth::new(
        ByteString::from(expected_audience.clone()),
        None,
    ));
    let response = dispatch(
        &client,
        Endpoint::Http(upstream_uri, None, Some(auth)),
        extra,
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);

    let captured = recorded.lock().unwrap();
    assert_eq!(
        captured.get("authorization").cloned().unwrap(),
        "Bearer user-supplied-token",
        "deployment-provided Authorization should be forwarded unchanged"
    );
    let x_bearer = captured
        .get("x-serverless-authorization")
        .cloned()
        .expect("X-Serverless-Authorization header present");
    assert!(x_bearer.starts_with("Bearer "));
    assert_eq!(extract_audience(&x_bearer), expected_audience);
}

#[tokio::test]
async fn bearer_uses_explicit_audience_when_provided() {
    let (upstream_addr, recorded) = upstream_recorder().await;
    let upstream_uri: hyper::Uri = format!("http://{upstream_addr}/").parse().unwrap();
    let explicit_audience = "https://canonical.example.com";

    let client = build_service_client();
    let token = fake_jwt_with_audience(explicit_audience);
    client
        .gcp_for_test()
        .seed_for_test(None, explicit_audience, token, Duration::from_secs(3600));

    let auth = HttpAuth::GoogleIdToken(GoogleIdTokenAuth::new(
        ByteString::from_static(explicit_audience),
        None,
    ));
    let response = dispatch(
        &client,
        Endpoint::Http(upstream_uri, None, Some(auth)),
        hyper::HeaderMap::new(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let captured = recorded.lock().unwrap();
    let bearer = captured.get("x-serverless-authorization").cloned().unwrap();
    assert_eq!(extract_audience(&bearer), explicit_audience);
}

#[tokio::test]
async fn mint_failure_does_not_send_unauthenticated_request() {
    // A token-mint failure must NOT trigger a fallback unauthenticated request. The upstream server
    // must never receive a request when mint fails.
    let (upstream_addr, recorded) = upstream_recorder().await;
    let upstream_uri: hyper::Uri = format!("http://{upstream_addr}/").parse().unwrap();

    let client = build_service_client();
    client
        .gcp_for_test()
        .force_mint_failure_for_test("simulated ADC failure");

    let auth = HttpAuth::GoogleIdToken(GoogleIdTokenAuth::new(
        ByteString::from_static("https://example.test"),
        None,
    ));

    let parts = Parts::new(
        ClientMethod::Post,
        Endpoint::Http(upstream_uri, None, Some(auth)),
        hyper::http::uri::PathAndQuery::from_static("/discover"),
        hyper::HeaderMap::new(),
    );
    let body: Full<Bytes> = Full::new(Bytes::new());
    let req = restate_service_client::Request::new(parts, body);
    let result = client.call(req).await;

    assert!(
        result.is_err(),
        "dispatch must return Err when mint fails, got Ok"
    );

    // Give the upstream server a moment in case a wayward request was sent.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let captured = recorded.lock().unwrap();
    assert!(
        captured.is_empty(),
        "upstream must NOT receive any request when mint fails, got headers: {captured:?}"
    );
}

#[tokio::test]
async fn no_bearer_attached_when_auth_absent() {
    let (upstream_addr, recorded) = upstream_recorder().await;
    let upstream_uri: hyper::Uri = format!("http://{upstream_addr}/").parse().unwrap();

    let client = build_service_client();
    let response = dispatch(
        &client,
        Endpoint::Http(upstream_uri, None, None),
        hyper::HeaderMap::new(),
    )
    .await;
    let _body = response.into_body().collect().await; // drain

    let captured = recorded.lock().unwrap();
    assert!(
        !captured.contains_key("authorization"),
        "Authorization must not appear when auth field is absent (legacy behavior)"
    );
    assert!(
        !captured.contains_key("x-serverless-authorization"),
        "X-Serverless-Authorization must not appear when auth field is absent"
    );
}
