// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The request identity `target` claim must name the request exactly as the deployment receives
//! it, so a verifier that sees the whole request line can compare the two byte for byte.

use std::collections::HashSet;
use std::convert::Infallible;
use std::io::Write;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde::Deserialize;
use tokio::net::TcpListener;

use restate_service_client::{AssumeRoleCacheMode, Endpoint, Method, Parts, ServiceClient};
use restate_types::config::ServiceClientOptions;

const PRIVATE_KEY: &[u8] = br#"-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIPe++4ZTPQDF81otpoU/mOGHC2vOAVp9WbiCblvn3nXO
-----END PRIVATE KEY-----"#;
const PUBLIC_KEY: &[u8] = br#"-----BEGIN PUBLIC KEY-----
MCowBQYDK2VwAyEAj5BTvH+WJo0QGHm2hdLOuk6P7szKgTQxmpnnmZe/DcU=
-----END PUBLIC KEY-----"#;

/// Method, path and query, and `x-restate-jwt-v1` of the last request received.
type Received = Arc<Mutex<Option<(String, String, String)>>>;

#[derive(Deserialize)]
struct Claims {
    aud: String,
    target: Target,
}

#[derive(Deserialize)]
struct Target {
    method: String,
    path_and_query: String,
}

async fn recorder() -> (SocketAddr, Received) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let received: Received = Arc::default();
    let received_for_task = Arc::clone(&received);

    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let received = Arc::clone(&received_for_task);
            tokio::spawn(async move {
                let svc = service_fn(move |req: Request<Incoming>| {
                    *received.lock().unwrap() = Some((
                        req.method().to_string(),
                        req.uri().path_and_query().unwrap().to_string(),
                        req.headers()["x-restate-jwt-v1"]
                            .to_str()
                            .unwrap()
                            .to_owned(),
                    ));
                    async {
                        Ok::<_, Infallible>(
                            Response::builder()
                                .status(StatusCode::OK)
                                .body(Full::new(Bytes::new()))
                                .unwrap(),
                        )
                    }
                });
                let _ = http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), svc)
                    .await;
            });
        }
    });

    (addr, received)
}

#[tokio::test]
async fn target_claim_matches_the_request_the_deployment_receives() {
    let mut pem = tempfile::NamedTempFile::new().unwrap();
    pem.write_all(PRIVATE_KEY).unwrap();
    let mut options = ServiceClientOptions::default();
    options.request_identity_private_key_pem_file = Some(pem.path().to_path_buf());
    let client = ServiceClient::from_options(&options, AssumeRoleCacheMode::Unbounded).unwrap();

    let mut validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::EdDSA);
    validation.required_spec_claims = HashSet::from(["aud".into(), "exp".into()]);
    validation.validate_aud = false;
    let decoding_key = jsonwebtoken::DecodingKey::from_ed_pem(PUBLIC_KEY).unwrap();

    let (addr, received) = recorder().await;

    for (base, method, path, expected) in [
        ("", Method::Post, "/discover", "/discover"),
        ("/", Method::Get, "/discover", "/discover"),
        (
            "/base/",
            Method::Post,
            "/invoke/Greeter/greet",
            "/base/invoke/Greeter/greet",
        ),
        (
            "/base//",
            Method::Post,
            "/invoke/Greeter/greet",
            "/base//invoke/Greeter/greet",
        ),
        (
            "/base?tenant=a",
            Method::Post,
            "/serdes/Greeter/encode/Name",
            "/base/serdes/Greeter/encode/Name?tenant=a",
        ),
    ] {
        let uri: hyper::Uri = format!("http://{addr}{base}").parse().unwrap();
        let parts = Parts::new(
            method,
            Endpoint::Http(uri, Some(http::Version::HTTP_11), None),
            hyper::http::uri::PathAndQuery::from_static(path),
            hyper::HeaderMap::new(),
        );
        let response = client
            .call(restate_service_client::Request::new(
                parts,
                Full::new(Bytes::new()),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let (received_method, received_path, jwt) = received.lock().unwrap().take().unwrap();
        let claims = jsonwebtoken::decode::<Claims>(&jwt, &decoding_key, &validation)
            .unwrap()
            .claims;

        assert_eq!(received_path, expected, "base {base:?}");
        assert_eq!(claims.target.path_and_query, received_path, "base {base:?}");
        assert_eq!(claims.target.method, received_method, "base {base:?}");
        assert_eq!(claims.aud, path, "aud stays relative to the deployment");
    }
}
