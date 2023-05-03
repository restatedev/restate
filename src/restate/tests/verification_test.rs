extern crate core;

use crate::Method::{Clear, Execute, Verify};
use assert_cmd::prelude::*;
use core::fmt;
use futures_util::TryFutureExt;
use hyper::{http, StatusCode};
use rand::distributions::{Alphanumeric, DistString};
use restate_common::retry_policy::RetryPolicy;
use restate_test_utils::test;
use std::process::Command;
use tempfile::Builder;
use tracing::{info, warn};

struct SafeChild(std::process::Child);

impl Drop for SafeChild {
    fn drop(&mut self) {
        match self.0.kill() {
            Err(e) => warn!("Could not kill child process: {}", e),
            Ok(_) => info!("Successfully killed child process"),
        }
    }
}

#[test(tokio::test)]
#[ignore = "Ignored because it requires the verification service running on localhost:8000. See .github/workflows/ci.yaml and https://github.com/restatedev/restate-verification for more details."]
async fn verification() -> Result<(), Box<dyn std::error::Error>> {
    let seed = Alphanumeric.sample_string(&mut rand::thread_rng(), 16);
    let seed = seed.as_str();
    let width = 10;
    let depth = 4;
    let dir = Builder::new().prefix("restate").tempdir()?;

    info!(
        "using test parameters seed: {seed}, width: {width}, depth: {depth}, dir: {}",
        dir.path().display(),
    );

    let child = Command::cargo_bin("restate")?
        .env("RUST_LOG", "debug,hyper=warn,h2=warn,mio=warn")
        .current_dir(dir.path())
        .spawn()?;
    let mut _safe_child = SafeChild(child);

    let client = &hyper::Client::new();

    let policy = RetryPolicy::fixed_delay(std::time::Duration::from_secs(1), 30);

    policy
        .retry_operation(|| discover(client, "http://127.0.0.1:8000"))
        .await?;

    let policy = RetryPolicy::fixed_delay(std::time::Duration::from_secs(2), 500);

    let verification_result = call(client, Execute, seed, width, depth)
        .and_then(|_| policy.retry_operation(|| call(client, Verify, seed, width, depth)))
        .await;

    // try and call clear even if verification failed
    call(client, Clear, seed, width, depth).await?;

    verification_result.unwrap_or_else(|e| {
        panic!("verification failed with seed {seed}, width {width}, depth {depth} and error {e:?}")
    });
    Ok(())
}

async fn discover(
    client: &hyper::Client<hyper::client::HttpConnector, hyper::Body>,
    uri: &str,
) -> Result<(), RequestError> {
    request(
        client,
        "http://127.0.0.1:8081/endpoint/discover".into(),
        format!("{{\"uri\": \"{}\"}}", uri),
    )
    .await
    .map(|_| ())
}

#[derive(Debug)]
enum Method {
    Execute,
    Verify,
    Clear,
}

impl fmt::Display for Method {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(thiserror::Error, Debug)]
enum CallError {
    #[error("error calling {0}: {1}")]
    Request(Method, RequestError),
}

async fn call(
    client: &hyper::Client<hyper::client::HttpConnector, hyper::Body>,
    method: Method,
    seed: &str,
    width: i32,
    depth: i32,
) -> Result<String, CallError> {
    request(
        client,
        format!(
            "http://127.0.0.1:9090/verifier.CommandVerifier/{:?}",
            method
        ),
        format!(
            "{{\"params\": {{\"seed\": \"{}\", \"width\": {}, \"depth\": {}}}}}",
            seed, width, depth
        ),
    )
    .await
    .map_err(|e| CallError::Request(method, e))
}

#[derive(thiserror::Error, Debug)]
enum RequestError {
    #[error(transparent)]
    Http(#[from] http::Error),
    #[error(transparent)]
    Hyper(#[from] hyper::Error),
    #[error(transparent)]
    FailedToReadBody(#[from] std::string::FromUtf8Error),
    #[error("status code {0} is not OK: {1}")]
    BadStatusCode(StatusCode, String),
}

async fn request(
    client: &hyper::Client<hyper::client::HttpConnector, hyper::Body>,
    uri: String,
    body: String,
) -> Result<String, RequestError> {
    let req = hyper::Request::builder()
        .method(hyper::Method::POST)
        .uri(uri)
        .header("Content-Type", "application/json")
        .body(hyper::Body::from(body))?;
    let resp = client.request(req).await?;
    let status = resp.status();
    let resp_body = String::from_utf8(hyper::body::to_bytes(resp.into_body()).await?.to_vec())?;
    match status {
        StatusCode::OK => Ok(resp_body),
        _ => Err(RequestError::BadStatusCode(status, resp_body)),
    }
}
