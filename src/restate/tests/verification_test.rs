use std::process::Command;

use assert_cmd::prelude::*;
use hyper::StatusCode;

// Run programs // Add methods on commands

#[tokio::test]
#[ignore]
async fn verification() -> Result<(), Box<dyn std::error::Error>> {
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (kill_tx, kill_rx) = tokio::sync::oneshot::channel();

    let restate = tokio::task::spawn(async {
        let mut child = Command::cargo_bin("restate")
            .unwrap()
            .env(
                "RUST_LOG",
                "debug,tokio=debug,runtime=debug,hyper=warn,h2=warn,mio=warn",
            )
            .spawn()
            .unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
        ready_tx.send(()).unwrap();
        kill_rx.await.unwrap();
        child.kill().unwrap();
    });

    let test = tokio::task::spawn(async {
        ready_rx.await.unwrap();

        let client = hyper::Client::new();

        let req = hyper::Request::builder()
            .method(hyper::Method::POST)
            .uri("http://127.0.0.1:8081/endpoint/discover")
            .header("Content-Type", "application/json")
            .body(hyper::Body::from(r#"{"uri": "http://127.0.0.1:8000"}"#))
            .unwrap();

        let resp = client.request(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "bad status code {} for discover: {}",
            resp.status(),
            String::from_utf8(
                hyper::body::to_bytes(resp.into_body())
                    .await
                    .unwrap()
                    .to_vec()
            )
            .unwrap()
        );
        let body = r#"{"width": 10, "depth": 4}"#;

        let req = hyper::Request::builder()
            .method(hyper::Method::POST)
            .uri("http://127.0.0.1:9090/verifier.CommandVerifier/Execute")
            .header("Content-Type", "application/json")
            .body(hyper::Body::from(body))
            .unwrap();

        let resp = client.request(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "bad status code {} for Execute: {}",
            resp.status(),
            String::from_utf8(
                hyper::body::to_bytes(resp.into_body())
                    .await
                    .unwrap()
                    .to_vec()
            )
            .unwrap()
        );

        // wait a bit for the various non synchronous calls to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10000)).await;

        let req = hyper::Request::builder()
            .method(hyper::Method::POST)
            .uri("http://127.0.0.1:9090/verifier.CommandVerifier/Verify")
            .header("Content-Type", "application/json")
            .body(hyper::Body::from(body))
            .unwrap();

        let resp = client.request(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "bad status code {} for Verify: {}",
            resp.status(),
            String::from_utf8(
                hyper::body::to_bytes(resp.into_body())
                    .await
                    .unwrap()
                    .to_vec()
            )
            .unwrap()
        );

        let req = hyper::Request::builder()
            .method(hyper::Method::POST)
            .uri("http://127.0.0.1:9090/verifier.CommandVerifier/Clear")
            .header("Content-Type", "application/json")
            .body(hyper::Body::from(body))
            .unwrap();

        let resp = client.request(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "bad status code {} for Clear: {}",
            resp.status(),
            String::from_utf8(
                hyper::body::to_bytes(resp.into_body())
                    .await
                    .unwrap()
                    .to_vec()
            )
            .unwrap()
        );
    });

    let test_result = test.await;
    kill_tx.send(()).unwrap();
    restate.await?;
    test_result?;
    Ok(())
}
