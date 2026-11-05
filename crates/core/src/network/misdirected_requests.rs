// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::{Ready, ready};
use std::task::{Context, Poll};

use futures::future::Either;
use http::header::HeaderValue;
use http::{HeaderName, Request, Response, StatusCode};
use tower::{Layer, Service};

const CLUSTER_NAME_HEADER: HeaderName = HeaderName::from_static("x-restate-cluster-name");

/// Rejects requests that name another cluster in `x-restate-cluster-name` with 421.
#[derive(Clone)]
pub struct MisdirectedRequestsLayer {
    cluster_name_header: HeaderValue,
}

impl MisdirectedRequestsLayer {
    pub fn new(cluster_name: impl Into<String>) -> Self {
        // The name lives for the process, keeping the per-request header clone a pointer copy.
        let cluster_name: &'static str = Box::leak(cluster_name.into().into_boxed_str());
        HeaderValue::from_str(cluster_name).expect(
            "reject-misdirected-requests requires cluster-name to be a valid header value; Configuration::validate enforces this",
        );

        Self {
            cluster_name_header: HeaderValue::from_static(cluster_name),
        }
    }
}

impl<S> Layer<S> for MisdirectedRequestsLayer {
    type Service = MisdirectedRequests<S>;

    fn layer(&self, inner: S) -> Self::Service {
        MisdirectedRequests {
            inner,
            cluster_name_header: self.cluster_name_header.clone(),
        }
    }
}

#[derive(Clone)]
pub struct MisdirectedRequests<S> {
    inner: S,
    cluster_name_header: HeaderValue,
}

impl<S> MisdirectedRequests<S> {
    fn is_misdirected<B>(&self, request: &Request<B>) -> bool {
        request
            .headers()
            .get_all(&CLUSTER_NAME_HEADER)
            .iter()
            .any(|value| value.as_bytes() != self.cluster_name_header.as_bytes())
    }

    fn rejection<B: Default>(&self) -> Response<B> {
        Response::builder()
            .status(StatusCode::MISDIRECTED_REQUEST)
            .header(CLUSTER_NAME_HEADER, self.cluster_name_header.clone())
            .body(B::default())
            .expect("valid rejection response")
    }
}

impl<S, ReqBody, B> Service<Request<ReqBody>> for MisdirectedRequests<S>
where
    S: Service<Request<ReqBody>, Response = Response<B>>,
    B: Default,
{
    type Response = Response<B>;
    type Error = S::Error;
    type Future = Either<Ready<Result<Response<B>, S::Error>>, S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<ReqBody>) -> Self::Future {
        if self.is_misdirected(&request) {
            Either::Left(ready(Ok(self.rejection())))
        } else {
            Either::Right(self.inner.call(request))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use http::Response;
    use tower::Service;

    use super::*;

    fn request(cluster_names: &[&str]) -> Request<axum::body::Body> {
        let mut request = Request::new(axum::body::Body::empty());
        for cluster_name in cluster_names {
            request
                .headers_mut()
                .append(CLUSTER_NAME_HEADER, cluster_name.parse().unwrap());
        }
        request
    }

    #[tokio::test]
    async fn guards_against_misdirected_requests() {
        let calls = Arc::new(AtomicUsize::new(0));
        let inner = tower::service_fn({
            let calls = Arc::clone(&calls);
            move |_request| {
                calls.fetch_add(1, Ordering::SeqCst);
                std::future::ready(Ok::<_, Infallible>(Response::new(axum::body::Body::from(
                    "served",
                ))))
            }
        });
        let mut guard = MisdirectedRequestsLayer::new("mine").layer(inner);

        // Matching and missing headers are served.
        let response = guard.call(request(&["mine"])).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let response = guard.call(request(&[])).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        // Every occurrence must match, and values are compared as raw bytes.
        let mut invalid_utf8 = request(&[]);
        invalid_utf8.headers_mut().insert(
            CLUSTER_NAME_HEADER,
            HeaderValue::from_bytes(b"\xFF").unwrap(),
        );
        for request in [
            request(&["mine", "other"]),
            request(&["other", "mine"]),
            invalid_utf8,
            request(&["other"]),
        ] {
            let response = guard.call(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::MISDIRECTED_REQUEST);
            assert_eq!(
                response.headers().get(&CLUSTER_NAME_HEADER).unwrap(),
                "mine"
            );
        }
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}
