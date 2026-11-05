// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::error::Error;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};

use bytes::Bytes;
use http::header::{CONTENT_TYPE, HeaderValue};
use http::{HeaderName, Request, Response, StatusCode};
use hyper::body::{Body, Frame, SizeHint};
use pin_project_lite::pin_project;
use tower::{Layer, Service};

const CLUSTER_NAME_HEADER: HeaderName = HeaderName::from_static("x-restate-cluster-name");
const MISDIRECTED_BODY: &[u8] = b"The request was sent to a different Restate cluster.\n";

/// Rejects requests that name another cluster in `x-restate-cluster-name` with 421.
#[derive(Clone)]
pub struct MisdirectedRequestsLayer {
    enabled: bool,
    cluster_name: Arc<str>,
    cluster_name_header: HeaderValue,
}

impl MisdirectedRequestsLayer {
    pub fn new(enabled: bool, cluster_name: impl Into<String>) -> Self {
        let cluster_name = cluster_name.into();
        let cluster_name_header = if enabled {
            HeaderValue::from_str(&cluster_name).expect(
                "reject-misdirected-requests requires cluster-name to be a valid header value",
            )
        } else {
            HeaderValue::from_static("")
        };

        Self {
            enabled,
            cluster_name: cluster_name.into(),
            cluster_name_header,
        }
    }
}

impl<S> Layer<S> for MisdirectedRequestsLayer {
    type Service = MisdirectedRequests<S>;

    fn layer(&self, inner: S) -> Self::Service {
        MisdirectedRequests {
            inner,
            enabled: self.enabled,
            cluster_name: self.cluster_name.clone(),
            cluster_name_header: self.cluster_name_header.clone(),
        }
    }
}

#[derive(Clone)]
pub struct MisdirectedRequests<S> {
    inner: S,
    enabled: bool,
    cluster_name: Arc<str>,
    cluster_name_header: HeaderValue,
}

impl<S> MisdirectedRequests<S> {
    fn is_misdirected<B>(&self, request: &Request<B>) -> bool {
        if !self.enabled {
            return false;
        }

        request
            .headers()
            .get_all(&CLUSTER_NAME_HEADER)
            .iter()
            .any(|value| value.as_bytes() != self.cluster_name.as_bytes())
    }

    fn rejection<B>(&self) -> Response<MisdirectedBody<B>> {
        Response::builder()
            .status(StatusCode::MISDIRECTED_REQUEST)
            .header(CONTENT_TYPE, "text/plain; charset=utf-8")
            .header(CLUSTER_NAME_HEADER, self.cluster_name_header.clone())
            .body(MisdirectedBody::Reject {
                body: Some(Bytes::from_static(MISDIRECTED_BODY)),
            })
            .expect("valid rejection response")
    }
}

impl<S, ReqBody, B> Service<Request<ReqBody>> for MisdirectedRequests<S>
where
    S: Service<Request<ReqBody>, Response = Response<B>>,
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn Error + Send + Sync>>,
{
    type Response = Response<MisdirectedBody<B>>;
    type Error = S::Error;
    type Future = MisdirectedRequestsFuture<S::Future, B>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<ReqBody>) -> Self::Future {
        let state = if self.is_misdirected(&request) {
            State::Reject {
                response: Some(self.rejection()),
            }
        } else {
            State::Inner {
                future: self.inner.call(request),
                _body: PhantomData,
            }
        };

        MisdirectedRequestsFuture { state }
    }
}

pin_project! {
    pub struct MisdirectedRequestsFuture<F, B> {
        #[pin]
        state: State<F, B>,
    }
}

pin_project! {
    #[project = StateProj]
    enum State<F, B> {
        Inner {
            #[pin]
            future: F,
            _body: PhantomData<B>,
        },
        Reject {
            response: Option<Response<MisdirectedBody<B>>>,
        },
    }
}

impl<F, B, E> Future for MisdirectedRequestsFuture<F, B>
where
    F: Future<Output = Result<Response<B>, E>>,
{
    type Output = Result<Response<MisdirectedBody<B>>, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.project().state.project() {
            StateProj::Inner { future, .. } => {
                let response = ready!(future.poll(cx))?;
                Poll::Ready(Ok(
                    response.map(|body| MisdirectedBody::Inner { inner: body })
                ))
            }
            StateProj::Reject { response } => Poll::Ready(Ok(response
                .take()
                .expect("misdirected response future polled after completion"))),
        }
    }
}

pin_project! {
    #[project = MisdirectedBodyProj]
    pub enum MisdirectedBody<B> {
        Inner {
            #[pin]
            inner: B,
        },
        Reject {
            body: Option<Bytes>,
        },
    }
}

impl<B> Body for MisdirectedBody<B>
where
    B: Body<Data = Bytes>,
    B::Error: Into<Box<dyn Error + Send + Sync>>,
{
    type Data = Bytes;
    type Error = Box<dyn Error + Send + Sync>;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        match self.project() {
            MisdirectedBodyProj::Inner { inner } => inner
                .poll_frame(cx)
                .map(|frame| frame.map(|frame| frame.map_err(Into::into))),
            MisdirectedBodyProj::Reject { body } => {
                Poll::Ready(body.take().map(|body| Ok(Frame::data(body))))
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        match self {
            Self::Inner { inner } => inner.is_end_stream(),
            Self::Reject { body } => body.is_none(),
        }
    }

    fn size_hint(&self) -> SizeHint {
        match self {
            Self::Inner { inner } => inner.size_hint(),
            Self::Reject { body } => body
                .as_ref()
                .map(|body| SizeHint::with_exact(body.len() as u64))
                .unwrap_or_default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
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

        // Disabled: a mismatching header is served as before.
        let mut disabled = MisdirectedRequestsLayer::new(false, "mine").layer(inner.clone());
        let response = disabled.call(request(&["other"])).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        // Enabled: matching and missing headers are served.
        let mut enabled = MisdirectedRequestsLayer::new(true, "mine").layer(inner);
        let response = enabled.call(request(&["mine"])).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let response = enabled.call(request(&[])).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(calls.load(Ordering::SeqCst), 3);

        // Enabled: every occurrence must match, and values are compared as raw bytes.
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
            let response = enabled.call(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::MISDIRECTED_REQUEST);
            assert_eq!(
                response.headers().get(&CLUSTER_NAME_HEADER).unwrap(),
                "mine"
            );
        }
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }
}
