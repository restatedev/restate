extern crate core;

use core::fmt;
use std::error::Error;
use std::fmt::Formatter;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{FutureExt, TryFutureExt};
use hyper::body::HttpBody;
use hyper::client::connect::Connect;
use hyper::client::HttpConnector;
use hyper::header::HeaderValue;
use hyper::http::uri::PathAndQuery;
pub use hyper::Body;
use hyper::{http, HeaderMap, Response, Uri};
use hyper_rustls::HttpsConnector;

pub use options::{Options, OptionsBuilder, OptionsBuilderError};
use restate_schema_api::endpoint::ProtocolType;
use restate_types::identifiers::LambdaARN;

use crate::lambda::LambdaClient;
use crate::proxy::ProxyConnector;

mod lambda;
mod options;
pub mod proxy;

pub trait Service:
    hyper::service::Service<
        Request<Body>,
        Response = Response<Body>,
        Error = ServiceClientError,
        Future = Pin<Box<dyn Future<Output = Result<Response<Body>, ServiceClientError>> + Send>>,
    > + Clone
    + Send
    + 'static
{
}

pub type Connector = ProxyConnector<HttpsConnector<HttpConnector>>;

#[derive(Debug)]
pub struct ServiceClient<C, B> {
    // TODO a single client uses the pooling provided by hyper, but this is not enough.
    //  See https://github.com/restatedev/restate/issues/76 for more background on the topic.
    http: hyper::Client<C, B>,
    lambda: LambdaClient,
}

impl<C, B> ServiceClient<C, B> {
    pub fn new(http: hyper::Client<C, B>, lambda: LambdaClient) -> Self {
        Self { http, lambda }
    }
}

impl<C: Clone, B> Clone for ServiceClient<C, B> {
    fn clone(&self) -> Self {
        Self {
            http: self.http.clone(),
            lambda: self.lambda.clone(),
        }
    }
}

impl<C, B> hyper::service::Service<Request<B>> for ServiceClient<C, B>
where
    C: Connect + Clone + Send + Sync + 'static,
    B: HttpBody + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn Error + Send + Sync>>,
{
    type Response = Response<Body>;
    type Error = ServiceClientError;
    type Future = Pin<Box<dyn Future<Output = Result<Response<Body>, ServiceClientError>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<B>) -> Self::Future {
        match req.head.address {
            ServiceEndpointAddress::Http(uri, protocol_type) => {
                let mut http_request_builder =
                    http::Request::builder().method(http::Method::POST).uri(uri);

                // In case it's bidi stream, force HTTP/2
                if protocol_type == ProtocolType::BidiStream {
                    http_request_builder = http_request_builder.version(http::Version::HTTP_2);
                }

                let http_request = http_request_builder
                    .body(req.body)
                    // This fails only in case the URI is malformed, which should never happen
                    .expect("The request builder shouldn't fail");

                self.http.request(http_request).map_err(Into::into).boxed()
            }
            ServiceEndpointAddress::Lambda(arn) => self
                .lambda
                .invoke(arn, req.body, req.head.path, req.head.headers)
                .map_err(Into::into)
                .boxed(),
        }
    }
}

impl<C> Service for ServiceClient<C, Body> where C: Connect + Clone + Send + Sync + 'static {}

#[derive(Debug, thiserror::Error)]
pub enum ServiceClientError {
    #[error(transparent)]
    Http(#[from] hyper::Error),
    #[error(transparent)]
    Lambda(#[from] lambda::LambdaError),
}

pub struct Request<B> {
    head: Parts,
    body: B,
}

impl<B> Request<B> {
    pub fn new(head: Parts, body: B) -> Self {
        Self { head, body }
    }

    pub fn address(&self) -> &ServiceEndpointAddress {
        &self.head.address
    }

    pub fn path(&self) -> &PathAndQuery {
        &self.head.path
    }
}

pub struct Parts {
    /// The request's target address
    pub address: ServiceEndpointAddress,

    // Can be /discover or /invoke/xyz/abc
    pub path: PathAndQuery,

    // Invoker can still add headers (in lambda case, mapped to apigatewayevent.headers
    pub headers: HeaderMap<HeaderValue>,
}

pub enum ServiceEndpointAddress {
    Http(Uri, ProtocolType),
    Lambda(LambdaARN),
}

impl fmt::Display for ServiceEndpointAddress {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Http(uri, _) => uri.fmt(f),
            Self::Lambda(arn) => write!(f, "lambda://{}", arn),
        }
    }
}
