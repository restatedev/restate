use core::fmt;

use std::fmt::Formatter;
use std::future;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{FutureExt, TryFutureExt};

use hyper::client::HttpConnector;
use hyper::header::HeaderValue;
use hyper::http::uri::PathAndQuery;
use hyper::Body;
use hyper::{http, HeaderMap, Response, Uri};
use hyper_rustls::HttpsConnector;

pub use options::{Options, OptionsBuilder, OptionsBuilderError};

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

#[derive(Debug, Clone)]
pub struct ServiceClient {
    // TODO a single client uses the pooling provided by hyper, but this is not enough.
    //  See https://github.com/restatedev/restate/issues/76 for more background on the topic.
    http: hyper::Client<Connector, Body>,
    lambda: LambdaClient,
}

impl ServiceClient {
    pub(crate) fn new(http: hyper::Client<Connector, Body>, lambda: LambdaClient) -> Self {
        Self { http, lambda }
    }
}

impl hyper::service::Service<Request<Body>> for ServiceClient {
    type Response = Response<Body>;
    type Error = ServiceClientError;
    type Future = Pin<Box<dyn Future<Output = Result<Response<Body>, ServiceClientError>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let (parts, body) = req.into_parts();
        match parts.address {
            ServiceEndpointAddress::Http(uri, version) => {
                let mut uri_parts = uri.into_parts();
                uri_parts.path_and_query = match uri_parts.path_and_query {
                    None => Some(parts.path),
                    Some(existing_path) => Some({
                        let path = format!(
                            "{}/{}",
                            existing_path
                                .path()
                                .strip_suffix('/')
                                .unwrap_or(existing_path.path()),
                            parts
                                .path
                                .path()
                                .strip_prefix('/')
                                .unwrap_or(parts.path.path()),
                        );
                        let path = if let Some(query) = existing_path.query() {
                            format!("{}?{}", path, query)
                        } else {
                            path
                        };

                        match path.try_into() {
                            Ok(path) => path,
                            Err(err) => {
                                return future::ready(Err(ServiceClientError::Http(
                                    HttpError::Http(err.into()),
                                )))
                                .boxed()
                            }
                        }
                    }),
                };

                let uri = match Uri::from_parts(uri_parts) {
                    Ok(uri) => uri,
                    Err(err) => {
                        return future::ready(Err(ServiceClientError::Http(HttpError::Http(
                            err.into(),
                        ))))
                        .boxed()
                    }
                };

                let mut http_request_builder =
                    http::Request::builder().method(http::Method::POST).uri(uri);

                for (header, value) in parts.headers.iter() {
                    http_request_builder = http_request_builder.header(header, value)
                }

                http_request_builder = http_request_builder.version(version);

                let http_request = match http_request_builder.body(body) {
                    Ok(http_request) => http_request,
                    Err(err) => return future::ready(Err(err.into())).boxed(),
                };

                self.http.request(http_request).map_err(Into::into).boxed()
            }
            ServiceEndpointAddress::Lambda(arn) => self
                .lambda
                .invoke(arn, body, parts.path, parts.headers)
                .map_err(Into::into)
                .boxed(),
        }
    }
}

impl Service for ServiceClient {}

#[derive(Debug, thiserror::Error)]
pub enum ServiceClientError {
    #[error(transparent)]
    Http(HttpError),
    #[error(transparent)]
    Lambda(#[from] lambda::LambdaError),
}

#[derive(Debug, thiserror::Error)]
pub enum HttpError {
    #[error(transparent)]
    Hyper(hyper::Error),
    #[error(transparent)]
    Http(http::Error),
}

impl From<hyper::Error> for ServiceClientError {
    fn from(value: hyper::Error) -> Self {
        Self::Http(HttpError::Hyper(value))
    }
}
impl From<http::Error> for ServiceClientError {
    fn from(value: http::Error) -> Self {
        Self::Http(HttpError::Http(value))
    }
}

pub struct Request<B> {
    head: Parts,
    body: B,
}

impl<B> Request<B> {
    pub fn new(head: Parts, body: B) -> Self {
        Self { head, body }
    }

    pub fn into_parts(self) -> (Parts, B) {
        (self.head, self.body)
    }

    pub fn address(&self) -> &ServiceEndpointAddress {
        &self.head.address
    }

    pub fn path(&self) -> &PathAndQuery {
        &self.head.path
    }
}

#[derive(Clone, Debug)]
pub struct Parts {
    /// The request's target address
    pub address: ServiceEndpointAddress,

    /// The request's path, for example /discover or /invoke/xyz/abc
    pub path: PathAndQuery,

    /// The request's headers - in lambda case, mapped to apigatewayevent.headers
    pub headers: HeaderMap<HeaderValue>,
}

impl Parts {
    pub fn new(
        address: ServiceEndpointAddress,
        path: PathAndQuery,
        headers: HeaderMap<HeaderValue>,
    ) -> Self {
        Self {
            address,
            path,
            headers,
        }
    }
}

#[derive(Clone, Debug)]
pub enum ServiceEndpointAddress {
    Http(Uri, http::Version),
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
