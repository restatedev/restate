// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::http::HttpClient;
use crate::lambda::LambdaClient;

use arc_swap::ArcSwapOption;
use bytestring::ByteString;
use core::fmt;

use futures::FutureExt;
use hyper::header::HeaderValue;
use hyper::http::uri::PathAndQuery;
use hyper::Body;
use hyper::{HeaderMap, Response, Uri};
use restate_types::config::ServiceClientOptions;
use restate_types::identifiers::LambdaARN;
use std::fmt::Formatter;
use std::future;
use std::future::Future;
use std::sync::Arc;

pub use crate::http::HttpError;
pub use crate::lambda::AssumeRoleCacheMode;
use crate::request_identity::SignRequest;

mod http;
mod lambda;
mod proxy;
mod request_identity;
mod utils;

#[derive(Debug, Clone)]
pub struct ServiceClient {
    // TODO a single client uses the pooling provided by hyper, but this is not enough.
    //  See https://github.com/restatedev/restate/issues/76 for more background on the topic.
    http: HttpClient,
    lambda: LambdaClient,
    // this can be changed to re-read periodically if necessary
    request_identity_key: Arc<ArcSwapOption<request_identity::v1::SigningKey>>,
}

impl ServiceClient {
    pub(crate) fn new(
        http: HttpClient,
        lambda: LambdaClient,
        request_identity_key: Arc<ArcSwapOption<request_identity::v1::SigningKey>>,
    ) -> Self {
        Self {
            http,
            lambda,
            request_identity_key,
        }
    }

    pub fn from_options(
        options: &ServiceClientOptions,
        assume_role_cache_mode: AssumeRoleCacheMode,
    ) -> Result<Self, BuildError> {
        let request_identity_key = if let Some(request_identity_private_key_pem_file) =
            options.request_identity_private_key_pem_file.clone()
        {
            Arc::new(ArcSwapOption::from_pointee(
                request_identity::v1::SigningKey::from_pem_file(
                    request_identity_private_key_pem_file,
                )?,
            ))
        } else {
            Arc::new(ArcSwapOption::empty())
        };

        Ok(Self::new(
            HttpClient::from_options(&options.http),
            LambdaClient::from_options(&options.lambda, assume_role_cache_mode),
            request_identity_key,
        ))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    #[error("Failed to read request identity private key: {0}")]
    SigningPrivateKeyReadError(#[from] request_identity::v1::SigningPrivateKeyReadError),
}

impl ServiceClient {
    pub fn call(
        &self,
        req: Request<Body>,
    ) -> impl Future<Output = Result<Response<Body>, ServiceClientError>> + Send + 'static {
        let (mut parts, body) = req.into_parts();

        let request_identity_key = self.request_identity_key.load();

        let signer = if let Some(request_identity_key) = request_identity_key.as_deref() {
            Some(request_identity::v1::Signer::new(
                parts.path.path(),
                request_identity_key,
            ))
        } else {
            None // will use null signing scheme
        };

        parts.headers = match signer.insert_identity(parts.headers) {
            Ok(headers) => headers,
            Err(err) => return future::ready(Err(err.into())).right_future(),
        };

        match parts.address {
            Endpoint::Http(uri, version) => {
                let fut = self.http.request(
                    uri,
                    version,
                    parts.method.into(),
                    body,
                    parts.path,
                    parts.headers,
                );
                async move { Ok(fut.await?) }.left_future()
            }
            Endpoint::Lambda(arn, assume_role_arn) => {
                let fut = self.lambda.invoke(
                    arn,
                    parts.method.into(),
                    assume_role_arn,
                    body,
                    parts.path,
                    parts.headers,
                );
                async move { Ok(fut.await?) }.right_future()
            }
        }
        .left_future()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ServiceClientError {
    #[error(transparent)]
    Http(#[from] http::HttpError),
    #[error(transparent)]
    Lambda(#[from] lambda::LambdaError),
    #[error(transparent)]
    IdentityV1(#[from] <request_identity::v1::Signer<'static, 'static> as SignRequest>::Error),
}

impl ServiceClientError {
    /// Retryable errors are those which can be caused by transient faults and where
    /// retrying can succeed.
    pub fn is_retryable(&self) -> bool {
        match self {
            ServiceClientError::Http(http_error) => http_error.is_retryable(),
            ServiceClientError::Lambda(lambda_error) => lambda_error.is_retryable(),
            ServiceClientError::IdentityV1(_) => false, // this really should never happen
        }
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

    pub fn address(&self) -> &Endpoint {
        &self.head.address
    }

    pub fn path(&self) -> &PathAndQuery {
        &self.head.path
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Method {
    POST,
    GET,
}

impl From<Method> for hyper::http::Method {
    fn from(value: Method) -> Self {
        match value {
            Method::POST => hyper::http::Method::POST,
            Method::GET => hyper::http::Method::GET,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Parts {
    /// The method to use
    method: Method,

    /// The request's target address
    address: Endpoint,

    /// The request's path, for example /discover or /invoke/xyz/abc
    path: PathAndQuery,

    /// The request's headers - in lambda case, mapped to apigatewayevent.headers
    headers: HeaderMap<HeaderValue>,
}

impl Parts {
    pub fn new(
        method: Method,
        address: Endpoint,
        path: PathAndQuery,
        headers: HeaderMap<HeaderValue>,
    ) -> Self {
        Self {
            method,
            address,
            path,
            headers,
        }
    }
}

#[derive(Clone, Debug)]
pub enum Endpoint {
    Http(Uri, hyper::http::Version),
    Lambda(LambdaARN, Option<ByteString>),
}

impl fmt::Display for Endpoint {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Http(uri, _) => uri.fmt(f),
            Self::Lambda(arn, _) => write!(f, "lambda://{}", arn),
        }
    }
}
