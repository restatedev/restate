// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub use crate::http::HttpClient;
use crate::lambda::LambdaClient;

pub use crate::http::HttpError;
pub use crate::lambda::AssumeRoleCacheMode;
use crate::request_identity::SignRequest;
use ::http::{HeaderName, HeaderValue, Version};
use arc_swap::ArcSwapOption;
use bytes::Bytes;
use bytestring::ByteString;
use core::fmt;
use futures::FutureExt;
use http_body_util::Full;
use hyper::body::Body;
use hyper::http::uri::PathAndQuery;
use hyper::{HeaderMap, Response, Uri};
use restate_types::config::ServiceClientOptions;
use restate_types::identifiers::LambdaARN;
use restate_types::schema::deployment::EndpointLambdaCompression;
use std::collections::HashMap;
use std::error::Error;
use std::fmt::Formatter;
use std::future;
use std::future::Future;
use std::sync::Arc;

mod http;
mod lambda;
mod proxy;
mod request_identity;
mod utils;

pub type ResponseBody = http_body_util::Either<hyper::body::Incoming, Full<Bytes>>;

#[derive(Debug, Clone)]
pub struct ServiceClient {
    // TODO a single client uses the pooling provided by hyper, but this is not enough.
    //  See https://github.com/restatedev/restate/issues/76 for more background on the topic.
    http: HttpClient,
    lambda: LambdaClient,
    // this can be changed to re-read periodically if necessary
    request_identity_key: Arc<ArcSwapOption<request_identity::v1::SigningKey>>,
    additional_request_headers: HashMap<HeaderName, HeaderValue>,
}

impl ServiceClient {
    pub(crate) fn new(
        http: HttpClient,
        lambda: LambdaClient,
        request_identity_key: Arc<ArcSwapOption<request_identity::v1::SigningKey>>,
        additional_request_headers: HashMap<HeaderName, HeaderValue>,
    ) -> Self {
        Self {
            http,
            lambda,
            request_identity_key,
            additional_request_headers,
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
            options
                .additional_request_headers
                .clone()
                .unwrap_or_default()
                .into(),
        ))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    #[error("Failed to read request identity private key: {0}")]
    SigningPrivateKeyReadError(#[from] request_identity::v1::SigningPrivateKeyReadError),
}

impl ServiceClient {
    pub fn call<B>(
        &self,
        req: Request<B>,
    ) -> impl Future<Output = Result<Response<ResponseBody>, ServiceClientError>> + Send + 'static
    where
        B: Body<Data = Bytes> + Send + Sync + Unpin + Sized + 'static,
        <B as Body>::Error: Error + Send + Sync + 'static,
    {
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

        parts.headers.extend(
            self.additional_request_headers
                .iter()
                .map(|(k, v)| (k.clone(), v.clone())),
        );

        match parts.address {
            Endpoint::Http(uri, version) => {
                let fut = self.http.request(
                    uri.clone(),
                    version,
                    parts.method.into(),
                    body,
                    parts.path,
                    parts.headers,
                );
                async move {
                    Ok(fut
                        .await
                        .map_err(|e| ServiceClientError::Http(uri, e))?
                        .map(http_body_util::Either::Left))
                }
                .left_future()
            }
            Endpoint::Lambda(arn, assume_role_arn, compression) => {
                let fut = self.lambda.invoke(
                    arn.clone(),
                    parts.method.into(),
                    assume_role_arn,
                    compression,
                    body,
                    parts.path,
                    parts.headers,
                );
                async move {
                    Ok(fut
                        .await
                        .map_err(|e| ServiceClientError::Lambda(arn, e))?
                        .map(http_body_util::Either::Right))
                }
                .right_future()
            }
        }
        .left_future()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ServiceClientError {
    #[error("error when calling '{0}': {1}")]
    Http(Uri, #[source] http::HttpError),
    #[error("error when calling '{0}': {1}")]
    Lambda(LambdaARN, #[source] lambda::LambdaError),
    #[error(transparent)]
    IdentityV1(#[from] <request_identity::v1::Signer<'static, 'static> as SignRequest>::Error),
}

impl ServiceClientError {
    /// Retryable errors are those which can be caused by transient faults and where
    /// retrying can succeed.
    pub fn is_retryable(&self) -> bool {
        match self {
            ServiceClientError::Http(_, http_error) => http_error.is_retryable(),
            ServiceClientError::Lambda(_, lambda_error) => lambda_error.is_retryable(),
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
    Http(Uri, Option<Version>),
    Lambda(
        LambdaARN,
        Option<ByteString>,
        Option<EndpointLambdaCompression>,
    ),
}

impl fmt::Display for Endpoint {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Http(uri, _) => uri.fmt(f),
            Self::Lambda(arn, _, _) => write!(f, "lambda://{arn}"),
        }
    }
}
