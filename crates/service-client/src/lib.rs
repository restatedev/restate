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

use bytestring::ByteString;
use core::fmt;
use futures::future::Either;
use http_body_util::Full;
use hyper::body::{Body, Bytes};
use hyper::header::HeaderValue;
use hyper::http::uri::PathAndQuery;
use hyper::{HeaderMap, Response, Uri};
use restate_types::identifiers::LambdaARN;
use std::fmt::Formatter;
use std::future::Future;

pub use crate::lambda::AssumeRoleCacheMode;
pub use options::{
    HttpClientOptionsBuilder, HttpClientOptionsBuilderError, LambdaClientOptionsBuilder,
    LambdaClientOptionsBuilderError, Options, OptionsBuilder, OptionsBuilderError,
};

mod http;
mod lambda;
mod options;
mod proxy;

#[derive(Debug, Clone)]
pub struct ServiceClient<B> {
    // TODO a single client uses the pooling provided by hyper, but this is not enough.
    //  See https://github.com/restatedev/restate/issues/76 for more background on the topic.
    http: HttpClient<B>,
    lambda: LambdaClient,
}

impl<B> ServiceClient<B> {
    pub(crate) fn new(http: HttpClient<B>, lambda: LambdaClient) -> Self {
        Self { http, lambda }
    }
}

impl<B> ServiceClient<B>
where
    B: Body + Send + Unpin + 'static,
    <B as Body>::Data: Send,
    <B as Body>::Error: std::error::Error + Send + Sync + 'static,
{
    pub fn call(
        &self,
        req: Request<B>,
    ) -> impl Future<
        Output = Result<
            Response<http_body_util::Either<hyper::body::Incoming, Full<Bytes>>>,
            ServiceClientError,
        >,
    > + Send
           + 'static {
        let (parts, body) = req.into_parts();

        match parts.address {
            Endpoint::Http(uri, version) => {
                let fut = self
                    .http
                    .request(uri, version, body, parts.path, parts.headers);
                Either::Left(async move { Ok(fut.await?.map(http_body_util::Either::Left)) })
            }
            Endpoint::Lambda(arn, assume_role_arn) => {
                let fut = self
                    .lambda
                    .invoke(arn, assume_role_arn, body, parts.path, parts.headers);
                Either::Right(async move { Ok(fut.await?.map(http_body_util::Either::Right)) })
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ServiceClientError {
    #[error(transparent)]
    Http(#[from] http::HttpError),
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

#[derive(Clone, Debug)]
pub struct Parts {
    /// The request's target address
    address: Endpoint,

    /// The request's path, for example /discover or /invoke/xyz/abc
    path: PathAndQuery,

    /// The request's headers - in lambda case, mapped to apigatewayevent.headers
    headers: HeaderMap<HeaderValue>,
}

impl Parts {
    pub fn new(address: Endpoint, path: PathAndQuery, headers: HeaderMap<HeaderValue>) -> Self {
        Self {
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
