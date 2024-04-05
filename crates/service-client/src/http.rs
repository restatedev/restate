// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::proxy::ProxyConnector;

use crate::request_identity::SignRequest;
use crate::utils::ErrorExt;
use arc_swap::ArcSwapOption;
use futures::future::Either;
use futures::FutureExt;
use hyper::client::HttpConnector;
use hyper::http::uri::PathAndQuery;
use hyper::http::HeaderValue;
use hyper::{Body, HeaderMap, Method, Request, Response, Uri, Version};
use hyper_rustls::HttpsConnector;
use restate_types::config::HttpOptions;
use std::fmt::Debug;
use std::future;
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;

type Connector = ProxyConnector<HttpsConnector<HttpConnector>>;

#[derive(Clone, Debug)]
pub struct HttpClient {
    client: hyper::Client<Connector, Body>,
    // this can be changed to re-read periodically if necessary
    request_identity_key: Arc<ArcSwapOption<crate::request_identity::v1::SigningKey>>,
}

impl HttpClient {
    pub fn new(
        client: hyper::Client<Connector, Body>,
        request_identity_private_key_pem_file: Option<PathBuf>,
    ) -> Result<Self, HttpClientError> {
        let request_identity_key = if let Some(request_identity_private_key_pem_file) =
            request_identity_private_key_pem_file
        {
            Arc::new(ArcSwapOption::from_pointee(
                crate::request_identity::v1::SigningKey::from_pem_file(
                    request_identity_private_key_pem_file,
                )?,
            ))
        } else {
            Arc::new(ArcSwapOption::empty())
        };

        Ok(Self {
            client,
            request_identity_key,
        })
    }

    pub fn from_options(options: &HttpOptions) -> Result<HttpClient, BuildError> {
        let mut builder = hyper::Client::builder();
        builder
            .http2_only(true)
            .http2_keep_alive_timeout(options.http_keep_alive_options.timeout.into())
            .http2_keep_alive_interval(Some(options.http_keep_alive_options.interval.into()));

        Ok(HttpClient::new(
            builder.build::<_, hyper::Body>(ProxyConnector::new(
                options.http_proxy.clone(),
                hyper_rustls::HttpsConnectorBuilder::new()
                    .with_native_roots()
                    .https_or_http()
                    .enable_http2()
                    .build(),
            )),
            options.request_identity_private_key_pem_file.clone(),
        )?)
    }

    fn build_request(
        uri: Uri,
        version: Version,
        body: Body,
        method: Method,
        path: PathAndQuery,
        headers: HeaderMap<HeaderValue>,
    ) -> Result<Request<Body>, hyper::http::Error> {
        let mut uri_parts = uri.into_parts();
        uri_parts.path_and_query = match uri_parts.path_and_query {
            None => Some(path),
            Some(existing_path) => Some({
                let path = format!(
                    "{}/{}",
                    existing_path
                        .path()
                        .strip_suffix('/')
                        .unwrap_or(existing_path.path()),
                    path.path().strip_prefix('/').unwrap_or(path.path()),
                );
                let path = if let Some(query) = existing_path.query() {
                    format!("{}?{}", path, query)
                } else {
                    path
                };

                path.try_into()?
            }),
        };

        let mut http_request_builder = Request::builder()
            .method(method)
            .uri(Uri::from_parts(uri_parts)?);

        for (header, value) in headers.iter() {
            http_request_builder = http_request_builder.header(header, value)
        }

        http_request_builder = http_request_builder.version(version);

        http_request_builder.body(body)
    }

    pub fn request(
        &self,
        uri: Uri,
        version: Version,
        body: Body,
        path: PathAndQuery,
        headers: HeaderMap<HeaderValue>,
    ) -> impl Future<Output = Result<Response<Body>, HttpError>> + Send + 'static {
        let method = Method::POST;

        let request_identity_key = self.request_identity_key.load();

        let signer = if let Some(request_identity_key) = request_identity_key.as_deref() {
            match crate::request_identity::v1::Signer::new(path.path().into(), request_identity_key)
            {
                Ok(signing_parameters_v1) => Some(signing_parameters_v1),
                Err(err) => return future::ready(Err(err)).right_future(),
            }
        } else {
            None // will use null signing scheme
        };

        let request = match Self::build_request(uri, version, body, method, path, headers) {
            Ok(request) => request,
            Err(err) => return future::ready(Err(err.into())).right_future(),
        };

        let request = match signer.sign_request(request) {
            Ok(request) => request,
            Err(err) => return future::ready(Err(err.into())).right_future(),
        };

        let fut = self.client.request(request);

        Either::Left(async move { Ok(fut.await?) })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    #[error(transparent)]
    HttpClient(#[from] HttpClientError),
}

#[derive(Debug, thiserror::Error)]
pub enum HttpClientError {
    #[error("Failed to read request signing private key: {0}")]
    SigningPrivateKeyReadError(#[from] SigningPrivateKeyReadError),
}

#[derive(Debug, thiserror::Error)]
pub enum SigningPrivateKeyReadError {
    #[error("Only one private key in PEM format is expected, found {0}")]
    OneKeyExpected(usize),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Pem(#[from] pem::PemError),
    #[error("Key was rejected by ring: {0}")]
    KeyRejected(ring::error::KeyRejected),
}

#[derive(Debug, thiserror::Error)]
pub enum HttpError {
    #[error(transparent)]
    Hyper(#[from] hyper::Error),
    #[error(transparent)]
    Http(#[from] hyper::http::Error),
    #[error(transparent)]
    IdentityV1(#[from] <super::request_identity::v1::Signer<'static> as SignRequest>::Error),
}

impl HttpError {
    /// Retryable errors are those which can be caused by transient faults and where
    /// retrying can succeed.
    pub fn is_retryable(&self) -> bool {
        match self {
            HttpError::Hyper(err) => err.is_retryable(),
            HttpError::Http(err) => err.is_retryable(),
            HttpError::IdentityV1(_) => false, // this really should never happen
        }
    }
}
