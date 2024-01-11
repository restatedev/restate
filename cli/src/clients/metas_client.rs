// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A wrapper client for meta HTTP service.

use http::StatusCode;
use serde::{de::DeserializeOwned, Serialize};
use std::time::Duration;
use thiserror::Error;
use tracing::{debug, info};
use url::Url;

use crate::build_info;
use crate::cli_env::CliEnv;

use super::errors::ApiError;

#[derive(Error, Debug)]
#[error(transparent)]
pub enum Error {
    // Error is boxed because ApiError can get quite large if the message body is large.
    Api(#[from] Box<ApiError>),
    #[error("(Protocol error) {0}")]
    Serialization(#[from] serde_json::Error),
    Network(#[from] reqwest::Error),
}

/// A lazy wrapper around a reqwest response that deserializes the body on
/// demand and decodes our custom error body on non-2xx responses.
pub struct Envelope<T> {
    inner: reqwest::Response,

    _phantom: std::marker::PhantomData<T>,
}

impl<T> Envelope<T>
where
    T: DeserializeOwned,
{
    pub fn status_code(&self) -> StatusCode {
        self.inner.status()
    }

    pub fn url(&self) -> &Url {
        self.inner.url()
    }

    pub async fn into_body(self) -> Result<T, Error> {
        let http_status_code = self.inner.status();
        let url = self.inner.url().clone();
        if !self.status_code().is_success() {
            let body = self.inner.text().await?;
            info!("Response from {} ({})", url, http_status_code);
            info!("  {}", body);
            // Wrap the error into ApiError
            return Err(Error::Api(Box::new(ApiError {
                http_status_code,
                url,
                body: serde_json::from_str(&body)?,
            })));
        }

        debug!("Response from {} ({})", url, http_status_code);
        let body = self.inner.text().await?;
        debug!("  {}", body);
        Ok(serde_json::from_str(&body)?)
    }

    pub async fn into_text(self) -> Result<String, Error> {
        Ok(self.inner.text().await?)
    }
    pub fn success_or_error(self) -> Result<StatusCode, Error> {
        let http_status_code = self.inner.status();
        let url = self.inner.url().clone();
        info!("Response from {} ({})", url, http_status_code);
        match self.inner.error_for_status() {
            Ok(_) => Ok(http_status_code),
            Err(e) => Err(Error::Network(e)),
        }
    }
}

impl<T> From<reqwest::Response> for Envelope<T> {
    fn from(value: reqwest::Response) -> Self {
        Self {
            inner: value,
            _phantom: Default::default(),
        }
    }
}

/// A handy client for the meta HTTP service.
#[derive(Clone)]
pub struct MetasClient {
    pub(crate) inner: reqwest::Client,
    pub(crate) base_url: Url,
    pub(crate) bearer_token: Option<String>,
    pub(crate) request_timeout: Duration,
}

const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

impl MetasClient {
    pub fn new(env: &CliEnv) -> reqwest::Result<Self> {
        let raw_client = reqwest::Client::builder()
            .user_agent(format!(
                "{}/{} {}-{}",
                env!("CARGO_PKG_NAME"),
                build_info::RESTATE_CLI_VERSION,
                std::env::consts::OS,
                std::env::consts::ARCH,
            ))
            .connect_timeout(env.connect_timeout)
            .build()?;

        Ok(Self {
            inner: raw_client,
            base_url: env.admin_base_url.clone(),
            bearer_token: env.bearer_token.clone(),
            request_timeout: env.request_timeout.unwrap_or(DEFAULT_REQUEST_TIMEOUT),
        })
    }

    /// Prepare a request builder for the given method and path.
    fn prepare(&self, method: reqwest::Method, path: Url) -> reqwest::RequestBuilder {
        let request_builder = self
            .inner
            .request(method, path)
            .timeout(self.request_timeout);

        match self.bearer_token.as_deref() {
            Some(token) => request_builder.bearer_auth(token),
            None => request_builder,
        }
    }

    /// Prepare a request builder that encodes the body as JSON.
    fn prepare_with_body<B>(
        &self,
        method: reqwest::Method,
        path: Url,
        body: B,
    ) -> reqwest::RequestBuilder
    where
        B: Serialize,
    {
        self.prepare(method, path).json(&body)
    }

    /// Execute a request and return the response as a lazy Envelope.
    pub(crate) async fn run<T>(
        &self,
        method: reqwest::Method,
        path: Url,
    ) -> reqwest::Result<Envelope<T>>
    where
        T: DeserializeOwned + Send,
    {
        debug!("Sending request {} ({})", method, path);
        let request = self.prepare(method, path);
        let resp = request.send().await?;
        Ok(resp.into())
    }

    pub(crate) async fn run_with_body<T, B>(
        &self,
        method: reqwest::Method,
        path: Url,
        body: B,
    ) -> reqwest::Result<Envelope<T>>
    where
        T: DeserializeOwned + Send,
        B: Serialize + std::fmt::Debug + Send,
    {
        debug!("Sending request {} ({}): {:?}", method, path, body);
        let request = self.prepare_with_body(method, path, body);
        let resp = request.send().await?;
        Ok(resp.into())
    }
}

// Ensure that MetaClient is Send + Sync. Compiler will fail if it's not.
const _: () = {
    const fn assert_send<T: Send + Sync>() {}
    assert_send::<MetasClient>();
};
