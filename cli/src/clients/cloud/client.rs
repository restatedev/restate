// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A wrapper client for Restate Cloud HTTP service.

use std::time::Duration;

use http::StatusCode;
use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;
use tracing::{debug, info};
use url::Url;

use restate_cli_util::CliContext;

use crate::build_info;
use crate::cli_env::CliEnv;

use super::super::errors::ApiError;

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
                body: serde_json::from_str(&body).unwrap_or(body.into()),
            })));
        }

        debug!("Response from {} ({})", url, http_status_code);
        let body = self.inner.text().await?;
        debug!("  {}", body);
        Ok(serde_json::from_str(&body)?)
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

/// A handy client for the Cloud HTTP service.
#[derive(Clone)]
pub struct CloudClient {
    pub(crate) inner: reqwest::Client,
    pub(crate) base_url: Url,
    pub(crate) access_token: String,
    pub(crate) request_timeout: Duration,
}

impl CloudClient {
    pub fn new(env: &CliEnv) -> anyhow::Result<Self> {
        let access_token = if let Some(credentials) = &env.config.cloud.credentials {
            credentials.access_token()?.to_string()
        } else {
            return Err(anyhow::anyhow!(
                "Restate Cloud credentials have not been provided; first run `restate cloud login`"
            ));
        };

        let raw_client = reqwest::Client::builder()
            .user_agent(format!(
                "{}/{} {}-{}",
                env!("CARGO_PKG_NAME"),
                build_info::RESTATE_CLI_VERSION,
                std::env::consts::OS,
                std::env::consts::ARCH,
            ))
            .connect_timeout(CliContext::get().connect_timeout())
            .build()?;

        Ok(Self {
            inner: raw_client,
            base_url: env.config.cloud.api_base_url(),
            access_token,
            request_timeout: CliContext::get().request_timeout(),
        })
    }

    /// Prepare a request builder for the given method and path.
    fn prepare(&self, method: reqwest::Method, path: Url) -> reqwest::RequestBuilder {
        self.inner
            .request(method, path)
            .timeout(self.request_timeout)
            .header(
                http::header::CONTENT_TYPE,
                http::HeaderValue::from_static("application/json"),
            )
            .bearer_auth(&self.access_token)
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
        let request = self.prepare(method, path.clone());
        let resp = request.send().await?;
        debug!("Response from {} ({})", path, resp.status());
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
        let request = self.prepare_with_body(method, path.clone(), body);
        let resp = request.send().await?;
        debug!("Response from {} ({})", path, resp.status());
        Ok(resp.into())
    }
}

// Ensure that CloudClient is Send + Sync. Compiler will fail if it's not.
const _: () = {
    const fn assert_send<T: Send + Sync>() {}
    assert_send::<CloudClient>();
};
