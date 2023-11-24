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

use crate::build_info;
use crate::cli_env::CliEnv;
use crate::console::Styled;
use crate::ui::stylesheet::Style;

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use thiserror::Error;
use tracing::{debug, info};
use url::Url;

#[derive(Deserialize, Debug, Clone)]
pub struct ApiErrorBody {
    restate_code: Option<String>,
    message: String,
}

#[derive(Debug, Clone)]
pub struct ApiError {
    http_status_code: reqwest::StatusCode,
    url: Url,
    body: ApiErrorBody,
}

#[derive(Error, Debug)]
#[error(transparent)]
pub enum Error {
    Api(#[from] ApiError),
    #[error("(Protocol error) {0}")]
    Serialization(#[from] serde_json::Error),
    Network(#[from] reqwest::Error),
}

impl std::fmt::Display for ApiErrorBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let code = self
            .restate_code
            .clone()
            .unwrap_or_else(|| "<UNKNOWN>".to_string());
        writeln!(f, "{} {}", Styled(Style::Warn, &code), self.message)?;
        Ok(())
    }
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{}", self.body)?;

        writeln!(
            f,
            "  -> Http status code {} at '{}'",
            Styled(Style::Warn, &self.http_status_code),
            Styled(Style::Info, &self.url),
        )?;
        Ok(())
    }
}

impl std::error::Error for ApiError {}

/// A lazy wrapper around a reqwest response that deserializes the body on
/// demand and decodes our custom error body on non-2xx responses.
pub struct Envelope<T> {
    inner: reqwest::Response,

    _phantom: std::marker::PhantomData<T>,
}

impl<T> Envelope<T>
where
    T: serde::de::DeserializeOwned,
{
    pub fn status_code(&self) -> reqwest::StatusCode {
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
            return Err(Error::Api(ApiError {
                http_status_code,
                url,
                body: serde_json::from_str(&body)?,
            }));
        }

        debug!("Response from {} ({})", url, http_status_code);
        let body = self.inner.text().await?;
        debug!("  {}", body);
        Ok(serde_json::from_str(&body)?)
    }

    pub async fn into_text(self) -> Result<String, Error> {
        Ok(self.inner.text().await?)
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
pub struct MetaClient {
    pub(crate) inner: reqwest::Client,
    pub(crate) base_url: reqwest::Url,
}

impl MetaClient {
    pub fn new(env: &CliEnv) -> reqwest::Result<Self> {
        let raw_client = reqwest::Client::builder()
            .user_agent(format!(
                "{}/{} {}-{}",
                env!("CARGO_PKG_NAME"),
                build_info::RESTATE_CLI_VERSION,
                std::env::consts::OS,
                std::env::consts::ARCH,
            ))
            .build()?;

        Ok(Self {
            inner: raw_client,
            base_url: env.meta_base_url.clone(),
        })
    }

    /// Prepare a request builder for the given method and path.
    fn prepare(&self, method: reqwest::Method, path: Url) -> reqwest::RequestBuilder {
        // TODO: Inject the secret token when available.
        //.bearer_auth(&self.secret_token);
        self.inner.request(method, path)
    }

    /// Prepare a request builder that encodes the body as JSON.
    fn _prepare_with_body<B>(
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

    pub(crate) async fn _run_with_body<T, B>(
        &self,
        method: reqwest::Method,
        path: Url,
        body: B,
    ) -> reqwest::Result<Envelope<T>>
    where
        T: DeserializeOwned + Send,
        B: Serialize + std::fmt::Debug + Send,
    {
        debug!("Sending request {} ({})", method, path);
        let request = self._prepare_with_body(method, path, body);
        let resp = request.send().await?;
        Ok(resp.into())
    }
}

// Ensure that MetaClient is Send + Sync. Compiler will fail if it's not.
const _: () = {
    fn assert_send<T: Send + Sync>() {}
    let _ = assert_send::<MetaClient>;
};
