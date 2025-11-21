// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A wrapper client for admin HTTP service.

use std::time::Duration;

use anyhow::bail;
use http::StatusCode;
use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;
use tracing::{debug, info};
use url::Url;

use restate_admin_rest_model::version::{AdminApiVersion, VersionInformation};
use restate_cli_util::{CliContext, c_warn};
use restate_types::SemanticRestateVersion;
use restate_types::net::address::PeerNetAddress;

use crate::build_info;
use crate::cli_env::CliEnv;
use crate::clients::AdminClientInterface;

use super::errors::ApiError;

/// Min/max supported admin API versions
pub const MIN_ADMIN_API_VERSION: AdminApiVersion = AdminApiVersion::V2;
pub const MAX_ADMIN_API_VERSION: AdminApiVersion = AdminApiVersion::V3;

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

    pub async fn into_api_error(self) -> Result<ApiError, Error> {
        let http_status_code = self.inner.status();
        let url = self.inner.url().clone();

        debug!("Response from {} ({})", url, http_status_code);
        let body = self.inner.text().await?;
        debug!("  {}", body);
        Ok(ApiError {
            http_status_code,
            url,
            body: serde_json::from_str(&body)?,
        })
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

/// A handy client for the admin HTTP service.
#[derive(Clone)]
pub struct AdminClient {
    pub(crate) inner: reqwest::Client,
    pub(crate) base_url: Url,
    pub(crate) bearer_token: Option<String>,
    pub(crate) request_timeout: Duration,
    pub(crate) admin_api_version: AdminApiVersion,
    pub(crate) restate_server_version: SemanticRestateVersion,
    pub(crate) advertised_ingress_address: Option<String>,
}

impl AdminClient {
    pub async fn new(env: &CliEnv) -> anyhow::Result<Self> {
        let advertised_address = env.admin_base_url()?.clone();

        let builder = reqwest::Client::builder()
            .user_agent(format!(
                "{}/{} {}-{}",
                env!("CARGO_PKG_NAME"),
                build_info::RESTATE_CLI_VERSION,
                std::env::consts::OS,
                std::env::consts::ARCH,
            ))
            .connect_timeout(CliContext::get().connect_timeout())
            .danger_accept_invalid_certs(CliContext::get().insecure_skip_tls_verify());

        let (raw_client, base_url) = match advertised_address.into_address()? {
            PeerNetAddress::Uds(path_buf) => {
                let client = builder.unix_socket(path_buf).build()?;
                (client, "http://localhost/".parse().unwrap())
            }
            PeerNetAddress::Http(uri) => {
                let client = builder.build()?;
                // forced to go to string and back because those are two types (Uri vs. Url)
                (client, uri.to_string().parse()?)
            }
        };

        let bearer_token = env.bearer_token()?.map(str::to_string);

        let client = Self {
            inner: raw_client,
            base_url,
            bearer_token,
            request_timeout: CliContext::get().request_timeout(),
            admin_api_version: AdminApiVersion::Unknown,
            restate_server_version: SemanticRestateVersion::unknown(),
            advertised_ingress_address: None,
        };

        if let Ok(envelope) = client.version().await {
            match envelope.into_body().await {
                Ok(version_information) => {
                    return Self::choose_api_version(client, version_information);
                }
                Err(err) => debug!("Failed parsing the version information: {err}"),
            }
        }

        // we couldn't validate the admin API. This could mean that the server is not running or
        // runs an old version which does not support version information. Query the health endpoint
        // to see whether the server is reachable and fail if not.
        if client
            .health()
            .await
            .map_err(Into::into)
            .and_then(|r| r.success_or_error())
            .is_err()
        {
            bail!(
                "Unable to connect to the Restate server '{}'. Please make sure that it is running and reachable.",
                client.base_url
            );
        }

        c_warn!(
            "Could not verify the admin API version. Please make sure that your CLI is compatible with the Restate server '{}'.",
            client.base_url
        );
        Ok(client)
    }

    pub fn versioned_url(&self, path: impl IntoIterator<Item = impl AsRef<str>>) -> Url {
        let mut url = self.base_url.clone();

        {
            let mut segments = url.path_segments_mut().expect("Bad url!");
            segments.pop_if_empty();

            match self.admin_api_version {
                AdminApiVersion::Unknown => segments.extend(path),
                // v1 clusters didn't support versioned urls
                AdminApiVersion::V1 => segments.extend(path),
                AdminApiVersion::V2 => segments.push("v2").extend(path),
                AdminApiVersion::V3 => segments.push("v3").extend(path),
            };
        }

        url
    }

    fn choose_api_version(
        mut client: AdminClient,
        version_information: VersionInformation,
    ) -> anyhow::Result<AdminClient> {
        if let Some(admin_api_version) = AdminApiVersion::choose_max_supported_version(
            MIN_ADMIN_API_VERSION..=MAX_ADMIN_API_VERSION,
            version_information.min_admin_api_version..=version_information.max_admin_api_version,
        ) {
            client.restate_server_version =
                match SemanticRestateVersion::parse(&version_information.version) {
                    Ok(version) => version,
                    Err(err) => {
                        debug!(
                            "Failed to parse Restate server version {}: {err}",
                            version_information.version
                        );
                        SemanticRestateVersion::unknown()
                    }
                };
            client.admin_api_version = admin_api_version;
            client.advertised_ingress_address =
                version_information.ingress_endpoint.map(|u| u.to_string());
            Ok(client)
        } else {
            bail!(
                "The CLI is not compatible with the Restate server '{}'. Please update the CLI to match the Restate server version '{}'.",
                client.base_url,
                version_information.version
            );
        }
    }

    /// Prepare a request builder for the given method and path.
    pub(crate) fn prepare(&self, method: reqwest::Method, path: Url) -> reqwest::RequestBuilder {
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
    pub(crate) fn run<T>(
        &self,
        method: reqwest::Method,
        path: Url,
    ) -> impl Future<Output = reqwest::Result<Envelope<T>>> + 'static
    where
        T: DeserializeOwned + Send,
    {
        debug!("Sending request {} ({})", method, path);
        let request = self.prepare(method, path.clone());
        async move {
            let resp = request.send().await?;
            debug!("Response from {} ({})", path, resp.status());
            Ok(resp.into())
        }
    }

    pub(crate) fn run_with_body<T, B>(
        &self,
        method: reqwest::Method,
        path: Url,
        body: B,
    ) -> impl Future<Output = reqwest::Result<Envelope<T>>> + 'static
    where
        T: DeserializeOwned + Send,
        B: Serialize + std::fmt::Debug + Send,
    {
        debug!("Sending request {} ({}): {:?}", method, path, body);
        let request = self.prepare_with_body(method, path.clone(), body);
        async move {
            let resp = request.send().await?;
            debug!("Response from {} ({})", path, resp.status());
            Ok(resp.into())
        }
    }
}

// Ensure that AdminClient is Send + Sync. Compiler will fail if it's not.
const _: () = {
    const fn assert_send<T: Send + Sync>() {}
    assert_send::<AdminClient>();
};
