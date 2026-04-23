// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::error::Error;

use bytes::Bytes;
use http::header::{ACCEPT, CONTENT_TYPE};
use http::uri::{InvalidUri, PathAndQuery};
use http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use http_body::Body;
use http_body_util::BodyExt;

use restate_service_client::{Method, Parts, Request, ServiceClient, ServiceClientError};
use restate_types::schema::deployment::Deployment;

const APPLICATION_JSON: HeaderValue = HeaderValue::from_static("application/json");
const APPLICATION_OCTET_STREAM: HeaderValue = HeaderValue::from_static("application/octet-stream");
const X_RESTATE_SERDES_PROTOCOL: HeaderName = HeaderName::from_static("x-restate-serdes-protocol");
const X_RESTATE_SERDES_PROTOCOL_V1: HeaderValue = HeaderValue::from_static("v1");

#[derive(Debug, thiserror::Error)]
pub enum SerdesError {
    #[error(
        "got status code '404'. This might indicate the SDK doesn't support the serdes endpoint, or the serde name doesn't exist. Response headers: {0:?}. Body: {1}"
    )]
    NotFound(HeaderMap, Cow<'static, str>),
    #[error("got status code '{0}'. Response headers: {1:?}. Body: {2}")]
    BadStatusCode(StatusCode, HeaderMap, Cow<'static, str>),
    #[error(transparent)]
    Client(#[from] ServiceClientError),
    #[error("invalid uri when building the serdes request: {0}")]
    InvalidUri(#[from] InvalidUri),
}

#[derive(Clone)]
pub struct SerdesClient {
    client: ServiceClient,
}

#[derive(Debug)]
pub struct SerdesRequest<B> {
    pub deployment: Deployment,
    pub service_name: String,
    pub serde_name: String,
    pub body: B,
}

impl SerdesClient {
    pub fn new(client: ServiceClient) -> Self {
        Self { client }
    }

    pub async fn encode<B>(
        &self,
        SerdesRequest {
            deployment,
            service_name,
            serde_name,
            body,
        }: SerdesRequest<B>,
    ) -> Result<restate_service_client::ResponseBody, SerdesError>
    where
        B: Body<Data = Bytes> + Send + Unpin + Sized + 'static,
        <B as Body>::Error: Error + Send + Sync + 'static,
    {
        let path: PathAndQuery =
            format!("/serdes/{service_name}/encode/{serde_name}",).try_into()?;

        let headers = HeaderMap::from_iter([
            (CONTENT_TYPE, APPLICATION_JSON),
            (ACCEPT, APPLICATION_OCTET_STREAM),
            (X_RESTATE_SERDES_PROTOCOL, X_RESTATE_SERDES_PROTOCOL_V1),
        ]);

        let (parts, response_body) = self
            .client
            .call(Request::new(
                Parts::from_deployment(deployment, Method::POST, path, headers),
                body,
            ))
            .await?
            .into_parts();

        if !parts.status.is_success() {
            let body_message = response_body
                .collect()
                .await
                .map(|b| String::from_utf8_lossy(&b.to_bytes()).to_string())
                .unwrap_or_else(|err| format!("Failed to read body {err}"));
            if parts.status == StatusCode::NOT_FOUND {
                return Err(SerdesError::NotFound(parts.headers, body_message.into()));
            }
            return Err(SerdesError::BadStatusCode(
                parts.status,
                parts.headers,
                body_message.into(),
            ));
        }

        Ok(response_body)
    }

    pub async fn decode<B>(
        &self,
        SerdesRequest {
            deployment,
            service_name,
            serde_name,
            body,
        }: SerdesRequest<B>,
    ) -> Result<restate_service_client::ResponseBody, SerdesError>
    where
        B: Body<Data = Bytes> + Send + Unpin + Sized + 'static,
        <B as Body>::Error: Error + Send + Sync + 'static,
    {
        let path: PathAndQuery =
            format!("/serdes/{service_name}/decode/{serde_name}",).try_into()?;

        let headers = HeaderMap::from_iter([
            (CONTENT_TYPE, APPLICATION_OCTET_STREAM),
            (ACCEPT, APPLICATION_JSON),
            (X_RESTATE_SERDES_PROTOCOL, X_RESTATE_SERDES_PROTOCOL_V1),
        ]);

        let (parts, response_body) = self
            .client
            .call(Request::new(
                Parts::from_deployment(deployment, Method::POST, path, headers),
                body,
            ))
            .await?
            .into_parts();

        if !parts.status.is_success() {
            let body_message = response_body
                .collect()
                .await
                .map(|b| String::from_utf8_lossy(&b.to_bytes()).to_string())
                .unwrap_or_else(|err| format!("Failed to read body {err}"));
            if parts.status == StatusCode::NOT_FOUND {
                return Err(SerdesError::NotFound(parts.headers, body_message.into()));
            }
            return Err(SerdesError::BadStatusCode(
                parts.status,
                parts.headers,
                body_message.into(),
            ));
        }

        Ok(response_body)
    }
}
