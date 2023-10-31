// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use aws_sdk_lambda::config;
use aws_sdk_lambda::config::Region;
use aws_sdk_lambda::operation::invoke::InvokeError;
use aws_sdk_lambda::primitives::Blob;
use base64::display::Base64Display;
use base64::Engine;
use futures::future::{BoxFuture, Shared};
use futures::FutureExt;
use hyper::body::Bytes;
use hyper::http::request::Parts;
use hyper::http::uri::PathAndQuery;
use hyper::http::HeaderValue;
use hyper::{body, Body, HeaderMap, Method, Response};
use restate_types::identifiers::LambdaARN;
use serde::ser::Error as _;
use serde::ser::SerializeMap;
use serde::{Deserialize, Serialize, Serializer};
use serde_with::serde_as;
use std::fmt::Debug;
use std::future::Future;

/// # Lambda client options
#[serde_as]
#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
#[cfg_attr(
    feature = "options_schema",
    schemars(rename = "LambdaClientOptions", default)
)]
#[builder(default)]
pub struct Options {
    /// # AWS Profile
    ///
    /// Name of the AWS profile to select. Defaults to 'AWS_PROFILE' env var, or otherwise
    /// the `default` profile.
    aws_profile: Option<String>,
}

impl Options {
    pub fn build(self) -> LambdaClient {
        LambdaClient::new(self.aws_profile)
    }
}

#[derive(Clone, Debug)]
pub struct LambdaClient {
    // we use Shared here to allow concurrent requests to all await this promise, each getting their
    // own `cloned` client on completion. A `tokio::sync::OnceCell` isn't ideal here because we would
    // have to store the input parameters (ie profile_name) in order to call `get_or_init` during every
    // `invoke`, whereas Shared drops the future (and its captured params) when it completes.
    client: Shared<BoxFuture<'static, aws_sdk_lambda::Client>>,
}

impl LambdaClient {
    pub fn new(profile_name: Option<String>) -> Self {
        // create client for a default region, region can be overridden per request
        let mut config = aws_config::from_env().region(Region::from_static("us-east-1"));
        if let Some(profile_name) = profile_name {
            config = config.profile_name(profile_name);
        };

        Self {
            client: async { aws_sdk_lambda::Client::new(&config.load().await) }
                .boxed()
                .shared(),
        }
    }

    pub fn invoke(
        &self,
        arn: LambdaARN,
        body: Body,
        path: PathAndQuery,
        headers: HeaderMap<HeaderValue>,
    ) -> impl Future<Output = Result<Response<Body>, LambdaError>> + Send + 'static {
        let function_name = arn.to_string();
        let region = Region::new(arn.region().to_string());
        let client = self.client.clone();
        let body = body::to_bytes(body);

        async move {
            let (body, client): (Result<Bytes, hyper::Error>, aws_sdk_lambda::Client) =
                futures::join!(body, client);

            let payload = ApiGatewayProxyRequest {
                path: Some(path.path().to_string()),
                http_method: Method::POST,
                headers,
                body: body?,
                is_base64_encoded: true,
            };

            let res = client
                .invoke()
                .function_name(function_name)
                .payload(Blob::new(
                    serde_json::to_vec(&payload).map_err(LambdaError::SerializationError)?,
                ))
                .customize()
                .await?
                .config_override(config::Builder::default().region(region))
                .send()
                .await?;

            if res.function_error().is_some() {
                return if let Some(payload) = res.payload() {
                    let error: serde_json::Value = serde_json::from_slice(payload.as_ref())
                        .map_err(LambdaError::DeserializationError)?;
                    Err(LambdaError::FunctionError(error))
                } else {
                    Err(LambdaError::FunctionError(serde_json::Value::Null))
                };
            }

            if let Some(payload) = res.payload() {
                let response: ApiGatewayProxyResponse = serde_json::from_slice(payload.as_ref())
                    .map_err(LambdaError::DeserializationError)?;
                return response.try_into();
            }

            Err(LambdaError::MissingResponse)
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum LambdaError {
    #[error("problem reading request body: {0}")]
    Body(#[from] hyper::Error),
    #[error("error returned from Invoke: {description}: {source}")]
    InvokeError {
        description: String,
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
    #[error("function returned an error during execution: {0}")]
    FunctionError(serde_json::Value),
    #[error("function request could not be serialized: {0}")]
    SerializationError(serde_json::Error),
    #[error("function response could not be deserialized: {0}")]
    DeserializationError(serde_json::Error),
    #[error("function response body could not be interpreted as base64: {0}")]
    Base64Error(base64::DecodeError),
    #[error("function returned neither a payload or an error")]
    MissingResponse,
}

impl<R: Send + Debug + Sync + 'static> From<aws_sdk_lambda::error::SdkError<InvokeError, R>>
    for LambdaError
{
    fn from(err: aws_sdk_lambda::error::SdkError<InvokeError, R>) -> Self {
        Self::InvokeError {
            description: err.to_string(),
            source: err.into_source().unwrap_or_else(|err| err.into()),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiGatewayProxyRequest {
    pub path: Option<String>,
    #[serde(serialize_with = "serialize_method")]
    pub http_method: Method,
    #[serde(serialize_with = "serialize_headers")]
    pub headers: HeaderMap,
    #[serde(serialize_with = "serialize_body")]
    pub body: Bytes,
    pub is_base64_encoded: bool,
}

impl From<(Parts, Bytes)> for ApiGatewayProxyRequest {
    fn from((parts, body): (Parts, Bytes)) -> Self {
        let uri_parts = parts.uri.into_parts();
        let path = uri_parts.path_and_query.map(|pq| pq.path().to_string());
        ApiGatewayProxyRequest {
            path,
            http_method: parts.method,
            headers: parts.headers,
            body,
            is_base64_encoded: true,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ApiGatewayProxyResponse {
    pub status_code: u16,
    #[serde(with = "http_serde::header_map")]
    pub headers: HeaderMap,
    pub body: Option<String>,
    #[serde(default)]
    pub is_base64_encoded: bool,
}

impl TryFrom<ApiGatewayProxyResponse> for Response<Body> {
    type Error = LambdaError;

    fn try_from(response: ApiGatewayProxyResponse) -> Result<Self, Self::Error> {
        let body = if let Some(body) = response.body {
            if response.is_base64_encoded {
                Body::from(
                    base64::engine::general_purpose::STANDARD
                        .decode(body)
                        .map_err(LambdaError::Base64Error)?,
                )
            } else {
                Body::from(body)
            }
        } else {
            Body::empty()
        };

        let builder = Response::builder().status(response.status_code);
        let builder = response
            .headers
            .iter()
            .fold(builder, |builder, (k, v)| builder.header(k, v));

        Ok(builder.body(body).expect("response must be created"))
    }
}

fn serialize_method<S: Serializer>(method: &Method, ser: S) -> Result<S::Ok, S::Error> {
    ser.serialize_str(method.as_str())
}

fn serialize_headers<S>(headers: &HeaderMap, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut map = serializer.serialize_map(Some(headers.keys_len()))?;
    for key in headers.keys() {
        let map_value = headers[key].to_str().map_err(S::Error::custom)?;
        map.serialize_entry(key.as_str(), map_value)?;
    }
    map.end()
}

fn serialize_body<S>(body: &Bytes, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.collect_str(&Base64Display::new(
        body.as_ref(),
        &base64::engine::general_purpose::STANDARD,
    ))
}
