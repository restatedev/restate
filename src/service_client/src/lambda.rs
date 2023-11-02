// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::assume_role::{AssumeRoleProvider, PreCachedCredentialsProvider};

use arc_swap::ArcSwap;
use aws_credential_types::cache::CredentialsCache;
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_sdk_lambda::config::Region;
use aws_sdk_lambda::operation::invoke::InvokeError;
use aws_sdk_lambda::primitives::Blob;
use aws_smithy_types::error::display::DisplayErrorContext;
use base64::display::Base64Display;
use base64::Engine;
use bytestring::ByteString;
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
use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::sync::Arc;

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
    #[cfg_attr(feature = "options_schema", schemars(with = "Option<String>"))]
    aws_profile: Option<ByteString>,

    /// # AssumeRole external ID
    ///
    /// An external ID to apply to any AssumeRole operations taken by this client.
    /// https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-user_externalid.html
    #[cfg_attr(feature = "options_schema", schemars(with = "Option<String>"))]
    assume_role_external_id: Option<ByteString>,
}

impl Options {
    pub fn build(self) -> LambdaClient {
        LambdaClient::new(self.aws_profile, self.assume_role_external_id)
    }
}

#[derive(Clone, Debug)]
pub struct LambdaClient {
    // we use Shared here to allow concurrent requests to all await this promise, each getting their
    // own `cloned` inner on completion. A `tokio::sync::OnceCell` isn't ideal here because we would
    // have to store the input parameters (eg aws_profile) in order to call `get_or_init` during every
    // `invoke`, whereas Shared drops the future (and its captured params) when it completes.
    inner: Shared<BoxFuture<'static, Arc<LambdaClientInner>>>,
}

#[derive(Debug)]
struct LambdaClientInner {
    lambda_client: aws_sdk_lambda::Client,
    sts_client: aws_sdk_sts::Client,

    // keep track of credentials to use if we aren't assuming a role
    default_credentials: SharedCredentialsProvider,
    // keep track of the parameters to use to create credential caches, as we need to do this on the fly
    // for new assume role arns
    credentials_cache_config: CredentialsCache,
    // threadsafe map of assume role arn -> cached credential provider
    assume_role_credentials: ArcSwap<HashMap<String, SharedCredentialsProvider>>,
    // external id to set on assume role requests
    assume_role_external_id: Option<ByteString>,
}

impl LambdaClient {
    pub fn new(
        profile_name: Option<ByteString>,
        assume_role_external_id: Option<ByteString>,
    ) -> Self {
        // create client for a default region, region can be overridden per request
        let mut config = aws_config::from_env().region(Region::from_static("us-east-1"));
        if let Some(profile_name) = profile_name {
            config = config.profile_name(profile_name);
        };

        let inner = async {
            let config = config.load().await;
            let credentials_cache_config = config
                .credentials_cache()
                .expect("default config must have a credentials cache config")
                .clone();

            let sts_conf = aws_sdk_sts::Config::from(&config);
            let default_credentials =
                SharedCredentialsProvider::new(PreCachedCredentialsProvider::new(
                    sts_conf
                        .credentials_cache()
                        .expect("sts client must be configured with a credentials provider"),
                ));
            let sts_client = aws_sdk_sts::Client::from_conf(sts_conf);

            // create a new lambda config without any source of credentials (these will be added dynamically)
            let mut lambda_conf = aws_sdk_lambda::config::Builder::from(&config);
            lambda_conf.set_credentials_provider(None);
            lambda_conf.set_credentials_cache(None);

            let lambda_client = aws_sdk_lambda::Client::from_conf(lambda_conf.build());

            Arc::new(LambdaClientInner {
                lambda_client,
                sts_client,
                default_credentials,
                credentials_cache_config,
                assume_role_credentials: Default::default(),
                assume_role_external_id,
            })
        }
        .boxed()
        .shared();

        Self { inner }
    }

    pub fn invoke(
        &self,
        arn: LambdaARN,
        assume_role_arn: Option<ByteString>,
        body: Body,
        path: PathAndQuery,
        headers: HeaderMap<HeaderValue>,
    ) -> impl Future<Output = Result<Response<Body>, LambdaError>> + Send + 'static {
        let function_name = arn.to_string();
        let region = Region::new(arn.region().to_string());
        let inner = self.inner.clone();
        let body = body::to_bytes(body);

        async move {
            let (body, inner): (Result<Bytes, hyper::Error>, Arc<LambdaClientInner>) =
                futures::join!(body, inner);

            let payload = ApiGatewayProxyRequest {
                path: Some(path.path().to_string()),
                http_method: Method::POST,
                headers,
                body: body?,
                is_base64_encoded: true,
            };

            let mut config_override = aws_sdk_lambda::config::Builder::default().region(region);

            // the library forces us to tell it how to cache credentials, instead of just giving it a cache.
            // As a result, just tell it not to cache; our providers will handle their own caching
            config_override = config_override.credentials_cache(CredentialsCache::no_caching());

            let credentials_provider = match assume_role_arn {
                // no role to assume, just use default creds
                None => inner.default_credentials.clone(),
                Some(assume_role_arn) => inner.get_provider(&assume_role_arn),
            };

            config_override.set_credentials_provider(Some(credentials_provider));

            let res = inner
                .lambda_client
                .invoke()
                .function_name(function_name)
                .payload(Blob::new(
                    serde_json::to_vec(&payload).map_err(LambdaError::SerializationError)?,
                ))
                .customize()
                .await
                .expect("customize() must not return an error") // the sdk is commented to say that this eventually won't be a Result
                .config_override(config_override)
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

impl LambdaClientInner {
    fn get_provider(&self, assume_role_arn: &ByteString) -> SharedCredentialsProvider {
        if let Some(provider) = self
            .assume_role_credentials
            .load()
            .get::<str>(assume_role_arn)
        {
            return provider.clone();
        }

        // create a new cached provider of credentials for this arn
        let provider = SharedCredentialsProvider::new(PreCachedCredentialsProvider::new(
            self.credentials_cache_config
                .clone()
                .create_cache(SharedCredentialsProvider::new(AssumeRoleProvider::new(
                    self.sts_client.clone(),
                    assume_role_arn.clone(),
                    self.assume_role_external_id.clone(),
                ))),
        ));

        // repeatedly try to clone the hashmap and compare and swap in a map with the new arn
        self.assume_role_credentials.rcu(|cache| {
            let mut cache = HashMap::clone(cache);
            cache.insert(assume_role_arn.to_string(), provider.clone());
            cache
        });
        provider
    }
}

#[derive(Debug, thiserror::Error)]
pub enum LambdaError {
    #[error("problem reading request body: {0}")]
    Body(#[from] hyper::Error),
    #[error("lambda service returned error: {}", DisplayErrorContext(&.0))]
    SdkError(
        #[from]
        aws_smithy_http::result::SdkError<InvokeError, Response<aws_smithy_http::body::SdkBody>>,
    ),
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
