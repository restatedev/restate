// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use arc_swap::ArcSwap;
use aws_credential_types::cache::{
    CredentialsCache, ProvideCachedCredentials, SharedCredentialsCache,
};
use aws_credential_types::provider::error::CredentialsError;
use aws_credential_types::provider::future::ProvideCredentials;
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_sdk_lambda::config;
use aws_sdk_lambda::config::Region;
use aws_sdk_lambda::operation::invoke::InvokeError;
use aws_sdk_lambda::primitives::Blob;
use aws_sdk_sts::operation::assume_role::AssumeRoleError;
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
use std::time::SystemTime;

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
    aws_profile: Option<ByteString>,

    /// # AssumeRole external ID
    ///
    /// An external ID to apply to any AssumeRole operations taken by this client.
    /// https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-user_externalid.html
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
    // own `cloned` client on completion. A `tokio::sync::OnceCell` isn't ideal here because we would
    // have to store the input parameters (ie profile_name) in order to call `get_or_init` during every
    // `invoke`, whereas Shared drops the future (and its captured params) when it completes.
    inner: Shared<BoxFuture<'static, LambdaClientInner>>,
    assumed_role_providers: Arc<ArcSwap<HashMap<Option<ByteString>, CachedAssumeRoleProvider>>>,
    assume_role_external_id: Option<ByteString>,
}

#[derive(Clone, Debug)]
struct LambdaClientInner {
    lambda_client: aws_sdk_lambda::Client,
    sts_client: aws_sdk_sts::Client,
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
            let sts_conf = aws_sdk_sts::Config::from(&config);
            let sts_client = aws_sdk_sts::Client::from_conf(sts_conf);

            // create a new lambda config without credentials set
            let mut lambda_conf = config::Builder::default();
            lambda_conf = lambda_conf.region(config.region().cloned());
            lambda_conf.set_use_fips(config.use_fips());
            lambda_conf.set_use_dual_stack(config.use_dual_stack());
            lambda_conf.set_endpoint_url(config.endpoint_url().map(|s| s.to_string()));
            lambda_conf.set_retry_config(config.retry_config().cloned());
            lambda_conf.set_timeout_config(config.timeout_config().cloned());
            lambda_conf.set_sleep_impl(config.sleep_impl());
            lambda_conf.set_http_connector(config.http_connector().cloned());
            lambda_conf.set_time_source(config.time_source());
            lambda_conf.set_app_name(config.app_name().cloned());

            let lambda_client = aws_sdk_lambda::Client::from_conf(lambda_conf.build());

            LambdaClientInner {
                lambda_client,
                sts_client,
            }
        }
        .boxed()
        .shared();

        Self {
            inner,
            assumed_role_providers: Default::default(),
            assume_role_external_id,
        }
    }

    pub fn invoke(
        &self,
        arn: LambdaARN,
        assume_role: Option<ByteString>,
        body: Body,
        path: PathAndQuery,
        headers: HeaderMap<HeaderValue>,
    ) -> impl Future<Output = Result<Response<Body>, LambdaError>> + Send + 'static {
        let function_name = arn.to_string();
        let region = Region::new(arn.region().to_string());
        let inner = self.inner.clone();
        let assumed_role_providers = self.assumed_role_providers.clone();
        let assume_role_external_id = self.assume_role_external_id.clone();
        let body = body::to_bytes(body);

        async move {
            let (body, inner): (Result<Bytes, hyper::Error>, LambdaClientInner) =
                futures::join!(body, inner);

            let payload = ApiGatewayProxyRequest {
                path: Some(path.path().to_string()),
                http_method: Method::POST,
                headers,
                body: body?,
                is_base64_encoded: true,
            };

            let mut config_override = config::Builder::default().region(region);

            let providers = assumed_role_providers.load();
            let provider = match providers.get(&assume_role) {
                Some(provider) => provider.clone(),
                None => {
                    let provider = CachedAssumeRoleProvider::new(
                        inner.sts_client.clone(),
                        assume_role.clone().map(|role_arn| {
                            AssumeRoleParameters::new(role_arn, assume_role_external_id.clone())
                        }),
                    )
                    .await;
                    assumed_role_providers.rcu(|cache| {
                        let mut cache = HashMap::clone(&cache);
                        cache.insert(assume_role.clone(), provider.clone());
                        cache
                    });
                    provider
                }
            };

            // no point building a cache for just this request; we use the providers cache
            config_override = config_override.credentials_cache(CredentialsCache::no_caching());
            config_override = config_override.credentials_provider(provider);

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

#[derive(Debug)]
struct AssumeRoleParameters {
    role_arn: ByteString,
    external_id: Option<ByteString>,
}

impl AssumeRoleParameters {
    fn new(role_arn: ByteString, external_id: Option<ByteString>) -> Self {
        Self {
            role_arn,
            external_id,
        }
    }
}

#[derive(Debug, Clone)]
struct CachedAssumeRoleProvider(SharedCredentialsCache);

impl CachedAssumeRoleProvider {
    async fn new(client: aws_sdk_sts::Client, assume_role: Option<AssumeRoleParameters>) -> Self {
        match assume_role {
            Some(assume_role) => Self(CredentialsCache::lazy().create_cache(
                SharedCredentialsProvider::new(AssumeRoleProvider(client, assume_role)),
            )),
            None => Self(
                client
                    .config()
                    .credentials_cache()
                    .expect("sts client must have a credentials cache"),
            ),
        }
    }
}

impl aws_credential_types::provider::ProvideCredentials for CachedAssumeRoleProvider {
    fn provide_credentials<'a>(&'a self) -> ProvideCredentials<'a>
    where
        Self: 'a,
    {
        self.0.provide_cached_credentials()
    }
}

/// AssumeRoleProvider implements ProvideCredentials
#[derive(Debug)]
struct AssumeRoleProvider(aws_sdk_sts::Client, AssumeRoleParameters);

impl aws_credential_types::provider::ProvideCredentials for AssumeRoleProvider {
    fn provide_credentials<'a>(&'a self) -> ProvideCredentials<'a>
    where
        Self: 'a,
    {
        ProvideCredentials::new(async {
            let mut fluent_builder = self
                .0
                .assume_role()
                .role_session_name(format!(
                    "restate-{}",
                    SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs()
                ))
                .role_arn(self.1.role_arn.clone());

            if let Some(external_id) = self.1.external_id.clone() {
                fluent_builder = fluent_builder.external_id(external_id);
            }

            let assumed = fluent_builder.send().await;
            match assumed {
                Ok(assumed) => into_credentials(assumed.credentials, "AssumeRoleProvider"),
                Err(aws_smithy_http::result::SdkError::ServiceError(ref context))
                    if matches!(
                        context.err(),
                        AssumeRoleError::RegionDisabledException(_)
                            | AssumeRoleError::MalformedPolicyDocumentException(_)
                    ) =>
                {
                    Err(CredentialsError::invalid_configuration(
                        assumed.err().unwrap(),
                    ))
                }

                Err(aws_smithy_http::result::SdkError::ServiceError(_)) => {
                    Err(CredentialsError::provider_error(assumed.err().unwrap()))
                }
                Err(err) => Err(CredentialsError::provider_error(err)),
            }
        })
    }
}

fn into_credentials(
    sts_credentials: Option<aws_sdk_sts::types::Credentials>,
    provider_name: &'static str,
) -> aws_credential_types::provider::Result {
    let sts_credentials = sts_credentials
        .ok_or_else(|| CredentialsError::unhandled("STS credentials must be defined"))?;
    let expiration = std::time::SystemTime::try_from(
        sts_credentials
            .expiration
            .ok_or_else(|| CredentialsError::unhandled("missing expiration"))?,
    )
    .map_err(|_| {
        CredentialsError::unhandled(
            "credential expiration time cannot be represented by a SystemTime",
        )
    })?;
    Ok(aws_credential_types::Credentials::new(
        sts_credentials
            .access_key_id
            .ok_or_else(|| CredentialsError::unhandled("access key id missing from result"))?,
        sts_credentials
            .secret_access_key
            .ok_or_else(|| CredentialsError::unhandled("secret access token missing"))?,
        sts_credentials.session_token,
        Some(expiration),
        provider_name,
    ))
}
