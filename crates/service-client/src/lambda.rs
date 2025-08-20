// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Some parts copied from https://github.com/awslabs/aws-sdk-rust/blob/0.55.x/sdk/aws-config/src/sts/assume_role.rs
//! License Apache-2.0

use crate::utils::ErrorExt;
use arc_swap::ArcSwap;
use assume_role::AssumeRoleProvider;
use aws_config::BehaviorVersion;
use aws_sdk_lambda::config::Region;
use aws_sdk_lambda::error::{DisplayErrorContext, SdkError};
use aws_sdk_lambda::operation::invoke::InvokeError;
use aws_sdk_lambda::primitives::Blob;
use base64::Engine;
use base64::display::Base64Display;
use bytes::{Buf, Bytes};
use bytestring::ByteString;
use futures::future::{BoxFuture, Shared};
use futures::{FutureExt, TryFutureExt};
use http::uri::PathAndQuery;
use http::{HeaderMap, HeaderValue, Method, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::body::Body;
use restate_types::config::AwsOptions;
use restate_types::identifiers::LambdaARN;
use restate_types::schema::deployment::EndpointLambdaCompression;
use serde::ser::Error as _;
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_with::{DeserializeAs, SerializeAs, serde_as};
use std::collections::HashMap;
use std::error::Error;
use std::fmt::Debug;
use std::future::Future;
use std::sync::Arc;

/// # AssumeRole Cache Mode
///
/// In unbounded case, store Lambda clients that are produced for AssumeRole role ARNs. This is recommended
/// on the invocation path (worker), but not on the discovery path (meta), as the unbounded cache would be a DoS
/// vector.
/// See https://github.com/restatedev/restate/issues/878
pub enum AssumeRoleCacheMode {
    None,
    Unbounded,
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
    /// STS client in order to call AssumeRole if needed
    sts_client: aws_sdk_sts::Client,
    /// Lambda client to use if we aren't assuming a role
    no_role_lambda_client: aws_sdk_lambda::Client,
    /// Lambda client config builder
    /// Keep track of how to create a lambda client with shared http connector etc with the main one
    lambda_client_builder: aws_sdk_lambda::config::Builder,
    /// Map of Role -> Client
    /// We use this map to cache pre-configured clients per role.
    /// If not set, do not cache; the discovery client for example should not cache because there is
    /// a DoS vector
    /// https://github.com/restatedev/restate/issues/878
    role_to_lambda_clients: Option<ArcSwap<HashMap<String, aws_sdk_lambda::Client>>>,
    /// External id to set on assume role requests
    assume_role_external_id: Option<String>,
}

impl LambdaClient {
    pub fn new(
        profile_name: Option<String>,
        assume_role_external_id: Option<String>,
        assume_role_cache_mode: AssumeRoleCacheMode,
    ) -> Self {
        // create client for a default region, region can be overridden per request
        let mut config = aws_config::defaults(BehaviorVersion::latest());
        if let Some(profile_name) = profile_name {
            config = config.profile_name(profile_name);
        };

        let inner = async move {
            let config = config.load().await;

            let sts_conf = aws_sdk_sts::Config::from(&config);
            let sts_client = aws_sdk_sts::Client::from_conf(sts_conf);

            let lambda_client_builder = aws_sdk_lambda::config::Builder::from(&config);

            // Restate has its own retry mechanisms, and the built in retry policy in this library could just confuse things
            let lambda_client_builder =
                lambda_client_builder.retry_config(aws_config::retry::RetryConfig::disabled());

            let lambda_client =
                aws_sdk_lambda::Client::from_conf(lambda_client_builder.clone().build());

            let role_to_lambda_clients = match assume_role_cache_mode {
                AssumeRoleCacheMode::Unbounded => Some(Default::default()),
                AssumeRoleCacheMode::None => None,
            };

            Arc::new(LambdaClientInner {
                no_role_lambda_client: lambda_client,
                sts_client,
                lambda_client_builder,
                role_to_lambda_clients,
                assume_role_external_id,
            })
        }
        .boxed()
        .shared();

        Self { inner }
    }

    pub fn from_options(
        options: &AwsOptions,
        assume_role_cache_mode: AssumeRoleCacheMode,
    ) -> LambdaClient {
        LambdaClient::new(
            options.aws_profile.clone(),
            options.aws_assume_role_external_id.clone(),
            assume_role_cache_mode,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn invoke<B>(
        &self,
        arn: LambdaARN,
        method: Method,
        assume_role_arn: Option<ByteString>,
        compression: Option<EndpointLambdaCompression>,
        body: B,
        path: PathAndQuery,
        mut headers: HeaderMap<HeaderValue>,
    ) -> impl Future<Output = Result<Response<Full<Bytes>>, LambdaError>> + Send + 'static
    where
        B: Body + Send + Unpin + 'static,
        <B as Body>::Data: Send,
        <B as Body>::Error: Error + Send + Sync + 'static,
    {
        let function_name = arn.to_string();
        let region = Region::new(arn.region().to_string());
        let inner = self.inner.clone();

        // Add encoding headers
        if let Some(compression) = compression {
            headers.insert(
                http::header::CONTENT_ENCODING,
                HeaderValue::from_static(compression.http_name()),
            );
            headers.insert(
                http::header::ACCEPT_ENCODING,
                HeaderValue::from_static(compression.http_name()),
            );
        }

        let body = body
            .map_err(|e| LambdaError::Body(Box::new(e)))
            .collect()
            .and_then(move |b| {
                std::future::ready(match compression {
                    None => Ok(b.to_bytes()),
                    Some(EndpointLambdaCompression::Zstd) => {
                        zstd::stream::encode_all(b.aggregate().reader(), 3)
                            .map(Bytes::from)
                            .map_err(|e| LambdaError::Body(Box::new(e)))
                    }
                })
            });

        async move {
            let (body, inner): (Result<Bytes, LambdaError>, Arc<LambdaClientInner>) =
                futures::future::join(body, inner).await;

            let payload = ApiGatewayProxyRequest {
                path: Some(path.path()),
                http_method: method,
                headers,
                body: body?,
                is_base64_encoded: true,
            };

            let res = inner
                .build_invoke(assume_role_arn)
                .function_name(function_name)
                .payload(Blob::new(
                    serde_json::to_vec(&payload).map_err(LambdaError::SerializationError)?,
                ))
                .customize()
                .config_override(aws_sdk_lambda::config::Builder::default().region(region))
                .send()
                .await
                .map_err(Box::new)?;

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
    fn build_invoke(
        &self,
        assume_role_arn: Option<ByteString>,
    ) -> aws_sdk_lambda::operation::invoke::builders::InvokeFluentBuilder {
        let assume_role_arn = if let Some(assume_role_arn) = assume_role_arn {
            assume_role_arn
        } else {
            // fastest path; no assumed role, don't bother with the shared hashmap
            return self.no_role_lambda_client.invoke();
        };

        if let Some(invoke) = self.role_to_lambda_clients.as_ref().and_then(|rlc| {
            rlc.load()
                .get(&*assume_role_arn)
                .map(|client| client.invoke())
        }) {
            // fast-ish path; we've seen this assumed role before
            return invoke;
        }

        // slow path; create the client for this assumed role

        let conf = self
            .lambda_client_builder
            .clone()
            .credentials_provider(AssumeRoleProvider::new(
                self.sts_client.clone(),
                assume_role_arn.to_string(),
                self.assume_role_external_id.clone(),
            ))
            .build();

        let mut client = aws_sdk_lambda::Client::from_conf(conf);

        if let Some(rlc) = &self.role_to_lambda_clients {
            // repeatedly try to clone the hashmap and compare and swap in a map with the new arn
            rlc.rcu(|cache| {
                if let Some(existing_client) = cache.get(&*assume_role_arn) {
                    // someone else got there first; instead of cloning the whole hashmap, just keep track
                    // of the winning client and write the existing hashmap back into the ArcSwap
                    client = existing_client.clone();
                    return Arc::clone(cache);
                }
                let mut cache = HashMap::clone(cache);
                cache.insert(assume_role_arn.to_string(), client.clone());
                cache.into()
            });
        }

        client.invoke()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum LambdaError {
    #[error("problem reading request body: {0}")]
    Body(#[from] Box<dyn Error + Send + Sync>),
    #[error("lambda service returned error: {}", DisplayErrorContext(&.0))]
    SdkError(#[from] Box<SdkError<InvokeError>>),
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

impl LambdaError {
    /// Retryable errors are those which can be caused by transient faults and where
    /// retrying can succeed.
    pub fn is_retryable(&self) -> bool {
        match self {
            LambdaError::SdkError(err) => err.is_retryable(),
            LambdaError::Body(_)
            | LambdaError::FunctionError(_)
            | LambdaError::SerializationError(_)
            | LambdaError::DeserializationError(_)
            | LambdaError::Base64Error(_)
            | LambdaError::MissingResponse => false,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiGatewayProxyRequest<'a> {
    pub path: Option<&'a str>,
    #[serde(serialize_with = "serialize_method")]
    pub http_method: Method,
    #[serde(serialize_with = "serialize_headers")]
    pub headers: HeaderMap,
    #[serde(serialize_with = "serialize_body")]
    pub body: Bytes,
    pub is_base64_encoded: bool,
}

#[serde_as]
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ApiGatewayProxyResponse {
    pub status_code: u16,
    #[serde_as(as = "Option<HttpHeaderMap>")]
    pub headers: Option<HeaderMap>,
    pub body: Option<String>,
    #[serde(default)]
    pub is_base64_encoded: bool,
}

struct HttpHeaderMap;

impl SerializeAs<HeaderMap> for HttpHeaderMap {
    fn serialize_as<S>(source: &HeaderMap, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        http_serde::header_map::serialize(source, serializer)
    }
}

impl<'de> DeserializeAs<'de, HeaderMap> for HttpHeaderMap {
    fn deserialize_as<D>(deserializer: D) -> Result<HeaderMap, D::Error>
    where
        D: Deserializer<'de>,
    {
        http_serde::header_map::deserialize(deserializer)
    }
}

impl TryFrom<ApiGatewayProxyResponse> for Response<Full<Bytes>> {
    type Error = LambdaError;

    fn try_from(response: ApiGatewayProxyResponse) -> Result<Self, Self::Error> {
        let body = if let Some(body) = response.body {
            if response.is_base64_encoded {
                Bytes::from(
                    base64::engine::general_purpose::STANDARD
                        .decode(body)
                        .map_err(LambdaError::Base64Error)?,
                )
            } else {
                Bytes::from(body)
            }
        } else {
            Bytes::default()
        };

        // Figure out if the response body is compressed
        let mut compression: Option<EndpointLambdaCompression> = None;
        if let Some(content_encoding_header) = response
            .headers
            .as_ref()
            .and_then(|hm| hm.get(http::header::CONTENT_ENCODING))
        {
            if content_encoding_header.to_str().is_ok_and(|hv| {
                hv.trim()
                    .eq_ignore_ascii_case(EndpointLambdaCompression::Zstd.http_name())
            }) {
                compression = Some(EndpointLambdaCompression::Zstd);
            }
        }

        // Run compression if needed
        let final_body = match compression {
            None => body,
            Some(EndpointLambdaCompression::Zstd) => zstd::stream::decode_all(body.reader())
                .map(Bytes::from)
                .map_err(|e| LambdaError::Body(Box::new(e)))?,
        };

        // And prepare the http response to give back
        let mut http_response = Response::new(final_body.into());
        *http_response.status_mut() =
            StatusCode::from_u16(response.status_code).unwrap_or_default();
        if let Some(headers) = response.headers {
            *http_response.headers_mut() = headers;
        }

        Ok(http_response)
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

mod assume_role {
    use aws_credential_types::provider::error::CredentialsError;
    use aws_credential_types::provider::future::ProvideCredentials;
    use aws_sdk_lambda::error::SdkError;
    use aws_sdk_sts::operation::assume_role::AssumeRoleError;
    use std::time::SystemTime;

    /// AssumeRoleProvider implements ProvideCredentials by assuming a provided role
    /// It is materially very similar to AssumeRoleProvider in the aws-config crate, except
    /// it is able to reuse the same sts client across many providers
    #[derive(Debug)]
    pub(crate) struct AssumeRoleProvider {
        client: aws_sdk_sts::Client,
        role_arn: String,
        external_id: Option<String>,
    }

    impl AssumeRoleProvider {
        pub(crate) fn new(
            client: aws_sdk_sts::Client,
            role_arn: String,
            external_id: Option<String>,
        ) -> Self {
            Self {
                client,
                role_arn,
                external_id,
            }
        }
    }

    impl aws_credential_types::provider::ProvideCredentials for AssumeRoleProvider {
        fn provide_credentials<'a>(&'a self) -> ProvideCredentials<'a>
        where
            Self: 'a,
        {
            ProvideCredentials::new(async {
                let mut fluent_builder = self
                    .client
                    .assume_role()
                    .role_session_name(format!(
                        "restate-{}",
                        SystemTime::now()
                            .duration_since(SystemTime::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs()
                    ))
                    .role_arn(self.role_arn.clone());

                if let Some(external_id) = &self.external_id {
                    fluent_builder = fluent_builder.external_id(external_id.clone());
                }

                let assumed = fluent_builder.send().await;
                match assumed {
                    Ok(assumed) => {
                        into_credentials(assumed.credentials, "RestateAssumeRoleProvider")
                    }
                    Err(SdkError::ServiceError(ref context))
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

                    Err(SdkError::ServiceError(_)) => {
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
        let expiration = SystemTime::try_from(sts_credentials.expiration).map_err(|_| {
            CredentialsError::unhandled(
                "credential expiration time cannot be represented by a SystemTime",
            )
        })?;
        Ok(aws_credential_types::Credentials::new(
            sts_credentials.access_key_id,
            sts_credentials.secret_access_key,
            Some(sts_credentials.session_token),
            Some(expiration),
            provider_name,
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::lambda::ApiGatewayProxyResponse;

    #[test]
    fn serde_api_gateway_proxy_response_wo_headers() -> googletest::Result<()> {
        let api_gateway_proxy_response: ApiGatewayProxyResponse = serde_json::from_str(
            r#"{"statusCode":200,"multiValueHeaders":null,"body":"foobar\n"}"#,
        )?;

        assert_eq!(api_gateway_proxy_response.status_code, 200);

        Ok(())
    }

    #[test]
    fn serde_api_gateway_proxy_response_headers_null() -> googletest::Result<()> {
        let api_gateway_proxy_response: ApiGatewayProxyResponse = serde_json::from_str(
            r#"{"statusCode":200,"headers":null,"multiValueHeaders":null,"body":"foobar\n"}"#,
        )?;

        assert_eq!(api_gateway_proxy_response.status_code, 200);

        Ok(())
    }
}
