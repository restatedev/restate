// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::error::Error;
use std::fmt::Formatter;
use std::sync::Arc;

use ::http::{HeaderName, HeaderValue, Version};
use arc_swap::ArcSwapOption;
use bytes::Bytes;
use bytestring::ByteString;
use core::fmt;
use futures::{FutureExt, future};
use http_body_util::Full;
use hyper::body::Body;
use hyper::http::uri::PathAndQuery;
use hyper::{HeaderMap, Response, Uri};

use restate_types::config::ServiceClientOptions;
use restate_types::deployment::HttpAuth;
use restate_types::identifiers::LambdaARN;
use restate_types::schema::deployment::{Deployment, DeploymentType, EndpointLambdaCompression};

pub use crate::gcp::{GcpAuthError, GcpTokenClient, IdTokenCacheMode};
pub use crate::http::HttpClient;
pub use crate::http::HttpError;
pub use crate::lambda::AssumeRoleCacheMode;
use crate::lambda::LambdaClient;
use crate::request_identity::SignRequest;

mod gcp;
mod http;
mod lambda;
pub mod pool;
mod proxy;
mod request_identity;
#[cfg(any(test, feature = "test_util"))]
mod test_util;
mod utils;

/// Header slot we always use for the Restate-minted Google ID token on HTTP deployments with GCP
/// auth enabled. Cloud Run validates this header in precedence over `Authorization` and strips it
/// before forwarding to the container, so customer-supplied `Authorization` in `additional_headers`
/// passes through to the workload unchanged.
const X_SERVERLESS_AUTHORIZATION: HeaderName =
    HeaderName::from_static("x-serverless-authorization");

pub type ResponseBody = http_body_util::Either<http::ResponseBody, Full<Bytes>>;

#[derive(Clone)]
pub struct ServiceClient {
    http: HttpClient,
    lambda: LambdaClient,
    pub(crate) gcp: GcpTokenClient,
    // this can be changed to re-read periodically if necessary
    request_identity_key: Arc<ArcSwapOption<request_identity::v1::SigningKey>>,
    additional_request_headers: HashMap<HeaderName, HeaderValue>,
}

impl ServiceClient {
    pub(crate) fn new(
        http: HttpClient,
        lambda: LambdaClient,
        gcp: GcpTokenClient,
        request_identity_key: Arc<ArcSwapOption<request_identity::v1::SigningKey>>,
        additional_request_headers: HashMap<HeaderName, HeaderValue>,
    ) -> Self {
        Self {
            http,
            lambda,
            gcp,
            request_identity_key,
            additional_request_headers,
        }
    }

    pub fn from_options(
        options: &ServiceClientOptions,
        assume_role_cache_mode: AssumeRoleCacheMode,
        io_runtime: tokio::runtime::Handle,
    ) -> Result<Self, BuildError> {
        // The GCP token-cache mode mirrors the Lambda assume-role-cache mode.
        // None on admin/discovery dispatch, Unbounded on the worker/invoker
        // dispatch. AssumeRoleCacheMode is the carrier we already plumb.
        let gcp_cache_mode = match assume_role_cache_mode {
            AssumeRoleCacheMode::None => IdTokenCacheMode::None,
            AssumeRoleCacheMode::Unbounded => IdTokenCacheMode::Unbounded,
        };

        let request_identity_key = if let Some(request_identity_private_key_pem_file) =
            options.request_identity_private_key_pem_file.clone()
        {
            Arc::new(ArcSwapOption::from_pointee(
                request_identity::v1::SigningKey::from_pem_file(
                    request_identity_private_key_pem_file,
                    options.request_identity_expiration().to_std(),
                )?,
            ))
        } else {
            Arc::new(ArcSwapOption::empty())
        };

        Ok(Self::new(
            HttpClient::from_options(&options.http, io_runtime),
            LambdaClient::from_options(&options.lambda, assume_role_cache_mode),
            GcpTokenClient::new(gcp_cache_mode),
            request_identity_key,
            options
                .additional_request_headers
                .clone()
                .unwrap_or_default()
                .into(),
        ))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    #[error("Failed to read request identity private key: {0}")]
    SigningPrivateKeyReadError(#[from] request_identity::v1::SigningPrivateKeyReadError),
}

impl ServiceClient {
    pub fn call<B>(
        &self,
        req: Request<B>,
    ) -> impl Future<Output = Result<Response<ResponseBody>, ServiceClientError>> + Send + 'static
    where
        B: Body<Data = Bytes> + Send + Unpin + Sized + 'static,
        <B as Body>::Error: Error + Send + Sync + 'static,
    {
        let (mut parts, body) = req.into_parts();

        let request_identity_key = self.request_identity_key.load();

        let signer = if let Some(request_identity_key) = request_identity_key.as_deref() {
            Some(request_identity::v1::Signer::new(
                parts.path.path(),
                parts.request_identity_sub_field.as_deref(),
                request_identity_key,
            ))
        } else {
            None // will use null signing scheme
        };

        parts.headers = match signer.insert_identity(parts.headers) {
            Ok(headers) => headers,
            Err(err) => return future::ready(Err(err.into())).right_future(),
        };

        parts.headers.extend(
            self.additional_request_headers
                .iter()
                .map(|(k, v)| (k.clone(), v.clone())),
        );

        match parts.address {
            Endpoint::Http(uri, version, auth) => {
                let http = self.http.clone();
                let gcp = self.gcp.clone();
                let method = parts.method.into();
                let path = parts.path;
                let mut headers = parts.headers;
                async move {
                    if let Some(HttpAuth::GoogleIdToken(auth)) = &auth {
                        // The persisted record carries a concrete audience; the wire-to-persisted
                        // conversion at register/re-register time derives one from the URI when the
                        // operator left it unset. No fallback is needed here.
                        let audience = auth.audience().to_string();
                        let impersonate = auth
                            .impersonate_service_account()
                            .map(|b| b.as_ref());
                        let token = gcp
                            .mint(impersonate, &audience)
                            .await
                            .map_err(|e| ServiceClientError::GcpAuth(uri.clone(), e))?;

                        let bearer = ::http::HeaderValue::try_from(format!("Bearer {token}"))
                            .map_err(|e| {
                                ServiceClientError::GcpAuth(
                                    uri.clone(),
                                    gcp::GcpAuthError::Mint {
                                        audience: audience.clone(),
                                        impersonate: impersonate
                                            .unwrap_or("(ambient)")
                                            .to_owned(),
                                        message: format!(
                                            "minted token cannot be used as an HTTP header value: {e}"
                                        ),
                                    },
                                )
                            })?;
                        headers.insert(X_SERVERLESS_AUTHORIZATION, bearer);
                    }
                    let resp = http
                        .request(uri.clone(), version, method, body, path, headers)
                        .await
                        .map_err(|e| ServiceClientError::Http(uri, e))?;
                    Ok(resp.map(http_body_util::Either::Left))
                }
                .left_future()
            }
            Endpoint::Lambda(arn, assume_role_arn, compression) => {
                let fut = self.lambda.invoke(
                    arn.clone(),
                    parts.method.into(),
                    assume_role_arn,
                    compression,
                    body,
                    parts.path,
                    parts.headers,
                );
                async move {
                    Ok(fut
                        .await
                        .map_err(|e| ServiceClientError::Lambda(arn, e))?
                        .map(http_body_util::Either::Right))
                }
                .right_future()
            }
        }
        .left_future()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ServiceClientError {
    #[error("error when calling '{0}': {1}")]
    Http(Uri, #[source] http::HttpError),
    #[error("error when calling '{0}': {1}")]
    Lambda(LambdaARN, #[source] lambda::LambdaError),
    #[error("error minting GCP ID token for '{0}': {1}")]
    GcpAuth(Uri, #[source] gcp::GcpAuthError),
    #[error(transparent)]
    IdentityV1(#[from] <request_identity::v1::Signer<'static, 'static> as SignRequest>::Error),
}

impl ServiceClientError {
    /// Retryable errors are those which can be caused by transient faults and where
    /// retrying can succeed.
    pub fn is_retryable(&self) -> bool {
        match self {
            ServiceClientError::Http(_, http_error) => http_error.is_retryable(),
            ServiceClientError::Lambda(_, lambda_error) => lambda_error.is_retryable(),
            // GCP token-mint errors:
            // - Application Default Credentials (`Adc`) load failure is treated as transient (e.g.
            //   metadata-server briefly unreachable).
            // - `Timeout` from the per-attempt deadline is transient by definition.
            // - `Build` (constructing the credentials builder) is most likely bad configuration.
            // - `Mint` (the actual `id_token().await` call) is blanket-retryable: the underlying
            //   SDK error mixes transient HTTP errors (429, 5xx, network failures from the metadata
            //   server or IAM Credentials API) with permanent failures (bad impersonation perms,
            //   audience refused by the upstream), and the surfaced error type does not expose the
            //   HTTP status cleanly enough to split. The trade-off is that permanent mint failures
            //   retry-and-fail-consistently rather than fail-fast; this is acceptable because the
            //   discovery / invoker retry loops already bound the attempt count so the worst-case
            //   overhead is bounded.
            // - `AmbientUnsupported` is a misconfiguration (the ambient ADC source cannot mint
            //   ID tokens directly); retrying cannot help.
            ServiceClientError::GcpAuth(_, gcp_error) => match gcp_error {
                gcp::GcpAuthError::Adc { .. }
                | gcp::GcpAuthError::Timeout { .. }
                | gcp::GcpAuthError::Mint { .. } => true,
                gcp::GcpAuthError::Build { .. } | gcp::GcpAuthError::AmbientUnsupported { .. } => {
                    false
                }
            },
            ServiceClientError::IdentityV1(_) => false, // this really should never happen
        }
    }
}

pub struct Request<B> {
    head: Parts,
    body: B,
}

impl<B> Request<B> {
    pub fn new(head: Parts, body: B) -> Self {
        Self { head, body }
    }

    pub fn into_parts(self) -> (Parts, B) {
        (self.head, self.body)
    }

    pub fn address(&self) -> &Endpoint {
        &self.head.address
    }

    pub fn path(&self) -> &PathAndQuery {
        &self.head.path
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Method {
    Post,
    Get,
}

impl From<Method> for hyper::http::Method {
    fn from(value: Method) -> Self {
        match value {
            Method::Post => hyper::http::Method::POST,
            Method::Get => hyper::http::Method::GET,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Parts {
    /// The method to use
    method: Method,

    /// The request's target address
    address: Endpoint,

    /// The request's path, for example /discover or /invoke/xyz/abc
    path: PathAndQuery,

    /// The request's headers - in lambda case, mapped to apigatewayevent.headers
    headers: HeaderMap<HeaderValue>,

    /// Additional 'sub' field for the request identity
    request_identity_sub_field: Option<ByteString>,
}

impl Parts {
    pub fn new(
        method: Method,
        address: Endpoint,
        path: PathAndQuery,
        headers: HeaderMap<HeaderValue>,
    ) -> Self {
        Self {
            method,
            address,
            path,
            headers,
            request_identity_sub_field: None,
        }
    }

    pub fn from_deployment(
        deployment: Deployment,
        method: Method,
        path: PathAndQuery,
        mut headers: HeaderMap<HeaderValue>,
    ) -> Self {
        let address = match deployment.ty {
            DeploymentType::Lambda {
                arn,
                assume_role_arn,
                compression,
            } => Endpoint::Lambda(arn, assume_role_arn, compression),
            DeploymentType::Http {
                address,
                http_version,
                auth,
                ..
            } => Endpoint::Http(address, Some(http_version), auth),
        };

        headers.extend(deployment.additional_headers);

        Self::new(method, address, path, headers)
    }

    pub fn with_request_identity_sub_field(mut self, sub_field: ByteString) -> Self {
        self.request_identity_sub_field = Some(sub_field);
        self
    }
}

#[derive(Clone, Debug)]
pub enum Endpoint {
    Http(Uri, Option<Version>, Option<HttpAuth>),
    Lambda(
        LambdaARN,
        Option<ByteString>,
        Option<EndpointLambdaCompression>,
    ),
}

impl fmt::Display for Endpoint {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Http(uri, _, _) => uri.fmt(f),
            Self::Lambda(arn, _, _) => write!(f, "lambda://{arn}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn uri() -> Uri {
        "https://svc.example.com/".parse().unwrap()
    }

    #[test]
    fn gcp_auth_retryability_splits_by_inner_variant() {
        let cases: &[(gcp::GcpAuthError, bool)] = &[
            (
                gcp::GcpAuthError::Adc {
                    audience: "https://svc.example.com".into(),
                    impersonate: "(ambient)".into(),
                    message: "metadata server unreachable".into(),
                },
                true,
            ),
            (
                gcp::GcpAuthError::Timeout {
                    audience: "https://svc.example.com".into(),
                    impersonate: "(ambient)".into(),
                    duration: Duration::from_secs(10),
                },
                true,
            ),
            (
                gcp::GcpAuthError::Build {
                    audience: "https://svc.example.com".into(),
                    message: "bad audience".into(),
                },
                false,
            ),
            (
                gcp::GcpAuthError::AmbientUnsupported {
                    audience: "https://svc.example.com".into(),
                },
                false,
            ),
            (
                gcp::GcpAuthError::Mint {
                    audience: "https://svc.example.com".into(),
                    impersonate: "sa@p.iam.gserviceaccount.com".into(),
                    message: "permission denied".into(),
                },
                // Mint is blanket-retryable: the SDK error type mixes
                // transient (429/5xx/network) with permanent
                // (permissions, audience) failures without a clean
                // status to split on. Permanent failures will retry
                // and fail consistently; the dispatch retry loop
                // bounds the cost.
                true,
            ),
        ];
        for (err, expected) in cases {
            let wrapped = ServiceClientError::GcpAuth(uri(), err.clone_for_test());
            assert_eq!(
                wrapped.is_retryable(),
                *expected,
                "unexpected retryability for {err:?}"
            );
        }
    }

    trait CloneForTest {
        fn clone_for_test(&self) -> Self;
    }
    impl CloneForTest for gcp::GcpAuthError {
        fn clone_for_test(&self) -> Self {
            match self {
                gcp::GcpAuthError::Adc {
                    audience,
                    impersonate,
                    message,
                } => gcp::GcpAuthError::Adc {
                    audience: audience.clone(),
                    impersonate: impersonate.clone(),
                    message: message.clone(),
                },
                gcp::GcpAuthError::Build { audience, message } => gcp::GcpAuthError::Build {
                    audience: audience.clone(),
                    message: message.clone(),
                },
                gcp::GcpAuthError::AmbientUnsupported { audience } => {
                    gcp::GcpAuthError::AmbientUnsupported {
                        audience: audience.clone(),
                    }
                }
                gcp::GcpAuthError::Mint {
                    audience,
                    impersonate,
                    message,
                } => gcp::GcpAuthError::Mint {
                    audience: audience.clone(),
                    impersonate: impersonate.clone(),
                    message: message.clone(),
                },
                gcp::GcpAuthError::Timeout {
                    audience,
                    impersonate,
                    duration,
                } => gcp::GcpAuthError::Timeout {
                    audience: audience.clone(),
                    impersonate: impersonate.clone(),
                    duration: *duration,
                },
            }
        }
    }
}
