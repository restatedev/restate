// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use http::{Uri, Version};
use restate_serde_util::SerdeableHeaderHashMap;
use restate_types::identifiers::ServiceRevision;
use restate_types::identifiers::{DeploymentId, LambdaARN};
use restate_types::schema::deployment::{EndpointLambdaCompression, ProtocolType};
use restate_types::schema::info::SchemaInfo;
use restate_types::schema::service::ServiceMetadata;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::collections::HashMap;

/// HTTP authentication details.
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum HttpAuth {
    GoogleIdToken(GoogleIdTokenAuth),
}

#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GoogleIdTokenAuth {
    /// Service account email to impersonate via `iamcredentials:generateIdToken`. Leave unset to
    /// use the ambient ADC identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "schema", schema(value_type = Option<String>))]
    pub impersonate_service_account: Option<bytestring::ByteString>,
    /// Explicit OIDC `aud` claim. Leave unset to automatically derive from the deployment URL.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "schema", schema(value_type = Option<String>))]
    pub audience: Option<bytestring::ByteString>,
    /// Full resource name of a GCP workload identity federation provider, e.g.
    /// `//iam.googleapis.com/projects/N/locations/global/workloadIdentityPools/P/providers/R`.
    /// When set, use AWS-to-GCP federation instead of ambient Application Default Credentials.
    /// Requires `impersonate_service_account`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "schema", schema(value_type = Option<String>))]
    pub workload_identity_provider: Option<bytestring::ByteString>,
}

/// Failure converting wire authentication into its persisted form.
#[derive(Debug, thiserror::Error)]
pub enum GoogleIdTokenAuthConversionError {
    #[error(
        "cannot derive OIDC audience from deployment URI '{uri}': missing scheme or host. \
         Specify auth.audience explicitly."
    )]
    UnderivableAudience { uri: String },
    #[error(transparent)]
    Invalid(#[from] restate_types::deployment::GoogleIdTokenAuthError),
}

impl GoogleIdTokenAuthConversionError {
    pub fn field(&self) -> &'static str {
        match self {
            Self::UnderivableAudience { .. } => "auth.audience",
            Self::Invalid(e) => e.field(),
        }
    }
}

impl HttpAuth {
    /// Convert this wire-side auth block into its persisted form, deriving any missing
    /// audience from the deployment URI. Persisted records always carry a concrete audience;
    /// callers from outside the REST surface should not construct persisted values directly.
    pub fn into_persisted(
        self,
        uri: &Uri,
    ) -> Result<restate_types::deployment::HttpAuth, GoogleIdTokenAuthConversionError> {
        match self {
            HttpAuth::GoogleIdToken(g) => Ok(restate_types::deployment::HttpAuth::GoogleIdToken(
                g.into_persisted(uri)?,
            )),
        }
    }
}

impl GoogleIdTokenAuth {
    pub fn into_persisted(
        self,
        uri: &Uri,
    ) -> Result<restate_types::deployment::GoogleIdTokenAuth, GoogleIdTokenAuthConversionError>
    {
        let audience = match self.audience {
            Some(a) => a,
            None => restate_types::deployment::derive_audience(uri)
                .map(bytestring::ByteString::from)
                .ok_or_else(|| GoogleIdTokenAuthConversionError::UnderivableAudience {
                    uri: uri.to_string(),
                })?,
        };
        Ok(restate_types::deployment::GoogleIdTokenAuth::new(
            audience,
            self.impersonate_service_account,
            self.workload_identity_provider,
        )?)
    }
}

impl From<restate_types::deployment::HttpAuth> for HttpAuth {
    fn from(value: restate_types::deployment::HttpAuth) -> Self {
        match value {
            restate_types::deployment::HttpAuth::GoogleIdToken(g) => {
                HttpAuth::GoogleIdToken(g.into())
            }
        }
    }
}

impl From<restate_types::deployment::GoogleIdTokenAuth> for GoogleIdTokenAuth {
    fn from(value: restate_types::deployment::GoogleIdTokenAuth) -> Self {
        GoogleIdTokenAuth {
            impersonate_service_account: value.impersonate_service_account().cloned(),
            audience: Some(value.audience().clone()),
            workload_identity_provider: value.workload_identity_provider().cloned(),
        }
    }
}

// This enum could be a struct with a nested enum to avoid repeating some fields, but serde(flatten) unfortunately breaks the openapi code generation
#[serde_as]
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RegisterDeploymentRequest {
    /// Register HTTP deployment request
    #[cfg_attr(feature = "schema", schema(title = "RegisterHttpDeploymentRequest"))]
    Http {
        /// # Uri
        ///
        /// Uri to use to discover/invoke the http deployment.
        #[serde_as(as = "serde_with::DisplayFromStr")]
        #[cfg_attr(feature = "schema", schema(value_type = String, format = "uri"))]
        uri: Uri,

        /// # Additional headers
        ///
        /// Additional headers added to every discover/invoke request to the deployment.
        ///
        /// You typically want to include here API keys and other tokens required to send requests to deployments.
        additional_headers: Option<SerdeableHeaderHashMap>,

        /// # Metadata
        ///
        /// Deployment metadata.
        #[serde(default, skip_serializing_if = "HashMap::is_empty")]
        metadata: HashMap<String, String>,

        /// # Use http1.1
        ///
        /// If `true`, discovery will be attempted using a client that defaults to HTTP1.1
        /// instead of a prior-knowledge HTTP2 client. HTTP2 may still be used for TLS servers
        /// that advertise HTTP2 support via ALPN. HTTP1.1 deployments will only work in
        /// request-response mode.
        ///
        #[serde(default = "restate_serde_util::default::bool::<false>")]
        use_http_11: bool,

        /// # Breaking
        ///
        /// If `true`, it allows registering new service revisions with
        /// schemas incompatible with previous service revisions, such as changing the service type.
        ///
        /// See the [versioning documentation](https://docs.restate.dev/services/versioning) for more information.
        #[serde(default = "restate_serde_util::default::bool::<false>")]
        breaking: bool,

        /// # Force
        ///
        /// If `true`, it overrides, if existing, any deployment using the same `uri`.
        /// Beware that this can lead inflight invocations to an unrecoverable error state.
        ///
        /// When set to `true`, it implies `breaking = true`.
        ///
        /// See the [versioning documentation](https://docs.restate.dev/services/versioning) for more information.
        #[cfg_attr(feature = "schema", schema(default = true))]
        force: Option<bool>,

        /// # Dry-run mode
        ///
        /// If `true`, discovery will run but the deployment will not be registered.
        /// This is useful to see the impact of a new deployment before registering it.
        /// `force` and `breaking` will be respected.
        #[serde(default = "restate_serde_util::default::bool::<false>")]
        dry_run: bool,

        /// # Authentication
        ///
        /// Optional per-deployment authentication configuration. When set to `GoogleIdToken`,
        /// Restate mints a Google-signed OIDC ID token for each request and attaches it as
        /// `X-Serverless-Authorization: Bearer <token>`. Cloud Run validates this header in
        /// precedence over `Authorization` and strips it before forwarding to the container, so any
        /// `Authorization` placed in `additional_headers` passes through to the workload unchanged.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        auth: Option<HttpAuth>,
    },
    /// Register Lambda deployment request
    #[cfg_attr(feature = "schema", schema(title = "RegisterLambdaDeploymentRequest"))]
    Lambda {
        /// # ARN
        ///
        /// ARN to use to discover/invoke the lambda deployment.
        arn: String,

        /// # Assume role ARN
        ///
        /// Optional ARN of a role to assume when invoking the addressed Lambda, to support role chaining
        assume_role_arn: Option<String>,

        /// # Additional headers
        ///
        /// Additional headers added to every discover/invoke request to the deployment.
        additional_headers: Option<SerdeableHeaderHashMap>,

        /// # Metadata
        ///
        /// Deployment metadata.
        #[serde(default, skip_serializing_if = "HashMap::is_empty")]
        metadata: HashMap<String, String>,

        /// # Breaking
        ///
        /// If `true`, it allows registering new service revisions with
        /// schemas incompatible with previous service revisions, such as changing the service type.
        ///
        /// See the [versioning documentation](https://docs.restate.dev/services/versioning) for more information.
        #[serde(default = "restate_serde_util::default::bool::<false>")]
        breaking: bool,

        /// # Force
        ///
        /// If `true`, it overrides, if existing, any deployment using the same `uri`.
        /// Beware that this can lead inflight invocations to an unrecoverable error state.
        ///
        /// This implies `breaking = true`.
        ///
        /// See the [versioning documentation](https://docs.restate.dev/services/versioning) for more information.
        #[cfg_attr(feature = "schema", schema(default = true))]
        force: Option<bool>,

        /// # Dry-run mode
        ///
        /// If `true`, discovery will run but the deployment will not be registered.
        /// This is useful to see the impact of a new deployment before registering it.
        /// `force` and `breaking` will be respected.
        #[serde(default = "restate_serde_util::default::bool::<false>")]
        dry_run: bool,
    },
}

#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServiceNameRevPair {
    pub name: String,
    #[cfg_attr(feature = "schema", schema(value_type = u32))]
    pub revision: ServiceRevision,
}

#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
#[derive(Debug, Serialize, Deserialize)]
pub struct RegisterDeploymentResponse {
    pub id: DeploymentId,
    pub services: Vec<ServiceMetadata>,

    /// # Minimum Service Protocol version
    ///
    /// During registration, the SDKs declare a range from minimum (included) to maximum (included) Service Protocol supported version.
    #[serde(default)] // To make sure CLI won't complain when interacting with old runtimes
    pub min_protocol_version: i32,

    /// # Maximum Service Protocol version
    ///
    /// During registration, the SDKs declare a range from minimum (included) to maximum (included) Service Protocol supported version.
    #[serde(default)] // To make sure CLI won't complain when interacting with old runtimes
    pub max_protocol_version: i32,

    /// # SDK version
    ///
    /// SDK library and version declared during registration.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub sdk_version: Option<String>,

    /// # Info
    ///
    /// List of configuration/deprecation information related to this deployment.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub info: Vec<SchemaInfo>,
}

/// List of all registered deployments
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
#[derive(Debug, Serialize, Deserialize)]
pub struct ListDeploymentsResponse {
    pub deployments: Vec<DeploymentResponse>,
}

// Why the deployment responses carry explicit `type` discriminators:
//
// `DeploymentResponse` and `DetailedDeploymentResponse` are `#[serde(untagged)]`, which utoipa
// renders as a `oneOf` without a discriminator. Client generators (e.g. openapi-generator for
// Java) then try every variant and fail when more than one matches, which happens because the
// HTTP and Lambda shapes overlap. utoipa cannot add a discriminator to tagged enums either
// (https://github.com/juhaku/utoipa/issues/1456), and a discriminator only works when the
// property exists on the wire.
//
// So each variant is a named struct with a `type` field whose value is fixed by a single-variant
// enum, and the outer enum declares `#[schema(discriminator(...))]` with an explicit mapping.
// `crate::schema_ref` derives the mapping targets from the variant schema names so they cannot
// drift. The field is `#[serde(default)]` so the CLI still deserializes responses of older
// servers that don't send it, while the schema marks it required because current servers always
// do.

/// Discriminator value for HTTP deployments.
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HttpDeploymentType {
    #[default]
    Http,
}

/// Discriminator value for Lambda deployments.
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LambdaDeploymentType {
    #[default]
    Lambda,
}

/// Deployment response for HTTP deployments
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HttpDeploymentResponse {
    /// # Type
    ///
    /// Deployment type discriminator, always `http`.
    // Defaulted so that responses of older servers, which don't carry this field, still deserialize.
    #[serde(rename = "type", default)]
    #[cfg_attr(feature = "schema", schema(required = true))]
    pub ty: HttpDeploymentType,

    /// # Deployment ID
    pub id: DeploymentId,

    /// # Deployment URI
    ///
    /// URI used to invoke this service deployment.
    #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
    #[cfg_attr(feature = "schema", schema(value_type = String, format = "uri"))]
    pub uri: Uri,

    /// # Protocol Type
    ///
    /// Protocol type used to invoke this service deployment.
    pub protocol_type: ProtocolType,

    /// # HTTP Version
    ///
    /// HTTP Version used to invoke this service deployment.
    #[serde(with = "http_serde::version")]
    #[cfg_attr(feature = "schema", schema(value_type = String))]
    pub http_version: Version,

    /// # Additional headers
    ///
    /// Additional headers used to invoke this service deployment.
    #[serde(default, skip_serializing_if = "SerdeableHeaderHashMap::is_empty")]
    pub additional_headers: SerdeableHeaderHashMap,

    /// # Metadata
    ///
    /// Deployment metadata.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub metadata: HashMap<String, String>,

    #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
    #[cfg_attr(feature = "schema", schema(value_type = String))]
    pub created_at: humantime::Timestamp,

    /// # Minimum Service Protocol version
    ///
    /// During registration, the SDKs declare a range from minimum (included) to maximum (included) Service Protocol supported version.
    pub min_protocol_version: i32,

    /// # Maximum Service Protocol version
    ///
    /// During registration, the SDKs declare a range from minimum (included) to maximum (included) Service Protocol supported version.
    pub max_protocol_version: i32,

    /// # SDK version
    ///
    /// SDK library and version declared during registration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sdk_version: Option<String>,

    /// # Services
    ///
    /// List of services exposed by this deployment.
    pub services: Vec<ServiceNameRevPair>,

    /// # Info
    ///
    /// List of configuration/deprecation information related to this deployment.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub info: Vec<SchemaInfo>,

    /// # Authentication
    ///
    /// Per-deployment authentication, if configured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<HttpAuth>,
}

/// Deployment response for Lambda deployments
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LambdaDeploymentResponse {
    /// # Type
    ///
    /// Deployment type discriminator, always `lambda`.
    // Defaulted so that responses of older servers, which don't carry this field, still deserialize.
    #[serde(rename = "type", default)]
    #[cfg_attr(feature = "schema", schema(required = true))]
    pub ty: LambdaDeploymentType,

    /// # Deployment ID
    pub id: DeploymentId,

    /// # Lambda ARN
    ///
    /// Lambda ARN used to invoke this service deployment.
    pub arn: LambdaARN,

    /// # Assume role ARN
    ///
    /// Assume role ARN used to invoke this deployment. Check https://docs.restate.dev/category/aws-lambda for more details.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assume_role_arn: Option<String>,

    /// # Compression
    ///
    /// Compression algorithm used for invoking Lambda.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compression: Option<EndpointLambdaCompression>,

    /// # Additional headers
    ///
    /// Additional headers used to invoke this service deployment.
    #[serde(default, skip_serializing_if = "SerdeableHeaderHashMap::is_empty")]
    pub additional_headers: SerdeableHeaderHashMap,

    /// # Metadata
    ///
    /// Deployment metadata.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub metadata: HashMap<String, String>,

    #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
    #[cfg_attr(feature = "schema", schema(value_type = String))]
    pub created_at: humantime::Timestamp,

    /// # Minimum Service Protocol version
    ///
    /// During registration, the SDKs declare a range from minimum (included) to maximum (included) Service Protocol supported version.
    pub min_protocol_version: i32,

    /// # Maximum Service Protocol version
    ///
    /// During registration, the SDKs declare a range from minimum (included) to maximum (included) Service Protocol supported version.
    pub max_protocol_version: i32,

    /// # SDK version
    ///
    /// SDK library and version declared during registration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sdk_version: Option<String>,

    /// # Services
    ///
    /// List of services exposed by this deployment.
    pub services: Vec<ServiceNameRevPair>,

    /// # Info
    ///
    /// List of configuration/deprecation information related to this deployment.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub info: Vec<SchemaInfo>,
}

/// Registered deployment. The `type` field tells HTTP and Lambda deployments apart.
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
#[cfg_attr(
    feature = "schema",
    schema(discriminator(
        property_name = "type",
        mapping(
            ("http" = crate::schema_ref::<HttpDeploymentResponse>()),
            ("lambda" = crate::schema_ref::<LambdaDeploymentResponse>())
        )
    ))
)]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DeploymentResponse {
    Http(HttpDeploymentResponse),
    Lambda(LambdaDeploymentResponse),
}

impl DeploymentResponse {
    pub fn id(&self) -> DeploymentId {
        match self {
            Self::Http(http) => http.id,
            Self::Lambda(lambda) => lambda.id,
        }
    }
}

/// Detailed deployment response for HTTP deployments
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HttpDetailedDeploymentResponse {
    /// # Type
    ///
    /// Deployment type discriminator, always `http`.
    // Defaulted so that responses of older servers, which don't carry this field, still deserialize.
    #[serde(rename = "type", default)]
    #[cfg_attr(feature = "schema", schema(required = true))]
    pub ty: HttpDeploymentType,

    /// # Deployment ID
    pub id: DeploymentId,

    /// # Deployment URI
    ///
    /// URI used to invoke this service deployment.
    #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
    #[cfg_attr(feature = "schema", schema(value_type = String, format = "uri"))]
    pub uri: Uri,

    /// # Protocol Type
    ///
    /// Protocol type used to invoke this service deployment.
    pub protocol_type: ProtocolType,

    /// # HTTP Version
    ///
    /// HTTP Version used to invoke this service deployment.
    #[serde(with = "http_serde::version")]
    #[cfg_attr(feature = "schema", schema(value_type = String))]
    pub http_version: Version,

    /// # Additional headers
    ///
    /// Additional headers used to invoke this service deployment.
    #[serde(default, skip_serializing_if = "SerdeableHeaderHashMap::is_empty")]
    pub additional_headers: SerdeableHeaderHashMap,

    /// # Metadata
    ///
    /// Deployment metadata.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub metadata: HashMap<String, String>,

    #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
    #[cfg_attr(feature = "schema", schema(value_type = String))]
    pub created_at: humantime::Timestamp,

    /// # Minimum Service Protocol version
    ///
    /// During registration, the SDKs declare a range from minimum (included) to maximum (included) Service Protocol supported version.
    pub min_protocol_version: i32,

    /// # Maximum Service Protocol version
    ///
    /// During registration, the SDKs declare a range from minimum (included) to maximum (included) Service Protocol supported version.
    pub max_protocol_version: i32,

    /// # SDK version
    ///
    /// SDK library and version declared during registration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sdk_version: Option<String>,

    /// # Services
    ///
    /// List of services exposed by this deployment.
    pub services: Vec<ServiceMetadata>,

    /// # Info
    ///
    /// List of configuration/deprecation information related to this deployment.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub info: Vec<SchemaInfo>,

    /// # Authentication
    ///
    /// Per-deployment authentication, if configured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<HttpAuth>,
}

/// Detailed deployment response for Lambda deployments
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LambdaDetailedDeploymentResponse {
    /// # Type
    ///
    /// Deployment type discriminator, always `lambda`.
    // Defaulted so that responses of older servers, which don't carry this field, still deserialize.
    #[serde(rename = "type", default)]
    #[cfg_attr(feature = "schema", schema(required = true))]
    pub ty: LambdaDeploymentType,

    /// # Deployment ID
    pub id: DeploymentId,

    /// # Lambda ARN
    ///
    /// Lambda ARN used to invoke this service deployment.
    pub arn: LambdaARN,

    /// # Assume role ARN
    ///
    /// Assume role ARN used to invoke this deployment. Check https://docs.restate.dev/category/aws-lambda for more details.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assume_role_arn: Option<String>,

    /// # Compression
    ///
    /// Compression algorithm used for invoking Lambda.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compression: Option<EndpointLambdaCompression>,

    /// # Additional headers
    ///
    /// Additional headers used to invoke this service deployment.
    #[serde(default, skip_serializing_if = "SerdeableHeaderHashMap::is_empty")]
    pub additional_headers: SerdeableHeaderHashMap,

    /// # Metadata
    ///
    /// Deployment metadata.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub metadata: HashMap<String, String>,

    #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
    #[cfg_attr(feature = "schema", schema(value_type = String))]
    pub created_at: humantime::Timestamp,

    /// # Minimum Service Protocol version
    ///
    /// During registration, the SDKs declare a range from minimum (included) to maximum (included) Service Protocol supported version.
    pub min_protocol_version: i32,

    /// # Maximum Service Protocol version
    ///
    /// During registration, the SDKs declare a range from minimum (included) to maximum (included) Service Protocol supported version.
    pub max_protocol_version: i32,

    /// # SDK version
    ///
    /// SDK library and version declared during registration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sdk_version: Option<String>,

    /// # Services
    ///
    /// List of services exposed by this deployment.
    pub services: Vec<ServiceMetadata>,

    /// # Info
    ///
    /// List of configuration/deprecation information related to this deployment.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub info: Vec<SchemaInfo>,
}

/// Detailed information about a registered deployment, including the metadata of its services.
/// The `type` field tells HTTP and Lambda deployments apart.
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
#[cfg_attr(
    feature = "schema",
    schema(discriminator(
        property_name = "type",
        mapping(
            ("http" = crate::schema_ref::<HttpDetailedDeploymentResponse>()),
            ("lambda" = crate::schema_ref::<LambdaDetailedDeploymentResponse>())
        )
    ))
)]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DetailedDeploymentResponse {
    Http(HttpDetailedDeploymentResponse),
    Lambda(LambdaDetailedDeploymentResponse),
}

impl DetailedDeploymentResponse {
    pub fn id(&self) -> DeploymentId {
        match self {
            Self::Http(http) => http.id,
            Self::Lambda(lambda) => lambda.id,
        }
    }
}

#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum UpdateDeploymentRequest {
    /// Update HTTP deployment request
    #[cfg_attr(feature = "schema", schema(title = "UpdateHttpDeploymentRequest"))]
    Http {
        /// # Uri
        ///
        /// Uri to use to discover/invoke the http deployment.
        #[serde(
            with = "serde_with::As::<Option<serde_with::DisplayFromStr>>",
            skip_serializing_if = "Option::is_none"
        )]
        #[cfg_attr(feature = "schema", schema(value_type = Option<String>, format = "uri"))]
        uri: Option<Uri>,

        /// # Additional headers
        ///
        /// Additional headers added to the discover/invoke requests to the deployment.
        /// When provided, this will overwrite all the headers previously configured for this deployment.
        #[serde(skip_serializing_if = "Option::is_none")]
        additional_headers: Option<SerdeableHeaderHashMap>,

        /// # Use http1.1
        ///
        /// If `true`, discovery will be attempted using a client that defaults to HTTP1.1
        /// instead of a prior-knowledge HTTP2 client. HTTP2 may still be used for TLS servers
        /// that advertise HTTP2 support via ALPN. HTTP1.1 deployments will only work in
        /// request-response mode.
        use_http_11: Option<bool>,

        /// # Overwrite
        ///
        /// If `true`, the update will overwrite the schema information, including the exposed service and handlers and service configuration, allowing **breaking changes** too. Use with caution.
        #[serde(default = "restate_serde_util::default::bool::<false>")]
        overwrite: bool,

        /// # Dry-run mode
        ///
        /// If `true`, discovery will run but the deployment will not be registered.
        /// This is useful to see the impact of a new deployment before registering it.
        #[serde(default = "restate_serde_util::default::bool::<false>")]
        dry_run: bool,
    },
    /// Update Lambda deployment request
    #[cfg_attr(feature = "schema", schema(title = "UpdateLambdaDeploymentRequest"))]
    Lambda {
        /// # ARN
        ///
        /// ARN to use to discover/invoke the lambda deployment.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        arn: Option<String>,

        /// # Assume role ARN
        ///
        /// Optional ARN of a role to assume when invoking the addressed Lambda, to support role chaining.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        assume_role_arn: Option<String>,

        /// # Additional headers
        ///
        /// Additional headers added to the discover/invoke requests to the deployment.
        /// When provided, this will overwrite all the headers previously configured for this deployment.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        additional_headers: Option<SerdeableHeaderHashMap>,

        /// # Overwrite
        ///
        /// If `true`, the update will overwrite the schema information, including the exposed service and handlers and service configuration, allowing **breaking changes** too. Use with caution.
        #[serde(default = "restate_serde_util::default::bool::<false>")]
        overwrite: bool,

        /// # Dry-run mode
        ///
        /// If `true`, discovery will run but the deployment will not be registered.
        /// This is useful to see the impact of a new deployment before registering it.
        #[serde(default = "restate_serde_util::default::bool::<false>")]
        dry_run: bool,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytestring::ByteString;

    fn http_deployment() -> HttpDeploymentResponse {
        HttpDeploymentResponse {
            ty: HttpDeploymentType::Http,
            id: DeploymentId::new(),
            uri: "http://localhost:9080/".parse().unwrap(),
            protocol_type: ProtocolType::BidiStream,
            http_version: Version::HTTP_2,
            additional_headers: Default::default(),
            metadata: Default::default(),
            created_at: std::time::SystemTime::UNIX_EPOCH.into(),
            min_protocol_version: 1,
            max_protocol_version: 5,
            sdk_version: None,
            services: vec![],
            info: vec![],
            auth: None,
        }
    }

    fn lambda_deployment() -> LambdaDeploymentResponse {
        LambdaDeploymentResponse {
            ty: LambdaDeploymentType::Lambda,
            id: DeploymentId::new(),
            arn: "arn:aws:lambda:eu-central-1:1234567890:function:svc:1"
                .parse()
                .unwrap(),
            assume_role_arn: None,
            compression: None,
            additional_headers: Default::default(),
            metadata: Default::default(),
            created_at: std::time::SystemTime::UNIX_EPOCH.into(),
            min_protocol_version: 1,
            max_protocol_version: 5,
            sdk_version: None,
            services: vec![],
            info: vec![],
        }
    }

    #[test]
    fn deployment_response_type_discriminator() {
        let http = serde_json::to_value(DeploymentResponse::Http(http_deployment())).unwrap();
        let lambda = serde_json::to_value(DeploymentResponse::Lambda(lambda_deployment())).unwrap();
        assert_eq!(http["type"], "http");
        assert_eq!(lambda["type"], "lambda");

        // The discriminator drives variant selection when present ...
        assert!(matches!(
            serde_json::from_value(http.clone()).unwrap(),
            DeploymentResponse::Http(_)
        ));
        assert!(matches!(
            serde_json::from_value(lambda.clone()).unwrap(),
            DeploymentResponse::Lambda(_)
        ));

        // ... and responses of older servers, which lack it, still deserialize.
        let strip = |mut v: serde_json::Value| {
            v.as_object_mut().unwrap().remove("type").unwrap();
            v
        };
        assert!(matches!(
            serde_json::from_value(strip(http)).unwrap(),
            DeploymentResponse::Http(_)
        ));
        let legacy_lambda = strip(lambda);
        assert!(matches!(
            serde_json::from_value(legacy_lambda.clone()).unwrap(),
            DeploymentResponse::Lambda(_)
        ));
        assert!(matches!(
            serde_json::from_value(legacy_lambda).unwrap(),
            DetailedDeploymentResponse::Lambda(_)
        ));
    }

    fn wire_auth(audience: Option<ByteString>) -> GoogleIdTokenAuth {
        GoogleIdTokenAuth {
            impersonate_service_account: None,
            audience,
            workload_identity_provider: None,
        }
    }

    #[test]
    fn into_persisted_derives_audience_from_uri_when_unset() {
        let uri: Uri = "https://svc.example.com/discover".parse().unwrap();
        let persisted = wire_auth(None).into_persisted(&uri).expect("derivable");
        assert_eq!(persisted.audience(), "https://svc.example.com");
    }

    #[test]
    fn into_persisted_preserves_explicit_audience() {
        let explicit = ByteString::from_static("https://canonical.example.com");
        let uri: Uri = "https://different.example.com/svc".parse().unwrap();
        let persisted = wire_auth(Some(explicit.clone()))
            .into_persisted(&uri)
            .expect("explicit accepted");
        assert_eq!(persisted.audience(), &explicit);
    }

    #[test]
    fn into_persisted_fails_when_audience_unset_and_uri_has_no_host() {
        // Path-only URI has no scheme or authority; derivation cannot succeed.
        let uri: Uri = "/discover".parse().unwrap();
        let err = wire_auth(None)
            .into_persisted(&uri)
            .expect_err("must surface error");
        match err {
            GoogleIdTokenAuthConversionError::UnderivableAudience { uri: got } => {
                assert_eq!(got, "/discover");
            }
            GoogleIdTokenAuthConversionError::Invalid(e) => panic!("unexpected: {e}"),
        }
    }

    #[test]
    fn into_persisted_rejects_provider_without_impersonation() {
        let uri: Uri = "https://svc.example.com/discover".parse().unwrap();
        let auth = GoogleIdTokenAuth {
            impersonate_service_account: None,
            audience: None,
            workload_identity_provider: Some(ByteString::from_static(
                "//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/p/providers/r",
            )),
        };
        let err = auth
            .into_persisted(&uri)
            .expect_err("provider without impersonation must be rejected");
        assert_eq!(err.field(), "auth.workload_identity_provider");
    }
}
