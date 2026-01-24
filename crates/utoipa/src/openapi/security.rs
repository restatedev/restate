// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Implements [OpenAPI Security Schema][security] types.
//!
//! Refer to [`SecurityScheme`] for usage and more details.
//!
//! [security]: https://spec.openapis.org/oas/latest.html#security-scheme-object
use std::{collections::BTreeMap, iter};

use serde::{Deserialize, Serialize};

use super::{builder, extensions::Extensions};

/// OpenAPI [security requirement][security] object.
///
/// Security requirement holds list of required [`SecurityScheme`] *names* and possible *scopes* required
/// to execute the operation. They can be defined in [`#[restate_utoipa::path(...)]`][path] or in `#[openapi(...)]`
/// of [`OpenApi`][openapi].
///
/// Applying the security requirement to [`OpenApi`][openapi] will make it globally
/// available to all operations. When applied to specific [`#[restate_utoipa::path(...)]`][path] will only
/// make the security requirements available for that operation. Only one of the requirements must be
/// satisfied.
///
/// [security]: https://spec.openapis.org/oas/latest.html#security-requirement-object
/// [path]: ../../attr.path.html
/// [openapi]: ../../derive.OpenApi.html
#[non_exhaustive]
#[derive(Serialize, Deserialize, Default, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "debug", derive(Debug))]
pub struct SecurityRequirement {
    #[serde(flatten)]
    value: BTreeMap<String, Vec<String>>,
}

impl SecurityRequirement {
    /// Construct a new [`SecurityRequirement`].
    ///
    /// Accepts name for the security requirement which must match to the name of available [`SecurityScheme`].
    /// Second parameter is [`IntoIterator`] of [`Into<String>`] scopes needed by the [`SecurityRequirement`].
    /// Scopes must match to the ones defined in [`SecurityScheme`].
    pub fn new<N: Into<String>, S: IntoIterator<Item = I>, I: Into<String>>(
        name: N,
        scopes: S,
    ) -> Self {
        Self {
            value: BTreeMap::from_iter(iter::once_with(|| {
                (
                    Into::<String>::into(name),
                    scopes
                        .into_iter()
                        .map(|scope| Into::<String>::into(scope))
                        .collect::<Vec<_>>(),
                )
            })),
        }
    }

    /// Allows to add multiple names to security requirement.
    pub fn add<N: Into<String>, S: IntoIterator<Item = I>, I: Into<String>>(
        mut self,
        name: N,
        scopes: S,
    ) -> Self {
        self.value.insert(
            Into::<String>::into(name),
            scopes.into_iter().map(Into::<String>::into).collect(),
        );

        self
    }
}

/// OpenAPI [security scheme][security] for path operations.
///
/// [security]: https://spec.openapis.org/oas/latest.html#security-scheme-object
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase")]
#[cfg_attr(feature = "debug", derive(Debug))]
pub enum SecurityScheme {
    /// Oauth flow authentication.
    #[serde(rename = "oauth2")]
    OAuth2(OAuth2),
    /// Api key authentication sent in *`header`*, *`cookie`* or *`query`*.
    ApiKey(ApiKey),
    /// Http authentication such as *`bearer`* or *`basic`*.
    Http(Http),
    /// Open id connect url to discover OAuth2 configuration values.
    OpenIdConnect(OpenIdConnect),
    /// Authentication is done via client side certificate.
    ///
    /// OpenApi 3.1 type
    #[serde(rename = "mutualTLS")]
    MutualTls {
        #[allow(missing_docs)]
        #[serde(skip_serializing_if = "Option::is_none")]
        description: Option<String>,
        /// Optional extensions "x-something".
        #[serde(skip_serializing_if = "Option::is_none", flatten)]
        extensions: Option<Extensions>,
    },
}

/// Api key authentication [`SecurityScheme`].
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(tag = "in", rename_all = "lowercase")]
#[cfg_attr(feature = "debug", derive(Debug))]
pub enum ApiKey {
    /// Create api key which is placed in HTTP header.
    Header(ApiKeyValue),
    /// Create api key which is placed in query parameters.
    Query(ApiKeyValue),
    /// Create api key which is placed in cookie value.
    Cookie(ApiKeyValue),
}

/// Value object for [`ApiKey`].
#[non_exhaustive]
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "debug", derive(Debug))]
pub struct ApiKeyValue {
    /// Name of the [`ApiKey`] parameter.
    pub name: String,

    /// Description of the the [`ApiKey`] [`SecurityScheme`]. Supports markdown syntax.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// Optional extensions "x-something".
    #[serde(skip_serializing_if = "Option::is_none", flatten)]
    pub extensions: Option<Extensions>,
}

impl ApiKeyValue {
    /// Constructs new api key value.
    pub fn new<S: Into<String>>(name: S) -> Self {
        Self {
            name: name.into(),
            description: None,
            extensions: Default::default(),
        }
    }

    /// Construct a new api key with optional description supporting markdown syntax.
    pub fn with_description<S: Into<String>>(name: S, description: S) -> Self {
        Self {
            name: name.into(),
            description: Some(description.into()),
            extensions: Default::default(),
        }
    }
}

builder! {
    HttpBuilder;

    /// Http authentication [`SecurityScheme`] builder.
    ///
    /// Methods can be chained to configure _bearer_format_ or to add _description_.
    #[non_exhaustive]
    #[derive(Serialize, Deserialize, Clone, Default, PartialEq, Eq)]
    #[serde(rename_all = "camelCase")]
    #[cfg_attr(feature = "debug", derive(Debug))]
    pub struct Http {
        /// Http authorization scheme in HTTP `Authorization` header value.
        pub scheme: HttpAuthScheme,

        /// Optional hint to client how the bearer token is formatted. Valid only with [`HttpAuthScheme::Bearer`].
        #[serde(skip_serializing_if = "Option::is_none")]
        pub bearer_format: Option<String>,

        /// Optional description of [`Http`] [`SecurityScheme`] supporting markdown syntax.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub description: Option<String>,

        /// Optional extensions "x-something".
        #[serde(skip_serializing_if = "Option::is_none", flatten)]
        pub extensions: Option<Extensions>,
    }
}

impl Http {
    /// Create new http authentication security schema.
    ///
    /// Accepts one argument which defines the scheme of the http authentication.
    pub fn new(scheme: HttpAuthScheme) -> Self {
        Self {
            scheme,
            bearer_format: None,
            description: None,
            extensions: Default::default(),
        }
    }
}

impl HttpBuilder {
    /// Add or change http authentication scheme used.
    pub fn scheme(mut self, scheme: HttpAuthScheme) -> Self {
        self.scheme = scheme;

        self
    }
    /// Add or change informative bearer format for http security schema.
    ///
    /// This is only applicable to [`HttpAuthScheme::Bearer`].
    pub fn bearer_format<S: Into<String>>(mut self, bearer_format: S) -> Self {
        if self.scheme == HttpAuthScheme::Bearer {
            self.bearer_format = Some(bearer_format.into());
        }

        self
    }

    /// Add or change optional description supporting markdown syntax.
    pub fn description<S: Into<String>>(mut self, description: Option<S>) -> Self {
        self.description = description.map(|description| description.into());

        self
    }
}

/// Implements types according [RFC7235](https://datatracker.ietf.org/doc/html/rfc7235#section-5.1).
///
/// Types are maintained at <https://www.iana.org/assignments/http-authschemes/http-authschemes.xhtml>.
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "debug", derive(Debug))]
#[serde(rename_all = "lowercase")]
#[allow(missing_docs)]
pub enum HttpAuthScheme {
    Basic,
    Bearer,
    Digest,
    Hoba,
    Mutual,
    Negotiate,
    OAuth,
    #[serde(rename = "scram-sha-1")]
    ScramSha1,
    #[serde(rename = "scram-sha-256")]
    ScramSha256,
    Vapid,
}

impl Default for HttpAuthScheme {
    fn default() -> Self {
        Self::Basic
    }
}

/// Open id connect [`SecurityScheme`].
#[non_exhaustive]
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "debug", derive(Debug))]
pub struct OpenIdConnect {
    /// Url of the [`OpenIdConnect`] to discover OAuth2 connect values.
    pub open_id_connect_url: String,

    /// Description of [`OpenIdConnect`] [`SecurityScheme`] supporting markdown syntax.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// Optional extensions "x-something".
    #[serde(skip_serializing_if = "Option::is_none", flatten)]
    pub extensions: Option<Extensions>,
}

impl OpenIdConnect {
    /// Construct a new open id connect security schema.
    pub fn new<S: Into<String>>(open_id_connect_url: S) -> Self {
        Self {
            open_id_connect_url: open_id_connect_url.into(),
            description: None,
            extensions: Default::default(),
        }
    }

    /// Construct a new [`OpenIdConnect`] [`SecurityScheme`] with optional description
    /// supporting markdown syntax.
    pub fn with_description<S: Into<String>>(open_id_connect_url: S, description: S) -> Self {
        Self {
            open_id_connect_url: open_id_connect_url.into(),
            description: Some(description.into()),
            extensions: Default::default(),
        }
    }
}

/// OAuth2 [`Flow`] configuration for [`SecurityScheme`].
#[non_exhaustive]
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "debug", derive(Debug))]
pub struct OAuth2 {
    /// Map of supported OAuth2 flows.
    pub flows: BTreeMap<String, Flow>,

    /// Optional description for the [`OAuth2`] [`Flow`] [`SecurityScheme`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// Optional extensions "x-something".
    #[serde(skip_serializing_if = "Option::is_none", flatten)]
    pub extensions: Option<Extensions>,
}

impl OAuth2 {
    /// Construct a new OAuth2 security schema configuration object.
    ///
    /// Oauth flow accepts slice of [`Flow`] configuration objects and can be optionally provided with description.
    pub fn new<I: IntoIterator<Item = Flow>>(flows: I) -> Self {
        Self {
            flows: BTreeMap::from_iter(
                flows
                    .into_iter()
                    .map(|auth_flow| (String::from(auth_flow.get_type_as_str()), auth_flow)),
            ),
            extensions: None,
            description: None,
        }
    }

    /// Construct a new OAuth2 flow with optional description supporting markdown syntax.
    pub fn with_description<I: IntoIterator<Item = Flow>, S: Into<String>>(
        flows: I,
        description: S,
    ) -> Self {
        Self {
            flows: BTreeMap::from_iter(
                flows
                    .into_iter()
                    .map(|auth_flow| (String::from(auth_flow.get_type_as_str()), auth_flow)),
            ),
            extensions: None,
            description: Some(description.into()),
        }
    }
}

/// [`OAuth2`] flow configuration object.
///
/// See more details at <https://spec.openapis.org/oas/latest.html#oauth-flows-object>.
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(untagged)]
#[cfg_attr(feature = "debug", derive(Debug))]
pub enum Flow {
    /// Define implicit [`Flow`] type. See [`Implicit::new`] for usage details.
    ///
    /// Soon to be deprecated by <https://datatracker.ietf.org/doc/html/draft-ietf-oauth-security-topics>.
    Implicit(Implicit),
    /// Define password [`Flow`] type. See [`Password::new`] for usage details.
    Password(Password),
    /// Define client credentials [`Flow`] type. See [`ClientCredentials::new`] for usage details.
    ClientCredentials(ClientCredentials),
    /// Define authorization code [`Flow`] type. See [`AuthorizationCode::new`] for usage details.
    AuthorizationCode(AuthorizationCode),
}

impl Flow {
    fn get_type_as_str(&self) -> &str {
        match self {
            Self::Implicit(_) => "implicit",
            Self::Password(_) => "password",
            Self::ClientCredentials(_) => "clientCredentials",
            Self::AuthorizationCode(_) => "authorizationCode",
        }
    }
}

/// Implicit [`Flow`] configuration for [`OAuth2`].
#[non_exhaustive]
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "debug", derive(Debug))]
pub struct Implicit {
    /// Authorization token url for the flow.
    pub authorization_url: String,

    /// Optional refresh token url for the flow.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refresh_url: Option<String>,

    /// Scopes required by the flow.
    #[serde(flatten)]
    pub scopes: Scopes,

    /// Optional extensions "x-something".
    #[serde(skip_serializing_if = "Option::is_none", flatten)]
    pub extensions: Option<Extensions>,
}

impl Implicit {
    /// Construct a new implicit oauth2 flow.
    ///
    /// Accepts two arguments: one which is authorization url and second map of scopes. Scopes can
    /// also be an empty map.
    pub fn new<S: Into<String>>(authorization_url: S, scopes: Scopes) -> Self {
        Self {
            authorization_url: authorization_url.into(),
            refresh_url: None,
            scopes,
            extensions: Default::default(),
        }
    }

    /// Construct a new implicit oauth2 flow with refresh url for getting refresh tokens.
    ///
    /// This is essentially same as [`Implicit::new`] but allows defining `refresh_url` for the [`Implicit`]
    /// oauth2 flow.
    pub fn with_refresh_url<S: Into<String>>(
        authorization_url: S,
        scopes: Scopes,
        refresh_url: S,
    ) -> Self {
        Self {
            authorization_url: authorization_url.into(),
            refresh_url: Some(refresh_url.into()),
            scopes,
            extensions: Default::default(),
        }
    }
}

/// Authorization code [`Flow`] configuration for [`OAuth2`].
#[non_exhaustive]
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "debug", derive(Debug))]
pub struct AuthorizationCode {
    /// Url for authorization token.
    pub authorization_url: String,
    /// Token url for the flow.
    pub token_url: String,

    /// Optional refresh token url for the flow.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refresh_url: Option<String>,

    /// Scopes required by the flow.
    #[serde(flatten)]
    pub scopes: Scopes,

    /// Optional extensions "x-something".
    #[serde(skip_serializing_if = "Option::is_none", flatten)]
    pub extensions: Option<Extensions>,
}

impl AuthorizationCode {
    /// Construct a new authorization code oauth flow.
    ///
    /// Accepts three arguments: one which is authorization url, two a token url and
    /// three a map of scopes for oauth flow.
    pub fn new<A: Into<String>, T: Into<String>>(
        authorization_url: A,
        token_url: T,
        scopes: Scopes,
    ) -> Self {
        Self {
            authorization_url: authorization_url.into(),
            token_url: token_url.into(),
            refresh_url: None,
            scopes,
            extensions: Default::default(),
        }
    }

    /// Construct a new  [`AuthorizationCode`] OAuth2 flow with additional refresh token url.
    ///
    /// This is essentially same as [`AuthorizationCode::new`] but allows defining extra parameter `refresh_url`
    /// for fetching refresh token.
    pub fn with_refresh_url<S: Into<String>>(
        authorization_url: S,
        token_url: S,
        scopes: Scopes,
        refresh_url: S,
    ) -> Self {
        Self {
            authorization_url: authorization_url.into(),
            token_url: token_url.into(),
            refresh_url: Some(refresh_url.into()),
            scopes,
            extensions: Default::default(),
        }
    }
}

/// Password [`Flow`] configuration for [`OAuth2`].
#[non_exhaustive]
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "debug", derive(Debug))]
pub struct Password {
    /// Token url for this OAuth2 flow. OAuth2 standard requires TLS.
    pub token_url: String,

    /// Optional refresh token url.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refresh_url: Option<String>,

    /// Scopes required by the flow.
    #[serde(flatten)]
    pub scopes: Scopes,

    /// Optional extensions "x-something".
    #[serde(skip_serializing_if = "Option::is_none", flatten)]
    pub extensions: Option<Extensions>,
}

impl Password {
    /// Construct a new password oauth flow.
    ///
    /// Accepts two arguments: one which is a token url and
    /// two a map of scopes for oauth flow.
    pub fn new<S: Into<String>>(token_url: S, scopes: Scopes) -> Self {
        Self {
            token_url: token_url.into(),
            refresh_url: None,
            scopes,
            extensions: Default::default(),
        }
    }

    /// Construct a new password oauth flow with additional refresh url.
    ///
    /// This is essentially same as [`Password::new`] but allows defining third parameter for `refresh_url`
    /// for fetching refresh tokens.
    pub fn with_refresh_url<S: Into<String>>(token_url: S, scopes: Scopes, refresh_url: S) -> Self {
        Self {
            token_url: token_url.into(),
            refresh_url: Some(refresh_url.into()),
            scopes,
            extensions: Default::default(),
        }
    }
}

/// Client credentials [`Flow`] configuration for [`OAuth2`].
#[non_exhaustive]
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "debug", derive(Debug))]
pub struct ClientCredentials {
    /// Token url used for [`ClientCredentials`] flow. OAuth2 standard requires TLS.
    pub token_url: String,

    /// Optional refresh token url.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refresh_url: Option<String>,

    /// Scopes required by the flow.
    #[serde(flatten)]
    pub scopes: Scopes,

    /// Optional extensions "x-something".
    #[serde(skip_serializing_if = "Option::is_none", flatten)]
    pub extensions: Option<Extensions>,
}

impl ClientCredentials {
    /// Construct a new client credentials oauth flow.
    ///
    /// Accepts two arguments: one which is a token url and
    /// two a map of scopes for oauth flow.
    pub fn new<S: Into<String>>(token_url: S, scopes: Scopes) -> Self {
        Self {
            token_url: token_url.into(),
            refresh_url: None,
            scopes,
            extensions: Default::default(),
        }
    }

    /// Construct a new client credentials oauth flow with additional refresh url.
    ///
    /// This is essentially same as [`ClientCredentials::new`] but allows defining third parameter for
    /// `refresh_url`.
    pub fn with_refresh_url<S: Into<String>>(token_url: S, scopes: Scopes, refresh_url: S) -> Self {
        Self {
            token_url: token_url.into(),
            refresh_url: Some(refresh_url.into()),
            scopes,
            extensions: Default::default(),
        }
    }
}

/// [`OAuth2`] flow scopes object defines required permissions for oauth flow.
///
/// Scopes must be given to oauth2 flow but depending on need one of few initialization methods
/// could be used.
///
/// * Create empty map of scopes you can use [`Scopes::new`].
/// * Create map with only one scope you can use [`Scopes::one`].
/// * Create multiple scopes from iterator with [`Scopes::from_iter`].
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "debug", derive(Debug))]
pub struct Scopes {
    scopes: BTreeMap<String, String>,
}

impl Scopes {
    /// Construct new [`Scopes`] with empty map of scopes. This is useful if oauth flow does not need
    /// any permission scopes.
    pub fn new() -> Self {
        Self {
            ..Default::default()
        }
    }

    /// Construct new [`Scopes`] with holding one scope.
    ///
    /// * `scope` Is be the permission required.
    /// * `description` Short description about the permission.
    pub fn one<S: Into<String>>(scope: S, description: S) -> Self {
        Self {
            scopes: BTreeMap::from_iter(iter::once_with(|| (scope.into(), description.into()))),
        }
    }
}

impl<I> FromIterator<(I, I)> for Scopes
where
    I: Into<String>,
{
    fn from_iter<T: IntoIterator<Item = (I, I)>>(iter: T) -> Self {
        Self {
            scopes: iter
                .into_iter()
                .map(|(key, value)| (key.into(), value.into()))
                .collect(),
        }
    }
}
