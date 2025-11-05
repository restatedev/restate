// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::time::Duration;

use serde::Deserialize;
use serde::Serialize;

use restate_time_util::FriendlyDuration;
use restate_ty::identifiers::DeploymentId;
use restate_ty::invocation::ServiceRevision;

use crate::config::{DEFAULT_ABORT_TIMEOUT, DEFAULT_INACTIVITY_TIMEOUT};
use crate::invocation::{
    InvocationTargetType, ServiceType, VirtualObjectHandlerType, WorkflowHandlerType,
};
use crate::net::address::AdvertisedAddress;
use crate::net::address::HttpIngressPort;
use crate::schema::info::Info;
use crate::schema::invocation_target::{DEFAULT_IDEMPOTENCY_RETENTION, OnMaxAttempts};

/// This API returns service metadata, as shown in the Admin API.
///
/// Changing the types returned here has effect on the Admin API returned types!
pub trait ServiceMetadataResolver {
    fn resolve_latest_service(&self, service_name: impl AsRef<str>) -> Option<ServiceMetadata>;

    fn resolve_latest_service_openapi(
        &self,
        service_name: impl AsRef<str>,
        ingress_address: AdvertisedAddress<HttpIngressPort>,
    ) -> Option<serde_json::Value>;

    fn list_services(&self) -> Vec<ServiceMetadata>;

    fn list_service_names(&self) -> Vec<String>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct ServiceMetadata {
    /// # Name
    ///
    /// Fully qualified name of the service
    pub name: String,

    /// # Type
    ///
    /// Service type
    pub ty: ServiceType,

    /// # Handlers
    ///
    /// Handlers for this service.
    #[serde(with = "serde_with::As::<restate_serde_util::MapAsVec>")]
    #[cfg_attr(feature = "schemars", schemars(with = "Vec<HandlerMetadata>"))]
    pub handlers: HashMap<String, HandlerMetadata>,

    /// # Documentation
    ///
    /// Documentation of the service, as propagated by the SDKs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub documentation: Option<String>,
    /// # Metadata
    ///
    /// Additional service metadata, as propagated by the SDKs.
    #[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]
    pub metadata: HashMap<String, String>,

    /// # Deployment Id
    ///
    /// Deployment exposing the latest revision of the service.
    pub deployment_id: DeploymentId,

    /// # Revision
    ///
    /// Latest revision of the service.
    pub revision: ServiceRevision,

    /// # Public
    ///
    /// If true, the service can be invoked through the ingress.
    /// If false, the service can be invoked only from another Restate service.
    #[serde(default = "restate_serde_util::default::bool::<true>")]
    pub public: bool,

    /// # Idempotency retention
    ///
    /// The retention duration of idempotent requests for this service.
    ///
    /// If not configured, this returns the default idempotency retention.
    ///
    /// Can be configured using the [`jiff::fmt::friendly`](https://docs.rs/jiff/latest/jiff/fmt/friendly/index.html) format or ISO8601, for example `5 hours`.
    #[serde(
        with = "serde_with::As::<FriendlyDuration>",
        default = "default_idempotency_retention"
    )]
    #[cfg_attr(feature = "schemars", schemars(with = "String" /* TODO(slinkydeveloper) https://github.com/restatedev/restate/issues/3766 */))]
    pub idempotency_retention: Duration,

    /// # Workflow completion retention
    ///
    /// The retention duration of workflows. Only available on workflow services.
    ///
    /// Can be configured using the [`jiff::fmt::friendly`](https://docs.rs/jiff/latest/jiff/fmt/friendly/index.html) format or ISO8601, for example `5 hours`.
    #[serde(
        with = "serde_with::As::<Option<FriendlyDuration>>",
        skip_serializing_if = "Option::is_none",
        default
    )]
    #[cfg_attr(feature = "schemars", schemars(with = "Option<String>" /* TODO(slinkydeveloper) https://github.com/restatedev/restate/issues/3766 */))]
    pub workflow_completion_retention: Option<Duration>,

    /// # Journal retention
    ///
    /// The journal retention. When set, this applies to all requests to all handlers of this service.
    ///
    /// In case the invocation has an idempotency key, the `idempotency_retention` caps the maximum `journal_retention` time.
    /// In case the invocation targets a workflow handler, the `workflow_completion_retention` caps the maximum `journal_retention` time.
    ///
    /// Can be configured using the [`jiff::fmt::friendly`](https://docs.rs/jiff/latest/jiff/fmt/friendly/index.html) format or ISO8601, for example `5 hours`.
    #[serde(
        with = "serde_with::As::<Option<FriendlyDuration>>",
        skip_serializing_if = "Option::is_none",
        default
    )]
    #[cfg_attr(feature = "schemars", schemars(with = "Option<String>" /* TODO(slinkydeveloper) https://github.com/restatedev/restate/issues/3766 */))]
    pub journal_retention: Option<Duration>,

    /// # Inactivity timeout
    ///
    /// This timer guards against stalled service/handler invocations. Once it expires,
    /// Restate triggers a graceful termination by asking the service invocation to
    /// suspend (which preserves intermediate progress).
    ///
    /// The 'abort timeout' is used to abort the invocation, in case it doesn't react to
    /// the request to suspend.
    ///
    /// Can be configured using the [`jiff::fmt::friendly`](https://docs.rs/jiff/latest/jiff/fmt/friendly/index.html) format or ISO8601, for example `5 hours`.
    ///
    /// If unset, this returns the default inactivity timeout configured in invoker options.
    #[serde(
        with = "serde_with::As::<FriendlyDuration>",
        default = "default_inactivity_timeout"
    )]
    #[cfg_attr(feature = "schemars", schemars(with = "String" /* TODO(slinkydeveloper) https://github.com/restatedev/restate/issues/3766 */))]
    pub inactivity_timeout: Duration,

    /// # Abort timeout
    ///
    /// This timer guards against stalled service/handler invocations that are supposed to
    /// terminate. The abort timeout is started after the 'inactivity timeout' has expired
    /// and the service/handler invocation has been asked to gracefully terminate. Once the
    /// timer expires, it will abort the service/handler invocation.
    ///
    /// This timer potentially **interrupts** user code. If the user code needs longer to
    /// gracefully terminate, then this value needs to be set accordingly.
    ///
    /// Can be configured using the [`jiff::fmt::friendly`](https://docs.rs/jiff/latest/jiff/fmt/friendly/index.html) format or ISO8601, for example `5 hours`.
    ///
    /// If unset, this returns the default abort timeout configured in invoker options.
    #[serde(
        with = "serde_with::As::<FriendlyDuration>",
        default = "default_abort_timeout"
    )]
    #[cfg_attr(feature = "schemars", schemars(with = "String" /* TODO(slinkydeveloper) https://github.com/restatedev/restate/issues/3766 */))]
    pub abort_timeout: Duration,

    /// # Enable lazy state
    ///
    /// If true, lazy state will be enabled for all invocations to this service.
    /// This is relevant only for Workflows and Virtual Objects.
    #[serde(default = "restate_serde_util::default::bool::<false>")]
    pub enable_lazy_state: bool,

    /// # Retry policy
    ///
    /// Retry policy applied to invocations of this service.
    ///
    /// If unset, it returns the default values configured in the Restate configuration.
    #[serde(default)]
    pub retry_policy: ServiceRetryPolicyMetadata,

    /// # Info
    ///
    /// List of configuration/deprecation information related to this service.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub info: Vec<Info>,
}

impl restate_serde_util::MapAsVecItem for ServiceMetadata {
    type Key = String;

    fn key(&self) -> Self::Key {
        self.name.clone()
    }
}

fn default_idempotency_retention() -> Duration {
    DEFAULT_IDEMPOTENCY_RETENTION
}

fn default_inactivity_timeout() -> Duration {
    DEFAULT_INACTIVITY_TIMEOUT
}

fn default_abort_timeout() -> Duration {
    DEFAULT_ABORT_TIMEOUT
}

/// # Service retry policy
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct ServiceRetryPolicyMetadata {
    /// # Initial Interval
    ///
    /// Initial interval for the first retry attempt.
    ///
    /// Can be configured using the [`jiff::fmt::friendly`](https://docs.rs/jiff/latest/jiff/fmt/friendly/index.html) format or ISO8601, for example `5 hours`.
    #[serde(
        default = "default_initial_interval",
        with = "serde_with::As::<FriendlyDuration>"
    )]
    #[cfg_attr(feature = "schemars", schemars(with = "String" /* TODO(slinkydeveloper) https://github.com/restatedev/restate/issues/3766 */))]
    pub initial_interval: Duration,

    /// # Factor
    ///
    /// The factor to use to compute the next retry attempt. Default: `2.0`.
    #[serde(default = "default_exponentiation_factor")]
    pub exponentiation_factor: f32,

    /// # Max attempts
    ///
    /// Number of maximum attempts (including the initial) before giving up. Infinite retries if unset. No retries if set to 1.
    #[serde(default)]
    pub max_attempts: Option<NonZeroUsize>,

    /// # Max interval
    ///
    /// Maximum interval between retries.
    ///
    /// Can be configured using the [`jiff::fmt::friendly`](https://docs.rs/jiff/latest/jiff/fmt/friendly/index.html) format or ISO8601, for example `5 hours`.
    #[serde(default, with = "serde_with::As::<Option<FriendlyDuration>>")]
    #[cfg_attr(feature = "schemars", schemars(with = "Option<String>" /* TODO(slinkydeveloper) https://github.com/restatedev/restate/issues/3766 */))]
    pub max_interval: Option<Duration>,

    /// # On max attempts
    ///
    /// Behavior when max attempts are reached.
    #[serde(default)]
    pub on_max_attempts: OnMaxAttempts,
}

impl Default for ServiceRetryPolicyMetadata {
    fn default() -> Self {
        Self {
            initial_interval: default_initial_interval(),
            exponentiation_factor: default_exponentiation_factor(),
            max_attempts: None,
            max_interval: None,
            on_max_attempts: Default::default(),
        }
    }
}

fn default_initial_interval() -> Duration {
    Duration::from_millis(100)
}

fn default_exponentiation_factor() -> f32 {
    2.0
}

// This type is used only for exposing the handler metadata, and not internally. See [ServiceAndHandlerType].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum HandlerMetadataType {
    Exclusive,
    Shared,
    Workflow,
}
impl HandlerMetadataType {
    pub fn from_invocation_target(value: InvocationTargetType) -> Option<Self> {
        match value {
            InvocationTargetType::Service => None,
            InvocationTargetType::VirtualObject(h_ty) => match h_ty {
                VirtualObjectHandlerType::Exclusive => Some(HandlerMetadataType::Exclusive),
                VirtualObjectHandlerType::Shared => Some(HandlerMetadataType::Shared),
            },
            InvocationTargetType::Workflow(h_ty) => match h_ty {
                WorkflowHandlerType::Workflow => Some(HandlerMetadataType::Workflow),
                WorkflowHandlerType::Shared => Some(HandlerMetadataType::Shared),
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct HandlerMetadata {
    /// # Name
    ///
    /// The handler name.
    pub name: String,

    /// # Type
    ///
    /// The handler type.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ty: Option<HandlerMetadataType>,

    /// # Documentation
    ///
    /// Documentation of the handler, as propagated by the SDKs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub documentation: Option<String>,
    /// # Metadata
    ///
    /// Additional handler metadata, as propagated by the SDKs.
    #[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]
    pub metadata: HashMap<String, String>,

    /// # Idempotency retention
    ///
    /// The retention duration of idempotent requests for this handler. If set, it overrides the value set in the service.
    ///
    /// Can be configured using the [`jiff::fmt::friendly`](https://docs.rs/jiff/latest/jiff/fmt/friendly/index.html) format or ISO8601, for example `5 hours`.
    #[serde(
        with = "serde_with::As::<Option<FriendlyDuration>>",
        skip_serializing_if = "Option::is_none",
        default
    )]
    #[cfg_attr(feature = "schemars", schemars(with = "Option<String>" /* TODO(slinkydeveloper) https://github.com/restatedev/restate/issues/3766 */))]
    pub idempotency_retention: Option<Duration>,

    /// # Journal retention
    ///
    /// The journal retention. When set, this applies to all requests to this handler.
    ///
    /// In case the invocation has an idempotency key, the `idempotency_retention` caps the maximum `journal_retention` time.
    /// In case this handler is a workflow handler, the `workflow_completion_retention` caps the maximum `journal_retention` time.
    ///
    /// Can be configured using the [`jiff::fmt::friendly`](https://docs.rs/jiff/latest/jiff/fmt/friendly/index.html) format or ISO8601, for example `5 hours`.
    ///
    /// If set, it overrides the value set in the service.
    #[serde(
        with = "serde_with::As::<Option<FriendlyDuration>>",
        skip_serializing_if = "Option::is_none",
        default
    )]
    #[cfg_attr(feature = "schemars", schemars(with = "Option<String>" /* TODO(slinkydeveloper) https://github.com/restatedev/restate/issues/3766 */))]
    pub journal_retention: Option<Duration>,

    /// # Inactivity timeout
    ///
    /// This timer guards against stalled service/handler invocations. Once it expires,
    /// Restate triggers a graceful termination by asking the service invocation to
    /// suspend (which preserves intermediate progress).
    ///
    /// The 'abort timeout' is used to abort the invocation, in case it doesn't react to
    /// the request to suspend.
    ///
    /// Can be configured using the [`jiff::fmt::friendly`](https://docs.rs/jiff/latest/jiff/fmt/friendly/index.html) format or ISO8601, for example `5 hours`.
    ///
    /// If set, it overrides the value set in the service.
    #[serde(
        with = "serde_with::As::<Option<FriendlyDuration>>",
        skip_serializing_if = "Option::is_none",
        default
    )]
    #[cfg_attr(feature = "schemars", schemars(with = "Option<String>" /* TODO(slinkydeveloper) https://github.com/restatedev/restate/issues/3766 */))]
    pub inactivity_timeout: Option<Duration>,

    /// # Abort timeout
    ///
    /// This timer guards against stalled service/handler invocations that are supposed to
    /// terminate. The abort timeout is started after the 'inactivity timeout' has expired
    /// and the service/handler invocation has been asked to gracefully terminate. Once the
    /// timer expires, it will abort the service/handler invocation.
    ///
    /// This timer potentially **interrupts** user code. If the user code needs longer to
    /// gracefully terminate, then this value needs to be set accordingly.
    ///
    /// Can be configured using the [`jiff::fmt::friendly`](https://docs.rs/jiff/latest/jiff/fmt/friendly/index.html) format or ISO8601, for example `5 hours`.
    ///
    /// If set, it overrides the value set in the service.
    #[serde(
        with = "serde_with::As::<Option<FriendlyDuration>>",
        skip_serializing_if = "Option::is_none",
        default
    )]
    #[cfg_attr(feature = "schemars", schemars(with = "Option<String>" /* TODO(slinkydeveloper) https://github.com/restatedev/restate/issues/3766 */))]
    pub abort_timeout: Option<Duration>,

    /// # Enable lazy state
    ///
    /// If true, lazy state will be enabled for all invocations to this service.
    /// This is relevant only for Workflows and Virtual Objects.
    ///
    /// If set, it overrides the value set in the service.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub enable_lazy_state: Option<bool>,

    /// # Public
    ///
    /// If true, this handler can be invoked through the ingress.
    /// If false, this handler can be invoked only from another Restate service.
    #[serde(default = "restate_serde_util::default::bool::<true>")]
    pub public: bool,

    /// # Human readable input description
    ///
    /// If empty, no schema was provided by the user at discovery time.
    pub input_description: String,

    /// # Human readable output description
    ///
    /// If empty, no schema was provided by the user at discovery time.
    pub output_description: String,

    /// # Input JSON Schema
    ///
    /// JSON Schema of the handler input
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_json_schema: Option<serde_json::Value>,

    /// # Output JSON Schema
    ///
    /// JSON Schema of the handler output
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_json_schema: Option<serde_json::Value>,

    /// # Retry policy
    ///
    /// Retry policy overrides applied for this handler.
    #[serde(default)]
    pub retry_policy: HandlerRetryPolicyMetadata,

    /// # Info
    ///
    /// List of configuration/deprecation information related to this handler.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub info: Vec<Info>,
}

impl restate_serde_util::MapAsVecItem for HandlerMetadata {
    type Key = String;

    fn key(&self) -> Self::Key {
        self.name.clone()
    }
}

/// # Handler retry policy overrides
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct HandlerRetryPolicyMetadata {
    /// # Initial Interval
    ///
    /// Initial interval for the first retry attempt.
    ///
    /// Can be configured using the [`jiff::fmt::friendly`](https://docs.rs/jiff/latest/jiff/fmt/friendly/index.html) format or ISO8601, for example `5 hours`.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "serde_with::As::<Option<FriendlyDuration>>"
    )]
    #[cfg_attr(feature = "schemars", schemars(with = "Option<String>" /* TODO(slinkydeveloper) https://github.com/restatedev/restate/issues/3766 */))]
    pub initial_interval: Option<Duration>,

    /// # Factor
    ///
    /// The factor to use to compute the next retry attempt.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exponentiation_factor: Option<f32>,

    /// # Max attempts
    ///
    /// Number of maximum attempts (including the initial) before giving up. Infinite retries if unset. No retries if set to 1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_attempts: Option<NonZeroUsize>,

    /// # Max interval
    ///
    /// Maximum interval between retries.
    ///
    /// Can be configured using the [`jiff::fmt::friendly`](https://docs.rs/jiff/latest/jiff/fmt/friendly/index.html) format or ISO8601, for example `5 hours`.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "serde_with::As::<Option<FriendlyDuration>>"
    )]
    #[cfg_attr(feature = "schemars", schemars(with = "Option<String>" /* TODO(slinkydeveloper) https://github.com/restatedev/restate/issues/3766 */))]
    pub max_interval: Option<Duration>,

    /// # On max attempts
    ///
    /// Behavior when max attempts are reached.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_max_attempts: Option<OnMaxAttempts>,
}

#[cfg(feature = "test-util")]
#[allow(dead_code)]
pub mod test_util {
    use super::*;

    use crate::schema::invocation_target::DEFAULT_IDEMPOTENCY_RETENTION;
    use serde_json::Value;
    use std::collections::HashMap;

    #[derive(Debug, Default, Clone)]
    pub struct MockServiceMetadataResolver(HashMap<String, ServiceMetadata>);

    impl MockServiceMetadataResolver {
        pub fn add(&mut self, service_metadata: ServiceMetadata) {
            self.0
                .insert(service_metadata.name.clone(), service_metadata);
        }
    }

    impl ServiceMetadataResolver for MockServiceMetadataResolver {
        fn resolve_latest_service(&self, service_name: impl AsRef<str>) -> Option<ServiceMetadata> {
            self.0.get(service_name.as_ref()).cloned()
        }

        fn resolve_latest_service_openapi(
            &self,
            _service_name: impl AsRef<str>,
            _ingress_address: AdvertisedAddress<HttpIngressPort>,
        ) -> Option<Value> {
            Some(Value::Null)
        }

        fn list_services(&self) -> Vec<ServiceMetadata> {
            self.0.values().cloned().collect()
        }

        fn list_service_names(&self) -> Vec<String> {
            self.0.values().map(|s| s.name.clone()).collect()
        }
    }

    impl ServiceMetadata {
        pub fn mock_service(
            name: impl AsRef<str>,
            handlers: impl IntoIterator<Item = impl AsRef<str>>,
        ) -> Self {
            Self {
                name: name.as_ref().to_string(),
                handlers: handlers
                    .into_iter()
                    .map(|s| {
                        (
                            s.as_ref().to_string(),
                            HandlerMetadata {
                                name: s.as_ref().to_string(),
                                ty: None,
                                documentation: None,
                                metadata: Default::default(),
                                idempotency_retention: None,
                                journal_retention: None,
                                inactivity_timeout: None,
                                abort_timeout: None,
                                enable_lazy_state: None,
                                public: true,
                                input_description: "any".to_string(),
                                output_description: "any".to_string(),
                                input_json_schema: None,
                                output_json_schema: None,
                                retry_policy: Default::default(),
                                info: vec![],
                            },
                        )
                    })
                    .collect(),
                ty: ServiceType::Service,
                documentation: None,
                metadata: Default::default(),
                deployment_id: Default::default(),
                revision: 0,
                public: true,
                idempotency_retention: DEFAULT_IDEMPOTENCY_RETENTION,
                workflow_completion_retention: None,
                journal_retention: None,
                inactivity_timeout: Duration::from_secs(60),
                abort_timeout: Duration::from_secs(60),
                enable_lazy_state: false,
                retry_policy: Default::default(),
                info: vec![],
            }
        }

        pub fn mock_virtual_object(
            name: impl AsRef<str>,
            handlers: impl IntoIterator<Item = impl AsRef<str>>,
        ) -> Self {
            Self {
                name: name.as_ref().to_string(),
                handlers: handlers
                    .into_iter()
                    .map(|s| {
                        (
                            s.as_ref().to_string(),
                            HandlerMetadata {
                                name: s.as_ref().to_string(),
                                ty: Some(HandlerMetadataType::Exclusive),
                                documentation: None,
                                metadata: Default::default(),
                                idempotency_retention: None,
                                journal_retention: None,
                                inactivity_timeout: None,
                                abort_timeout: None,
                                enable_lazy_state: None,
                                public: true,
                                input_description: "any".to_string(),
                                output_description: "any".to_string(),
                                input_json_schema: None,
                                output_json_schema: None,
                                retry_policy: Default::default(),
                                info: vec![],
                            },
                        )
                    })
                    .collect(),
                ty: ServiceType::VirtualObject,
                documentation: None,
                metadata: Default::default(),
                deployment_id: Default::default(),
                revision: 0,
                public: true,
                idempotency_retention: DEFAULT_IDEMPOTENCY_RETENTION,
                workflow_completion_retention: None,
                journal_retention: None,
                inactivity_timeout: Duration::from_secs(60),
                abort_timeout: Duration::from_secs(60),
                enable_lazy_state: false,
                retry_policy: Default::default(),
                info: vec![],
            }
        }
    }
}
