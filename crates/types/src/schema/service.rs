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
use std::time::Duration;

use serde::Deserialize;
use serde::Serialize;

use crate::config::{DEFAULT_ABORT_TIMEOUT, DEFAULT_INACTIVITY_TIMEOUT};
use crate::identifiers::{DeploymentId, ServiceRevision};
use crate::invocation::{
    InvocationTargetType, ServiceType, VirtualObjectHandlerType, WorkflowHandlerType,
};
use crate::schema::invocation_target::DEFAULT_IDEMPOTENCY_RETENTION;
use restate_serde_util::DurationString;

/// This API returns service metadata, as shown in the Admin API.
///
/// Changing the types returned here has effect on the Admin API returned types!
pub trait ServiceMetadataResolver {
    fn resolve_latest_service(&self, service_name: impl AsRef<str>) -> Option<ServiceMetadata>;

    fn resolve_latest_service_openapi(
        &self,
        service_name: impl AsRef<str>,
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
    #[serde(
        with = "serde_with::As::<DurationString>",
        default = "default_idempotency_retention"
    )]
    #[cfg_attr(feature = "schemars", schemars(with = "DurationString"))]
    pub idempotency_retention: Duration,

    /// # Workflow completion retention
    ///
    /// The retention duration of workflows. Only available on workflow services.
    #[serde(
        with = "serde_with::As::<Option<DurationString>>",
        skip_serializing_if = "Option::is_none",
        default
    )]
    #[cfg_attr(feature = "schemars", schemars(with = "Option<DurationString>"))]
    pub workflow_completion_retention: Option<Duration>,

    /// # Journal retention
    ///
    /// The journal retention. When set, this applies to all requests to all handlers of this service.
    ///
    /// In case the invocation has an idempotency key, the `idempotency_retention` caps the maximum `journal_retention` time.
    /// In case the invocation targets a workflow handler, the `workflow_completion_retention` caps the maximum `journal_retention` time.
    #[serde(
        with = "serde_with::As::<Option<DurationString>>",
        skip_serializing_if = "Option::is_none",
        default
    )]
    #[cfg_attr(feature = "schemars", schemars(with = "Option<DurationString>"))]
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
        with = "serde_with::As::<DurationString>",
        default = "default_inactivity_timeout"
    )]
    #[cfg_attr(feature = "schemars", schemars(with = "DurationString"))]
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
        with = "serde_with::As::<DurationString>",
        default = "default_abort_timeout"
    )]
    #[cfg_attr(feature = "schemars", schemars(with = "DurationString"))]
    pub abort_timeout: Duration,

    /// # Enable lazy state
    ///
    /// If true, lazy state will be enabled for all invocations to this service.
    /// This is relevant only for Workflows and Virtual Objects.
    #[serde(default = "restate_serde_util::default::bool::<false>")]
    pub enable_lazy_state: bool,
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

// This type is used only for exposing the handler metadata, and not internally. See [ServiceAndHandlerType].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum HandlerMetadataType {
    Exclusive,
    Shared,
    Workflow,
}

impl From<InvocationTargetType> for Option<HandlerMetadataType> {
    fn from(value: InvocationTargetType) -> Self {
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
    #[serde(
        with = "serde_with::As::<Option<restate_serde_util::DurationString>>",
        skip_serializing_if = "Option::is_none",
        default
    )]
    #[cfg_attr(feature = "schemars", schemars(with = "Option<String>"))]
    pub idempotency_retention: Option<Duration>,

    /// # Journal retention
    ///
    /// The journal retention. When set, this applies to all requests to this handler.
    ///
    /// In case the invocation has an idempotency key, the `idempotency_retention` caps the maximum `journal_retention` time.
    /// In case this handler is a workflow handler, the `workflow_completion_retention` caps the maximum `journal_retention` time.
    ///
    /// If set, it overrides the value set in the service.
    #[serde(
        with = "serde_with::As::<Option<restate_serde_util::DurationString>>",
        skip_serializing_if = "Option::is_none",
        default
    )]
    #[cfg_attr(feature = "schemars", schemars(with = "Option<String>"))]
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
    /// Can be configured using the [`humantime`](https://docs.rs/humantime/latest/humantime/fn.parse_duration.html) format.
    ///
    /// If set, it overrides the value set in the service.
    #[serde(
        with = "serde_with::As::<Option<restate_serde_util::DurationString>>",
        skip_serializing_if = "Option::is_none",
        default
    )]
    #[cfg_attr(feature = "schemars", schemars(with = "Option<String>"))]
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
    /// Can be configured using the [`humantime`](https://docs.rs/humantime/latest/humantime/fn.parse_duration.html) format.
    ///
    /// If set, it overrides the value set in the service.
    #[serde(
        with = "serde_with::As::<Option<restate_serde_util::DurationString>>",
        skip_serializing_if = "Option::is_none",
        default
    )]
    #[cfg_attr(feature = "schemars", schemars(with = "Option<String>"))]
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
}

impl restate_serde_util::MapAsVecItem for HandlerMetadata {
    type Key = String;

    fn key(&self) -> Self::Key {
        self.name.clone()
    }
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

        fn resolve_latest_service_openapi(&self, _service_name: impl AsRef<str>) -> Option<Value> {
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
            }
        }
    }
}
