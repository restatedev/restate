mod openapi;
mod serde_hacks;
pub mod updater;

use crate::config::Configuration;
use crate::identifiers::{DeploymentId, SubscriptionId};
use crate::invocation::{InvocationTargetType, ServiceType, WorkflowHandlerType};
use crate::metadata::GlobalMetadata;
use crate::net::metadata::{MetadataContainer, MetadataKind};
use crate::schema::deployment::{DeliveryOptions, DeploymentResolver, DeploymentType};
use crate::schema::invocation_target::{
    DEFAULT_IDEMPOTENCY_RETENTION, DEFAULT_WORKFLOW_COMPLETION_RETENTION, InputRules,
    InvocationTargetMetadata, InvocationTargetResolver, OutputRules,
};
use crate::schema::metadata::openapi::ServiceOpenAPI;
use crate::schema::service::ServiceMetadataResolver;
use crate::schema::subscriptions::{ListSubscriptionFilter, Subscription, SubscriptionResolver};
use crate::schema::{deployment, service};
use crate::time::MillisSinceEpoch;
use crate::{Version, Versioned, identifiers};
use arc_swap::ArcSwapOption;
use restate_serde_util::MapAsVecItem;
use serde_json::Value;
use serde_with::serde_as;
use std::collections::HashMap;
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(from = "serde_hacks::Schema", into = "serde_hacks::Schema")]
pub struct Schema {
    /// This gets bumped on each update.
    version: Version,

    deployments: HashMap<DeploymentId, Deployment>,
    active_service_revisions: HashMap<String, ActiveServiceRevision>,
    subscriptions: HashMap<SubscriptionId, Subscription>,
}

impl Default for Schema {
    fn default() -> Self {
        Self {
            version: Version::INVALID,
            active_service_revisions: HashMap::default(),
            deployments: HashMap::default(),
            subscriptions: HashMap::default(),
        }
    }
}

impl GlobalMetadata for Schema {
    const KEY: &'static str = "schema_registry";

    const KIND: MetadataKind = MetadataKind::Schema;

    fn into_container(self: Arc<Self>) -> MetadataContainer {
        MetadataContainer::Schema(self)
    }
}

impl Versioned for Schema {
    fn version(&self) -> Version {
        self.version
    }
}

mod storage {
    use crate::flexbuffers_storage_encode_decode;

    use super::Schema;

    flexbuffers_storage_encode_decode!(Schema);
}

// -- Data model

#[derive(Debug, Clone)]
struct ActiveServiceRevision {
    deployment_id: DeploymentId,
    service_revision: Arc<ServiceRevision>,
}

impl ActiveServiceRevision {
    fn as_service_metadata(&self) -> service::ServiceMetadata {
        self.service_revision
            .to_service_metadata(self.deployment_id)
    }
}

#[serde_as]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct Deployment {
    id: DeploymentId,
    ty: DeploymentType,
    delivery_options: DeliveryOptions,
    supported_protocol_versions: RangeInclusive<i32>,
    /// Declared SDK during discovery
    sdk_version: Option<String>,
    created_at: MillisSinceEpoch,

    #[serde_as(as = "restate_serde_util::MapAsVec")]
    services: HashMap<String, Arc<ServiceRevision>>,
}

impl MapAsVecItem for Deployment {
    type Key = DeploymentId;

    fn key(&self) -> Self::Key {
        self.id
    }
}

impl Deployment {
    fn to_deployment(&self) -> deployment::Deployment {
        deployment::Deployment {
            id: self.id,
            metadata: deployment::DeploymentMetadata {
                ty: self.ty.clone(),
                delivery_options: self.delivery_options.clone(),
                supported_protocol_versions: self.supported_protocol_versions.clone(),
                sdk_version: self.sdk_version.clone(),
                created_at: self.created_at,
            },
        }
    }
}

#[serde_as]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ServiceRevision {
    /// Fully qualified name of the service
    name: String,

    #[serde_as(as = "restate_serde_util::MapAsVec")]
    handlers: HashMap<String, Handler>,

    ty: ServiceType,

    /// Documentation of the service, as propagated by the SDKs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    documentation: Option<String>,
    /// Additional service metadata, as propagated by the SDKs.
    #[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]
    metadata: HashMap<String, String>,
    /// Latest revision of the service.
    revision: identifiers::ServiceRevision,

    /// If true, the service can be invoked through the ingress.
    /// If false, the service can be invoked only from another Restate service.
    public: bool,

    /// The retention duration of idempotent requests for this service.
    #[serde(
        with = "serde_with::As::<Option<restate_serde_util::DurationString>>",
        skip_serializing_if = "Option::is_none",
        default
    )]
    idempotency_retention: Option<Duration>,

    /// The retention duration of workflows. Only available on workflow services.
    #[serde(
        with = "serde_with::As::<Option<restate_serde_util::DurationString>>",
        skip_serializing_if = "Option::is_none",
        default
    )]
    workflow_completion_retention: Option<Duration>,

    /// The journal retention. When set, this applies to all requests to all handlers of this service.
    ///
    /// In case the request has an idempotency key, the `idempotency_retention` caps the maximum `journal_retention` time.
    /// In case the request targets a workflow handler, the `workflow_completion_retention` caps the maximum `journal_retention` time.
    #[serde(
        with = "serde_with::As::<Option<restate_serde_util::DurationString>>",
        skip_serializing_if = "Option::is_none",
        default
    )]
    journal_retention: Option<Duration>,

    /// This timer guards against stalled service/handler invocations. Once it expires,
    /// Restate triggers a graceful termination by asking the service invocation to
    /// suspend (which preserves intermediate progress).
    ///
    /// The 'abort timeout' is used to abort the invocation, in case it doesn't react to
    /// the request to suspend.
    ///
    /// This overrides the default inactivity timeout set in invoker options.
    #[serde(
        with = "serde_with::As::<Option<restate_serde_util::DurationString>>",
        skip_serializing_if = "Option::is_none",
        default
    )]
    inactivity_timeout: Option<Duration>,

    /// This timer guards against stalled service/handler invocations that are supposed to
    /// terminate. The abort timeout is started after the 'inactivity timeout' has expired
    /// and the service/handler invocation has been asked to gracefully terminate. Once the
    /// timer expires, it will abort the service/handler invocation.
    ///
    /// This timer potentially **interrupts** user code. If the user code needs longer to
    /// gracefully terminate, then this value needs to be set accordingly.
    ///
    /// This overrides the default abort timeout set in invoker options.
    #[serde(
        with = "serde_with::As::<Option<restate_serde_util::DurationString>>",
        skip_serializing_if = "Option::is_none",
        default
    )]
    abort_timeout: Option<Duration>,

    /// If true, lazy state will be enabled for all invocations to this service.
    /// This is relevant only for Workflows and Virtual Objects.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    enable_lazy_state: Option<bool>,

    /// This is a cache for the computed value of ServiceOpenAPI
    #[serde(skip)]
    service_openapi_cache: Arc<ArcSwapOption<ServiceOpenAPI>>,
}

impl MapAsVecItem for ServiceRevision {
    type Key = String;

    fn key(&self) -> Self::Key {
        self.name.clone()
    }
}

impl ServiceRevision {
    fn to_service_metadata(&self, deployment_id: DeploymentId) -> service::ServiceMetadata {
        service::ServiceMetadata {
            name: self.name.clone(),
            handlers: self
                .handlers
                .iter()
                .map(|(name, handler)| (name.clone(), handler.as_handler_metadata(self.public)))
                .collect(),
            ty: self.ty,
            documentation: self.documentation.clone(),
            metadata: self.metadata.clone(),
            deployment_id,
            revision: self.revision,
            public: self.public,
            idempotency_retention: self.idempotency_retention,
            workflow_completion_retention: self.workflow_completion_retention,
            journal_retention: self.journal_retention,
            inactivity_timeout: self.inactivity_timeout,
            abort_timeout: self.abort_timeout,
            enable_lazy_state: self.enable_lazy_state,
        }
    }

    fn as_openapi_spec(&self) -> serde_json::Value {
        let service_openapi = {
            let cached_openapi = self.service_openapi_cache.load();
            if let Some(result) = cached_openapi.as_ref() {
                Arc::clone(result)
            } else {
                // Not present, compute it!
                let computed = Arc::new(ServiceOpenAPI::infer(
                    &self.name,
                    self.ty,
                    &self.metadata,
                    &self.handlers,
                ));
                self.service_openapi_cache.store(Some(computed.clone()));
                computed
            }
        };

        let advertised_ingress_endpoint = Configuration::pinned()
            .ingress
            .advertised_ingress_endpoint
            .as_ref()
            .map(|u| u.to_string());
        service_openapi.to_openapi_contract(
            &self.name,
            advertised_ingress_endpoint.as_deref(),
            self.documentation.as_deref(),
            self.revision,
        )
    }
}

#[serde_as]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct Handler {
    name: String,
    target_ty: InvocationTargetType,
    input_rules: InputRules,
    output_rules: OutputRules,
    /// Override of public for this handler. If unspecified, the `public` from `service` is used instead.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    public: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    idempotency_retention: Option<Duration>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    workflow_completion_retention: Option<Duration>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    journal_retention: Option<Duration>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    inactivity_timeout: Option<Duration>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    abort_timeout: Option<Duration>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    documentation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    enable_lazy_state: Option<bool>,
    #[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]
    metadata: HashMap<String, String>,
}

impl MapAsVecItem for Handler {
    type Key = String;

    fn key(&self) -> Self::Key {
        self.name.clone()
    }
}

impl Handler {
    fn as_handler_metadata(&self, service_level_public: bool) -> service::HandlerMetadata {
        service::HandlerMetadata {
            name: self.name.clone(),
            ty: self.target_ty.into(),
            documentation: self.documentation.clone(),
            metadata: self.metadata.clone(),
            public: self.public.unwrap_or(service_level_public),
            input_description: self.input_rules.to_string(),
            output_description: self.output_rules.to_string(),
            input_json_schema: self.input_rules.json_schema(),
            output_json_schema: self.output_rules.json_schema(),
            idempotency_retention: self.idempotency_retention,
            workflow_completion_retention: self.workflow_completion_retention,
            journal_retention: self.journal_retention,
            inactivity_timeout: self.inactivity_timeout,
            abort_timeout: self.abort_timeout,
            enable_lazy_state: self.enable_lazy_state,
        }
    }
}

// --- Interface implementations

impl DeploymentResolver for Schema {
    fn resolve_latest_deployment_for_service(
        &self,
        service_name: impl AsRef<str>,
    ) -> Option<deployment::Deployment> {
        let service_name = service_name.as_ref();
        let active_service_revision = self.active_service_revisions.get(service_name)?;
        self.deployments
            .get(&active_service_revision.deployment_id)
            .map(|dp| dp.to_deployment())
    }

    fn get_deployment(&self, deployment_id: &DeploymentId) -> Option<deployment::Deployment> {
        self.deployments
            .get(deployment_id)
            .map(|dp| dp.to_deployment())
    }

    fn get_deployment_and_services(
        &self,
        deployment_id: &DeploymentId,
    ) -> Option<(deployment::Deployment, Vec<service::ServiceMetadata>)> {
        self.deployments.get(deployment_id).map(|dp| {
            (
                dp.to_deployment(),
                dp.services
                    .values()
                    .map(|s| s.to_service_metadata(*deployment_id))
                    .collect(),
            )
        })
    }

    fn get_deployments(
        &self,
    ) -> Vec<(
        deployment::Deployment,
        Vec<(String, identifiers::ServiceRevision)>,
    )> {
        self.deployments
            .values()
            .map(|dp| {
                (
                    dp.to_deployment(),
                    dp.services
                        .values()
                        .map(|s| (s.name.clone(), s.revision))
                        .collect(),
                )
            })
            .collect()
    }
}

impl InvocationTargetResolver for Schema {
    fn resolve_latest_invocation_target(
        &self,
        service_name: impl AsRef<str>,
        handler_name: impl AsRef<str>,
    ) -> Option<InvocationTargetMetadata> {
        let service_name = service_name.as_ref();
        let handler_name = handler_name.as_ref();

        let ActiveServiceRevision {
            service_revision, ..
        } = self.active_service_revisions.get(service_name)?;
        let handler = service_revision.handlers.get(handler_name)?;

        let completion_retention =
            if handler.target_ty == InvocationTargetType::Workflow(WorkflowHandlerType::Workflow) {
                handler
                    .workflow_completion_retention
                    .or(service_revision.workflow_completion_retention)
                    .unwrap_or(DEFAULT_WORKFLOW_COMPLETION_RETENTION)
            } else {
                handler
                    .idempotency_retention
                    .or(service_revision.idempotency_retention)
                    .unwrap_or(DEFAULT_IDEMPOTENCY_RETENTION)
            };
        let journal_retention = Configuration::pinned()
            .admin
            .experimental_feature_force_journal_retention
            .or(handler.journal_retention)
            .or(service_revision.journal_retention)
            .unwrap_or(Duration::ZERO);

        Some(InvocationTargetMetadata {
            public: handler.public.unwrap_or(service_revision.public),
            completion_retention,
            journal_retention,
            target_ty: handler.target_ty,
            input_rules: handler.input_rules.clone(),
            output_rules: handler.output_rules.clone(),
        })
    }
}

impl ServiceMetadataResolver for Schema {
    fn resolve_latest_service(
        &self,
        service_name: impl AsRef<str>,
    ) -> Option<service::ServiceMetadata> {
        self.active_service_revisions
            .get(service_name.as_ref())
            .map(|revision| revision.as_service_metadata())
    }

    fn resolve_invocation_attempt_options(
        &self,
        deployment_id: &DeploymentId,
        service_name: impl AsRef<str>,
        handler_name: impl AsRef<str>,
    ) -> Option<service::InvocationAttemptOptions> {
        let service_name = service_name.as_ref();
        let handler_name = handler_name.as_ref();

        let deployment = self.deployments.get(deployment_id)?;
        let service_revision = deployment.services.get(service_name)?;
        let handler = service_revision.handlers.get(handler_name)?;

        Some(service::InvocationAttemptOptions {
            abort_timeout: handler.abort_timeout.or(service_revision.abort_timeout),
            inactivity_timeout: handler
                .inactivity_timeout
                .or(service_revision.inactivity_timeout),
            enable_lazy_state: handler
                .enable_lazy_state
                .or(service_revision.enable_lazy_state),
        })
    }

    fn resolve_latest_service_openapi(&self, service_name: impl AsRef<str>) -> Option<Value> {
        self.active_service_revisions
            .get(service_name.as_ref())
            .map(|revision| revision.service_revision.as_openapi_spec())
    }

    fn resolve_latest_service_type(&self, service_name: impl AsRef<str>) -> Option<ServiceType> {
        self.active_service_revisions
            .get(service_name.as_ref())
            .map(|revision| revision.service_revision.ty)
    }

    fn list_services(&self) -> Vec<service::ServiceMetadata> {
        self.active_service_revisions
            .values()
            .map(|revision| revision.as_service_metadata())
            .collect()
    }
}

impl SubscriptionResolver for Schema {
    fn get_subscription(&self, id: SubscriptionId) -> Option<Subscription> {
        self.subscriptions.get(&id).cloned()
    }

    fn list_subscriptions(&self, filters: &[ListSubscriptionFilter]) -> Vec<Subscription> {
        self.subscriptions
            .values()
            .filter(|sub| {
                for f in filters {
                    if !f.matches(sub) {
                        return false;
                    }
                }
                true
            })
            .cloned()
            .collect()
    }
}

// --- Mocks

#[cfg(feature = "test-util")]
mod test_util {

    use super::*;

    use super::service::ServiceMetadata;
    use super::service::ServiceMetadataResolver;
    use crate::identifiers::ServiceRevision;
    use restate_test_util::assert_eq;

    impl Schema {
        #[track_caller]
        pub fn assert_service_handler(
            &self,
            service_name: &str,
            handler_name: &str,
        ) -> InvocationTargetMetadata {
            self.resolve_latest_invocation_target(service_name, handler_name)
                .unwrap_or_else(|| {
                    panic!("Invocation target for {service_name}/{handler_name} must exists")
                })
        }

        #[track_caller]
        pub fn assert_service_revision(&self, service_name: &str, revision: ServiceRevision) {
            assert_eq!(
                self.resolve_latest_service(service_name).unwrap().revision,
                revision
            );
        }

        #[track_caller]
        pub fn assert_service_deployment(&self, service_name: &str, deployment_id: DeploymentId) {
            assert_eq!(
                self.resolve_latest_service(service_name)
                    .unwrap()
                    .deployment_id,
                deployment_id
            );
        }

        #[track_caller]
        pub fn assert_service(&self, service_name: &str) -> ServiceMetadata {
            self.resolve_latest_service(service_name).unwrap()
        }
    }
}
