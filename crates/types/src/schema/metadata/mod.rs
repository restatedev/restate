mod openapi;
mod serde_hacks;
pub mod updater;

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwapOption;
use http::{HeaderName, HeaderValue};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_with::serde_as;

use restate_serde_util::MapAsVecItem;
use restate_time_util::FriendlyDuration;

use crate::config::{Configuration, InvocationRetryPolicyOptions};
use crate::deployment::{
    DeploymentAddress, Headers, HttpDeploymentAddress, LambdaDeploymentAddress,
};
use crate::identifiers::{DeploymentId, SubscriptionId};
use crate::invocation::{InvocationTargetType, ServiceType, WorkflowHandlerType};
use crate::live::Pinned;
use crate::metadata::GlobalMetadata;
use crate::net::address::{AdvertisedAddress, HttpIngressPort};
use crate::net::metadata::{MetadataContainer, MetadataKind};
use crate::retries::{RetryIter, RetryPolicy};
use crate::schema::deployment::{DeploymentResolver, DeploymentType, ProtocolType};
use crate::schema::info::Info;
use crate::schema::invocation_target::{
    DEFAULT_IDEMPOTENCY_RETENTION, DEFAULT_WORKFLOW_COMPLETION_RETENTION, DeploymentStatus,
    InputRules, InvocationAttemptOptions, InvocationTargetMetadata, InvocationTargetResolver,
    OnMaxAttempts, OutputRules,
};
use crate::schema::kafka::{KafkaCluster, KafkaClusterResolver};
use crate::schema::metadata::openapi::ServiceOpenAPI;
use crate::schema::service::{
    HandlerRetryPolicyMetadata, ServiceMetadataResolver, ServiceRetryPolicyMetadata,
};
use crate::schema::subscriptions::{
    ListSubscriptionFilter, Source, Subscription, SubscriptionResolver,
};
use crate::schema::{Redaction, deployment, service};
use crate::service_protocol::ServiceProtocolVersion;
use crate::time::MillisSinceEpoch;
use crate::{Version, Versioned, identifiers};

/// Serializable data structure representing the schema registry
///
/// Do not leak the representation as this data structure, as it strictly depends on SchemaUpdater, SchemaRegistry and the Admin API.
#[derive(derive_more::Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(from = "serde_hacks::Schema", into = "serde_hacks::Schema")]
#[debug("Schema(version: {version})")]
pub struct Schema {
    /// This gets bumped on each update.
    version: Version,

    deployments: HashMap<DeploymentId, Deployment>,
    active_service_revisions: HashMap<String, ActiveServiceRevision>,
    subscriptions: HashMap<SubscriptionId, Subscription>,
    kafka_clusters: HashMap<String, KafkaCluster>,
}

impl Default for Schema {
    fn default() -> Self {
        Self {
            version: Version::INVALID,
            active_service_revisions: HashMap::default(),
            deployments: HashMap::default(),
            subscriptions: HashMap::default(),
            kafka_clusters: HashMap::default(),
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
    fn as_service_metadata(
        &self,
        served_using_protocol_type: Option<ProtocolType>,
    ) -> service::ServiceMetadata {
        self.service_revision
            .to_service_metadata(self.deployment_id, served_using_protocol_type)
    }

    fn create_index<'a>(
        deployments: impl IntoIterator<Item = &'a Deployment>,
    ) -> HashMap<String, Self> {
        let mut active_service_revisions = HashMap::new();
        for deployment in deployments {
            for service in deployment.services.values() {
                active_service_revisions
                    .entry(service.name.clone())
                    .and_modify(|registered_service_revision: &mut ActiveServiceRevision| {
                        if registered_service_revision.service_revision.revision < service.revision
                        {
                            *registered_service_revision = ActiveServiceRevision {
                                deployment_id: deployment.id,
                                service_revision: Arc::clone(service),
                            }
                        }
                    })
                    .or_insert(ActiveServiceRevision {
                        deployment_id: deployment.id,
                        service_revision: Arc::clone(service),
                    });
            }
        }
        active_service_revisions
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct DeliveryOptions {
    #[serde(
        with = "serde_with::As::<serde_with::FromInto<restate_serde_util::SerdeableHeaderHashMap>>"
    )]
    pub additional_headers: HashMap<HeaderName, HeaderValue>,
}

impl DeliveryOptions {
    fn new(additional_headers: HashMap<HeaderName, HeaderValue>) -> Self {
        Self { additional_headers }
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

    /// User provided metadata during registration
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    metadata: HashMap<String, String>,

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
            ty: self.ty.clone(),
            supported_protocol_versions: self.supported_protocol_versions.clone(),
            sdk_version: self.sdk_version.clone(),
            created_at: self.created_at,
            metadata: self.metadata.clone(),
            additional_headers: self.delivery_options.additional_headers.clone(),
            info: vec![],
        }
    }
    /// This returns true if the two deployments are to be considered the "same".
    pub fn semantic_eq_with_address_and_headers(
        &self,
        other_addess: &DeploymentAddress,
        other_additional_headers: &Headers,
    ) -> bool {
        match (&self.ty, other_addess) {
            (
                DeploymentType::Http {
                    address: this_address,
                    ..
                },
                DeploymentAddress::Http(HttpDeploymentAddress { uri: other_address }),
            ) => deployment::Deployment::semantic_eq_http(
                this_address,
                other_address,
                &self.delivery_options.additional_headers,
                other_additional_headers,
            ),
            (
                DeploymentType::Lambda { arn: this_arn, .. },
                DeploymentAddress::Lambda(LambdaDeploymentAddress { arn: other_arn, .. }),
            ) => deployment::Deployment::semantic_eq_lambda(this_arn, other_arn),
            _ => false,
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
        with = "serde_with::As::<Option<FriendlyDuration>>",
        skip_serializing_if = "Option::is_none",
        default
    )]
    idempotency_retention: Option<Duration>,

    /// The retention duration of workflows. Only available on workflow services.
    #[serde(
        with = "serde_with::As::<Option<FriendlyDuration>>",
        skip_serializing_if = "Option::is_none",
        default
    )]
    workflow_completion_retention: Option<Duration>,

    /// The journal retention. When set, this applies to all requests to all handlers of this service.
    ///
    /// In case the invocation has an idempotency key, the `idempotency_retention` caps the maximum `journal_retention` time.
    /// In case the invocation targets a workflow handler, the `workflow_completion_retention` caps the maximum `journal_retention` time.
    #[serde(
        with = "serde_with::As::<Option<FriendlyDuration>>",
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
        with = "serde_with::As::<Option<FriendlyDuration>>",
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
        with = "serde_with::As::<Option<FriendlyDuration>>",
        skip_serializing_if = "Option::is_none",
        default
    )]
    abort_timeout: Option<Duration>,

    /// If true, lazy state will be enabled for all invocations to this service.
    /// This is relevant only for Workflows and Virtual Objects.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    enable_lazy_state: Option<bool>,

    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "serde_with::As::<Option<FriendlyDuration>>"
    )]
    retry_policy_initial_interval: Option<Duration>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    retry_policy_exponentiation_factor: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    retry_policy_max_attempts: Option<NonZeroUsize>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "serde_with::As::<Option<FriendlyDuration>>"
    )]
    retry_policy_max_interval: Option<Duration>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    retry_policy_on_max_attempts: Option<OnMaxAttempts>,

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
    fn to_service_metadata(
        &self,
        deployment_id: DeploymentId,
        served_using_protocol_type: Option<ProtocolType>,
    ) -> service::ServiceMetadata {
        let configuration = Configuration::pinned();

        let mut retry_policy = configuration.resolve_default_retry_policy();
        retry_policy.merge_with_service_revision_overrides(self);

        let mut info = vec![];
        if let Some(inactivity_timeout) = self.inactivity_timeout
            && served_using_protocol_type == Some(ProtocolType::RequestResponse)
        {
            info.push(
                Info::new_with_code(
                    &restate_errors::RT0021,
                    format!("The configured inactivity_timeout {} will not be applied because the service is exposed in Request/Response mode.", FriendlyDuration::new(inactivity_timeout))
                )
            )
        }

        let (journal_retention, got_journal_retention_clamped) = configuration
            .clamp_journal_retention(
                self.journal_retention.or(configuration
                    .invocation
                    .default_journal_retention
                    .to_non_zero_std()),
            );
        if got_journal_retention_clamped {
            info.push(Info::new_with_code(
                &restate_errors::RT0022,
                "The configured journal_retention is clamped to the maximum server limit."
                    .to_string(),
            ))
        }

        service::ServiceMetadata {
            name: self.name.clone(),
            handlers: self
                .handlers
                .iter()
                .map(|(name, handler)| {
                    (
                        name.clone(),
                        handler.as_handler_metadata(
                            &configuration,
                            self.public,
                            served_using_protocol_type,
                        ),
                    )
                })
                .collect(),
            ty: self.ty,
            documentation: self.documentation.clone(),
            metadata: self.metadata.clone(),
            deployment_id,
            revision: self.revision,
            public: self.public,
            idempotency_retention: self
                .idempotency_retention
                .unwrap_or(DEFAULT_IDEMPOTENCY_RETENTION),
            workflow_completion_retention: if self.ty == ServiceType::Workflow {
                Some(
                    self.handlers
                        .iter()
                        .find(|(_, h)| h.workflow_completion_retention.is_some())
                        .and_then(|(_, h)| h.workflow_completion_retention)
                        .or(self.workflow_completion_retention)
                        .unwrap_or(DEFAULT_WORKFLOW_COMPLETION_RETENTION),
                )
            } else {
                None
            },
            journal_retention,
            inactivity_timeout: if served_using_protocol_type == Some(ProtocolType::RequestResponse)
            {
                Duration::ZERO
            } else {
                self.inactivity_timeout
                    .unwrap_or_else(|| configuration.worker.invoker.inactivity_timeout.into())
            },
            abort_timeout: self
                .abort_timeout
                .unwrap_or_else(|| configuration.worker.invoker.abort_timeout.into()),
            enable_lazy_state: self.enable_lazy_state.unwrap_or(false),
            retry_policy,
            info,
        }
    }

    fn as_openapi_spec(
        &self,
        ingress_address: AdvertisedAddress<HttpIngressPort>,
    ) -> serde_json::Value {
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

        service_openapi.to_openapi_contract(
            &self.name,
            ingress_address,
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
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "serde_with::As::<Option<FriendlyDuration>>"
    )]
    retry_policy_initial_interval: Option<Duration>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    retry_policy_exponentiation_factor: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    retry_policy_max_attempts: Option<NonZeroUsize>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "serde_with::As::<Option<FriendlyDuration>>"
    )]
    retry_policy_max_interval: Option<Duration>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    retry_policy_on_max_attempts: Option<OnMaxAttempts>,
}

impl MapAsVecItem for Handler {
    type Key = String;

    fn key(&self) -> Self::Key {
        self.name.clone()
    }
}

impl Handler {
    fn as_handler_metadata(
        &self,
        configuration: &Pinned<Configuration>,
        service_level_public: bool,
        served_using_protocol_type: Option<ProtocolType>,
    ) -> service::HandlerMetadata {
        let mut info = vec![];
        if let Some(inactivity_timeout) = self.inactivity_timeout
            && served_using_protocol_type == Some(ProtocolType::RequestResponse)
        {
            info.push(
                Info::new_with_code(
                    &restate_errors::RT0021,
                    format!("The configured inactivity_timeout {} will not be applied because the handler is exposed in Request/Response mode.", FriendlyDuration::new(inactivity_timeout))
                )
            )
        }

        let (journal_retention, got_journal_retention_clamped) =
            configuration.clamp_journal_retention(self.journal_retention);
        if got_journal_retention_clamped {
            info.push(Info::new_with_code(
                &restate_errors::RT0022,
                "The configured journal_retention is clamped to the maximum server limit."
                    .to_string(),
            ))
        }

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
            journal_retention,
            inactivity_timeout: if served_using_protocol_type == Some(ProtocolType::RequestResponse)
            {
                None
            } else {
                self.inactivity_timeout
            },
            abort_timeout: self.abort_timeout,
            enable_lazy_state: self.enable_lazy_state,
            retry_policy: HandlerRetryPolicyMetadata {
                initial_interval: self.retry_policy_initial_interval,
                exponentiation_factor: self.retry_policy_exponentiation_factor,
                max_attempts: self.retry_policy_max_attempts,
                max_interval: self.retry_policy_max_interval,
                on_max_attempts: self.retry_policy_on_max_attempts,
            },
            info,
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

    fn find_deployment(
        &self,
        deployment_address: &DeploymentAddress,
        additional_headers: &Headers,
    ) -> Option<(deployment::Deployment, Vec<service::ServiceMetadata>)> {
        self.deployments
            .iter()
            .find(|(_, d)| {
                d.semantic_eq_with_address_and_headers(deployment_address, additional_headers)
            })
            .map(|(dp_id, dp)| {
                (
                    dp.to_deployment(),
                    dp.services
                        .values()
                        .map(|s| s.to_service_metadata(*dp_id, Some(dp.ty.protocol_type())))
                        .collect(),
                )
            })
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
                    .map(|s| s.to_service_metadata(*deployment_id, Some(dp.ty.protocol_type())))
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
        let configuration = Configuration::pinned();

        let service_name = service_name.as_ref();
        let handler_name = handler_name.as_ref();

        let ActiveServiceRevision {
            service_revision,
            deployment_id,
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
        let journal_retention = configuration
            .clamp_journal_retention(
                handler
                    .journal_retention
                    .or(service_revision.journal_retention)
                    .or_else(|| {
                        configuration
                            .invocation
                            .default_journal_retention
                            .to_non_zero_std()
                    }),
            )
            .0
            .unwrap_or(Duration::ZERO);

        let deployment_status = self
            .deployments
            .get(deployment_id)
            .map(|dp| {
                if ServiceProtocolVersion::is_acceptable_for_new_invocations(
                    *dp.supported_protocol_versions.start(),
                    *dp.supported_protocol_versions.end(),
                ) {
                    DeploymentStatus::Enabled
                } else {
                    DeploymentStatus::Deprecated(dp.id)
                }
            })
            // It should never happen that the deployment doesn't exist,
            // this is an invalid schema registry otherwise.
            // But let's not panic yet, this will fail later on.
            .unwrap_or_default();

        Some(InvocationTargetMetadata {
            public: handler.public.unwrap_or(service_revision.public),
            completion_retention,
            journal_retention,
            target_ty: handler.target_ty,
            input_rules: handler.input_rules.clone(),
            output_rules: handler.output_rules.clone(),
            deployment_status,
        })
    }

    fn resolve_invocation_attempt_options(
        &self,
        deployment_id: &DeploymentId,
        service_name: impl AsRef<str>,
        handler_name: impl AsRef<str>,
    ) -> Option<InvocationAttemptOptions> {
        let service_name = service_name.as_ref();
        let handler_name = handler_name.as_ref();

        let deployment = self.deployments.get(deployment_id)?;
        let service_revision = deployment.services.get(service_name)?;
        let handler = service_revision.handlers.get(handler_name)?;

        Some(InvocationAttemptOptions {
            abort_timeout: handler.abort_timeout.or(service_revision.abort_timeout),
            inactivity_timeout: handler
                .inactivity_timeout
                .or(service_revision.inactivity_timeout),
            enable_lazy_state: handler
                .enable_lazy_state
                .or(service_revision.enable_lazy_state),
        })
    }

    fn resolve_latest_service_type(&self, service_name: impl AsRef<str>) -> Option<ServiceType> {
        self.active_service_revisions
            .get(service_name.as_ref())
            .map(|revision| revision.service_revision.ty)
    }

    fn resolve_invocation_retry_policy(
        &self,
        deployment_id: Option<&DeploymentId>,
        service_name: impl AsRef<str>,
        handler_name: impl AsRef<str>,
    ) -> (RetryIter<'static>, OnMaxAttempts) {
        let configuration = Configuration::pinned();
        let mut retry_policy = configuration.resolve_default_retry_policy();

        let Some(service_revision) = (if let Some(deployment_id) = deployment_id {
            self.deployments
                .get(deployment_id)
                .and_then(|dp| dp.services.get(service_name.as_ref()))
        } else {
            self.active_service_revisions
                .get(service_name.as_ref())
                .map(|a| &a.service_revision)
        }) else {
            return (
                retry_policy.as_retry_policy().into_iter(),
                retry_policy.on_max_attempts,
            );
        };

        retry_policy.merge_with_service_revision_overrides(service_revision);

        let Some(handler) = service_revision.handlers.get(handler_name.as_ref()) else {
            return (
                retry_policy.as_retry_policy().into_iter(),
                retry_policy.on_max_attempts,
            );
        };

        retry_policy.merge_with_handler_overrides(handler);

        retry_policy.max_attempts = configuration.clamp_max_attempts(retry_policy.max_attempts);

        (
            retry_policy.as_retry_policy().into_iter(),
            retry_policy.on_max_attempts,
        )
    }
}

impl ServiceMetadataResolver for Schema {
    fn resolve_latest_service(
        &self,
        service_name: impl AsRef<str>,
    ) -> Option<service::ServiceMetadata> {
        self.active_service_revisions
            .get(service_name.as_ref())
            .map(|revision| {
                let protocol_type = self
                    .deployments
                    .get(&revision.deployment_id)
                    .map(|dp| dp.ty.protocol_type());

                revision.as_service_metadata(protocol_type)
            })
    }

    fn resolve_latest_service_openapi(
        &self,
        service_name: impl AsRef<str>,
        ingress_address: AdvertisedAddress<HttpIngressPort>,
    ) -> Option<Value> {
        self.active_service_revisions
            .get(service_name.as_ref())
            .map(|revision| revision.service_revision.as_openapi_spec(ingress_address))
    }

    fn list_services(&self) -> Vec<service::ServiceMetadata> {
        self.active_service_revisions
            .values()
            .map(|revision| {
                let protocol_type = self
                    .deployments
                    .get(&revision.deployment_id)
                    .map(|dp| dp.ty.protocol_type());
                revision.as_service_metadata(protocol_type)
            })
            .collect()
    }

    fn list_service_names(&self) -> Vec<String> {
        self.active_service_revisions
            .values()
            .map(|revision| revision.service_revision.name.clone())
            .collect()
    }
}

impl SubscriptionResolver for Schema {
    fn get_subscription(
        &self,
        id: SubscriptionId,
        redact_secrets: Redaction,
    ) -> Option<Subscription> {
        self.subscriptions
            .get(&id)
            .cloned()
            .map(|sub| sub.redact(redact_secrets))
    }

    fn list_subscriptions(
        &self,
        filters: &[ListSubscriptionFilter],
        redact_secrets: Redaction,
    ) -> Vec<Subscription> {
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
            .map(|sub| sub.redact(redact_secrets))
            .collect()
    }
}

impl KafkaClusterResolver for Schema {
    fn get_kafka_cluster(
        &self,
        cluster_name: &str,
        redact_secrets: Redaction,
    ) -> Option<KafkaCluster> {
        self.kafka_clusters
            .get(cluster_name)
            .cloned()
            .or_else(|| {
                Configuration::pinned()
                    .ingress
                    .get_kafka_cluster(cluster_name)
                    .cloned()
                    .map(Into::into)
            })
            .map(|cluster| cluster.redact(redact_secrets))
    }

    fn get_kafka_cluster_and_subscriptions(
        &self,
        cluster_name: &str,
        redact_secrets: Redaction,
    ) -> Option<(KafkaCluster, Vec<Subscription>)> {
        let cluster = KafkaClusterResolver::get_kafka_cluster(self, cluster_name, redact_secrets)?;

        let subscriptions = self
            .subscriptions
            .values()
            .filter(|sub| {
                let Source::Kafka { cluster, .. } = sub.source();
                cluster == cluster_name
            })
            .map(|sub| sub.clone().redact(redact_secrets))
            .collect();
        Some((cluster, subscriptions))
    }

    fn list_kafka_clusters(&self, redact_secrets: Redaction) -> Vec<KafkaCluster> {
        self.kafka_clusters
            .values()
            .map(|cluster| cluster.clone().redact(redact_secrets))
            .chain(
                Configuration::pinned()
                    .ingress
                    .kafka_clusters
                    .iter()
                    .map(|cluster| KafkaCluster::from(cluster.clone()).redact(redact_secrets)),
            )
            .collect()
    }
}

const REDACTION_VALUE: &str = "***";

impl KafkaCluster {
    fn redact(mut self, redact_secrets: Redaction) -> KafkaCluster {
        match redact_secrets {
            Redaction::Yes => {
                let redacted_properties = self.properties_mut();
                for (key, value) in redacted_properties.iter_mut() {
                    if is_sensitive_kafka_property(key) {
                        *value = REDACTION_VALUE.to_string();
                    }
                }
            }
            Redaction::No => {}
        };
        self
    }
}

impl Subscription {
    fn redact(mut self, redact_secrets: Redaction) -> Subscription {
        match redact_secrets {
            Redaction::Yes => {
                let redacted_properties = self.metadata_mut();
                for (key, value) in redacted_properties.iter_mut() {
                    if is_sensitive_kafka_property(key) {
                        *value = REDACTION_VALUE.to_string();
                    }
                }
            }
            Redaction::No => {}
        };
        self
    }
}

/// Checks if a Kafka property key is sensitive and should be redacted
/// Based on librdkafka CONFIGURATION.md
fn is_sensitive_kafka_property(key: &str) -> bool {
    let key_lower = key.to_lowercase();

    // Redact anything with password, secret, passphrase, or token in the name
    if key_lower.contains("password")
        || key_lower.contains("secret")
        || key_lower.contains("passphrase")
    {
        return true;
    }

    // Redact SASL credentials
    if key_lower.contains("sasl.username") {
        return true;
    }

    // Redact SASL JAAS config (contains credentials)
    if key_lower.contains("sasl.jaas.config") {
        return true;
    }

    // Redact OAuth/OIDC configs (may contain credentials)
    if key_lower.contains("sasl.oauthbearer.config") {
        return true;
    }

    // Redact OAuth token endpoints (may contain secrets in URL params)
    if key_lower.contains("token.endpoint.url") {
        return true;
    }

    // Redact SSL/TLS private keys (PEM format or file locations)
    if key_lower.contains("ssl.key.location") || key_lower.contains("ssl.key.pem") {
        return true;
    }

    // Redact keystore and truststore locations (can reveal filesystem structure)
    if key_lower.contains("keystore.location") || key_lower.contains("truststore.location") {
        return true;
    }

    // Redact OAuth assertion private keys (JWT signing)
    if key_lower.contains("sasl.oauthbearer.assertion.private.key") {
        return true;
    }

    false
}

impl Configuration {
    fn clamp_journal_retention(
        &self,
        requested: Option<Duration>,
    ) -> (Option<Duration>, /* got clamped */ bool) {
        clamp_max(
            requested,
            self.invocation.max_journal_retention.map(Duration::from),
        )
    }

    fn clamp_max_attempts(&self, requested: Option<NonZeroUsize>) -> Option<NonZeroUsize> {
        clamp_max(requested, self.invocation.max_retry_policy_max_attempts).0
    }
}

fn clamp_max<T: Ord>(
    requested: Option<T>,
    limit: Option<T>,
) -> (Option<T>, /* got clamped */ bool) {
    match (requested, limit) {
        (None, Some(_)) => (None, false),
        (None, None) => (None, false),
        (Some(requested_duration), None) => (Some(requested_duration), false),
        (Some(requested_duration), Some(global_limit)) if requested_duration > global_limit => {
            (Some(global_limit), true)
        }
        (Some(requested_duration), Some(_)) => (Some(requested_duration), true),
    }
}

type ComputedRetryPolicy = ServiceRetryPolicyMetadata;

impl ComputedRetryPolicy {
    fn merge_with_service_revision_overrides(&mut self, service_revision: &ServiceRevision) {
        if let Some(initial_interval) = service_revision.retry_policy_initial_interval {
            self.initial_interval = initial_interval;
        }
        if let Some(max_attempts) = service_revision.retry_policy_max_attempts {
            self.max_attempts = Some(max_attempts);
        }
        if let Some(max_interval) = service_revision.retry_policy_max_interval {
            self.max_interval = Some(max_interval);
        }
        if let Some(exponentiation_factor) = service_revision.retry_policy_exponentiation_factor {
            self.exponentiation_factor = exponentiation_factor;
        }
        if let Some(on_max_attempts) = service_revision.retry_policy_on_max_attempts {
            self.on_max_attempts = on_max_attempts;
        }
    }

    fn merge_with_handler_overrides(&mut self, handler: &Handler) {
        if let Some(initial_interval) = handler.retry_policy_initial_interval {
            self.initial_interval = initial_interval;
        }
        if let Some(max_attempts) = handler.retry_policy_max_attempts {
            self.max_attempts = Some(max_attempts);
        }
        if let Some(max_interval) = handler.retry_policy_max_interval {
            self.max_interval = Some(max_interval);
        }
        if let Some(exponentiation_factor) = handler.retry_policy_exponentiation_factor {
            self.exponentiation_factor = exponentiation_factor;
        }
        if let Some(on_max_attempts) = handler.retry_policy_on_max_attempts {
            self.on_max_attempts = on_max_attempts;
        }
    }

    pub(super) fn as_retry_policy(&self) -> RetryPolicy {
        if self.max_attempts == Some(NonZeroUsize::MIN) {
            // No retries!
            return RetryPolicy::None;
        }
        RetryPolicy::Exponential {
            initial_interval: self.initial_interval,
            factor: self.exponentiation_factor,
            max_attempts: self
                .max_attempts
                .map(|n| NonZeroUsize::new(n.get() - 1).expect("max_attempts > 1")),
            max_interval: self.max_interval,
        }
    }
}

impl Configuration {
    fn resolve_default_retry_policy(&self) -> ComputedRetryPolicy {
        let InvocationRetryPolicyOptions {
            initial_interval,
            exponentiation_factor,
            max_attempts,
            on_max_attempts,
            max_interval,
        } = &self.invocation.default_retry_policy;

        ComputedRetryPolicy {
            initial_interval: **initial_interval,
            exponentiation_factor: *exponentiation_factor,
            max_attempts: max_attempts.clone().into(),
            max_interval: Some(**max_interval),
            on_max_attempts: match on_max_attempts {
                crate::config::OnMaxAttempts::Pause => OnMaxAttempts::Pause,
                crate::config::OnMaxAttempts::Kill => OnMaxAttempts::Kill,
            },
        }
    }
}

// --- Mocks

#[cfg(feature = "test-util")]
mod test_util {

    use super::*;

    use super::service::ServiceMetadata;
    use super::service::ServiceMetadataResolver;
    use crate::identifiers::ServiceRevision;
    use crate::schema::service::HandlerMetadata;
    use restate_test_util::assert_eq;

    impl Schema {
        #[track_caller]
        pub fn assert_invocation_target(
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

        #[track_caller]
        pub fn assert_handler(&self, service_name: &str, handler_name: &str) -> HandlerMetadata {
            self.resolve_latest_service(service_name)
                .unwrap()
                .handlers
                .get(handler_name)
                .cloned()
                .unwrap()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::config::{
        ConfigurationBuilder, InvocationOptionsBuilder, InvocationRetryPolicyOptionsBuilder,
        MaxAttempts,
    };
    use googletest::prelude::*;

    #[test]
    fn retry_policy_correctly_inferred_for_max_attempts_eq_to_1() {
        let configuration = ConfigurationBuilder::default()
            .invocation(
                InvocationOptionsBuilder::default()
                    .default_retry_policy(
                        InvocationRetryPolicyOptionsBuilder::default()
                            .max_attempts(MaxAttempts::Bounded(NonZeroUsize::new(1).unwrap()))
                            .build()
                            .unwrap(),
                    )
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();

        // If no retry policy, number of attempts = 1 -> no retry
        let retry_policy = configuration
            .resolve_default_retry_policy()
            .as_retry_policy();
        assert_that!(retry_policy.iter().collect::<Vec<_>>(), len(eq(0)));
    }

    #[test]
    fn retry_policy_correctly_inferred_for_max_attempts_eq_to_2() {
        let configuration = ConfigurationBuilder::default()
            .invocation(
                InvocationOptionsBuilder::default()
                    .default_retry_policy(
                        InvocationRetryPolicyOptionsBuilder::default()
                            .max_attempts(MaxAttempts::Bounded(NonZeroUsize::new(2).unwrap()))
                            .build()
                            .unwrap(),
                    )
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();

        // If no retry policy, number of attempts = 2 -> 1 retry
        let retry_policy = configuration
            .resolve_default_retry_policy()
            .as_retry_policy();
        assert_that!(retry_policy.iter().collect::<Vec<_>>(), len(eq(1)));
    }

    #[test]
    fn retry_policy_correctly_inferred_for_max_attempts_eq_to_3() {
        let configuration = ConfigurationBuilder::default()
            .invocation(
                InvocationOptionsBuilder::default()
                    .default_retry_policy(
                        InvocationRetryPolicyOptionsBuilder::default()
                            .max_attempts(MaxAttempts::Bounded(NonZeroUsize::new(3).unwrap()))
                            .build()
                            .unwrap(),
                    )
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();

        // If no retry policy, number of attempts = 3 -> 2 retry
        let retry_policy = configuration
            .resolve_default_retry_policy()
            .as_retry_policy();
        assert_that!(retry_policy.iter().collect::<Vec<_>>(), len(eq(2)));
    }
}
