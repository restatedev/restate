mod openapi;
mod serde_hacks;
pub mod updater;

use std::cmp;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwapOption;
use serde_json::Value;
use serde_with::serde_as;

use restate_serde_util::{DurationString, MapAsVecItem};

use crate::config::{Configuration, InvocationRetryPolicyOptions};
use crate::identifiers::{DeploymentId, SubscriptionId};
use crate::invocation::{InvocationTargetType, ServiceType, WorkflowHandlerType};
use crate::live::Pinned;
use crate::metadata::GlobalMetadata;
use crate::net::metadata::{MetadataContainer, MetadataKind};
use crate::retries::{RetryIter, RetryPolicy};
use crate::schema::deployment::{DeliveryOptions, DeploymentResolver, DeploymentType};
use crate::schema::invocation_target::{
    DEFAULT_IDEMPOTENCY_RETENTION, DEFAULT_WORKFLOW_COMPLETION_RETENTION, InputRules,
    InvocationAttemptOptions, InvocationTargetMetadata, InvocationTargetResolver, OnMaxAttempts,
    OutputRules,
};
use crate::schema::metadata::openapi::ServiceOpenAPI;
use crate::schema::service::{
    HandlerRetryPolicyMetadata, ServiceMetadataResolver, ServiceRetryPolicyMetadata,
};
use crate::schema::subscriptions::{ListSubscriptionFilter, Subscription, SubscriptionResolver};
use crate::schema::{deployment, service};
use crate::time::MillisSinceEpoch;
use crate::{Version, Versioned, identifiers};

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
    /// In case the invocation has an idempotency key, the `idempotency_retention` caps the maximum `journal_retention` time.
    /// In case the invocation targets a workflow handler, the `workflow_completion_retention` caps the maximum `journal_retention` time.
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

    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "serde_with::As::<Option<restate_serde_util::DurationString>>"
    )]
    retry_policy_initial_interval: Option<Duration>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    retry_policy_exponentiation_factor: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    retry_policy_max_attempts: Option<usize>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "serde_with::As::<Option<restate_serde_util::DurationString>>"
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
    fn to_service_metadata(&self, deployment_id: DeploymentId) -> service::ServiceMetadata {
        let configuration = Configuration::pinned();

        let mut retry_policy = configuration.resolve_default_retry_policy();
        retry_policy.merge_with_service_revision_overrides(self);

        service::ServiceMetadata {
            name: self.name.clone(),
            handlers: self
                .handlers
                .iter()
                .map(|(name, handler)| {
                    (
                        name.clone(),
                        handler.as_handler_metadata(&configuration, self.public),
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
            journal_retention: configuration.clamp_journal_retention(
                self.journal_retention.or(configuration
                    .invocation
                    .default_journal_retention
                    .to_non_zero()),
            ),
            inactivity_timeout: self
                .inactivity_timeout
                .unwrap_or_else(|| configuration.worker.invoker.inactivity_timeout.into()),
            abort_timeout: self
                .abort_timeout
                .unwrap_or_else(|| configuration.worker.invoker.abort_timeout.into()),
            enable_lazy_state: self.enable_lazy_state.unwrap_or(false),
            retry_policy,
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
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "serde_with::As::<Option<restate_serde_util::DurationString>>"
    )]
    retry_policy_initial_interval: Option<Duration>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    retry_policy_exponentiation_factor: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    retry_policy_max_attempts: Option<usize>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "serde_with::As::<Option<restate_serde_util::DurationString>>"
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
    ) -> service::HandlerMetadata {
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
            journal_retention: configuration.clamp_journal_retention(self.journal_retention),
            inactivity_timeout: self.inactivity_timeout,
            abort_timeout: self.abort_timeout,
            enable_lazy_state: self.enable_lazy_state,
            retry_policy: HandlerRetryPolicyMetadata {
                initial_interval: self.retry_policy_initial_interval,
                exponentiation_factor: self.retry_policy_exponentiation_factor,
                max_attempts: self.retry_policy_max_attempts,
                max_interval: self.retry_policy_max_interval,
                on_max_attempts: self.retry_policy_on_max_attempts,
            },
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
        let configuration = Configuration::pinned();

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
        let journal_retention = configuration
            .clamp_journal_retention(
                handler
                    .journal_retention
                    .or(service_revision.journal_retention)
                    .or_else(|| {
                        configuration
                            .invocation
                            .default_journal_retention
                            .to_non_zero()
                    }),
            )
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
            .map(|revision| revision.as_service_metadata())
    }

    fn resolve_latest_service_openapi(&self, service_name: impl AsRef<str>) -> Option<Value> {
        self.active_service_revisions
            .get(service_name.as_ref())
            .map(|revision| revision.service_revision.as_openapi_spec())
    }

    fn list_services(&self) -> Vec<service::ServiceMetadata> {
        self.active_service_revisions
            .values()
            .map(|revision| revision.as_service_metadata())
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

impl Configuration {
    fn clamp_journal_retention(&self, requested: Option<Duration>) -> Option<Duration> {
        clamp_max(
            requested,
            self.invocation.max_journal_retention.map(Duration::from),
        )
    }

    fn clamp_max_attempts(&self, requested: Option<usize>) -> Option<usize> {
        clamp_max(requested, self.invocation.max_retry_policy_max_attempts)
    }
}

fn clamp_max<T: Ord>(requested: Option<T>, limit: Option<T>) -> Option<T> {
    match (requested, limit) {
        (None, Some(_)) => None,
        (None, None) => None,
        (Some(requested_duration), None) => Some(requested_duration),
        (Some(requested_duration), Some(global_limit)) => {
            Some(cmp::min(requested_duration, global_limit))
        }
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
        if self.max_attempts == Some(0) {
            // No retries!
            return RetryPolicy::None;
        }
        RetryPolicy::Exponential {
            initial_interval: self.initial_interval,
            factor: self.exponentiation_factor,
            max_attempts: self.max_attempts.map(|u| NonZeroUsize::new(u).unwrap()),
            max_interval: self.max_interval,
        }
    }
}

impl Configuration {
    // TODO this method can be removed when the old retry policy configuration gets merged
    fn resolve_default_retry_policy(&self) -> ComputedRetryPolicy {
        if let Some(InvocationRetryPolicyOptions {
            initial_interval,
            exponentiation_factor,
            max_attempts,
            on_max_attempts,
            max_interval,
        }) = &self.invocation.default_retry_policy
        {
            return ComputedRetryPolicy {
                initial_interval: **initial_interval,
                exponentiation_factor: *exponentiation_factor,
                max_attempts: *max_attempts,
                max_interval: max_interval.map(DurationString::into_inner),
                on_max_attempts: match on_max_attempts {
                    crate::config::OnMaxAttempts::Pause => OnMaxAttempts::Pause,
                    crate::config::OnMaxAttempts::Kill => OnMaxAttempts::Kill,
                },
            };
        }

        #[allow(deprecated)]
        match self
            .worker
            .invoker
            .retry_policy
            .as_ref()
            .unwrap_or(&RetryPolicy::None)
        {
            RetryPolicy::None => ComputedRetryPolicy {
                initial_interval: Default::default(),
                exponentiation_factor: 1.0,
                max_attempts: Some(0),
                max_interval: None,
                on_max_attempts: OnMaxAttempts::Kill,
            },
            RetryPolicy::FixedDelay {
                max_attempts,
                interval,
            } => ComputedRetryPolicy {
                initial_interval: *interval,
                exponentiation_factor: 1.0,
                max_attempts: max_attempts.map(|u| u.get()),
                max_interval: Some(*interval),
                on_max_attempts: OnMaxAttempts::Kill,
            },
            RetryPolicy::Exponential {
                max_attempts,
                initial_interval,
                factor,
                max_interval,
            } => ComputedRetryPolicy {
                initial_interval: *initial_interval,
                exponentiation_factor: *factor,
                max_attempts: max_attempts.map(|u| u.get()),
                max_interval: *max_interval,
                on_max_attempts: OnMaxAttempts::Kill,
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
