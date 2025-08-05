// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::schema_registry::error::{
    DeploymentError, SchemaError, ServiceError, SubscriptionError,
};
use crate::schema_registry::{ModifyServiceChange, ServiceName};
use http::{HeaderValue, Uri};
use restate_types::endpoint_manifest;
use restate_types::identifiers::{DeploymentId, SubscriptionId};
use restate_types::invocation::{
    InvocationTargetType, ServiceType, VirtualObjectHandlerType, WorkflowHandlerType,
};
use restate_types::schema::Schema;
use restate_types::schema::deployment::DeploymentMetadata;
use restate_types::schema::deployment::DeploymentSchemas;
use restate_types::schema::invocation_target::{
    DEFAULT_IDEMPOTENCY_RETENTION, DEFAULT_WORKFLOW_COMPLETION_RETENTION, InputRules,
    InputValidationRule, InvocationTargetMetadata, OutputContentTypeRule, OutputRules,
};
use restate_types::schema::service::{HandlerSchemas, ServiceLocation, ServiceSchemas};
use restate_types::schema::subscriptions::{
    EventInvocationTargetTemplate, Sink, Source, Subscription, SubscriptionValidator,
};
use serde_json::Value;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::error::Error;
use std::ops::Not;
use std::time::Duration;
use tracing::{debug, info, warn};

/// Behavior when a handler is removed during service update
enum RemovedHandlerBehavior {
    /// Fail with an error when a handler is removed
    Fail,
    /// Log a warning but allow the removal
    Warn,
}

/// Behavior when service type changes during update
enum ServiceTypeMismatchBehavior {
    /// Fail with an error when service type changes
    Fail,
    /// Log a warning but allow the type change
    Warn,
}

/// Behavior for service level settings during update
enum ServiceLevelSettingsBehavior {
    /// Preserve existing service level settings
    Preserve,
    /// Reset to defaults
    #[allow(dead_code)]
    UseDefaults,
}

impl ServiceLevelSettingsBehavior {
    fn preserve(&self) -> bool {
        matches!(self, ServiceLevelSettingsBehavior::Preserve)
    }
}

/// Responsible for updating the provided [`Schema`] with new
/// schema information. It makes sure that the version of schema information
/// is incremented on changes.
#[derive(Debug, Default)]
pub struct SchemaUpdater {
    schema_information: Schema,
    modified: bool,
}

impl SchemaUpdater {
    pub(crate) fn new(schema_information: Schema) -> Self {
        Self {
            schema_information,
            modified: false,
        }
    }
}

impl SchemaUpdater {
    pub fn into_inner(mut self) -> Schema {
        if self.modified {
            self.schema_information.increment_version()
        }

        self.schema_information
    }

    pub fn add_deployment(
        &mut self,
        deployment_metadata: DeploymentMetadata,
        services: Vec<endpoint_manifest::Service>,
        force: bool,
    ) -> Result<DeploymentId, SchemaError> {
        let proposed_services: HashMap<_, _> = services
            .into_iter()
            .map(|c| ServiceName::try_from(c.name.to_string()).map(|name| (name, c)))
            .collect::<Result<HashMap<_, _>, _>>()?;

        // Did we find an existing deployment with a conflicting endpoint url?
        let mut existing_deployments = self
            .schema_information
            .find_existing_deployments_by_endpoint(&deployment_metadata.ty);

        let mut services_to_remove = Vec::default();

        let deployment_id = if let Some((existing_deployment_id, existing_deployment)) =
            existing_deployments.next()
        {
            if force {
                // Even under force we will only accept exactly one existing deployment with this endpoint
                if let Some((another_existing_deployment_id, _)) = existing_deployments.next() {
                    let mut existing_deployment_ids =
                        vec![*existing_deployment_id, *another_existing_deployment_id];
                    existing_deployment_ids
                        .extend(existing_deployments.map(|(deployment_id, _)| *deployment_id));

                    return Err(SchemaError::Deployment(
                        DeploymentError::MultipleExistingDeployments(existing_deployment_ids),
                    ));
                }

                for service in existing_deployment.services.values() {
                    // If a service is not available anymore in the new deployment, we need to remove it
                    if !proposed_services.contains_key(&service.name) {
                        warn!(
                            restate.deployment.id = %existing_deployment_id,
                            restate.deployment.address = %existing_deployment.metadata.address_display(),
                            "Going to remove service {} due to a forced deployment update",
                            service.name
                        );
                        services_to_remove.push(service.name.clone());
                    }
                }

                *existing_deployment_id
            } else {
                return Err(SchemaError::Override(format!(
                    "deployment with id '{existing_deployment_id}'"
                )));
            }
        } else {
            DeploymentId::new()
        };

        let mut services_to_add = HashMap::with_capacity(proposed_services.len());

        // Compute service schemas
        for (service_name, service) in proposed_services {
            let service_schema = self.create_or_update_service_schemas(
                &deployment_metadata,
                deployment_id,
                &service_name,
                service,
                if force {
                    RemovedHandlerBehavior::Warn
                } else {
                    RemovedHandlerBehavior::Fail
                },
                if force {
                    ServiceTypeMismatchBehavior::Warn
                } else {
                    ServiceTypeMismatchBehavior::Fail
                },
                ServiceLevelSettingsBehavior::Preserve,
            )?;

            services_to_add.insert(service_name, service_schema);
        }

        drop(existing_deployments);

        for service_to_remove in services_to_remove {
            self.schema_information.services.remove(&service_to_remove);
        }

        let services_metadata = services_to_add
            .into_iter()
            .map(|(name, schema)| {
                let metadata = schema.as_service_metadata(name.clone().into_inner());
                self.schema_information
                    .services
                    .insert(name.clone().into_inner(), schema);
                (name.into_inner(), metadata)
            })
            .collect();

        self.schema_information.deployments.insert(
            deployment_id,
            DeploymentSchemas {
                services: services_metadata,
                metadata: deployment_metadata,
            },
        );

        self.modified = true;

        Ok(deployment_id)
    }

    pub fn update_deployment(
        &mut self,
        deployment_id: DeploymentId,
        deployment_metadata: DeploymentMetadata,
        services: Vec<endpoint_manifest::Service>,
    ) -> Result<(), SchemaError> {
        let proposed_services: HashMap<_, _> = services
            .into_iter()
            .map(|c| ServiceName::try_from(c.name.to_string()).map(|name| (name, c)))
            .collect::<Result<HashMap<_, _>, _>>()?;

        // Look for an existing deployment with this ID
        let Some((_, existing_deployment)) = self
            .schema_information
            .find_existing_deployment_by_id(&deployment_id)
        else {
            return Err(SchemaError::NotFound(format!(
                "deployment with id '{deployment_id}'"
            )));
        };

        if existing_deployment.metadata.supported_protocol_versions
            != deployment_metadata.supported_protocol_versions
        {
            return Err(SchemaError::Deployment(
                DeploymentError::DifferentSupportedProtocolVersions(
                    existing_deployment
                        .metadata
                        .supported_protocol_versions
                        .clone(),
                    deployment_metadata.supported_protocol_versions.clone(),
                ),
            ));
        };

        let mut services_to_remove = Vec::default();

        for service in existing_deployment.services.values() {
            if !proposed_services.contains_key(&service.name) {
                services_to_remove.push(service.name.clone());
            }
        }

        if !services_to_remove.is_empty() {
            // we don't allow removing services as part of update deployment
            return Err(SchemaError::Deployment(DeploymentError::RemovedServices(
                services_to_remove,
            )));
        }

        let mut services_to_add = HashMap::with_capacity(proposed_services.len());

        // Compute service schemas
        for (service_name, service) in proposed_services {
            let service_schema = self.create_or_update_service_schemas(
                &deployment_metadata,
                deployment_id,
                &service_name,
                service,
                RemovedHandlerBehavior::Fail,
                ServiceTypeMismatchBehavior::Fail,
                ServiceLevelSettingsBehavior::Preserve,
            )?;

            services_to_add.insert(service_name, service_schema);
        }

        let services_metadata = services_to_add
            .into_iter()
            .map(|(name, schema)| {
                let metadata = schema.as_service_metadata(name.clone().into_inner());
                match self.schema_information.services.get(name.as_ref()) {
                    Some(ServiceSchemas {
                        location: ServiceLocation { latest_deployment, .. },
                        ..
                    }) if latest_deployment == &deployment_id => {
                        // This deployment is the latest for this service, so we should update the service schema
                        info!(
                            rpc.service = %name,
                            "Overwriting existing service schemas"
                        );
                        self.schema_information
                            .services
                            .insert(name.clone().into_inner(), schema);
                    },
                    Some(_) => {
                        debug!(
                            rpc.service = %name,
                            "Keeping existing service schema as this update operation affected a draining deployment"
                        );
                    },
                    None => {
                        // we have a new service, it deserves a schema as normal
                        self.schema_information
                            .services
                            .insert(name.clone().into_inner(), schema);
                    }
                }

                (name.into_inner(), metadata)
            })
            .collect();

        self.schema_information.deployments.insert(
            deployment_id,
            DeploymentSchemas {
                services: services_metadata,
                metadata: deployment_metadata,
            },
        );

        self.modified = true;

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn create_or_update_service_schemas(
        &self,
        deployment_metadata: &DeploymentMetadata,
        deployment_id: DeploymentId,
        service_name: &ServiceName,
        service: endpoint_manifest::Service,
        removed_handler_behavior: RemovedHandlerBehavior,
        service_type_mismatch_behavior: ServiceTypeMismatchBehavior,
        service_level_settings_behavior: ServiceLevelSettingsBehavior,
    ) -> Result<ServiceSchemas, SchemaError> {
        let service_type = ServiceType::from(service.ty);

        // For the time being when updating we overwrite existing data
        let service_schema = if let Some(existing_service) =
            self.schema_information.services.get(service_name.as_ref())
        {
            // Little problem here that can lead to some surprising stuff.
            //
            // When updating a service, and setting the service/handler options (e.g. the idempotency retention, private, etc)
            // in the manifest, I override whatever was stored before. Makes sense.
            //
            // But when not setting these values, we keep whatever was configured before (either from manifest, or from Admin REST API).
            // This makes little sense now that we have these settings in the manifest, but it preserves the old behavior.
            // At some point we should "break" this behavior, and on service updates simply apply defaults when nothing is configured in the manifest,
            // in order to favour people using annotations, rather than the clunky Admin REST API.
            let public = service.ingress_private.map(bool::not).unwrap_or(
                if service_level_settings_behavior.preserve() {
                    existing_service.location.public
                } else {
                    true
                },
            );
            let idempotency_retention = service.idempotency_retention_duration().or(
                if service_level_settings_behavior.preserve() {
                    existing_service.idempotency_retention
                } else {
                    // TODO(slinydeveloper) Remove this in Restate 1.5, no need for this defaulting anymore!
                    Some(DEFAULT_IDEMPOTENCY_RETENTION)
                },
            );
            let journal_retention = service.journal_retention_duration().or(
                if service_level_settings_behavior.preserve() {
                    existing_service.journal_retention
                } else {
                    None
                },
            );
            let workflow_completion_retention = if service_level_settings_behavior.preserve()
                // Retain previous value only if new service and old one are both workflows
                && service_type == ServiceType::Workflow
                && existing_service.ty == ServiceType::Workflow
            {
                existing_service.workflow_completion_retention
            } else if service_type == ServiceType::Workflow {
                Some(DEFAULT_WORKFLOW_COMPLETION_RETENTION)
            } else {
                None
            };
            let inactivity_timeout = service.inactivity_timeout_duration().or(
                if service_level_settings_behavior.preserve() {
                    existing_service.inactivity_timeout
                } else {
                    None
                },
            );
            let abort_timeout = service.abort_timeout_duration().or(
                if service_level_settings_behavior.preserve() {
                    existing_service.abort_timeout
                } else {
                    None
                },
            );

            let handlers = DiscoveredHandlerMetadata::compute_handlers(
                service
                    .handlers
                    .into_iter()
                    .map(|h| {
                        DiscoveredHandlerMetadata::from_schema(
                            service_name.as_ref(),
                            service_type,
                            public,
                            h,
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                public,
                idempotency_retention,
                workflow_completion_retention,
                journal_retention,
            );

            let removed_handlers: Vec<String> = existing_service
                .handlers
                .keys()
                .filter(|name| !handlers.contains_key(*name))
                .map(|name| name.to_string())
                .collect();

            if !removed_handlers.is_empty() {
                if matches!(removed_handler_behavior, RemovedHandlerBehavior::Fail) {
                    return Err(SchemaError::Service(ServiceError::RemovedHandlers(
                        service_name.clone(),
                        removed_handlers,
                    )));
                } else {
                    warn!(
                        restate.deployment.id = %deployment_id,
                        restate.deployment.address = %deployment_metadata.address_display(),
                        "Going to remove the following methods from service type {} due to a forced deployment update: {:?}.",
                        service.name.as_str(),
                        removed_handlers
                    );
                }
            }

            if existing_service.ty != service_type {
                if matches!(
                    service_type_mismatch_behavior,
                    ServiceTypeMismatchBehavior::Fail
                ) {
                    return Err(SchemaError::Service(ServiceError::DifferentType(
                        service_name.clone(),
                    )));
                } else {
                    warn!(
                        restate.deployment.id = %deployment_id,
                        restate.deployment.address = %deployment_metadata.address_display(),
                        "Going to overwrite service type {} due to a forced deployment update: {:?} != {:?}. This is a potentially dangerous operation, and might result in data loss.",
                        service_name,
                        existing_service.ty,
                        service_type
                    );
                }
            }

            info!(
                rpc.service = %service_name,
                "Overwriting existing service schemas"
            );
            let mut service_schemas = existing_service.clone();
            service_schemas.revision = existing_service.revision.wrapping_add(1);
            service_schemas.ty = service_type;
            service_schemas.handlers = handlers;
            service_schemas.location.latest_deployment = deployment_id;
            service_schemas.location.public = public;
            service_schemas.service_openapi_cache = Default::default();
            service_schemas.documentation = service.documentation;
            service_schemas.metadata = service.metadata;
            service_schemas.journal_retention = journal_retention;
            service_schemas.workflow_completion_retention = workflow_completion_retention;
            service_schemas.idempotency_retention = idempotency_retention;
            service_schemas.inactivity_timeout = inactivity_timeout;
            service_schemas.abort_timeout = abort_timeout;
            service_schemas.enable_lazy_state = service.enable_lazy_state;
            service_schemas
        } else {
            let public = service.ingress_private.map(bool::not).unwrap_or(true);
            let idempotency_retention = service
                .idempotency_retention_duration()
                // TODO(slinydeveloper) Remove this in Restate 1.5, no need for this defaulting anymore!
                .or(Some(DEFAULT_IDEMPOTENCY_RETENTION));
            let journal_retention = service.journal_retention_duration();
            let workflow_completion_retention = if service_type == ServiceType::Workflow {
                Some(DEFAULT_WORKFLOW_COMPLETION_RETENTION)
            } else {
                None
            };
            let inactivity_timeout = service.inactivity_timeout_duration();
            let abort_timeout = service.abort_timeout_duration();

            ServiceSchemas {
                revision: 1,
                handlers: DiscoveredHandlerMetadata::compute_handlers(
                    service
                        .handlers
                        .into_iter()
                        .map(|h| {
                            DiscoveredHandlerMetadata::from_schema(
                                service_name.as_ref(),
                                service_type,
                                public,
                                h,
                            )
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                    public,
                    idempotency_retention,
                    workflow_completion_retention,
                    journal_retention,
                ),
                ty: service_type,
                location: ServiceLocation {
                    latest_deployment: deployment_id,
                    public,
                },
                idempotency_retention,
                workflow_completion_retention,
                journal_retention,
                inactivity_timeout,
                abort_timeout,
                service_openapi_cache: Default::default(),
                documentation: service.documentation,
                metadata: service.metadata,
                enable_lazy_state: service.enable_lazy_state,
            }
        };
        Ok(service_schema)
    }

    pub fn remove_deployment(&mut self, deployment_id: DeploymentId) {
        if let Some(deployment) = self.schema_information.deployments.remove(&deployment_id) {
            for (_, service_metadata) in deployment.services {
                match self
                    .schema_information
                    .services
                    .entry(service_metadata.name)
                {
                    // we need to check for the right revision in the service has been overwritten
                    // by a different deployment.
                    Entry::Occupied(entry) if entry.get().revision == service_metadata.revision => {
                        entry.remove();
                    }
                    _ => {}
                }
            }
            self.modified = true;
        }
    }

    pub fn add_subscription<V: SubscriptionValidator>(
        &mut self,
        id: Option<SubscriptionId>,
        source: Uri,
        sink: Uri,
        metadata: Option<HashMap<String, String>>,
        validator: &V,
    ) -> Result<SubscriptionId, SchemaError> {
        // generate id if not provided
        let id = id.unwrap_or_default();

        if self.schema_information.subscriptions.contains_key(&id) {
            return Err(SchemaError::Override(format!(
                "subscription with id '{id}'"
            )));
        }

        // TODO This logic to parse source and sink should be moved elsewhere to abstract over the known source/sink providers
        //  Maybe together with the validator?

        // Parse source
        let source = match source.scheme_str() {
            Some("kafka") => {
                let cluster_name = source
                    .authority()
                    .ok_or_else(|| {
                        SchemaError::Subscription(SubscriptionError::InvalidKafkaSourceAuthority(
                            source.clone(),
                        ))
                    })?
                    .as_str();
                let topic_name = &source.path()[1..];
                Source::Kafka {
                    cluster: cluster_name.to_string(),
                    topic: topic_name.to_string(),
                }
            }
            _ => {
                return Err(SchemaError::Subscription(
                    SubscriptionError::InvalidSourceScheme(source),
                ));
            }
        };

        // Parse sink
        let sink = match sink.scheme_str() {
            Some("service") => {
                let service_name = sink
                    .authority()
                    .ok_or_else(|| {
                        SchemaError::Subscription(SubscriptionError::InvalidServiceSinkAuthority(
                            sink.clone(),
                        ))
                    })?
                    .as_str();
                let handler_name = &sink.path()[1..];

                // Retrieve service and handler in the schema registry
                let service_schemas = self
                    .schema_information
                    .services
                    .get(service_name)
                    .ok_or_else(|| {
                        SchemaError::Subscription(SubscriptionError::SinkServiceNotFound(
                            sink.clone(),
                        ))
                    })?;
                let handler_schemas =
                    service_schemas.handlers.get(handler_name).ok_or_else(|| {
                        SchemaError::Subscription(SubscriptionError::SinkServiceNotFound(
                            sink.clone(),
                        ))
                    })?;

                Sink::Invocation {
                    event_invocation_target_template: match handler_schemas.target_meta.target_ty {
                        InvocationTargetType::Service => EventInvocationTargetTemplate::Service {
                            name: service_name.to_owned(),
                            handler: handler_name.to_owned(),
                        },
                        InvocationTargetType::VirtualObject(handler_ty) => {
                            EventInvocationTargetTemplate::VirtualObject {
                                name: service_name.to_owned(),
                                handler: handler_name.to_owned(),
                                handler_ty,
                            }
                        }
                        InvocationTargetType::Workflow(handler_ty) => {
                            EventInvocationTargetTemplate::Workflow {
                                name: service_name.to_owned(),
                                handler: handler_name.to_owned(),
                                handler_ty,
                            }
                        }
                    },
                }
            }
            _ => {
                return Err(SchemaError::Subscription(
                    SubscriptionError::InvalidSinkScheme(sink),
                ));
            }
        };

        let subscription = validator
            .validate(Subscription::new(
                id,
                source,
                sink,
                metadata.unwrap_or_default(),
            ))
            .map_err(|e| SchemaError::Subscription(SubscriptionError::Validation(e.into())))?;

        self.schema_information
            .subscriptions
            .insert(id, subscription);
        self.modified = true;

        Ok(id)
    }

    pub fn remove_subscription(&mut self, subscription_id: SubscriptionId) {
        if self
            .schema_information
            .subscriptions
            .remove(&subscription_id)
            .is_some()
        {
            self.modified = true;
        }
    }

    pub fn modify_service(
        &mut self,
        name: String,
        changes: Vec<ModifyServiceChange>,
    ) -> Result<(), SchemaError> {
        if let Some(schemas) = self.schema_information.services.get_mut(&name) {
            for command in changes {
                match command {
                    ModifyServiceChange::Public(new_public_value) => {
                        schemas.location.public = new_public_value;
                        for h in schemas.handlers.values_mut() {
                            h.target_meta.public = new_public_value;
                        }
                        // Cleanup generated OpenAPI
                        schemas.service_openapi_cache = Default::default();
                    }
                    ModifyServiceChange::IdempotencyRetention(new_idempotency_retention) => {
                        schemas.idempotency_retention = Some(new_idempotency_retention);
                        for h in schemas.handlers.values_mut().filter(|w| {
                            // idempotency retention doesn't apply to workflow handlers
                            w.target_meta.target_ty
                                != InvocationTargetType::Workflow(WorkflowHandlerType::Workflow)
                        }) {
                            h.target_meta.completion_retention = new_idempotency_retention;
                        }
                    }
                    ModifyServiceChange::WorkflowCompletionRetention(
                        new_workflow_completion_retention,
                    ) => {
                        // This applies only to workflow services
                        if schemas.ty != ServiceType::Workflow {
                            return Err(SchemaError::Service(
                                ServiceError::CannotModifyRetentionTime(schemas.ty),
                            ));
                        }
                        schemas.workflow_completion_retention =
                            Some(new_workflow_completion_retention);
                        for h in schemas.handlers.values_mut().filter(|w| {
                            w.target_meta.target_ty
                                == InvocationTargetType::Workflow(WorkflowHandlerType::Workflow)
                        }) {
                            h.target_meta.completion_retention = new_workflow_completion_retention;
                        }
                    }
                    ModifyServiceChange::InactivityTimeout(inactivity_timeout) => {
                        schemas.inactivity_timeout = Some(inactivity_timeout);
                    }
                    ModifyServiceChange::AbortTimeout(abort_timeout) => {
                        schemas.abort_timeout = Some(abort_timeout);
                    }
                }
            }
        }

        self.modified = true;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DiscoveredHandlerMetadata {
    name: String,
    ty: InvocationTargetType,
    documentation: Option<String>,
    metadata: HashMap<String, String>,
    journal_retention: Option<Duration>,
    idempotency_retention: Option<Duration>,
    workflow_completion_retention: Option<Duration>,
    inactivity_timeout: Option<Duration>,
    abort_timeout: Option<Duration>,
    enable_lazy_state: Option<bool>,
    ingress_private: Option<bool>,
    input: InputRules,
    output: OutputRules,
}

impl DiscoveredHandlerMetadata {
    fn from_schema(
        service_name: &str,
        service_type: ServiceType,
        is_service_public: bool,
        handler: endpoint_manifest::Handler,
    ) -> Result<Self, ServiceError> {
        let ty = match (service_type, handler.ty) {
            (ServiceType::Service, None | Some(endpoint_manifest::HandlerType::Shared)) => {
                InvocationTargetType::Service
            }
            (
                ServiceType::VirtualObject,
                None | Some(endpoint_manifest::HandlerType::Exclusive),
            ) => InvocationTargetType::VirtualObject(VirtualObjectHandlerType::Exclusive),
            (ServiceType::VirtualObject, Some(endpoint_manifest::HandlerType::Shared)) => {
                InvocationTargetType::VirtualObject(VirtualObjectHandlerType::Shared)
            }
            (ServiceType::Workflow, None | Some(endpoint_manifest::HandlerType::Shared)) => {
                InvocationTargetType::Workflow(WorkflowHandlerType::Shared)
            }
            (ServiceType::Workflow, Some(endpoint_manifest::HandlerType::Workflow)) => {
                InvocationTargetType::Workflow(WorkflowHandlerType::Workflow)
            }
            _ => {
                return Err(ServiceError::BadServiceAndHandlerType(
                    service_type,
                    handler.ty,
                ));
            }
        };

        if handler.workflow_completion_retention.is_some()
            && ty != InvocationTargetType::Workflow(WorkflowHandlerType::Workflow)
        {
            return Err(ServiceError::UnexpectedWorkflowCompletionRetention(
                handler.name.as_str().to_owned(),
            ));
        }
        if handler.idempotency_retention.is_some()
            && ty == InvocationTargetType::Workflow(WorkflowHandlerType::Workflow)
        {
            return Err(ServiceError::UnexpectedIdempotencyRetention(
                handler.name.to_string(),
            ));
        }

        let journal_retention = handler.journal_retention_duration();
        let idempotency_retention = handler.idempotency_retention_duration();
        let workflow_completion_retention = handler.workflow_completion_retention_duration();
        let inactivity_timeout = handler.inactivity_timeout_duration();
        let abort_timeout = handler.abort_timeout_duration();

        if !is_service_public && handler.ingress_private == Some(false) {
            return Err(ServiceError::BadHandlerVisibility {
                service: service_name.to_owned(),
                handler: handler.name.to_string(),
            });
        }

        Ok(Self {
            name: handler.name.to_string(),
            ty,
            documentation: handler.documentation,
            metadata: handler.metadata,
            journal_retention,
            idempotency_retention,
            workflow_completion_retention,
            inactivity_timeout,
            abort_timeout,
            enable_lazy_state: handler.enable_lazy_state,
            ingress_private: handler.ingress_private,
            input: handler
                .input
                .map(|input_payload| {
                    DiscoveredHandlerMetadata::input_rules_from_schema(
                        service_name,
                        &handler.name,
                        input_payload,
                    )
                })
                .transpose()?
                .unwrap_or_default(),
            output: handler
                .output
                .map(|output_payload| {
                    DiscoveredHandlerMetadata::output_rules_from_schema(
                        service_name,
                        &handler.name,
                        output_payload,
                    )
                })
                .transpose()?
                .unwrap_or_default(),
        })
    }

    fn input_rules_from_schema(
        svc_name: &str,
        handler_name: &str,
        schema: endpoint_manifest::InputPayload,
    ) -> Result<InputRules, ServiceError> {
        let required = schema.required.unwrap_or(false);

        let mut input_validation_rules = vec![];

        // Add rule for empty if input not required
        if !required {
            input_validation_rules.push(InputValidationRule::NoBodyAndContentType);
        }

        // Add content-type validation rule
        if let Some(content_type) = schema
            .content_type
            .map(|s| {
                s.parse()
                    .map_err(|e| ServiceError::BadInputContentType(handler_name.to_owned(), e))
            })
            .transpose()?
        {
            if let Some(schema) = schema.json_schema {
                // Validate schema is valid
                if let Err(e) = jsonschema::options()
                    .with_retriever(UnsupportedExternalRefRetriever)
                    .build(&schema)
                {
                    return Err(ServiceError::BadJsonSchema {
                        service: svc_name.to_owned(),
                        handler: handler_name.to_owned(),
                        position: "input",
                        error: Box::new(e),
                    });
                }

                input_validation_rules.push(InputValidationRule::JsonValue {
                    content_type,
                    schema: Some(schema),
                });
            } else {
                input_validation_rules.push(InputValidationRule::ContentType { content_type });
            }
        }

        Ok(InputRules {
            input_validation_rules,
        })
    }

    fn output_rules_from_schema(
        svc_name: &str,
        handler_name: &str,
        schema: endpoint_manifest::OutputPayload,
    ) -> Result<OutputRules, ServiceError> {
        Ok(if let Some(ct) = schema.content_type {
            if let Some(schema) = &schema.json_schema {
                if let Err(e) = jsonschema::options()
                    .with_retriever(UnsupportedExternalRefRetriever)
                    .build(schema)
                {
                    return Err(ServiceError::BadJsonSchema {
                        service: svc_name.to_owned(),
                        handler: handler_name.to_owned(),
                        position: "output",
                        error: Box::new(e),
                    });
                }
            }

            OutputRules {
                content_type_rule: OutputContentTypeRule::Set {
                    content_type: HeaderValue::from_str(&ct)
                        .map_err(|e| ServiceError::BadOutputContentType(ct, e))?,
                    set_content_type_if_empty: schema.set_content_type_if_empty.unwrap_or(false),
                    has_json_schema: schema.json_schema.is_some(),
                },
                json_schema: schema.json_schema,
            }
        } else {
            OutputRules {
                content_type_rule: OutputContentTypeRule::None,
                json_schema: None,
            }
        })
    }

    fn compute_handlers(
        handlers: Vec<DiscoveredHandlerMetadata>,
        service_level_public: bool,
        service_level_idempotency_retention: Option<Duration>,
        service_level_workflow_completion_retention: Option<Duration>,
        service_level_journal_retention: Option<Duration>,
    ) -> HashMap<String, HandlerSchemas> {
        handlers
            .into_iter()
            .map(|handler| {
                // Defaulting and overriding
                let completion_retention = if handler.ty
                    == InvocationTargetType::Workflow(WorkflowHandlerType::Workflow)
                {
                    handler
                        .workflow_completion_retention
                        .or(service_level_workflow_completion_retention)
                        .unwrap_or(DEFAULT_WORKFLOW_COMPLETION_RETENTION)
                } else {
                    handler
                        .idempotency_retention
                        .or(service_level_idempotency_retention)
                        .unwrap_or(DEFAULT_IDEMPOTENCY_RETENTION)
                };
                // TODO enable this in Restate 1.4
                // let journal_retention =
                //     handler.journal_retention.or(service_level_journal_retention).unwrap_or(
                //      if target_ty == InvocationTargetType::Workflow(WorkflowHandlerType::Workflow) {
                //         DEFAULT_WORKFLOW_COMPLETION_RETENTION
                //      } else {
                //         Duration::ZERO
                //      }
                //     );
                let journal_retention = handler
                    .journal_retention
                    .or(service_level_journal_retention)
                    .unwrap_or(Duration::ZERO);

                let public = if let Some(ingress_private) = handler.ingress_private {
                    !ingress_private
                } else {
                    service_level_public
                };

                (
                    handler.name,
                    HandlerSchemas {
                        target_meta: InvocationTargetMetadata {
                            public,
                            completion_retention,
                            journal_retention,
                            target_ty: handler.ty,
                            input_rules: handler.input,
                            output_rules: handler.output,
                        },
                        idempotency_retention: handler.idempotency_retention,
                        workflow_completion_retention: handler.workflow_completion_retention,
                        journal_retention: handler.journal_retention,
                        inactivity_timeout: handler.inactivity_timeout,
                        abort_timeout: handler.abort_timeout,
                        enable_lazy_state: handler.enable_lazy_state,
                        documentation: handler.documentation,
                        metadata: handler.metadata,
                    },
                )
            })
            .collect()
    }
}

#[derive(Debug, thiserror::Error)]
#[error(
    "the schema contains an external reference {0}. This is not supported, all schemas uploaded to Restate should be normalized first, bundling the external references."
)]
struct UnsupportedExternalRefRetrieveError(String);

struct UnsupportedExternalRefRetriever;

impl jsonschema::Retrieve for UnsupportedExternalRefRetriever {
    fn retrieve(&self, uri: &jsonschema::Uri<&str>) -> Result<Value, Box<dyn Error + Send + Sync>> {
        Err(UnsupportedExternalRefRetrieveError(uri.to_string()).into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use http::HeaderName;
    use restate_test_util::{assert, assert_eq};
    use restate_types::schema::deployment::{Deployment, DeploymentResolver};
    use restate_types::schema::service::ServiceMetadataResolver;

    use restate_types::Versioned;
    use test_log::test;

    const GREETER_SERVICE_NAME: &str = "greeter.Greeter";
    const GREET_HANDLER_NAME: &str = "greet";
    const ANOTHER_GREETER_SERVICE_NAME: &str = "greeter.AnotherGreeter";

    fn greeter_service_greet_handler() -> endpoint_manifest::Handler {
        endpoint_manifest::Handler {
            abort_timeout: None,
            documentation: None,
            idempotency_retention: None,
            name: GREET_HANDLER_NAME.parse().unwrap(),
            ty: None,
            input: None,
            output: None,
            metadata: Default::default(),
            inactivity_timeout: None,
            journal_retention: None,
            workflow_completion_retention: None,
            enable_lazy_state: None,
            ingress_private: None,
        }
    }

    fn greeter_workflow_greet_handler() -> endpoint_manifest::Handler {
        endpoint_manifest::Handler {
            abort_timeout: None,
            documentation: None,
            idempotency_retention: None,
            name: "greet".parse().unwrap(),
            ty: Some(endpoint_manifest::HandlerType::Workflow),
            input: None,
            output: None,
            metadata: Default::default(),
            inactivity_timeout: None,
            journal_retention: None,
            workflow_completion_retention: None,
            enable_lazy_state: None,
            ingress_private: None,
        }
    }

    fn greeter_service() -> endpoint_manifest::Service {
        endpoint_manifest::Service {
            abort_timeout: None,
            documentation: None,
            ingress_private: None,
            ty: endpoint_manifest::ServiceType::Service,
            name: GREETER_SERVICE_NAME.parse().unwrap(),
            handlers: vec![greeter_service_greet_handler()],
            idempotency_retention: None,
            inactivity_timeout: None,
            journal_retention: None,
            metadata: Default::default(),
            enable_lazy_state: None,
        }
    }

    fn greeter_virtual_object() -> endpoint_manifest::Service {
        endpoint_manifest::Service {
            abort_timeout: None,
            documentation: None,
            ingress_private: None,
            ty: endpoint_manifest::ServiceType::VirtualObject,
            name: GREETER_SERVICE_NAME.parse().unwrap(),
            handlers: vec![endpoint_manifest::Handler {
                abort_timeout: None,
                documentation: None,
                idempotency_retention: None,
                name: "greet".parse().unwrap(),
                ty: None,
                input: None,
                output: None,
                metadata: Default::default(),
                inactivity_timeout: None,
                journal_retention: None,
                workflow_completion_retention: None,
                enable_lazy_state: None,
                ingress_private: None,
            }],
            idempotency_retention: None,
            inactivity_timeout: None,
            journal_retention: None,
            metadata: Default::default(),
            enable_lazy_state: None,
        }
    }

    fn greeter_workflow() -> endpoint_manifest::Service {
        endpoint_manifest::Service {
            abort_timeout: None,
            documentation: None,
            ingress_private: None,
            ty: endpoint_manifest::ServiceType::Workflow,
            name: GREETER_SERVICE_NAME.parse().unwrap(),
            handlers: vec![greeter_workflow_greet_handler()],
            idempotency_retention: None,
            inactivity_timeout: None,
            journal_retention: None,
            metadata: Default::default(),
            enable_lazy_state: None,
        }
    }

    fn another_greeter_service() -> endpoint_manifest::Service {
        endpoint_manifest::Service {
            abort_timeout: None,
            documentation: None,
            ingress_private: None,
            ty: endpoint_manifest::ServiceType::Service,
            name: ANOTHER_GREETER_SERVICE_NAME.parse().unwrap(),
            handlers: vec![endpoint_manifest::Handler {
                abort_timeout: None,
                documentation: None,
                idempotency_retention: None,
                name: "another_greeter".parse().unwrap(),
                ty: None,
                input: None,
                output: None,
                metadata: Default::default(),
                inactivity_timeout: None,
                journal_retention: None,
                workflow_completion_retention: None,
                enable_lazy_state: None,
                ingress_private: None,
            }],
            idempotency_retention: None,
            inactivity_timeout: None,
            journal_retention: None,
            metadata: Default::default(),
            enable_lazy_state: None,
        }
    }

    #[test]
    fn register_new_deployment() {
        let schema_information = Schema::default();
        let initial_version = schema_information.version();
        let mut updater = SchemaUpdater::new(schema_information);

        let mut deployment = Deployment::mock();
        deployment.id = updater
            .add_deployment(deployment.metadata.clone(), vec![greeter_service()], false)
            .unwrap();

        let schema = updater.into_inner();

        assert!(initial_version < schema.version());
        schema.assert_service_revision(GREETER_SERVICE_NAME, 1);
        schema.assert_service_deployment(GREETER_SERVICE_NAME, deployment.id);
        schema.assert_service_handler(GREETER_SERVICE_NAME, "greet");
    }

    #[test]
    fn register_new_deployment_add_unregistered_service() {
        let mut updater = SchemaUpdater::default();

        let mut deployment_1 = Deployment::mock_with_uri("http://localhost:9080");
        let mut deployment_2 = Deployment::mock_with_uri("http://localhost:9081");

        // Register first deployment
        deployment_1.id = updater
            .add_deployment(
                deployment_1.metadata.clone(),
                vec![greeter_service()],
                false,
            )
            .unwrap();

        let schemas = updater.into_inner();

        schemas.assert_service_deployment(GREETER_SERVICE_NAME, deployment_1.id);
        assert!(
            schemas
                .resolve_latest_service(ANOTHER_GREETER_SERVICE_NAME)
                .is_none()
        );

        updater = SchemaUpdater::new(schemas);
        deployment_2.id = updater
            .add_deployment(
                deployment_2.metadata.clone(),
                vec![greeter_service(), another_greeter_service()],
                false,
            )
            .unwrap();
        let schemas = updater.into_inner();

        schemas.assert_service_deployment(GREETER_SERVICE_NAME, deployment_2.id);
        schemas.assert_service_revision(GREETER_SERVICE_NAME, 2);
        schemas.assert_service_deployment(ANOTHER_GREETER_SERVICE_NAME, deployment_2.id);
        schemas.assert_service_revision(ANOTHER_GREETER_SERVICE_NAME, 1);
    }

    /// This test case ensures that https://github.com/restatedev/restate/issues/1205 works
    #[test]
    fn force_deploy_private_service() -> Result<(), SchemaError> {
        let mut updater = SchemaUpdater::default();
        let mut deployment = Deployment::mock();

        deployment.id =
            updater.add_deployment(deployment.metadata.clone(), vec![greeter_service()], false)?;

        let schemas = updater.into_inner();

        assert!(schemas.assert_service(GREETER_SERVICE_NAME).public);
        assert!(
            schemas
                .assert_service_handler(GREETER_SERVICE_NAME, "greet")
                .public
        );

        let version_before_modification = schemas.version();
        updater = SchemaUpdater::new(schemas);
        updater.modify_service(
            GREETER_SERVICE_NAME.to_owned(),
            vec![ModifyServiceChange::Public(false)],
        )?;
        let schemas = updater.into_inner();

        assert!(version_before_modification < schemas.version());
        assert!(!schemas.assert_service(GREETER_SERVICE_NAME).public);
        assert!(
            !schemas
                .assert_service_handler(GREETER_SERVICE_NAME, "greet")
                .public
        );

        updater = SchemaUpdater::new(schemas);
        deployment.id =
            updater.add_deployment(deployment.metadata.clone(), vec![greeter_service()], true)?;

        let schemas = updater.into_inner();
        assert!(!schemas.assert_service(GREETER_SERVICE_NAME).public);
        assert!(
            !schemas
                .assert_service_handler(GREETER_SERVICE_NAME, "greet")
                .public
        );

        Ok(())
    }

    mod change_service_type {
        use super::*;

        use restate_test_util::{assert, assert_eq};
        use test_log::test;

        #[test]
        fn fails_without_force() {
            let mut updater = SchemaUpdater::default();

            let mut deployment_1 = Deployment::mock_with_uri("http://localhost:9080");
            let deployment_2 = Deployment::mock_with_uri("http://localhost:9081");

            deployment_1.id = updater
                .add_deployment(
                    deployment_1.metadata.clone(),
                    vec![greeter_service()],
                    false,
                )
                .unwrap();
            let schemas = updater.into_inner();

            schemas.assert_service_deployment(GREETER_SERVICE_NAME, deployment_1.id);

            let compute_result = SchemaUpdater::new(schemas).add_deployment(
                deployment_2.metadata,
                vec![greeter_virtual_object()],
                false,
            );

            assert!(let &SchemaError::Service(
                ServiceError::DifferentType(_)
            ) = compute_result.unwrap_err());
        }

        #[test]
        fn works_with_force() {
            let mut updater = SchemaUpdater::default();

            let mut deployment_1 = Deployment::mock_with_uri("http://localhost:9080");
            let mut deployment_2 = Deployment::mock_with_uri("http://localhost:9081");

            deployment_1.id = updater
                .add_deployment(
                    deployment_1.metadata.clone(),
                    vec![greeter_service()],
                    false,
                )
                .unwrap();
            let schemas = updater.into_inner();
            schemas.assert_service_deployment(GREETER_SERVICE_NAME, deployment_1.id);

            updater = SchemaUpdater::new(schemas);
            deployment_2.id = updater
                .add_deployment(deployment_2.metadata, vec![greeter_virtual_object()], true)
                .unwrap();
            let schemas = updater.into_inner();
            schemas.assert_service_deployment(GREETER_SERVICE_NAME, deployment_2.id);

            assert_eq!(
                schemas.assert_service(GREETER_SERVICE_NAME).ty,
                ServiceType::VirtualObject
            );
        }

        #[test]
        fn works_with_force_and_correctly_set_workflow_retention() {
            let mut updater = SchemaUpdater::default();

            let mut deployment_1 = Deployment::mock_with_uri("http://localhost:9080");
            let mut deployment_2 = Deployment::mock_with_uri("http://localhost:9081");

            deployment_1.id = updater
                .add_deployment(
                    deployment_1.metadata.clone(),
                    vec![greeter_service()],
                    false,
                )
                .unwrap();
            let schemas = updater.into_inner();
            schemas.assert_service_deployment(GREETER_SERVICE_NAME, deployment_1.id);

            updater = SchemaUpdater::new(schemas);
            deployment_2.id = updater
                .add_deployment(deployment_2.metadata, vec![greeter_workflow()], true)
                .unwrap();
            let schemas = updater.into_inner();
            schemas.assert_service_deployment(GREETER_SERVICE_NAME, deployment_2.id);

            let new_svc = schemas.assert_service(GREETER_SERVICE_NAME);
            assert_eq!(new_svc.ty, ServiceType::Workflow);
            assert_eq!(
                new_svc.workflow_completion_retention,
                Some(DEFAULT_WORKFLOW_COMPLETION_RETENTION)
            );
        }
    }

    #[test]
    fn override_existing_deployment_removing_a_service() {
        let mut updater = SchemaUpdater::default();

        let mut deployment = Deployment::mock();
        deployment.id = updater
            .add_deployment(
                deployment.metadata.clone(),
                vec![greeter_service(), another_greeter_service()],
                false,
            )
            .unwrap();

        let schemas = updater.into_inner();
        schemas.assert_service_deployment(GREETER_SERVICE_NAME, deployment.id);
        schemas.assert_service_deployment(ANOTHER_GREETER_SERVICE_NAME, deployment.id);

        updater = SchemaUpdater::new(schemas);
        assert_eq!(
            updater
                .add_deployment(deployment.metadata.clone(), vec![greeter_service()], true,)
                .unwrap(),
            deployment.id
        );

        let schemas = updater.into_inner();

        schemas.assert_service_deployment(GREETER_SERVICE_NAME, deployment.id);
        assert!(
            schemas
                .resolve_latest_service(ANOTHER_GREETER_SERVICE_NAME)
                .is_none()
        );
    }

    #[test]
    fn cannot_override_existing_deployment_endpoint_conflict() {
        let mut updater = SchemaUpdater::default();

        let mut deployment = Deployment::mock();
        deployment.id = updater
            .add_deployment(deployment.metadata.clone(), vec![greeter_service()], false)
            .unwrap();

        assert!(let SchemaError::Override(_) = updater.add_deployment(
            deployment.metadata,
            vec![greeter_service()],
            false).unwrap_err()
        );
    }

    #[test]
    fn register_two_deployments_then_remove_first() {
        let mut updater = SchemaUpdater::default();

        let mut deployment_1 = Deployment::mock_with_uri("http://localhost:9080");
        let mut deployment_2 = Deployment::mock_with_uri("http://localhost:9081");

        deployment_1.id = updater
            .add_deployment(
                deployment_1.metadata.clone(),
                vec![greeter_service(), another_greeter_service()],
                false,
            )
            .unwrap();
        deployment_2.id = updater
            .add_deployment(
                deployment_2.metadata.clone(),
                vec![greeter_service()],
                false,
            )
            .unwrap();
        let schemas = updater.into_inner();

        schemas.assert_service_deployment(GREETER_SERVICE_NAME, deployment_2.id);
        schemas.assert_service_revision(GREETER_SERVICE_NAME, 2);
        schemas.assert_service_deployment(ANOTHER_GREETER_SERVICE_NAME, deployment_1.id);
        schemas.assert_service_revision(ANOTHER_GREETER_SERVICE_NAME, 1);

        let version_before_removal = schemas.version();
        updater = SchemaUpdater::new(schemas);
        updater.remove_deployment(deployment_1.id);
        let schemas = updater.into_inner();

        schemas.assert_service_deployment(GREETER_SERVICE_NAME, deployment_2.id);
        schemas.assert_service_revision(GREETER_SERVICE_NAME, 2);
        assert!(version_before_removal < schemas.version());
        assert!(
            schemas
                .resolve_latest_service(ANOTHER_GREETER_SERVICE_NAME)
                .is_none()
        );
        assert!(schemas.get_deployment(&deployment_1.id).is_none());
    }

    mod remove_handler {
        use super::*;

        use restate_test_util::{check, let_assert};
        use test_log::test;

        fn greeter_v1_service() -> endpoint_manifest::Service {
            endpoint_manifest::Service {
                abort_timeout: None,
                documentation: None,
                ingress_private: None,
                ty: endpoint_manifest::ServiceType::Service,
                name: GREETER_SERVICE_NAME.parse().unwrap(),
                handlers: vec![
                    endpoint_manifest::Handler {
                        abort_timeout: None,
                        documentation: None,
                        idempotency_retention: None,
                        name: "greet".parse().unwrap(),
                        ty: None,
                        input: None,
                        output: None,
                        metadata: Default::default(),
                        inactivity_timeout: None,
                        journal_retention: None,
                        workflow_completion_retention: None,
                        enable_lazy_state: None,
                        ingress_private: None,
                    },
                    endpoint_manifest::Handler {
                        abort_timeout: None,
                        documentation: None,
                        idempotency_retention: None,
                        name: "doSomething".parse().unwrap(),
                        ty: None,
                        input: None,
                        output: None,
                        metadata: Default::default(),
                        inactivity_timeout: None,
                        journal_retention: None,
                        workflow_completion_retention: None,
                        enable_lazy_state: None,
                        ingress_private: None,
                    },
                ],
                idempotency_retention: None,
                inactivity_timeout: None,
                journal_retention: None,
                metadata: Default::default(),
                enable_lazy_state: None,
            }
        }

        fn greeter_v2_service() -> endpoint_manifest::Service {
            endpoint_manifest::Service {
                abort_timeout: None,
                documentation: None,
                ingress_private: None,
                ty: endpoint_manifest::ServiceType::Service,
                name: GREETER_SERVICE_NAME.parse().unwrap(),
                handlers: vec![endpoint_manifest::Handler {
                    abort_timeout: None,
                    documentation: None,
                    idempotency_retention: None,
                    name: "greet".parse().unwrap(),
                    ty: None,
                    input: None,
                    output: None,
                    metadata: Default::default(),
                    inactivity_timeout: None,
                    journal_retention: None,
                    workflow_completion_retention: None,
                    enable_lazy_state: None,
                    ingress_private: None,
                }],
                idempotency_retention: None,
                inactivity_timeout: None,
                journal_retention: None,
                metadata: Default::default(),
                enable_lazy_state: None,
            }
        }

        #[test]
        fn reject_removing_existing_methods() {
            let mut updater = SchemaUpdater::default();

            let mut deployment_1 = Deployment::mock_with_uri("http://localhost:9080");
            let deployment_2 = Deployment::mock_with_uri("http://localhost:9081");

            deployment_1.id = updater
                .add_deployment(deployment_1.metadata, vec![greeter_v1_service()], false)
                .unwrap();
            let schemas = updater.into_inner();
            schemas.assert_service_revision(GREETER_SERVICE_NAME, 1);

            updater = SchemaUpdater::new(schemas);
            let rejection = updater
                .add_deployment(deployment_2.metadata, vec![greeter_v2_service()], false)
                .unwrap_err();

            let schemas = updater.into_inner();
            schemas.assert_service_revision(GREETER_SERVICE_NAME, 1); // unchanged

            let_assert!(
                SchemaError::Service(ServiceError::RemovedHandlers(service, missing_methods)) =
                    rejection
            );
            check!(service.as_ref() == GREETER_SERVICE_NAME);
            check!(missing_methods == &["doSomething"]);
        }
    }

    #[test]
    fn update_latest_deployment() {
        let mut updater = SchemaUpdater::default();

        let mut deployment_1 = Deployment::mock_with_uri("http://localhost:9080");

        deployment_1.id = updater
            .add_deployment(
                deployment_1.metadata.clone(),
                vec![greeter_virtual_object()],
                false,
            )
            .unwrap();

        assert!(let &SchemaError::NotFound(_) = updater.update_deployment(
            DeploymentId::new(),
            deployment_1.metadata.clone(),
            vec![],
        ).unwrap_err());

        assert!(let &SchemaError::Deployment(
            DeploymentError::RemovedServices(_)
        ) = updater.update_deployment(
            deployment_1.id,
            deployment_1.metadata.clone(),
            vec![],
        ).unwrap_err());

        {
            let mut greeter_virtual_object = greeter_virtual_object();
            greeter_virtual_object.ty = endpoint_manifest::ServiceType::Service;

            assert!(let &SchemaError::Service(
                ServiceError::DifferentType(_)
            ) = updater.update_deployment(
                deployment_1.id,
                deployment_1.metadata.clone(),
                vec![greeter_virtual_object],
            ).unwrap_err());
        }

        assert!(let &SchemaError::Service(
            ServiceError::RemovedHandlers(_, _)
        ) = updater.update_deployment(
            deployment_1.id,
            deployment_1.metadata.clone(),
            vec![endpoint_manifest::Service {
                handlers: Default::default(),
                ..greeter_virtual_object()
            }],
        ).unwrap_err());

        deployment_1
            .metadata
            .delivery_options
            .additional_headers
            .insert(
                HeaderName::from_static("foo"),
                HeaderValue::from_static("bar"),
            );

        updater
            .update_deployment(
                deployment_1.id,
                deployment_1.metadata.clone(),
                vec![greeter_virtual_object()],
            )
            .unwrap();

        let schemas = updater.into_inner();

        schemas.assert_service_deployment(GREETER_SERVICE_NAME, deployment_1.id);
        schemas.assert_service_revision(GREETER_SERVICE_NAME, 2);

        let (_, updated_deployment) = schemas
            .find_existing_deployment_by_id(&deployment_1.id)
            .unwrap();

        assert!(
            updated_deployment
                .metadata
                .delivery_options
                .additional_headers
                .contains_key(&HeaderName::from_static("foo"))
        );
    }

    #[test]
    fn update_draining_deployment() {
        let mut updater = SchemaUpdater::default();

        let mut deployment_1 = Deployment::mock_with_uri("http://localhost:9080");
        let mut deployment_2 = Deployment::mock_with_uri("http://localhost:9081");

        deployment_1.id = updater
            .add_deployment(
                deployment_1.metadata.clone(),
                vec![greeter_service()],
                false,
            )
            .unwrap();

        deployment_2.id = updater
            .add_deployment(
                deployment_2.metadata.clone(),
                vec![greeter_service(), another_greeter_service()],
                false,
            )
            .unwrap();

        deployment_1
            .metadata
            .delivery_options
            .additional_headers
            .insert(
                HeaderName::from_static("foo"),
                HeaderValue::from_static("bar"),
            );

        updater
            .update_deployment(
                deployment_1.id,
                deployment_1.metadata.clone(),
                vec![greeter_service()],
            )
            .unwrap();

        let schemas = updater.into_inner();

        schemas.assert_service_deployment(GREETER_SERVICE_NAME, deployment_2.id);
        schemas.assert_service_revision(GREETER_SERVICE_NAME, 2);

        let (_, updated_deployment) = schemas
            .find_existing_deployment_by_id(&deployment_1.id)
            .unwrap();

        assert!(
            updated_deployment
                .metadata
                .delivery_options
                .additional_headers
                .contains_key(&HeaderName::from_static("foo"))
        );
    }

    #[test]
    fn update_deployment_same_uri() {
        let mut updater = SchemaUpdater::default();

        let mut deployment_1 = Deployment::mock_with_uri("http://localhost:9080");
        let mut deployment_2 = Deployment::mock_with_uri("http://localhost:9081");

        deployment_1.id = updater
            .add_deployment(
                deployment_1.metadata.clone(),
                vec![greeter_service()],
                false,
            )
            .unwrap();

        // patching new invocations
        deployment_2.id = updater
            .add_deployment(
                deployment_2.metadata.clone(),
                vec![greeter_service()],
                false,
            )
            .unwrap();

        // oh, i have some old failing invocations, wish those were on the patched version too

        deployment_1.metadata.ty = deployment_2.metadata.ty.clone();
        updater
            .update_deployment(
                deployment_1.id,
                deployment_1.metadata.clone(),
                vec![greeter_service()],
            )
            .unwrap();

        // there are now two deployment IDs pointing to :9081, so we shouldn't be able to force either of them
        assert!(let &SchemaError::Deployment(
            DeploymentError::MultipleExistingDeployments(_)
        ) = updater.add_deployment(
            deployment_1.metadata.clone(),
            vec![greeter_service(), greeter_virtual_object()],
            true,
        ).unwrap_err());

        assert!(let &SchemaError::Deployment(
            DeploymentError::MultipleExistingDeployments(_)
        ) = updater.add_deployment(
            deployment_2.metadata.clone(),
            vec![greeter_service(), greeter_virtual_object()],
            true,
        ).unwrap_err());

        updater
            .schema_information
            .assert_service_deployment(GREETER_SERVICE_NAME, deployment_2.id);
        updater
            .schema_information
            .assert_service_revision(GREETER_SERVICE_NAME, 2);

        let (_, updated_deployment_1) = updater
            .schema_information
            .find_existing_deployment_by_id(&deployment_1.id)
            .unwrap();

        let (_, updated_deployment_2) = updater
            .schema_information
            .find_existing_deployment_by_id(&deployment_2.id)
            .unwrap();

        assert_eq!(
            updated_deployment_1.metadata.ty,
            updated_deployment_2.metadata.ty
        );

        // the failing invocations have drained so I can safely delete the original deployment
        updater.remove_deployment(deployment_1.id);

        assert_eq!(
            deployment_2.id,
            updater
                .add_deployment(
                    deployment_2.metadata.clone(),
                    vec![greeter_service(), greeter_virtual_object()],
                    true,
                )
                .unwrap()
        );
    }

    #[test]
    fn update_latest_deployment_add_handler() {
        let mut updater = SchemaUpdater::default();

        let mut deployment_1 = Deployment::mock_with_uri("http://localhost:9081");

        deployment_1.id = updater
            .add_deployment(
                deployment_1.metadata.clone(),
                vec![greeter_service()],
                false,
            )
            .unwrap();

        let mut updated_greeter_service = greeter_service();
        updated_greeter_service
            .handlers
            .push(endpoint_manifest::Handler {
                abort_timeout: None,
                documentation: None,
                idempotency_retention: None,
                name: "greetAgain".parse().unwrap(),
                ty: None,
                input: None,
                output: None,
                metadata: Default::default(),
                inactivity_timeout: None,
                journal_retention: None,
                workflow_completion_retention: None,
                enable_lazy_state: None,
                ingress_private: None,
            });

        updater
            .update_deployment(
                deployment_1.id,
                deployment_1.metadata.clone(),
                vec![updated_greeter_service],
            )
            .unwrap();

        updater
            .schema_information
            .assert_service_deployment(GREETER_SERVICE_NAME, deployment_1.id);
        updater
            .schema_information
            .assert_service_revision(GREETER_SERVICE_NAME, 2);

        let (_, updated_deployment_1) = updater
            .schema_information
            .find_existing_deployment_by_id(&deployment_1.id)
            .unwrap();

        assert_eq!(
            updated_deployment_1
                .services
                .get(GREETER_SERVICE_NAME)
                .unwrap()
                .handlers
                .len(),
            2
        );
    }

    #[test]
    fn update_draining_deployment_add_handler() {
        let mut updater = SchemaUpdater::default();

        let mut deployment_1 = Deployment::mock_with_uri("http://localhost:9080");
        let mut deployment_2 = Deployment::mock_with_uri("http://localhost:9081");

        deployment_1.id = updater
            .add_deployment(
                deployment_1.metadata.clone(),
                vec![greeter_service()],
                false,
            )
            .unwrap();

        deployment_2.id = updater
            .add_deployment(
                deployment_2.metadata.clone(),
                vec![greeter_service()],
                false,
            )
            .unwrap();

        let mut updated_greeter_service = greeter_service();
        updated_greeter_service
            .handlers
            .push(endpoint_manifest::Handler {
                abort_timeout: None,
                documentation: None,
                idempotency_retention: None,
                name: "greetAgain".parse().unwrap(),
                ty: None,
                input: None,
                output: None,
                metadata: Default::default(),
                inactivity_timeout: None,
                journal_retention: None,
                workflow_completion_retention: None,
                enable_lazy_state: None,
                ingress_private: None,
            });

        updater
            .update_deployment(
                deployment_1.id,
                deployment_1.metadata.clone(),
                vec![updated_greeter_service],
            )
            .unwrap();

        updater
            .schema_information
            .assert_service_deployment(GREETER_SERVICE_NAME, deployment_2.id);
        updater
            .schema_information
            .assert_service_revision(GREETER_SERVICE_NAME, 2);

        let (_, updated_deployment_1) = updater
            .schema_information
            .find_existing_deployment_by_id(&deployment_1.id)
            .unwrap();

        let (_, updated_deployment_2) = updater
            .schema_information
            .find_existing_deployment_by_id(&deployment_2.id)
            .unwrap();

        assert_eq!(
            updated_deployment_1
                .services
                .get(GREETER_SERVICE_NAME)
                .unwrap()
                .handlers
                .len(),
            2
        );

        assert_eq!(
            updated_deployment_2
                .services
                .get(GREETER_SERVICE_NAME)
                .unwrap()
                .handlers
                .len(),
            1
        )
    }

    #[test]
    fn update_latest_deployment_add_service() {
        let mut updater = SchemaUpdater::default();

        let mut deployment_1 = Deployment::mock_with_uri("http://localhost:9080");

        deployment_1.id = updater
            .add_deployment(
                deployment_1.metadata.clone(),
                vec![greeter_service()],
                false,
            )
            .unwrap();

        updater
            .update_deployment(
                deployment_1.id,
                deployment_1.metadata.clone(),
                vec![greeter_service(), another_greeter_service()],
            )
            .unwrap();

        updater
            .schema_information
            .assert_service_deployment(GREETER_SERVICE_NAME, deployment_1.id);
        updater
            .schema_information
            .assert_service_deployment(ANOTHER_GREETER_SERVICE_NAME, deployment_1.id);
        updater
            .schema_information
            .assert_service_revision(GREETER_SERVICE_NAME, 2);
        updater
            .schema_information
            .assert_service_revision(ANOTHER_GREETER_SERVICE_NAME, 1);
    }

    #[test]
    fn update_draining_deployment_add_service() {
        let mut updater = SchemaUpdater::default();

        let mut deployment_1 = Deployment::mock_with_uri("http://localhost:9080");
        let mut deployment_2 = Deployment::mock_with_uri("http://localhost:9081");

        deployment_1.id = updater
            .add_deployment(
                deployment_1.metadata.clone(),
                vec![greeter_service()],
                false,
            )
            .unwrap();

        deployment_2.id = updater
            .add_deployment(
                deployment_2.metadata.clone(),
                vec![greeter_service()],
                false,
            )
            .unwrap();

        updater
            .update_deployment(
                deployment_1.id,
                deployment_1.metadata.clone(),
                vec![greeter_service(), another_greeter_service()],
            )
            .unwrap();

        updater
            .schema_information
            .assert_service_deployment(GREETER_SERVICE_NAME, deployment_2.id);
        updater
            .schema_information
            .assert_service_deployment(ANOTHER_GREETER_SERVICE_NAME, deployment_1.id);
        updater
            .schema_information
            .assert_service_revision(GREETER_SERVICE_NAME, 2);
        updater
            .schema_information
            .assert_service_revision(ANOTHER_GREETER_SERVICE_NAME, 1);
    }

    #[test]
    fn update_deployment_with_private_service() -> Result<(), SchemaError> {
        let mut updater = SchemaUpdater::default();
        let mut deployment = Deployment::mock();

        deployment.id =
            updater.add_deployment(deployment.metadata.clone(), vec![greeter_service()], false)?;

        let schemas = updater.into_inner();

        assert!(schemas.assert_service(GREETER_SERVICE_NAME).public);
        assert!(
            schemas
                .assert_service_handler(GREETER_SERVICE_NAME, "greet")
                .public
        );

        let version_before_modification = schemas.version();
        updater = SchemaUpdater::new(schemas);
        updater.modify_service(
            GREETER_SERVICE_NAME.to_owned(),
            vec![ModifyServiceChange::Public(false)],
        )?;
        let schemas = updater.into_inner();
        let version_after_modification = schemas.version();

        assert!(version_before_modification < version_after_modification);
        assert!(!schemas.assert_service(GREETER_SERVICE_NAME).public);
        assert!(
            !schemas
                .assert_service_handler(GREETER_SERVICE_NAME, "greet")
                .public
        );

        updater = SchemaUpdater::new(schemas);
        updater.update_deployment(
            deployment.id,
            deployment.metadata.clone(),
            vec![greeter_service()],
        )?;

        let schemas = updater.into_inner();
        assert!(version_before_modification < schemas.version());
        assert!(!schemas.assert_service(GREETER_SERVICE_NAME).public);
        assert!(
            !schemas
                .assert_service_handler(GREETER_SERVICE_NAME, "greet")
                .public
        );

        Ok(())
    }

    mod endpoint_manifest_options_propagation {
        use super::*;

        use googletest::prelude::*;
        use restate_types::config::Configuration;
        use restate_types::invocation::InvocationRetention;
        use restate_types::schema::invocation_target::InvocationTargetResolver;
        use restate_types::schema::service::{InvocationAttemptOptions, ServiceMetadata};
        use std::time::Duration;
        use test_log::test;

        fn init_discover_and_resolve_target(
            svc: endpoint_manifest::Service,
            service_name: &str,
            handler_name: &str,
        ) -> InvocationTargetMetadata {
            let schema_information = Schema::default();
            let mut updater = SchemaUpdater::new(schema_information);

            let mut deployment = Deployment::mock();
            deployment.id = updater
                .add_deployment(deployment.metadata.clone(), vec![svc], false)
                .unwrap();

            let schema = updater.into_inner();

            schema.assert_service_revision(service_name, 1);
            schema.assert_service_deployment(service_name, deployment.id);

            schema.assert_service_handler(service_name, handler_name)
        }

        #[test]
        fn private_service() {
            let target = init_discover_and_resolve_target(
                endpoint_manifest::Service {
                    ingress_private: Some(true),
                    ..greeter_service()
                },
                GREETER_SERVICE_NAME,
                GREET_HANDLER_NAME,
            );
            assert_that!(target.public, eq(false));
        }

        #[test]
        fn public_service_with_private_handler() {
            let schema_information = Schema::default();
            let mut updater = SchemaUpdater::new(schema_information);
            let mut deployment = Deployment::mock();
            deployment.id = updater
                .add_deployment(
                    deployment.metadata.clone(),
                    vec![endpoint_manifest::Service {
                        // Mock two handlers, one explicitly private, the other just default settings
                        handlers: vec![
                            endpoint_manifest::Handler {
                                ingress_private: Some(true),
                                name: "my_private_handler".parse().unwrap(),
                                ..greeter_service_greet_handler()
                            },
                            greeter_service_greet_handler(),
                        ],
                        ..greeter_service()
                    }],
                    false,
                )
                .unwrap();

            let schema = updater.into_inner();

            // The explicitly private handler is private
            let private_handler_target =
                schema.assert_service_handler(GREETER_SERVICE_NAME, "my_private_handler");
            assert_that!(private_handler_target.public, eq(false));

            // The other handler is by default public
            let public_handler_target =
                schema.assert_service_handler(GREETER_SERVICE_NAME, GREET_HANDLER_NAME);
            assert_that!(public_handler_target.public, eq(true));
        }

        #[test]
        fn public_handler_in_private_service() {
            let schema_information = Schema::default();
            let mut updater = SchemaUpdater::new(schema_information);
            let deployment = Deployment::mock();

            assert_that!(
                updater.add_deployment(
                    deployment.metadata.clone(),
                    vec![endpoint_manifest::Service {
                        ingress_private: Some(true),
                        handlers: vec![endpoint_manifest::Handler {
                            ingress_private: Some(false),
                            name: "my_private_handler".parse().unwrap(),
                            ..greeter_service_greet_handler()
                        }],
                        ..greeter_service()
                    }],
                    false
                ),
                err(pat!(SchemaError::Service(pat!(
                    ServiceError::BadHandlerVisibility { .. }
                ))))
            );
        }

        #[test]
        fn workflow_retention() {
            let schema_information = Schema::default();
            let mut updater = SchemaUpdater::new(schema_information);

            let mut deployment = Deployment::mock();
            deployment.id = updater
                .add_deployment(
                    deployment.metadata.clone(),
                    vec![endpoint_manifest::Service {
                        handlers: vec![endpoint_manifest::Handler {
                            workflow_completion_retention: Some(30 * 1000),
                            ..greeter_workflow_greet_handler()
                        }],
                        ..greeter_workflow()
                    }],
                    false,
                )
                .unwrap();
            let schema = updater.into_inner();

            assert_that!(
                schema.assert_service(GREETER_SERVICE_NAME),
                pat!(ServiceMetadata {
                    revision: eq(1),
                    deployment_id: eq(deployment.id),
                    workflow_completion_retention: eq(Some(Duration::from_secs(30)))
                })
            );
            assert_that!(
                schema
                    .resolve_latest_invocation_target(GREETER_SERVICE_NAME, GREET_HANDLER_NAME)
                    .unwrap()
                    .compute_retention(false),
                eq(InvocationRetention {
                    completion_retention: Duration::from_secs(30),
                    journal_retention: Duration::ZERO,
                })
            );
        }

        #[test]
        fn service_level_journal_retention() {
            let target = init_discover_and_resolve_target(
                endpoint_manifest::Service {
                    journal_retention: Some(60 * 1000),
                    ..greeter_service()
                },
                GREETER_SERVICE_NAME,
                GREET_HANDLER_NAME,
            );
            assert_that!(
                target.compute_retention(false),
                eq(InvocationRetention {
                    completion_retention: Duration::from_secs(60),
                    journal_retention: Duration::from_secs(60),
                })
            )
        }

        #[test]
        fn handler_level_journal_retention() {
            let target = init_discover_and_resolve_target(
                endpoint_manifest::Service {
                    handlers: vec![endpoint_manifest::Handler {
                        journal_retention: Some(30 * 1000),
                        ..greeter_service_greet_handler()
                    }],
                    ..greeter_service()
                },
                GREETER_SERVICE_NAME,
                GREET_HANDLER_NAME,
            );
            assert_that!(
                target.compute_retention(false),
                eq(InvocationRetention {
                    completion_retention: Duration::from_secs(30),
                    journal_retention: Duration::from_secs(30),
                })
            )
        }

        #[test]
        fn handler_level_journal_retention_overrides_the_service_level_journal_retention() {
            let target = init_discover_and_resolve_target(
                endpoint_manifest::Service {
                    journal_retention: Some(60 * 1000),
                    handlers: vec![endpoint_manifest::Handler {
                        journal_retention: Some(30 * 1000),
                        ..greeter_service_greet_handler()
                    }],
                    ..greeter_service()
                },
                GREETER_SERVICE_NAME,
                GREET_HANDLER_NAME,
            );
            assert_that!(
                target.compute_retention(false),
                eq(InvocationRetention {
                    completion_retention: Duration::from_secs(30),
                    journal_retention: Duration::from_secs(30),
                })
            )
        }

        #[test]
        fn service_level_journal_retention_with_idempotency_key() {
            let target = init_discover_and_resolve_target(
                endpoint_manifest::Service {
                    idempotency_retention: Some(120 * 1000),
                    journal_retention: Some(60 * 1000),
                    ..greeter_service()
                },
                GREETER_SERVICE_NAME,
                GREET_HANDLER_NAME,
            );
            assert_that!(
                target.compute_retention(true),
                eq(InvocationRetention {
                    completion_retention: Duration::from_secs(120),
                    journal_retention: Duration::from_secs(60),
                })
            )
        }

        #[test]
        fn handler_level_journal_retention_with_idempotency_key() {
            let target = init_discover_and_resolve_target(
                endpoint_manifest::Service {
                    idempotency_retention: Some(120 * 1000),
                    journal_retention: Some(60 * 1000),
                    handlers: vec![endpoint_manifest::Handler {
                        journal_retention: Some(30 * 1000),
                        ..greeter_service_greet_handler()
                    }],
                    ..greeter_service()
                },
                GREETER_SERVICE_NAME,
                GREET_HANDLER_NAME,
            );
            assert_that!(
                target.compute_retention(true),
                eq(InvocationRetention {
                    completion_retention: Duration::from_secs(120),
                    journal_retention: Duration::from_secs(30),
                })
            )
        }

        #[test]
        fn handler_level_journal_retention_overrides_the_service_level_journal_retention_with_idempotency_key()
         {
            let target = init_discover_and_resolve_target(
                endpoint_manifest::Service {
                    journal_retention: Some(60 * 1000),
                    handlers: vec![endpoint_manifest::Handler {
                        idempotency_retention: Some(120 * 1000),
                        journal_retention: Some(30 * 1000),
                        ..greeter_service_greet_handler()
                    }],
                    ..greeter_service()
                },
                GREETER_SERVICE_NAME,
                GREET_HANDLER_NAME,
            );
            assert_that!(
                target.compute_retention(true),
                eq(InvocationRetention {
                    completion_retention: Duration::from_secs(120),
                    journal_retention: Duration::from_secs(30),
                })
            )
        }

        #[test]
        fn journal_retention_greater_than_completion_retention() {
            let target = init_discover_and_resolve_target(
                endpoint_manifest::Service {
                    journal_retention: Some(120 * 1000),
                    handlers: vec![endpoint_manifest::Handler {
                        idempotency_retention: Some(60 * 1000),
                        ..greeter_service_greet_handler()
                    }],
                    ..greeter_service()
                },
                GREETER_SERVICE_NAME,
                GREET_HANDLER_NAME,
            );
            assert_that!(
                target.compute_retention(true),
                eq(InvocationRetention {
                    completion_retention: Duration::from_secs(60),
                    journal_retention: Duration::from_secs(60),
                })
            )
        }

        #[test]
        fn journal_retention_global_override_is_respected() {
            let mut config = Configuration::default();
            config.admin.experimental_feature_force_journal_retention =
                Some(Duration::from_secs(300));
            restate_types::config::set_current_config(config);

            let target = init_discover_and_resolve_target(
                greeter_service(),
                GREETER_SERVICE_NAME,
                GREET_HANDLER_NAME,
            );
            assert_that!(
                target.compute_retention(false),
                eq(InvocationRetention {
                    completion_retention: Duration::from_secs(300),
                    journal_retention: Duration::from_secs(300),
                })
            )
        }

        fn init_discover_and_resolve_timeouts(
            svc: endpoint_manifest::Service,
            service_name: &str,
            handler_name: &str,
        ) -> InvocationAttemptOptions {
            let schema_information = Schema::default();
            let mut updater = SchemaUpdater::new(schema_information);

            let mut deployment = Deployment::mock();
            deployment.id = updater
                .add_deployment(deployment.metadata.clone(), vec![svc], false)
                .unwrap();

            let schema = updater.into_inner();

            schema.assert_service_revision(service_name, 1);
            schema.assert_service_deployment(service_name, deployment.id);

            schema
                .resolve_invocation_attempt_options(&deployment.id, service_name, handler_name)
                .unwrap_or_else(|| {
                    panic!(
                        "Invocation target for deployment {} and target {}/{} must exists",
                        deployment.id, service_name, handler_name
                    )
                })
        }

        #[test]
        fn service_level_timeouts() {
            let timeouts = init_discover_and_resolve_timeouts(
                endpoint_manifest::Service {
                    abort_timeout: Some(120 * 1000),
                    inactivity_timeout: Some(60 * 1000),
                    ..greeter_service()
                },
                GREETER_SERVICE_NAME,
                GREET_HANDLER_NAME,
            );
            assert_that!(
                timeouts,
                eq(InvocationAttemptOptions {
                    abort_timeout: Some(Duration::from_secs(120)),
                    inactivity_timeout: Some(Duration::from_secs(60)),
                    enable_lazy_state: None,
                })
            )
        }

        #[test]
        fn service_level_timeouts_with_handler_overrides() {
            let timeouts = init_discover_and_resolve_timeouts(
                endpoint_manifest::Service {
                    abort_timeout: Some(120 * 1000),
                    inactivity_timeout: Some(60 * 1000),
                    handlers: vec![endpoint_manifest::Handler {
                        inactivity_timeout: Some(30 * 1000),
                        ..greeter_service_greet_handler()
                    }],
                    ..greeter_service()
                },
                GREETER_SERVICE_NAME,
                GREET_HANDLER_NAME,
            );
            assert_that!(
                timeouts,
                eq(InvocationAttemptOptions {
                    abort_timeout: Some(Duration::from_secs(120)),
                    inactivity_timeout: Some(Duration::from_secs(30)),
                    enable_lazy_state: None,
                })
            )
        }
    }

    mod modify_service {
        use super::*;

        use googletest::prelude::*;
        use restate_types::invocation::InvocationRetention;
        use restate_types::schema::invocation_target::InvocationTargetResolver;
        use restate_types::schema::service::ServiceMetadata;
        use test_log::test;

        #[test]
        fn workflow_retention() {
            // Register a plain workflow first
            let schema_information = Schema::default();
            let mut updater = SchemaUpdater::new(schema_information);

            let mut deployment = Deployment::mock();
            deployment.id = updater
                .add_deployment(deployment.metadata.clone(), vec![greeter_workflow()], false)
                .unwrap();

            let schema = updater.into_inner();

            assert_that!(
                schema.assert_service(GREETER_SERVICE_NAME),
                pat!(ServiceMetadata {
                    revision: eq(1),
                    deployment_id: eq(deployment.id),
                    workflow_completion_retention: eq(Some(DEFAULT_WORKFLOW_COMPLETION_RETENTION))
                })
            );
            assert_that!(
                schema
                    .resolve_latest_invocation_target(GREETER_SERVICE_NAME, GREET_HANDLER_NAME)
                    .unwrap()
                    .compute_retention(false),
                eq(InvocationRetention {
                    completion_retention: DEFAULT_WORKFLOW_COMPLETION_RETENTION,
                    journal_retention: Duration::ZERO,
                })
            );

            // Now update it
            let new_retention = Duration::from_secs(30);
            let mut updater = SchemaUpdater::new(schema);

            updater
                .modify_service(
                    GREETER_SERVICE_NAME.to_owned(),
                    vec![ModifyServiceChange::WorkflowCompletionRetention(
                        new_retention,
                    )],
                )
                .unwrap();

            let schema = updater.into_inner();

            assert_that!(
                schema.assert_service(GREETER_SERVICE_NAME),
                pat!(ServiceMetadata {
                    workflow_completion_retention: eq(Some(new_retention))
                })
            );
            assert_that!(
                schema
                    .resolve_latest_invocation_target(GREETER_SERVICE_NAME, GREET_HANDLER_NAME)
                    .unwrap()
                    .compute_retention(false),
                eq(InvocationRetention {
                    completion_retention: new_retention,
                    journal_retention: Duration::ZERO,
                })
            );
        }
    }
}
