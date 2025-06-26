// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::{ActiveServiceRevision, Deployment, Handler, Schema, ServiceRevision};

use crate::endpoint_manifest;
use crate::endpoint_manifest::HandlerType;
use crate::errors::GenericError;
use crate::identifiers::{DeploymentId, SubscriptionId};
use crate::invocation::{
    InvocationTargetType, ServiceType, VirtualObjectHandlerType, WorkflowHandlerType,
};
use crate::schema::deployment::{DeploymentMetadata, DeploymentType};
use crate::schema::invocation_target::{
    BadInputContentType, DEFAULT_IDEMPOTENCY_RETENTION, DEFAULT_WORKFLOW_COMPLETION_RETENTION,
    InputRules, InputValidationRule, OutputContentTypeRule, OutputRules,
};
use crate::schema::subscriptions::{
    EventInvocationTargetTemplate, Sink, Source, Subscription, SubscriptionValidator,
};
use http::{HeaderValue, Uri};
use serde_json::Value;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::error::Error;
use std::ops::{Not, RangeInclusive};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};

/// Whether to force the registration of an existing endpoint or not
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Force {
    Yes,
    No,
}

impl Force {
    pub fn force_enabled(&self) -> bool {
        *self == Self::Yes
    }
}

/// Whether to apply the changes or not
#[derive(Clone, Default, PartialEq, Eq, Debug)]
pub enum ApplyMode {
    DryRun,
    #[default]
    Apply,
}

impl ApplyMode {
    pub fn should_apply(&self) -> bool {
        *self == Self::Apply
    }
}

#[derive(Debug, Clone)]
pub enum ModifyServiceChange {
    Public(bool),
    IdempotencyRetention(Duration),
    WorkflowCompletionRetention(Duration),
    InactivityTimeout(Duration),
    AbortTimeout(Duration),
}

/// Newtype for service names
#[derive(Debug, Clone, PartialEq, Eq, Hash, derive_more::Display)]
#[display("{}", _0)]
struct ServiceName(String);

impl TryFrom<String> for ServiceName {
    type Error = ServiceError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.to_lowercase().starts_with("restate")
            || value.to_lowercase().eq_ignore_ascii_case("openapi")
        {
            Err(ServiceError::ReservedName(value))
        } else {
            Ok(ServiceName(value))
        }
    }
}

impl AsRef<str> for ServiceName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl ServiceName {
    fn into_inner(self) -> String {
        self.0
    }
}

impl Borrow<String> for ServiceName {
    fn borrow(&self) -> &String {
        &self.0
    }
}

#[derive(Debug, thiserror::Error, codederror::CodedError)]
pub enum SchemaError {
    // Those are generic and used by all schema resources
    #[error("not found in the schema registry: {0}")]
    #[code(unknown)]
    NotFound(String),
    #[error("already exists in the schema registry: {0}")]
    #[code(unknown)]
    Override(String),

    // Specific resources errors
    #[error(transparent)]
    Service(
        #[from]
        #[code]
        ServiceError,
    ),
    #[error(transparent)]
    Deployment(
        #[from]
        #[code]
        DeploymentError,
    ),
    #[error(transparent)]
    Subscription(
        #[from]
        #[code]
        SubscriptionError,
    ),
}

#[derive(Debug, thiserror::Error, codederror::CodedError)]
pub enum ServiceError {
    #[error("cannot insert/modify service '{0}' as it contains a reserved name")]
    #[code(restate_errors::META0005)]
    ReservedName(String),
    #[error(
        "detected a new service '{0}' revision with a service type different from the previous revision. Service type cannot be changed across revisions"
    )]
    #[code(restate_errors::META0006)]
    DifferentType(String),
    #[error("the service '{0}' already exists but the new revision removed the handlers {1:?}")]
    #[code(restate_errors::META0006)]
    RemovedHandlers(String, Vec<String>),
    #[error("the handler '{0}' input content-type is not valid: {1}")]
    #[code(unknown)]
    BadInputContentType(String, BadInputContentType),
    #[error("the handler '{0}' output content-type is not valid: {1}")]
    #[code(unknown)]
    BadOutputContentType(String, http::header::InvalidHeaderValue),
    #[error("invalid combination of service type and handler type '({0}, {1:?})'")]
    #[code(unknown)]
    BadServiceAndHandlerType(ServiceType, Option<endpoint_manifest::HandlerType>),
    #[error(
        "{0} sets the workflow_completion_retention, but it's not a {t} handler",
        t = HandlerType::Workflow
    )]
    #[code(unknown)]
    UnexpectedWorkflowCompletionRetention(String),
    #[error(
        "{0} sets the idempotency_retention, but it's a {t} handler",
        t = HandlerType::Workflow
    )]
    #[code(unknown)]
    UnexpectedIdempotencyRetention(String),
    #[error(
        "The service {service} is private, but the {handler} is explicitly set as public. This is not supported. Please make the service public, and hide the individual handlers"
    )]
    #[code(unknown)]
    BadHandlerVisibility { service: String, handler: String },
    #[error("the json schema for {service}/{handler} {position} is invalid: {error}")]
    #[code(unknown)]
    BadJsonSchema {
        service: String,
        handler: String,
        position: &'static str,
        #[source]
        error: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
    #[error("modifying retention time for service type {0} is unsupported")]
    #[code(unknown)]
    CannotModifyRetentionTime(ServiceType),
}

#[derive(Debug, thiserror::Error, codederror::CodedError)]
#[code(restate_errors::META0009)]
pub enum SubscriptionError {
    #[error(
        "invalid source URI '{0}': must have a scheme segment, with supported schemes: [kafka]."
    )]
    InvalidSourceScheme(Uri),
    #[error(
        "invalid source URI '{0}': source URI of Kafka type must have a authority segment containing the cluster name."
    )]
    InvalidKafkaSourceAuthority(Uri),

    #[error(
        "invalid sink URI '{0}': must have a scheme segment, with supported schemes: [service]."
    )]
    InvalidSinkScheme(Uri),
    #[error(
        "invalid sink URI '{0}': sink URI of service type must have a authority segment containing the service name."
    )]
    InvalidServiceSinkAuthority(Uri),
    #[error("invalid sink URI '{0}': cannot find service/handler specified in the sink URI.")]
    SinkServiceNotFound(Uri),

    #[error(transparent)]
    #[code(unknown)]
    Validation(GenericError),
}

#[derive(Debug, thiserror::Error, codederror::CodedError)]
pub enum DeploymentError {
    #[error(
        "an update deployment operation must provide an endpoint with the same services and handlers. The update tried to remove the services {0:?}"
    )]
    #[code(restate_errors::META0016)]
    RemovedServices(Vec<String>),
    #[error(
        "multiple deployments ({0:?}) were found that reference the discovered endpoint. A deployment can only be force updated when it uniquely owns its endpoint. First delete one or more of the deployments"
    )]
    #[code(restate_errors::META0017)]
    MultipleExistingDeployments(Vec<DeploymentId>),
    #[error(
        "an update deployment operation must provide an endpoint with the same services and handlers. The update tried to change the supported protocol versions from {0:?} to {1:?}"
    )]
    #[code(restate_errors::META0016)]
    DifferentSupportedProtocolVersions(RangeInclusive<i32>, RangeInclusive<i32>),
}

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
    schema: Schema,
    modified: bool,
}

impl SchemaUpdater {
    pub fn update<E>(
        schema: Schema,
        updater_fn: impl FnOnce(&mut SchemaUpdater) -> Result<(), E>,
    ) -> Result<Schema, E> {
        let mut schema_updater = SchemaUpdater::new(schema);
        updater_fn(&mut schema_updater)?;
        Ok(schema_updater.into_inner())
    }

    pub fn update_and_return<T, E>(
        schema: Schema,
        updater_fn: impl FnOnce(&mut SchemaUpdater) -> Result<T, E>,
    ) -> Result<(T, Schema), E> {
        let mut schema_updater = SchemaUpdater::new(schema);
        let t = updater_fn(&mut schema_updater)?;
        Ok((t, schema_updater.into_inner()))
    }

    pub fn new(schema: Schema) -> Self {
        Self {
            schema,
            modified: false,
        }
    }

    pub fn into_inner(mut self) -> Schema {
        if self.modified {
            self.schema.version = self.schema.version.next()
        }

        self.schema
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
        let mut existing_deployments = self.schema.deployments.iter().filter(|(_, schemas)| {
            schemas.ty.protocol_type() == deployment_metadata.ty.protocol_type()
                && schemas.ty.normalized_address() == deployment_metadata.ty.normalized_address()
        });

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
                            restate.deployment.address = %existing_deployment.ty.address_display(),
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

        let mut computed_services = HashMap::with_capacity(proposed_services.len());

        // Compute service schemas
        for (service_name, service) in proposed_services {
            let new_service_revision = self.create_service_revision(
                deployment_id,
                &deployment_metadata.ty,
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
            computed_services.insert(service_name.to_string(), Arc::new(new_service_revision));
        }

        drop(existing_deployments);

        // Update the active_service_revision index
        // TODO do we even need this?!
        for service_revision in computed_services.values() {
            self.schema.active_service_revisions.insert(
                service_revision.name.clone(),
                ActiveServiceRevision {
                    deployment_id,
                    service_revision: Arc::clone(service_revision),
                },
            );
        }
        for service_to_remove in services_to_remove {
            self.schema
                .active_service_revisions
                .remove(&service_to_remove);
        }

        self.schema.deployments.insert(
            deployment_id,
            Deployment {
                id: deployment_id,
                ty: deployment_metadata.ty,
                delivery_options: deployment_metadata.delivery_options,
                supported_protocol_versions: deployment_metadata.supported_protocol_versions,
                sdk_version: deployment_metadata.sdk_version,
                created_at: deployment_metadata.created_at,
                services: computed_services,
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
        let Some(existing_deployment) = self.schema.deployments.get(&deployment_id) else {
            return Err(SchemaError::NotFound(format!(
                "deployment with id '{deployment_id}'"
            )));
        };

        if existing_deployment.supported_protocol_versions
            != deployment_metadata.supported_protocol_versions
        {
            return Err(SchemaError::Deployment(
                DeploymentError::DifferentSupportedProtocolVersions(
                    existing_deployment.supported_protocol_versions.clone(),
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

        let mut computed_services = HashMap::with_capacity(proposed_services.len());

        // Compute service schemas
        for (service_name, service) in proposed_services {
            let service_revision = self.create_service_revision(
                deployment_id,
                &deployment_metadata.ty,
                &service_name,
                service,
                RemovedHandlerBehavior::Fail,
                ServiceTypeMismatchBehavior::Fail,
                ServiceLevelSettingsBehavior::Preserve,
            )?;

            computed_services.insert(service_name, Arc::new(service_revision));
        }

        let services_metadata = computed_services
            .into_iter()
            .map(|(name, schema)| {
                match self.schema.active_service_revisions.get(name.as_ref()) {
                    Some(ActiveServiceRevision {
                             deployment_id: latest_deployment,
                             ..
                         }) if latest_deployment == &deployment_id => {
                        // This deployment is the latest for this service, so we should update the service schema
                        info!(
                            rpc.service = %name,
                            "Overwriting existing service schemas"
                        );
                        self.schema
                            .active_service_revisions
                            .insert(name.clone().into_inner(), ActiveServiceRevision {
                                deployment_id,
                                service_revision: Arc::clone(&schema),
                            });
                    },
                    Some(_) => {
                        debug!(
                            rpc.service = %name,
                            "Keeping existing service schema as this update operation affected a draining deployment"
                        );
                    },
                    None => {
                        // we have a new service, it deserves a schema as normal
                        self.schema
                            .active_service_revisions
                            .insert(name.clone().into_inner(), ActiveServiceRevision {
                                deployment_id,
                                service_revision: Arc::clone(&schema),
                            });
                    }
                }

                (name.into_inner(), schema)
            })
            .collect();

        self.schema.deployments.insert(
            deployment_id,
            Deployment {
                id: deployment_id,
                ty: deployment_metadata.ty,
                delivery_options: deployment_metadata.delivery_options,
                supported_protocol_versions: deployment_metadata.supported_protocol_versions,
                sdk_version: deployment_metadata.sdk_version,
                created_at: deployment_metadata.created_at,
                services: services_metadata,
            },
        );

        self.modified = true;

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn validate_existing_service_revision_constraints(
        &self,
        deployment_id: DeploymentId,
        deployment_ty: &DeploymentType,
        service_name: &ServiceName,
        service: &endpoint_manifest::Service,
        removed_handler_behavior: RemovedHandlerBehavior,
        service_type_mismatch_behavior: ServiceTypeMismatchBehavior,
    ) -> Result<(), SchemaError> {
        let Some(ActiveServiceRevision {
            service_revision: existing_service,
            ..
        }) = self
            .schema
            .active_service_revisions
            .get(service_name.as_ref())
        else {
            // New service, nothing to validate
            return Ok(());
        };

        let service_type = ServiceType::from(service.ty);
        if existing_service.ty != service_type {
            if matches!(
                service_type_mismatch_behavior,
                ServiceTypeMismatchBehavior::Fail
            ) {
                return Err(SchemaError::Service(ServiceError::DifferentType(
                    service_name.clone().into_inner(),
                )));
            } else {
                warn!(
                    restate.deployment.id = %deployment_id,
                    restate.deployment.address = %deployment_ty.address_display(),
                    "Going to overwrite service type {} due to a forced deployment update: {:?} != {:?}. This is a potentially dangerous operation, and might result in data loss.",
                    service_name,
                    existing_service.ty,
                    service_type
                );
            }
        }

        let removed_handlers: Vec<String> = existing_service
            .handlers
            .keys()
            .filter(|name| {
                !service
                    .handlers
                    .iter()
                    .any(|new_handler| (*new_handler.name).eq(*name))
            })
            .map(|name| name.to_string())
            .collect();

        if !removed_handlers.is_empty() {
            if matches!(removed_handler_behavior, RemovedHandlerBehavior::Fail) {
                return Err(SchemaError::Service(ServiceError::RemovedHandlers(
                    service_name.clone().into_inner(),
                    removed_handlers,
                )));
            } else {
                warn!(
                    restate.deployment.id = %deployment_id,
                    restate.deployment.address = %deployment_ty.address_display(),
                    "Going to remove the following methods from service type {} due to a forced deployment update: {:?}.",
                    service.name.as_str(),
                    removed_handlers
                );
            }
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn create_service_revision(
        &self,
        deployment_id: DeploymentId,
        deployment_ty: &DeploymentType,
        service_name: &ServiceName,
        service: endpoint_manifest::Service,
        removed_handler_behavior: RemovedHandlerBehavior,
        service_type_mismatch_behavior: ServiceTypeMismatchBehavior,
        service_level_settings_behavior: ServiceLevelSettingsBehavior,
    ) -> Result<ServiceRevision, SchemaError> {
        self.validate_existing_service_revision_constraints(
            deployment_id,
            deployment_ty,
            service_name,
            &service,
            removed_handler_behavior,
            service_type_mismatch_behavior,
        )?;

        let active_revision = self
            .schema
            .active_service_revisions
            .get(service_name.as_ref())
            .map(|revision| revision.service_revision.as_ref());

        let service_type = ServiceType::from(service.ty);

        // --- Figure out service options

        // Little problem here that can lead to some surprising stuff.
        //
        // When updating a service, and setting the service/handler options (e.g. the idempotency retention, private, etc)
        // in the manifest, I override whatever was stored before. Makes sense.
        //
        // But when not setting these values, we keep whatever was configured before (either from manifest, or from Admin REST API).
        // This makes little sense now that we have these settings in the manifest, but it preserves the old behavior.
        // At some point we should "break" this behavior, and on service updates simply apply defaults when nothing is configured in the manifest,
        // in order to favour people using annotations, rather than the clunky Admin REST API.
        let public = service
            .ingress_private
            .map(bool::not)
            .or(if service_level_settings_behavior.preserve() {
                active_revision.map(|old_svc| old_svc.public)
            } else {
                None
            })
            .unwrap_or(true);
        let idempotency_retention = service.idempotency_retention_duration().or(
            if service_level_settings_behavior.preserve() {
                active_revision.and_then(|old_svc| old_svc.idempotency_retention)
            } else {
                // TODO(slinydeveloper) Remove this in Restate 1.5, no need for this defaulting anymore!
                Some(DEFAULT_IDEMPOTENCY_RETENTION)
            },
        );
        let journal_retention = service.journal_retention_duration().or(
            if service_level_settings_behavior.preserve() {
                active_revision.and_then(|old_svc| old_svc.journal_retention)
            } else {
                None
            },
        );
        let workflow_completion_retention = if service_level_settings_behavior.preserve()
            // Retain previous value only if new service and old one are both workflows
            && service_type == ServiceType::Workflow
            && active_revision.map(|old_svc| old_svc.ty == ServiceType::Workflow).unwrap_or(false)
        {
            active_revision.and_then(|old_svc| old_svc.workflow_completion_retention)
        } else if service_type == ServiceType::Workflow {
            Some(DEFAULT_WORKFLOW_COMPLETION_RETENTION)
        } else {
            None
        };
        let inactivity_timeout = service.inactivity_timeout_duration().or(
            if service_level_settings_behavior.preserve() {
                active_revision.and_then(|old_svc| old_svc.inactivity_timeout)
            } else {
                None
            },
        );
        let abort_timeout =
            service
                .abort_timeout_duration()
                .or(if service_level_settings_behavior.preserve() {
                    active_revision.and_then(|old_svc| old_svc.abort_timeout)
                } else {
                    None
                });

        let handlers = service
            .handlers
            .into_iter()
            .map(|h| {
                Ok((
                    h.name.to_string(),
                    Handler::from_schema(service_name.as_ref(), service_type, public, h)?,
                ))
            })
            .collect::<Result<HashMap<_, _>, SchemaError>>()?;

        Ok(ServiceRevision {
            name: service_name.to_string(),
            handlers,
            ty: service_type,
            documentation: service.documentation,
            metadata: service.metadata,
            revision: active_revision
                .map(|old_svc| old_svc.revision.wrapping_add(1))
                .unwrap_or(1),
            public,
            idempotency_retention,
            workflow_completion_retention,
            journal_retention,
            inactivity_timeout,
            abort_timeout,
            enable_lazy_state: service.enable_lazy_state,
            service_openapi_cache: Default::default(),
        })
    }

    pub fn remove_deployment(&mut self, deployment_id: DeploymentId) {
        if let Some(deployment) = self.schema.deployments.remove(&deployment_id) {
            for (_, service_metadata) in deployment.services {
                match self
                    .schema
                    .active_service_revisions
                    .entry(service_metadata.name.clone())
                {
                    // we need to check for the right revision in the service has been overwritten
                    // by a different deployment.
                    Entry::Occupied(entry)
                        if entry.get().service_revision.revision == service_metadata.revision =>
                    {
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

        if self.schema.subscriptions.contains_key(&id) {
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
                    .schema
                    .active_service_revisions
                    .get(service_name)
                    .ok_or_else(|| {
                        SchemaError::Subscription(SubscriptionError::SinkServiceNotFound(
                            sink.clone(),
                        ))
                    })?;
                let handler_schemas = service_schemas
                    .service_revision
                    .handlers
                    .get(handler_name)
                    .ok_or_else(|| {
                        SchemaError::Subscription(SubscriptionError::SinkServiceNotFound(
                            sink.clone(),
                        ))
                    })?;

                Sink::Invocation {
                    event_invocation_target_template: match handler_schemas.target_ty {
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

        self.schema.subscriptions.insert(id, subscription);
        self.modified = true;

        Ok(id)
    }

    pub fn remove_subscription(&mut self, subscription_id: SubscriptionId) {
        if self.schema.subscriptions.remove(&subscription_id).is_some() {
            self.modified = true;
        }
    }

    pub fn modify_service(
        &mut self,
        name: &str,
        changes: Vec<ModifyServiceChange>,
    ) -> Result<(), SchemaError> {
        self.apply_change_to_active_service_revision(name, |svc| {
            for command in changes {
                match command {
                    ModifyServiceChange::Public(new_public_value) => {
                        svc.public = new_public_value;
                        // Cleanup generated OpenAPI
                        svc.service_openapi_cache = Default::default();
                    }
                    ModifyServiceChange::IdempotencyRetention(new_idempotency_retention) => {
                        svc.idempotency_retention = Some(new_idempotency_retention);
                    }
                    ModifyServiceChange::WorkflowCompletionRetention(
                        new_workflow_completion_retention,
                    ) => {
                        // This applies only to workflow services
                        if svc.ty != ServiceType::Workflow {
                            return Err(SchemaError::Service(
                                ServiceError::CannotModifyRetentionTime(svc.ty),
                            ));
                        }
                        svc.workflow_completion_retention = Some(new_workflow_completion_retention);
                    }
                    ModifyServiceChange::JournalRetention(new_journal_retention) => {
                        svc.journal_retention = Some(new_journal_retention);
                    }
                    ModifyServiceChange::InactivityTimeout(inactivity_timeout) => {
                        svc.inactivity_timeout = Some(inactivity_timeout);
                    }
                    ModifyServiceChange::AbortTimeout(abort_timeout) => {
                        svc.abort_timeout = Some(abort_timeout);
                    }
                }
            }
            Ok(())
        })?;

        self.modified = true;

        Ok(())
    }

    fn apply_change_to_active_service_revision(
        &mut self,
        svc_name: &str,
        mutate: impl FnOnce(&mut ServiceRevision) -> Result<(), SchemaError>,
    ) -> Result<(), SchemaError> {
        // Find related deployment
        let Some(active_service_revision) = self.schema.active_service_revisions.get_mut(svc_name)
        else {
            return Ok(());
        };
        let deployment_id = active_service_revision.deployment_id;
        let Some(deployment) = self.schema.deployments.get_mut(&deployment_id) else {
            return Ok(());
        };
        let Some(old_svc) = deployment.services.remove(svc_name) else {
            return Ok(());
        };

        // Create new ServiceRevision instance, copying from the existing one, and mutate it
        let mut new_svc = Arc::unwrap_or_clone(old_svc);
        mutate(&mut new_svc)?;
        let new_svc_arc = Arc::new(new_svc);

        // Update the related deployment
        deployment
            .services
            .insert(svc_name.to_owned(), Arc::clone(&new_svc_arc));

        // Update index
        active_service_revision.service_revision = Arc::clone(&new_svc_arc);

        Ok(())
    }
}

impl Handler {
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
            target_ty: ty,
            input_rules: handler
                .input
                .map(|input_payload| {
                    Self::input_rules_from_schema(service_name, &handler.name, input_payload)
                })
                .transpose()?
                .unwrap_or_default(),
            output_rules: handler
                .output
                .map(|output_payload| {
                    Self::output_rules_from_schema(service_name, &handler.name, output_payload)
                })
                .transpose()?
                .unwrap_or_default(),
            documentation: handler.documentation,
            metadata: handler.metadata,
            journal_retention,
            idempotency_retention,
            workflow_completion_retention,
            inactivity_timeout,
            abort_timeout,
            enable_lazy_state: handler.enable_lazy_state,
            public: handler.ingress_private.map(bool::not),
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
    use test_log::test;

    use crate::Versioned;
    use crate::schema::deployment::Deployment;
    use crate::schema::deployment::DeploymentResolver;
    use crate::schema::service::ServiceMetadataResolver;

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
        schema.assert_invocation_target(GREETER_SERVICE_NAME, "greet");
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
                .assert_invocation_target(GREETER_SERVICE_NAME, "greet")
                .public
        );

        let version_before_modification = schemas.version();
        updater = SchemaUpdater::new(schemas);
        updater.modify_service(
            GREETER_SERVICE_NAME,
            vec![ModifyServiceChange::Public(false)],
        )?;
        let schemas = updater.into_inner();

        assert!(version_before_modification < schemas.version());
        assert!(!schemas.assert_service(GREETER_SERVICE_NAME).public);
        assert!(
            !schemas
                .assert_invocation_target(GREETER_SERVICE_NAME, "greet")
                .public
        );

        updater = SchemaUpdater::new(schemas);
        deployment.id =
            updater.add_deployment(deployment.metadata.clone(), vec![greeter_service()], true)?;

        let schemas = updater.into_inner();
        assert!(!schemas.assert_service(GREETER_SERVICE_NAME).public);
        assert!(
            !schemas
                .assert_invocation_target(GREETER_SERVICE_NAME, "greet")
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
            check!(service == GREETER_SERVICE_NAME);
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

        let updated_deployment = schemas.get_deployment(&deployment_1.id).unwrap();

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

        let updated_deployment = schemas.get_deployment(&deployment_1.id).unwrap();

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
            .schema
            .assert_service_deployment(GREETER_SERVICE_NAME, deployment_2.id);
        updater
            .schema
            .assert_service_revision(GREETER_SERVICE_NAME, 2);

        let updated_deployment_1 = updater.schema.get_deployment(&deployment_1.id).unwrap();

        let updated_deployment_2 = updater.schema.get_deployment(&deployment_2.id).unwrap();

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
            .schema
            .assert_service_deployment(GREETER_SERVICE_NAME, deployment_1.id);
        updater
            .schema
            .assert_service_revision(GREETER_SERVICE_NAME, 2);

        let (_, services) = updater
            .schema
            .get_deployment_and_services(&deployment_1.id)
            .unwrap();

        assert_eq!(
            services
                .iter()
                .find(|s| s.name == GREETER_SERVICE_NAME)
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
            .schema
            .assert_service_deployment(GREETER_SERVICE_NAME, deployment_2.id);
        updater
            .schema
            .assert_service_revision(GREETER_SERVICE_NAME, 2);

        let (_, updated_deployment_1_services) = updater
            .schema
            .get_deployment_and_services(&deployment_1.id)
            .unwrap();

        assert_eq!(
            updated_deployment_1_services
                .iter()
                .find(|s| s.name == GREETER_SERVICE_NAME)
                .unwrap()
                .handlers
                .len(),
            2
        );

        let (_, updated_deployment_2_services) = updater
            .schema
            .get_deployment_and_services(&deployment_2.id)
            .unwrap();

        assert_eq!(
            updated_deployment_2_services
                .iter()
                .find(|s| s.name == GREETER_SERVICE_NAME)
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
            .schema
            .assert_service_deployment(GREETER_SERVICE_NAME, deployment_1.id);
        updater
            .schema
            .assert_service_deployment(ANOTHER_GREETER_SERVICE_NAME, deployment_1.id);
        updater
            .schema
            .assert_service_revision(GREETER_SERVICE_NAME, 2);
        updater
            .schema
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
            .schema
            .assert_service_deployment(GREETER_SERVICE_NAME, deployment_2.id);
        updater
            .schema
            .assert_service_deployment(ANOTHER_GREETER_SERVICE_NAME, deployment_1.id);
        updater
            .schema
            .assert_service_revision(GREETER_SERVICE_NAME, 2);
        updater
            .schema
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
                .assert_invocation_target(GREETER_SERVICE_NAME, "greet")
                .public
        );

        let version_before_modification = schemas.version();
        updater = SchemaUpdater::new(schemas);
        updater.modify_service(
            GREETER_SERVICE_NAME,
            vec![ModifyServiceChange::Public(false)],
        )?;
        let schemas = updater.into_inner();
        let version_after_modification = schemas.version();

        assert!(version_before_modification < version_after_modification);
        assert!(!schemas.assert_service(GREETER_SERVICE_NAME).public);
        assert!(
            !schemas
                .assert_invocation_target(GREETER_SERVICE_NAME, "greet")
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
                .assert_invocation_target(GREETER_SERVICE_NAME, "greet")
                .public
        );

        Ok(())
    }

    mod endpoint_manifest_options_propagation {
        use super::*;

        use crate::config::Configuration;
        use crate::invocation::InvocationRetention;
        use crate::schema::deployment::Deployment;
        use crate::schema::invocation_target::InvocationTargetMetadata;
        use crate::schema::service::InvocationAttemptOptions;
        use crate::schema::service::{HandlerMetadata, ServiceMetadata};
        use googletest::prelude::*;
        use std::time::Duration;
        use test_log::test;

        fn init_discover_and_resolve_target(
            svc: endpoint_manifest::Service,
            service_name: &str,
            handler_name: &str,
        ) -> InvocationTargetMetadata {
            let mut deployment = Deployment::mock();
            let (deployment_id, schema) =
                SchemaUpdater::update_and_return(Schema::default(), move |updater| {
                    updater.add_deployment(deployment.metadata.clone(), vec![svc], false)
                })
                .unwrap();
            deployment.id = deployment_id;

            schema.assert_service_revision(service_name, 1);
            schema.assert_service_deployment(service_name, deployment.id);

            schema.assert_invocation_target(service_name, handler_name)
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
            let schema = SchemaUpdater::update(Schema::default(), |updater| {
                updater
                    .add_deployment(
                        Deployment::mock().metadata,
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
                    .map(|_| ())
            })
            .unwrap();

            // The explicitly private handler is private
            let private_handler_target =
                schema.assert_invocation_target(GREETER_SERVICE_NAME, "my_private_handler");
            assert_that!(private_handler_target.public, eq(false));

            // The other handler is by default public
            let public_handler_target =
                schema.assert_invocation_target(GREETER_SERVICE_NAME, GREET_HANDLER_NAME);
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
                    .assert_service_handler(GREETER_SERVICE_NAME, GREET_HANDLER_NAME)
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
        fn journal_retention_default_is_respected() {
            let mut config = Configuration::default();
            config.invocation.default_journal_retention = Some(Duration::from_secs(300));
            crate::config::set_current_config(config);

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

        #[test]
        fn journal_retention_default_is_overridden_by_discovery() {
            let mut config = Configuration::default();
            config.invocation.default_journal_retention = Some(Duration::from_secs(300));
            crate::config::set_current_config(config);

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
        fn journal_retention_last_one_wins_scenario() {
            // 1. User configures journal retention A in SDK configuration, registers the service, now journal retention is A
            crate::config::set_current_config(Configuration::default());

            let mut deployment = Deployment::mock();
            let (deployment_id, schema) =
                SchemaUpdater::update_and_return(Schema::default(), move |updater| {
                    updater.add_deployment(
                        deployment.metadata.clone(),
                        vec![endpoint_manifest::Service {
                            journal_retention: Some(60 * 1000), // A = 60 seconds
                            ..greeter_service()
                        }],
                        false,
                    )
                })
                .unwrap();
            deployment.id = deployment_id;

            schema.assert_service_revision(GREETER_SERVICE_NAME, 1);
            schema.assert_service_deployment(GREETER_SERVICE_NAME, deployment.id);

            assert_that!(
                schema
                    .assert_invocation_target(GREETER_SERVICE_NAME, GREET_HANDLER_NAME)
                    .compute_retention(false),
                eq(InvocationRetention {
                    completion_retention: Duration::from_secs(60),
                    journal_retention: Duration::from_secs(60),
                })
            );
            assert_that!(
                schema.assert_service(GREETER_SERVICE_NAME),
                pat!(ServiceMetadata {
                    journal_retention: some(eq(Duration::from_secs(60))),
                })
            );

            // 2. User updates journal retention to B in the UI, now journal retention is B
            let schema = SchemaUpdater::update(schema, |updater| {
                updater.modify_service(
                    GREETER_SERVICE_NAME.to_string(),
                    vec![ModifyServiceChange::JournalRetention(Duration::from_secs(
                        120,
                    ))], // B = 120 seconds
                )
            })
            .unwrap();

            assert_that!(
                schema
                    .assert_invocation_target(GREETER_SERVICE_NAME, GREET_HANDLER_NAME)
                    .compute_retention(false),
                eq(InvocationRetention {
                    completion_retention: Duration::from_secs(120),
                    journal_retention: Duration::from_secs(120),
                })
            );
            assert_that!(
                schema.assert_service(GREETER_SERVICE_NAME),
                pat!(ServiceMetadata {
                    journal_retention: some(eq(Duration::from_secs(120))),
                })
            );

            // 3. User registers a new revision of the service defining in the SDK journal retention A, now journal retention is again A
            let mut deployment = Deployment::mock_with_uri("http://localhost:9081");
            let (deployment_id, schema) =
                SchemaUpdater::update_and_return(schema, move |updater| {
                    updater.add_deployment(
                        deployment.metadata.clone(),
                        vec![endpoint_manifest::Service {
                            journal_retention: Some(60 * 1000), // A = 60 seconds again
                            ..greeter_service()
                        }],
                        false,
                    )
                })
                .unwrap();
            deployment.id = deployment_id;

            schema.assert_service_revision(GREETER_SERVICE_NAME, 2); // Revision should be incremented
            schema.assert_service_deployment(GREETER_SERVICE_NAME, deployment.id);

            assert_that!(
                schema
                    .assert_invocation_target(GREETER_SERVICE_NAME, GREET_HANDLER_NAME)
                    .compute_retention(false),
                eq(InvocationRetention {
                    completion_retention: Duration::from_secs(60),
                    journal_retention: Duration::from_secs(60),
                })
            );
            assert_that!(
                schema.assert_service(GREETER_SERVICE_NAME),
                pat!(ServiceMetadata {
                    journal_retention: some(eq(Duration::from_secs(60))),
                })
            );

            // 4. Operator updates RESTATE_DEFAULT_JOURNAL_RETENTION with value C, journal retention is still A
            let mut config = Configuration::default();
            config.invocation.default_journal_retention = Some(Duration::from_secs(180)); // C = 180 seconds
            crate::config::set_current_config(config);

            assert_that!(
                schema
                    .assert_invocation_target(GREETER_SERVICE_NAME, GREET_HANDLER_NAME)
                    .compute_retention(false),
                eq(InvocationRetention {
                    completion_retention: Duration::from_secs(60),
                    journal_retention: Duration::from_secs(60),
                })
            );
            assert_that!(
                schema.assert_service(GREETER_SERVICE_NAME),
                pat!(ServiceMetadata {
                    journal_retention: some(eq(Duration::from_secs(60))),
                })
            );

            // 5. Operator updates RESTATE_MAX_JOURNAL_RETENTION with value D, journal retention will be min(A, D)
            let mut config = Configuration::default();
            config.invocation.default_journal_retention = Some(Duration::from_secs(180)); // C = 180 seconds -> this should be ignored
            config.invocation.max_journal_retention = Some(Duration::from_secs(30)); // D = 30 seconds
            crate::config::set_current_config(config);

            assert_that!(
                schema
                    .assert_invocation_target(GREETER_SERVICE_NAME, GREET_HANDLER_NAME)
                    .compute_retention(false),
                eq(InvocationRetention {
                    completion_retention: Duration::from_secs(30),
                    journal_retention: Duration::from_secs(30),
                })
            );
            assert_that!(
                schema.assert_service(GREETER_SERVICE_NAME),
                pat!(ServiceMetadata {
                    journal_retention: some(eq(Duration::from_secs(30))),
                })
            );
        }

        #[test]
        fn max_journal_retention_clamps_value_for_service() {
            // Set max_journal_retention to 60 seconds
            let mut config = Configuration::default();
            config.invocation.max_journal_retention = Some(Duration::from_secs(60));
            crate::config::set_current_config(config);

            // Create a service with journal_retention of 120 seconds (higher than max)
            let target = init_discover_and_resolve_target(
                endpoint_manifest::Service {
                    journal_retention: Some(120 * 1000), // 120 seconds
                    ..greeter_service()
                },
                GREETER_SERVICE_NAME,
                GREET_HANDLER_NAME,
            );

            // Verify that the journal_retention is clamped to 60 seconds (the max)
            assert_that!(
                target.compute_retention(false),
                eq(InvocationRetention {
                    completion_retention: Duration::from_secs(60), // Clamped to max
                    journal_retention: Duration::from_secs(60),    // Clamped to max
                })
            );
        }
        #[test]
        fn max_journal_retention_higher_than_set_value() {
            // Set max_journal_retention to 60 seconds
            let mut config = Configuration::default();
            config.invocation.max_journal_retention = Some(Duration::from_secs(60));
            crate::config::set_current_config(config);

            // Create a service with journal_retention of 30 seconds (lower than max)
            let target = init_discover_and_resolve_target(
                endpoint_manifest::Service {
                    journal_retention: Some(30 * 1000), // 30 seconds
                    ..greeter_service()
                },
                GREETER_SERVICE_NAME,
                GREET_HANDLER_NAME,
            );

            // Verify that the journal_retention is not affected (still 30 seconds)
            assert_that!(
                target.compute_retention(false),
                eq(InvocationRetention {
                    completion_retention: Duration::from_secs(30), // Not clamped
                    journal_retention: Duration::from_secs(30),    // Not clamped
                })
            );
        }

        #[test]
        fn max_journal_retention_clamps_value_for_handler() {
            // Set max_journal_retention to 60 seconds
            let mut config = Configuration::default();
            config.invocation.max_journal_retention = Some(Duration::from_secs(60));
            crate::config::set_current_config(config);

            // Create a handler with journal_retention of 300 seconds (higher than max)
            let mut deployment = Deployment::mock();
            let (deployment_id, schema) =
                SchemaUpdater::update_and_return(Schema::default(), move |updater| {
                    updater.add_deployment(
                        deployment.metadata.clone(),
                        vec![endpoint_manifest::Service {
                            journal_retention: Some(120 * 1000), // Service sets 120 seconds
                            handlers: vec![endpoint_manifest::Handler {
                                journal_retention: Some(300 * 1000), // 300 seconds
                                ..greeter_service_greet_handler()
                            }],
                            ..greeter_service()
                        }],
                        false,
                    )
                })
                .unwrap();
            deployment.id = deployment_id;

            // Verify that the journal_retention is clamped to 60 seconds (the max)
            assert_that!(
                schema
                    .assert_invocation_target(GREETER_SERVICE_NAME, GREET_HANDLER_NAME)
                    .compute_retention(false),
                eq(InvocationRetention {
                    completion_retention: Duration::from_secs(60), // Clamped to max
                    journal_retention: Duration::from_secs(60),    // Clamped to max
                })
            );
            assert_that!(
                schema.assert_service(GREETER_SERVICE_NAME),
                pat!(ServiceMetadata {
                    journal_retention: some(eq(Duration::from_secs(60))), // This was never set
                })
            );
            assert_that!(
                schema.assert_handler(GREETER_SERVICE_NAME, GREET_HANDLER_NAME),
                pat!(HandlerMetadata {
                    journal_retention: some(eq(Duration::from_secs(60))), // Clamped to max
                })
            );

            // Disable max
            let mut config = Configuration::default();
            config.invocation.max_journal_retention = None;
            crate::config::set_current_config(config);

            assert_that!(
                schema
                    .assert_invocation_target(GREETER_SERVICE_NAME, GREET_HANDLER_NAME)
                    .compute_retention(false),
                eq(InvocationRetention {
                    completion_retention: Duration::from_secs(300),
                    journal_retention: Duration::from_secs(300),
                })
            );
            assert_that!(
                schema.assert_service(GREETER_SERVICE_NAME),
                pat!(ServiceMetadata {
                    journal_retention: some(eq(Duration::from_secs(120))), // The initial value set
                })
            );
            assert_that!(
                schema.assert_handler(GREETER_SERVICE_NAME, GREET_HANDLER_NAME),
                pat!(HandlerMetadata {
                    journal_retention: some(eq(Duration::from_secs(300))), // The initial value set
                })
            );
        }

        #[test]
        fn max_journal_retention_unset_means_no_limit() {
            // Set default_journal_retention but leave max_journal_retention unset
            let mut config = Configuration::default();
            config.invocation.default_journal_retention = Some(Duration::from_secs(60));
            config.invocation.max_journal_retention = None; // Explicitly unset
            crate::config::set_current_config(config);

            // Create a service with a very high journal_retention
            let target = init_discover_and_resolve_target(
                endpoint_manifest::Service {
                    journal_retention: Some(3600 * 1000), // 1 hour
                    ..greeter_service()
                },
                GREETER_SERVICE_NAME,
                GREET_HANDLER_NAME,
            );

            // Verify that the journal_retention is not clamped (no limit)
            assert_that!(
                target.compute_retention(false),
                eq(InvocationRetention {
                    completion_retention: Duration::from_secs(3600), // Not clamped
                    journal_retention: Duration::from_secs(3600),    // Not clamped
                })
            );
        }

        #[test]
        fn max_journal_retention_zero_disables_journal_retention() {
            // Set max_journal_retention to 0 (always disabled)
            let mut config = Configuration::default();
            config.invocation.max_journal_retention = Some(Duration::from_secs(0));
            crate::config::set_current_config(config);

            // Create a service with journal_retention set
            let target = init_discover_and_resolve_target(
                endpoint_manifest::Service {
                    journal_retention: Some(60 * 1000), // 60 seconds
                    ..greeter_service()
                },
                GREETER_SERVICE_NAME,
                GREET_HANDLER_NAME,
            );

            // Verify that the journal_retention is clamped to 0 (disabled)
            assert_that!(
                target.compute_retention(false),
                eq(InvocationRetention {
                    completion_retention: Duration::from_secs(0), // Clamped to 0
                    journal_retention: Duration::from_secs(0),    // Clamped to 0
                })
            );
        }

        #[test]
        fn max_journal_retention_zero_wins_over_default_and_set_values() {
            // Create a service with default journal_retention
            let mut config = Configuration::default();
            config.invocation.default_journal_retention = Some(Duration::from_secs(300));
            config.invocation.max_journal_retention = Some(Duration::from_secs(0));
            crate::config::set_current_config(config);

            let target = init_discover_and_resolve_target(
                greeter_service(), // No explicit journal_retention
                GREETER_SERVICE_NAME,
                GREET_HANDLER_NAME,
            );

            // Verify that the journal_retention is still 0 (disabled)
            assert_that!(
                target.compute_retention(false),
                eq(InvocationRetention {
                    completion_retention: Duration::from_secs(0), // Clamped to 0
                    journal_retention: Duration::from_secs(0),    // Clamped to 0
                })
            );
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

        use crate::invocation::InvocationRetention;
        use crate::schema::invocation_target::InvocationTargetResolver;
        use crate::schema::service::ServiceMetadata;
        use googletest::prelude::*;
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
                    GREETER_SERVICE_NAME,
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
                    .assert_service_handler(GREETER_SERVICE_NAME, GREET_HANDLER_NAME)
                    .compute_retention(false),
                eq(InvocationRetention {
                    completion_retention: new_retention,
                    journal_retention: Duration::ZERO,
                })
            );
        }
    }
}
