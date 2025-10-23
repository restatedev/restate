// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::{ActiveServiceRevision, DeliveryOptions, Deployment, Handler, Schema, ServiceRevision};

use crate::config::{Configuration, IngressOptions};
use crate::deployment::{DeploymentAddress, Headers};
use crate::endpoint_manifest::HandlerType;
use crate::errors::GenericError;
use crate::identifiers::{DeploymentId, SubscriptionId};
use crate::invocation::{
    InvocationTargetType, ServiceType, VirtualObjectHandlerType, WorkflowHandlerType,
};
use crate::schema::deployment::DeploymentType;
use crate::schema::invocation_target::{
    BadInputContentType, DEFAULT_IDEMPOTENCY_RETENTION, DEFAULT_WORKFLOW_COMPLETION_RETENTION,
    InputRules, InputValidationRule, OnMaxAttempts, OutputContentTypeRule, OutputRules,
};
use crate::schema::registry::{DeploymentConnectionParameters, DiscoveryResponse};
use crate::schema::subscriptions::{EventInvocationTargetTemplate, Sink, Source, Subscription};
use crate::time::MillisSinceEpoch;
use crate::{deployment, endpoint_manifest, identifiers};
use http::{HeaderValue, Uri};
use serde_json::Value;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::error::Error;
use std::num::NonZeroUsize;
use std::ops::{Deref, Not, RangeInclusive};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};

/// Whether to allow breaking schema changes between the existing service revision and the new service revision.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AllowBreakingChanges {
    Yes,
    No,
}

/// Whether to overwrite the existing endpoint.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Overwrite {
    Yes,
    No,
}

#[derive(Debug, thiserror::Error, codederror::CodedError)]
pub(in crate::schema) enum SchemaError {
    // Those are generic and used by all schema resources
    #[error("not found in the schema registry: {0}")]
    #[code(unknown)]
    NotFound(String),

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
pub(in crate::schema) enum ServiceError {
    #[error("cannot insert/modify service '{0}' as it contains a reserved name")]
    #[code(restate_errors::META0005)]
    ReservedName(String),
    #[error(
        "detected a new service revision for '{0}' changing the service type from previous revision. Service type cannot be changed across revisions."
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
pub(in crate::schema) enum SubscriptionError {
    #[error("subscription {0} already exists")]
    Override(SubscriptionId),

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
pub(in crate::schema) enum DeploymentError {
    #[error(
        "an update deployment operation must provide an endpoint with the same services and handlers. The update tried to change the supported protocol versions from {0:?} to {1:?}"
    )]
    #[code(restate_errors::META0016)]
    DifferentSupportedProtocolVersions(RangeInclusive<i32>, RangeInclusive<i32>),
}

/// Behavior when a handler is removed during service update
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum RemovedHandlerBehavior {
    /// Fail with an error when a handler is removed
    Fail,
    /// Log a warning but allow the removal
    Warn,
}

/// Behavior when service type changes during update
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum ServiceTypeMismatchBehavior {
    /// Fail with an error when service type changes
    Fail,
    /// Log a warning but allow the type change
    Warn,
}

/// Behavior for service level settings during update
enum ServiceLevelSettingsBehavior {
    /// Preserve existing service level settings
    #[allow(dead_code)]
    Preserve,
    /// Reset to defaults
    UseDefaults,
}

impl ServiceLevelSettingsBehavior {
    fn preserve(&self) -> bool {
        matches!(self, ServiceLevelSettingsBehavior::Preserve)
    }
}

#[derive(Debug, Clone)]
pub(in crate::schema) struct AddDeploymentRequest {
    pub(in crate::schema) deployment_address: DeploymentAddress,
    pub(in crate::schema) additional_headers: Headers,
    pub(in crate::schema) metadata: deployment::Metadata,
    pub(in crate::schema) discovery_response: DiscoveryResponse,
    pub(in crate::schema) allow_breaking_changes: AllowBreakingChanges,
    pub(in crate::schema) overwrite: Overwrite,
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum AddDeploymentResult {
    Created,
    Unchanged,
    Overwritten,
}

#[derive(Debug, Clone)]
pub struct UpdateDeploymentRequest {
    pub(in crate::schema) deployment_id: DeploymentId,
    pub(in crate::schema) deployment_address: DeploymentAddress,
    pub(in crate::schema) additional_headers: Headers,
    pub(in crate::schema) discovery_response: DiscoveryResponse,
    pub(in crate::schema) overwrite: Overwrite,
}

#[derive(Debug, Clone, Default)]
pub struct ModifyServiceRequest {
    pub public: Option<bool>,
    pub idempotency_retention: Option<Duration>,
    pub journal_retention: Option<Duration>,
    pub workflow_completion_retention: Option<Duration>,
    pub inactivity_timeout: Option<Duration>,
    pub abort_timeout: Option<Duration>,
}

/// Responsible for updating the provided [`Schema`] with new
/// schema information. It makes sure that the version of schema information
/// is incremented on changes.
#[derive(Debug, Default)]
pub(in crate::schema) struct SchemaUpdater {
    schema: Schema,
    modified: bool,
}

impl SchemaUpdater {
    pub(in crate::schema) fn update<E>(
        schema: Schema,
        updater_fn: impl FnOnce(&mut SchemaUpdater) -> Result<(), E>,
    ) -> Result<Schema, E> {
        let mut schema_updater = SchemaUpdater::new(schema);
        updater_fn(&mut schema_updater)?;
        Ok(schema_updater.into_inner())
    }

    pub(in crate::schema) fn update_and_return<T, E>(
        schema: Schema,
        updater_fn: impl FnOnce(&mut SchemaUpdater) -> Result<T, E>,
    ) -> Result<(T, Schema), E> {
        let mut schema_updater = SchemaUpdater::new(schema);
        let t = updater_fn(&mut schema_updater)?;
        Ok((t, schema_updater.into_inner()))
    }

    fn new(schema: Schema) -> Self {
        Self {
            schema,
            modified: false,
        }
    }

    fn into_inner(mut self) -> Schema {
        if self.modified {
            self.schema.version = self.schema.version.next()
        }

        self.schema
    }

    fn mark_updated(&mut self) {
        self.schema.active_service_revisions =
            ActiveServiceRevision::create_index(self.schema.deployments.values());
        self.modified = true;
    }

    pub(in crate::schema) fn add_deployment(
        &mut self,
        AddDeploymentRequest {
            deployment_address,
            additional_headers,
            metadata,
            discovery_response,
            allow_breaking_changes,
            overwrite,
        }: AddDeploymentRequest,
    ) -> Result<(AddDeploymentResult, DeploymentId), SchemaError> {
        let mut add_deployment_result = AddDeploymentResult::Created;

        let proposed_services: HashMap<_, _> = discovery_response
            .services
            .into_iter()
            .map(|svc| {
                let service_name = svc.name.to_string();
                validate_service_name(&service_name)?;
                Ok::<_, ServiceError>((service_name, svc))
            })
            .collect::<Result<HashMap<_, _>, _>>()?;

        // Did we find an existing deployment with a conflicting endpoint url?
        let existing_deployment = self
            .schema
            .deployments
            .iter()
            .filter(|(_, deployment)| {
                deployment
                    .semantic_eq_with_address_and_headers(&deployment_address, &additional_headers)
            })
            // There are few situations where we might have multiple deployments for the same endpoint:
            // * If there is some different configuration of the Configuration.admin.deployment_routing_headers between nodes,
            //   and some registration was previously accepted.
            // * If update_deployment was used on at least one deployment, pointing to the same address of another deployment,
            //   resulting in having two deployments pointing at the same address.
            //
            // We pick max_by created_at, because with force the user wants to override the last deployment version, and not old ones.
            .max_by(|(_, x), (_, y)| x.created_at.cmp(&y.created_at));

        let mut services_to_remove = Vec::default();

        let deployment_id = if let Some((existing_deployment_id, existing_deployment)) =
            existing_deployment
        {
            if overwrite == Overwrite::Yes {
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

                add_deployment_result = AddDeploymentResult::Overwritten;
                *existing_deployment_id
            } else {
                // Not going to perform any action here
                return Ok((AddDeploymentResult::Unchanged, *existing_deployment_id));
            }
        } else {
            DeploymentId::new()
        };

        let mut computed_services = HashMap::with_capacity(proposed_services.len());

        // Compute service schemas
        for (service_name, new_service) in proposed_services {
            let previous_service_revision = self
                .schema
                .active_service_revisions
                .get(&service_name)
                .map(|revision| revision.service_revision.as_ref());

            // Validate changes with previous revision, if any
            if let Some(previous_service_revision) = previous_service_revision {
                self.validate_existing_service_revision_constraints(
                    deployment_id,
                    &deployment_address,
                    &service_name,
                    &new_service,
                    previous_service_revision,
                    if allow_breaking_changes == AllowBreakingChanges::Yes {
                        RemovedHandlerBehavior::Warn
                    } else {
                        RemovedHandlerBehavior::Fail
                    },
                    if allow_breaking_changes == AllowBreakingChanges::Yes {
                        ServiceTypeMismatchBehavior::Warn
                    } else {
                        ServiceTypeMismatchBehavior::Fail
                    },
                )?;
            }

            let new_revision = previous_service_revision
                .map(|old_svc| old_svc.revision.wrapping_add(1))
                .unwrap_or(1);
            let new_service_revision = self.create_service_revision(
                &service_name,
                new_service,
                new_revision,
                previous_service_revision,
                ServiceLevelSettingsBehavior::UseDefaults,
            )?;
            computed_services.insert(service_name.to_string(), Arc::new(new_service_revision));
        }

        self.schema.deployments.insert(
            deployment_id,
            Deployment {
                id: deployment_id,
                ty: Self::create_deployment_ty(
                    deployment_address,
                    discovery_response.deployment_type_parameters,
                ),
                delivery_options: DeliveryOptions::new(additional_headers),
                supported_protocol_versions: discovery_response.supported_protocol_versions,
                sdk_version: discovery_response.sdk_version,
                created_at: MillisSinceEpoch::now(),
                metadata,
                services: computed_services,
            },
        );

        self.mark_updated();

        Ok((add_deployment_result, deployment_id))
    }

    fn create_deployment_ty(
        deployment_address: DeploymentAddress,
        deployment_connection_params: DeploymentConnectionParameters,
    ) -> DeploymentType {
        match (deployment_address, deployment_connection_params) {
            (
                DeploymentAddress::Http(a),
                DeploymentConnectionParameters::Http {
                    http_version,
                    protocol_type,
                },
            ) => DeploymentType::Http {
                address: a.uri,
                protocol_type,
                http_version,
            },
            (
                DeploymentAddress::Lambda(a),
                DeploymentConnectionParameters::Lambda { compression },
            ) => DeploymentType::Lambda {
                arn: a.arn,
                assume_role_arn: a.assume_role_arn.map(Into::into),
                compression,
            },
            _ => unreachable!(
                "deployment address and discovered deployment parameters are not of the same type"
            ),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn validate_existing_service_revision_constraints(
        &self,
        deployment_id: DeploymentId,
        deployment_address: &DeploymentAddress,
        service_name: &String,
        new_service_manifest: &endpoint_manifest::Service,
        existing_service: &ServiceRevision,
        removed_handler_behavior: RemovedHandlerBehavior,
        service_type_mismatch_behavior: ServiceTypeMismatchBehavior,
    ) -> Result<(), SchemaError> {
        let service_type = ServiceType::from(new_service_manifest.ty);
        if existing_service.ty != service_type {
            if service_type_mismatch_behavior == ServiceTypeMismatchBehavior::Fail {
                return Err(SchemaError::Service(ServiceError::DifferentType(
                    service_name.clone(),
                )));
            } else {
                warn!(
                    restate.deployment.id = %deployment_id,
                    restate.deployment.address = %deployment_address,
                    "Going to change service type for {} due to an update: {:?} != {:?}. \
                    This is a potentially dangerous operation, and might result in data loss.",
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
                !new_service_manifest
                    .handlers
                    .iter()
                    .any(|new_handler| (*new_handler.name).eq(*name))
            })
            .map(|name| name.to_string())
            .collect();

        if !removed_handlers.is_empty() {
            if removed_handler_behavior == RemovedHandlerBehavior::Fail {
                return Err(SchemaError::Service(ServiceError::RemovedHandlers(
                    service_name.clone(),
                    removed_handlers,
                )));
            } else {
                warn!(
                    restate.deployment.id = %deployment_id,
                    restate.deployment.address = %deployment_address,
                    "Going to remove the following methods from service type {} due to an update: {:?}.",
                    new_service_manifest.name.as_str(),
                    removed_handlers
                );
            }
        }

        Ok(())
    }

    fn create_service_revision(
        &self,
        service_name: &String,
        service: endpoint_manifest::Service,
        new_revision: identifiers::ServiceRevision,
        previous_service_revision: Option<&ServiceRevision>,
        service_level_settings_behavior: ServiceLevelSettingsBehavior,
    ) -> Result<ServiceRevision, SchemaError> {
        let service_type = ServiceType::from(service.ty);

        // --- Figure out service options

        macro_rules! resolve_optional_config_option {
            ($get_from_current:expr, $name_from_previous:ident) => {
                $get_from_current.or(if service_level_settings_behavior.preserve() {
                    previous_service_revision.and_then(|old_svc| old_svc.$name_from_previous)
                } else {
                    None
                })
            };
        }

        let public = service
            .ingress_private
            .map(bool::not)
            .or(if service_level_settings_behavior.preserve() {
                previous_service_revision.map(|old_svc| old_svc.public)
            } else {
                None
            })
            .unwrap_or(true);
        let idempotency_retention = service.idempotency_retention_duration().or(
            if service_level_settings_behavior.preserve() {
                previous_service_revision.and_then(|old_svc| old_svc.idempotency_retention)
            } else {
                // TODO(slinydeveloper) Remove this in Restate 1.6, no need for this defaulting anymore!
                Some(DEFAULT_IDEMPOTENCY_RETENTION)
            },
        );
        let journal_retention = resolve_optional_config_option!(
            service.journal_retention_duration(),
            journal_retention
        );
        let workflow_completion_retention = if service_level_settings_behavior.preserve()
            // Retain previous value only if new service and old one are both workflows
            && service_type == ServiceType::Workflow
            && previous_service_revision.map(|old_svc| old_svc.ty == ServiceType::Workflow).unwrap_or(false)
        {
            previous_service_revision.and_then(|old_svc| old_svc.workflow_completion_retention)
        } else if service_type == ServiceType::Workflow {
            // TODO(slinydeveloper) Remove this in Restate 1.6, no need for this defaulting anymore!
            Some(DEFAULT_WORKFLOW_COMPLETION_RETENTION)
        } else {
            None
        };
        let inactivity_timeout = resolve_optional_config_option!(
            service.inactivity_timeout_duration(),
            inactivity_timeout
        );
        let abort_timeout =
            resolve_optional_config_option!(service.abort_timeout_duration(), abort_timeout);
        let retry_policy_initial_interval = resolve_optional_config_option!(
            service.retry_policy_initial_interval(),
            retry_policy_initial_interval
        );
        let retry_policy_max_interval = resolve_optional_config_option!(
            service.retry_policy_max_interval(),
            retry_policy_max_interval
        );
        let retry_policy_exponentiation_factor = resolve_optional_config_option!(
            service.retry_policy_exponentiation_factor.map(|f| f as f32),
            retry_policy_exponentiation_factor
        );
        let retry_policy_max_attempts = resolve_optional_config_option!(
            service
                .retry_policy_max_attempts
                .map(|n| NonZeroUsize::new(n as usize).unwrap_or(NonZeroUsize::MIN)),
            retry_policy_max_attempts
        );
        let retry_policy_on_max_attempts = resolve_optional_config_option!(
            service.retry_policy_on_max_attempts.map(|f| match f {
                endpoint_manifest::RetryPolicyOnMaxAttempts::Pause => OnMaxAttempts::Pause,
                endpoint_manifest::RetryPolicyOnMaxAttempts::Kill => OnMaxAttempts::Kill,
            }),
            retry_policy_on_max_attempts
        );

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
            revision: new_revision,
            public,
            idempotency_retention,
            workflow_completion_retention,
            journal_retention,
            inactivity_timeout,
            abort_timeout,
            enable_lazy_state: service.enable_lazy_state,
            retry_policy_initial_interval,
            retry_policy_exponentiation_factor,
            retry_policy_max_attempts,
            retry_policy_max_interval,
            retry_policy_on_max_attempts,
            service_openapi_cache: Default::default(),
        })
    }

    pub(in crate::schema) fn update_deployment(
        &mut self,
        UpdateDeploymentRequest {
            deployment_id,
            deployment_address,
            additional_headers,
            discovery_response,
            overwrite,
        }: UpdateDeploymentRequest,
    ) -> Result<(), SchemaError> {
        // Look for an existing deployment with this ID
        let Some(existing_deployment) = self.schema.deployments.get(&deployment_id) else {
            return Err(SchemaError::NotFound(format!(
                "deployment with id '{deployment_id}'"
            )));
        };

        // Check protocol versions are equals
        if existing_deployment.supported_protocol_versions
            != discovery_response.supported_protocol_versions
        {
            return Err(SchemaError::Deployment(
                DeploymentError::DifferentSupportedProtocolVersions(
                    existing_deployment.supported_protocol_versions.clone(),
                    discovery_response.supported_protocol_versions.clone(),
                ),
            ));
        };

        // At this point there are two ways to go about this:
        // * The user didn't ask for overwriting, and in this case we simply update the type and delivery options as requested
        // * The user asked for the overwriting, just allow everything, and it's their business to not break things
        if overwrite == Overwrite::No {
            self.schema.deployments.insert(
                deployment_id,
                Deployment {
                    // We update only these 3 fields
                    ty: Self::create_deployment_ty(
                        deployment_address,
                        discovery_response.deployment_type_parameters,
                    ),
                    delivery_options: DeliveryOptions::new(additional_headers),
                    sdk_version: discovery_response.sdk_version,

                    // We keep these the same
                    id: deployment_id,
                    supported_protocol_versions: existing_deployment
                        .supported_protocol_versions
                        .clone(),
                    created_at: existing_deployment.created_at,
                    metadata: existing_deployment.metadata.clone(),
                    services: existing_deployment.services.clone(),
                },
            );

            self.mark_updated();

            Ok(())
        } else {
            let proposed_services: HashMap<_, _> = discovery_response
                .services
                .into_iter()
                .map(|svc| {
                    let service_name = svc.name.to_string();
                    validate_service_name(&service_name)?;
                    Ok::<_, ServiceError>((service_name, svc))
                })
                .collect::<Result<HashMap<_, _>, _>>()?;

            for service in existing_deployment.services.values() {
                // If a service is not available anymore in the new deployment, we need to remove it
                if !proposed_services.contains_key(&service.name) {
                    warn!(
                        restate.deployment.id = %deployment_id,
                        restate.deployment.address = %existing_deployment.ty.address_display(),
                        "Going to remove service {} due to a forced deployment update",
                        service.name
                    );
                }
            }

            // Compute service schemas
            let mut computed_services = HashMap::with_capacity(proposed_services.len());
            for (service_name, new_service) in proposed_services {
                let previous_service_revision = existing_deployment
                    .services
                    .get(&service_name)
                    .map(|arc| arc.deref());

                // Validate changes with previous revision, if any
                if let Some(previous_service_revision) = previous_service_revision {
                    self.validate_existing_service_revision_constraints(
                        deployment_id,
                        &deployment_address,
                        &service_name,
                        &new_service,
                        previous_service_revision,
                        RemovedHandlerBehavior::Warn,
                        ServiceTypeMismatchBehavior::Warn,
                    )?;
                }

                let service_revision = self.create_service_revision(
                    &service_name,
                    new_service,
                    previous_service_revision
                        .map(|old_svc| old_svc.revision)
                        .unwrap_or(1),
                    previous_service_revision,
                    ServiceLevelSettingsBehavior::UseDefaults,
                )?;

                match self.schema.active_service_revisions.get(&service_name) {
                    Some(ActiveServiceRevision {
                        deployment_id: latest_deployment,
                        ..
                    }) if latest_deployment == &deployment_id => {
                        info!(
                            rpc.service = %service_name,
                            "Overwriting existing service schemas for latest service"
                        );
                        // We let SchemaUpdater::into_inner do the index update
                    }
                    Some(_) => {
                        debug!(
                            rpc.service = %service_name,
                            "Keeping existing service schema as this update operation affected a draining deployment"
                        );
                    }
                    None => {
                        // We let SchemaUpdater::into_inner do the index update
                    }
                }

                computed_services.insert(service_name, Arc::new(service_revision));
            }

            self.schema.deployments.insert(
                deployment_id,
                Deployment {
                    // We update all these fields
                    ty: Self::create_deployment_ty(
                        deployment_address,
                        discovery_response.deployment_type_parameters,
                    ),
                    delivery_options: DeliveryOptions::new(additional_headers),
                    supported_protocol_versions: discovery_response.supported_protocol_versions,
                    sdk_version: discovery_response.sdk_version,
                    services: computed_services,

                    // We keep only these same as before
                    id: deployment_id,
                    created_at: existing_deployment.created_at,
                    metadata: existing_deployment.metadata.clone(),
                },
            );

            self.mark_updated();

            Ok(())
        }
    }

    /// Returns true if it was removed
    pub fn remove_deployment(&mut self, deployment_id: DeploymentId) -> bool {
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
            self.mark_updated();
            return true;
        }
        false
    }

    pub(in crate::schema) fn add_subscription(
        &mut self,
        id: Option<SubscriptionId>,
        source: Uri,
        sink: Uri,
        metadata: Option<HashMap<String, String>>,
    ) -> Result<SubscriptionId, SchemaError> {
        // generate id if not provided
        let id = id.unwrap_or_default();

        if self.schema.subscriptions.contains_key(&id) {
            return Err(SchemaError::Subscription(SubscriptionError::Override(id)));
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

        let subscription = Configuration::pinned()
            .ingress
            .create_kafka_subscription(id, source, sink, metadata.unwrap_or_default())
            .map_err(|e| SchemaError::Subscription(SubscriptionError::Validation(e.into())))?;

        self.schema.subscriptions.insert(id, subscription);
        self.mark_updated();

        Ok(id)
    }

    // Returns true if it was removed
    pub fn remove_subscription(&mut self, subscription_id: SubscriptionId) -> bool {
        if self.schema.subscriptions.remove(&subscription_id).is_some() {
            self.mark_updated();
            return true;
        }
        false
    }

    pub(in crate::schema) fn modify_service(
        &mut self,
        name: &str,
        modify_service_request: ModifyServiceRequest,
    ) -> Result<(), SchemaError> {
        self.apply_change_to_active_service_revision(name, |svc| {
            if let Some(new_public_value) = modify_service_request.public {
                svc.public = new_public_value;
                // Cleanup generated OpenAPI
                svc.service_openapi_cache = Default::default();
            }
            if let Some(new_idempotency_retention) = modify_service_request.idempotency_retention {
                svc.idempotency_retention = Some(new_idempotency_retention);
            }
            if let Some(new_workflow_completion_retention) =
                modify_service_request.workflow_completion_retention
            {
                // This applies only to workflow services
                if svc.ty != ServiceType::Workflow {
                    return Err(SchemaError::Service(
                        ServiceError::CannotModifyRetentionTime(svc.ty),
                    ));
                }
                svc.workflow_completion_retention = Some(new_workflow_completion_retention);
            }
            if let Some(new_journal_retention) = modify_service_request.journal_retention {
                svc.journal_retention = Some(new_journal_retention);
            }
            if let Some(new_inactivity_timeout) = modify_service_request.inactivity_timeout {
                svc.inactivity_timeout = Some(new_inactivity_timeout);
            }
            if let Some(new_abort_timeout) = modify_service_request.abort_timeout {
                svc.abort_timeout = Some(new_abort_timeout);
            }
            Ok(())
        })?;

        self.mark_updated();

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

        // Index gets updated by SchemaUpdate::into_inner

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
        let retry_policy_initial_interval = handler.retry_policy_initial_interval();
        let retry_policy_max_interval = handler.retry_policy_max_interval();
        let retry_policy_exponentiation_factor =
            handler.retry_policy_exponentiation_factor.map(|f| f as f32);
        let retry_policy_max_attempts = handler
            .retry_policy_max_attempts
            .map(|n| NonZeroUsize::new(n as usize).unwrap_or(NonZeroUsize::MIN));
        let retry_policy_on_max_attempts = handler.retry_policy_on_max_attempts.map(|f| match f {
            endpoint_manifest::RetryPolicyOnMaxAttempts::Pause => OnMaxAttempts::Pause,
            endpoint_manifest::RetryPolicyOnMaxAttempts::Kill => OnMaxAttempts::Kill,
        });

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
            retry_policy_initial_interval,
            retry_policy_exponentiation_factor,
            retry_policy_max_attempts,
            retry_policy_max_interval,
            journal_retention,
            idempotency_retention,
            workflow_completion_retention,
            inactivity_timeout,
            abort_timeout,
            enable_lazy_state: handler.enable_lazy_state,
            public: handler.ingress_private.map(bool::not),
            retry_policy_on_max_attempts,
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
            if let Some(schema) = &schema.json_schema
                && let Err(e) = jsonschema::options()
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

fn validate_service_name(name: &str) -> Result<(), ServiceError> {
    let lower_name = name.to_lowercase();
    if lower_name.starts_with("restate") || lower_name.eq_ignore_ascii_case("openapi") {
        Err(ServiceError::ReservedName(name.to_string()))
    } else {
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("invalid option '{name}'. Reason: {reason}")]
pub struct ValidationError {
    name: &'static str,
    reason: &'static str,
}

impl IngressOptions {
    fn create_kafka_subscription(
        &self,
        id: SubscriptionId,
        source: Source,
        sink: Sink,
        mut metadata: HashMap<String, String>,
    ) -> Result<Subscription, ValidationError> {
        // Retrieve the cluster option and merge them with subscription metadata
        let Source::Kafka { cluster, .. } = &source;
        let cluster_options = &self.get_kafka_cluster(cluster).ok_or(ValidationError {
            name: "source",
            reason: "specified cluster in the source URI does not exist. Make sure it is defined in the KafkaOptions",
        })?.additional_options;

        if cluster_options.contains_key("enable.auto.commit")
            || metadata.contains_key("enable.auto.commit")
        {
            warn!(
                "The configuration option enable.auto.commit should not be set and it will be ignored."
            );
        }
        if cluster_options.contains_key("enable.auto.offset.store")
            || metadata.contains_key("enable.auto.offset.store")
        {
            warn!(
                "The configuration option enable.auto.offset.store should not be set and it will be ignored."
            );
        }

        let group_id = metadata
            .get("group.id")
            .or_else(|| cluster_options.get("group.id"))
            .cloned()
            .unwrap_or_else(|| id.to_string());

        metadata.insert("group.id".into(), group_id);

        // Set client.id if unset
        if !(cluster_options.contains_key("client.id") || metadata.contains_key("client.id")) {
            metadata.insert("client.id".to_string(), "restate".to_string());
        }

        Ok(Subscription::new(id, source, sink, metadata))
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
mod tests;
