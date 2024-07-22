// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
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
use restate_types::schema::deployment::DeploymentMetadata;
use restate_types::schema::deployment::DeploymentSchemas;
use restate_types::schema::invocation_target::{
    InputRules, InputValidationRule, InvocationTargetMetadata, OutputContentTypeRule, OutputRules,
    DEFAULT_IDEMPOTENCY_RETENTION, DEFAULT_WORKFLOW_COMPLETION_RETENTION,
};
use restate_types::schema::service::{HandlerSchemas, ServiceLocation, ServiceSchemas};
use restate_types::schema::subscriptions::{
    EventReceiverServiceType, Sink, Source, Subscription, SubscriptionValidator,
};
use restate_types::schema::Schema;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use tracing::{info, warn};

/// Responsible for updating the provided [`Schema`] with new
/// schema information. It makes sure that the version of schema information
/// is incremented on changes.
#[derive(Debug, Default)]
pub struct SchemaUpdater {
    schema_information: Schema,
    modified: bool,
}

impl From<Schema> for SchemaUpdater {
    fn from(schema_information: Schema) -> Self {
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
        requested_deployment_id: Option<DeploymentId>,
        deployment_metadata: DeploymentMetadata,
        services: Vec<endpoint_manifest::Service>,
        force: bool,
    ) -> Result<DeploymentId, SchemaError> {
        let deployment_id: Option<DeploymentId>;

        let proposed_services: HashMap<_, _> = services
            .into_iter()
            .map(|c| ServiceName::try_from(c.name.to_string()).map(|name| (name, c)))
            .collect::<Result<HashMap<_, _>, _>>()?;

        // Did we find an existing deployment with same id or with a conflicting endpoint url?
        let found_existing_deployment = requested_deployment_id
            .and_then(|id| self.schema_information.find_existing_deployment_by_id(&id))
            .or_else(|| {
                self.schema_information
                    .find_existing_deployment_by_endpoint(&deployment_metadata.ty)
            });

        let mut services_to_remove = Vec::default();

        if let Some((existing_deployment_id, existing_deployment)) = found_existing_deployment {
            if requested_deployment_id.is_some_and(|dp| &dp != existing_deployment_id) {
                // The deployment id is different from the existing one, we don't accept that even
                // if force is used. It means that the user intended to update another deployment.
                return Err(SchemaError::Deployment(DeploymentError::IncorrectId {
                    requested: requested_deployment_id.expect("must be set"),
                    existing: *existing_deployment_id,
                }));
            }

            if force {
                deployment_id = Some(*existing_deployment_id);

                for service in &existing_deployment.services {
                    // If a service is not available anymore in the new deployment, we need to remove it
                    if !proposed_services.contains_key(&service.name) {
                        warn!(
                            restate.deployment.id = %existing_deployment_id,
                            restate.deployment.address = %deployment_metadata.address_display(),
                            "Going to remove service {} due to a forced deployment update",
                            service.name
                        );
                        services_to_remove.push(service.name.clone());
                    }
                }
            } else {
                return Err(SchemaError::Override(format!(
                    "deployment with id '{existing_deployment_id}'"
                )));
            }
        } else {
            // New deployment. Use the supplied deployment_id if passed, otherwise, generate one.
            deployment_id = requested_deployment_id.or_else(|| Some(DeploymentId::new()));
        }

        // We must have a deployment id by now, either a new or existing one.
        let deployment_id = deployment_id.unwrap();

        let mut services_to_add = HashMap::with_capacity(proposed_services.len());

        // Compute service schemas
        for (service_name, service) in proposed_services {
            let service_type = ServiceType::from(service.ty);
            let handlers = DiscoveredHandlerMetadata::compute_handlers(
                service
                    .handlers
                    .into_iter()
                    .map(|h| DiscoveredHandlerMetadata::from_schema(service_type, h))
                    .collect::<Result<Vec<_>, _>>()?,
            );

            // For the time being when updating we overwrite existing data
            let service_schema = if let Some(existing_service) =
                self.schema_information.services.get(service_name.as_ref())
            {
                let removed_handlers: Vec<String> = existing_service
                    .handlers
                    .keys()
                    .filter(|name| !handlers.contains_key(*name))
                    .map(|name| name.to_string())
                    .collect();

                if !removed_handlers.is_empty() {
                    if force {
                        warn!(
                            restate.deployment.id = %deployment_id,
                            restate.deployment.address = %deployment_metadata.address_display(),
                            "Going to remove the following methods from service type {} due to a forced deployment update: {:?}.",
                            service.name.as_str(),
                            removed_handlers
                        );
                    } else {
                        return Err(SchemaError::Service(ServiceError::RemovedHandlers(
                            service_name,
                            removed_handlers,
                        )));
                    }
                }

                if existing_service.ty != service_type {
                    if force {
                        warn!(
                            restate.deployment.id = %deployment_id,
                            restate.deployment.address = %deployment_metadata.address_display(),
                            "Going to overwrite service type {} due to a forced deployment update: {:?} != {:?}. This is a potentially dangerous operation, and might result in data loss.",
                            service_name,
                            existing_service.ty,
                            service_type
                        );
                    } else {
                        return Err(SchemaError::Service(ServiceError::DifferentType(
                            service_name,
                        )));
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

                service_schemas
            } else {
                ServiceSchemas {
                    revision: 1,
                    handlers,
                    ty: service_type,
                    location: ServiceLocation {
                        latest_deployment: deployment_id,
                        public: true,
                    },
                    idempotency_retention: DEFAULT_IDEMPOTENCY_RETENTION,
                    workflow_completion_retention: if service_type == ServiceType::Workflow {
                        Some(DEFAULT_WORKFLOW_COMPLETION_RETENTION)
                    } else {
                        None
                    },
                }
            };

            services_to_add.insert(service_name, service_schema);
        }

        for service_to_remove in services_to_remove {
            self.schema_information.services.remove(&service_to_remove);
        }

        let services_metadata = services_to_add
            .into_iter()
            .map(|(name, schema)| {
                let metadata = schema.as_service_metadata(name.clone().into_inner());
                self.schema_information
                    .services
                    .insert(name.into_inner(), schema);
                metadata
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

    pub fn remove_deployment(&mut self, deployment_id: DeploymentId) {
        if let Some(deployment) = self.schema_information.deployments.remove(&deployment_id) {
            for service_metadata in deployment.services {
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
                ))
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

                let ty = match handler_schemas.target_meta.target_ty {
                    InvocationTargetType::Workflow(WorkflowHandlerType::Workflow) => {
                        EventReceiverServiceType::Workflow
                    }
                    InvocationTargetType::VirtualObject(VirtualObjectHandlerType::Exclusive) => {
                        EventReceiverServiceType::VirtualObject
                    }
                    InvocationTargetType::Workflow(_) | InvocationTargetType::VirtualObject(_) => {
                        return Err(SchemaError::Subscription(
                            SubscriptionError::InvalidSinkSharedHandler(sink),
                        ))
                    }
                    InvocationTargetType::Service => EventReceiverServiceType::Service,
                };

                Sink::Service {
                    name: service_name.to_owned(),
                    handler: handler_name.to_owned(),
                    ty,
                }
            }
            _ => {
                return Err(SchemaError::Subscription(
                    SubscriptionError::InvalidSinkScheme(sink),
                ))
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
                    }
                    ModifyServiceChange::IdempotencyRetention(new_idempotency_retention) => {
                        schemas.idempotency_retention = new_idempotency_retention;
                        for h in schemas.handlers.values_mut() {
                            h.target_meta.idempotency_retention = new_idempotency_retention;
                        }
                    }
                    ModifyServiceChange::WorkflowCompletionRetention(
                        new_workflow_completion_retention,
                    ) => {
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
                            h.target_meta.completion_retention =
                                Some(new_workflow_completion_retention);
                        }
                    }
                }
            }
        }

        self.modified = true;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct DiscoveredHandlerMetadata {
    name: String,
    ty: InvocationTargetType,
    input: InputRules,
    output: OutputRules,
}

impl DiscoveredHandlerMetadata {
    fn from_schema(
        service_type: ServiceType,
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
                ))
            }
        };

        Ok(Self {
            name: handler.name.to_string(),
            ty,
            input: handler
                .input
                .map(|s| DiscoveredHandlerMetadata::input_rules_from_schema(&handler.name, s))
                .transpose()?
                .unwrap_or_default(),
            output: handler
                .output
                .map(DiscoveredHandlerMetadata::output_rules_from_schema)
                .transpose()?
                .unwrap_or_default(),
        })
    }

    fn input_rules_from_schema(
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
            if schema.json_schema.is_some() {
                input_validation_rules.push(InputValidationRule::JsonValue { content_type });
            } else {
                input_validation_rules.push(InputValidationRule::ContentType { content_type });
            }
        }

        Ok(InputRules {
            input_validation_rules,
        })
    }

    fn output_rules_from_schema(
        schema: endpoint_manifest::OutputPayload,
    ) -> Result<OutputRules, ServiceError> {
        Ok(if let Some(ct) = schema.content_type {
            OutputRules {
                content_type_rule: OutputContentTypeRule::Set {
                    content_type: HeaderValue::from_str(&ct)
                        .map_err(|e| ServiceError::BadOutputContentType(ct, e))?,
                    set_content_type_if_empty: schema.set_content_type_if_empty.unwrap_or(false),
                    has_json_schema: schema.json_schema.is_some(),
                },
            }
        } else {
            OutputRules {
                content_type_rule: OutputContentTypeRule::None,
            }
        })
    }

    fn compute_handlers(
        handlers: Vec<DiscoveredHandlerMetadata>,
    ) -> HashMap<String, HandlerSchemas> {
        handlers
            .into_iter()
            .map(|handler| {
                (
                    handler.name,
                    HandlerSchemas {
                        target_meta: InvocationTargetMetadata {
                            public: true,
                            idempotency_retention: DEFAULT_IDEMPOTENCY_RETENTION,
                            completion_retention: if handler.ty
                                == InvocationTargetType::Workflow(WorkflowHandlerType::Workflow)
                            {
                                Some(DEFAULT_WORKFLOW_COMPLETION_RETENTION)
                            } else {
                                None
                            },
                            target_ty: handler.ty,
                            input_rules: handler.input,
                            output_rules: handler.output,
                        },
                    },
                )
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use restate_test_util::{assert, assert_eq, let_assert};
    use restate_types::schema::deployment::{Deployment, DeploymentResolver};
    use restate_types::schema::service::ServiceMetadataResolver;

    use restate_types::Versioned;
    use test_log::test;

    const GREETER_SERVICE_NAME: &str = "greeter.Greeter";
    const ANOTHER_GREETER_SERVICE_NAME: &str = "greeter.AnotherGreeter";

    fn greeter_service() -> endpoint_manifest::Service {
        endpoint_manifest::Service {
            ty: endpoint_manifest::ServiceType::Service,
            name: GREETER_SERVICE_NAME.parse().unwrap(),
            handlers: vec![endpoint_manifest::Handler {
                name: "greet".parse().unwrap(),
                ty: None,
                input: None,
                output: None,
            }],
        }
    }

    fn greeter_virtual_object() -> endpoint_manifest::Service {
        endpoint_manifest::Service {
            ty: endpoint_manifest::ServiceType::VirtualObject,
            name: GREETER_SERVICE_NAME.parse().unwrap(),
            handlers: vec![endpoint_manifest::Handler {
                name: "greet".parse().unwrap(),
                ty: None,
                input: None,
                output: None,
            }],
        }
    }

    fn another_greeter_service() -> endpoint_manifest::Service {
        endpoint_manifest::Service {
            ty: endpoint_manifest::ServiceType::Service,
            name: ANOTHER_GREETER_SERVICE_NAME.parse().unwrap(),
            handlers: vec![endpoint_manifest::Handler {
                name: "another_greeter".parse().unwrap(),
                ty: None,
                input: None,
                output: None,
            }],
        }
    }

    #[test]
    fn register_new_deployment() {
        let schema_information = Schema::default();
        let initial_version = schema_information.version();
        let mut updater = SchemaUpdater::from(schema_information);

        let deployment = Deployment::mock();
        let deployment_id = updater
            .add_deployment(
                Some(deployment.id),
                deployment.metadata.clone(),
                vec![greeter_service()],
                false,
            )
            .unwrap();

        // Ensure we are using the pre-determined id
        assert_eq!(deployment.id, deployment_id);

        let schema = updater.into_inner();

        assert!(initial_version < schema.version());
        schema.assert_service_revision(GREETER_SERVICE_NAME, 1);
        schema.assert_service_deployment(GREETER_SERVICE_NAME, deployment_id);
        schema.assert_service_handler(GREETER_SERVICE_NAME, "greet");
    }

    #[test]
    fn register_new_deployment_add_unregistered_service() {
        let mut updater = SchemaUpdater::default();

        let deployment_1 = Deployment::mock_with_uri("http://localhost:9080");
        let deployment_2 = Deployment::mock_with_uri("http://localhost:9081");

        // Register first deployment
        updater
            .add_deployment(
                Some(deployment_1.id),
                deployment_1.metadata.clone(),
                vec![greeter_service()],
                false,
            )
            .unwrap();

        let schemas = updater.into_inner();

        schemas.assert_service_deployment(GREETER_SERVICE_NAME, deployment_1.id);
        assert!(schemas
            .resolve_latest_service(ANOTHER_GREETER_SERVICE_NAME)
            .is_none());

        updater = schemas.into();
        updater
            .add_deployment(
                Some(deployment_2.id),
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
        let deployment = Deployment::mock();

        updater.add_deployment(
            Some(deployment.id),
            deployment.metadata.clone(),
            vec![greeter_service()],
            false,
        )?;

        let schemas = updater.into_inner();

        assert!(schemas.assert_service(GREETER_SERVICE_NAME).public);

        let version_before_modification = schemas.version();
        updater = SchemaUpdater::from(schemas);
        updater.modify_service(
            GREETER_SERVICE_NAME.to_owned(),
            vec![ModifyServiceChange::Public(false)],
        )?;
        let schemas = updater.into_inner();

        assert!(version_before_modification < schemas.version());
        assert!(!schemas.assert_service(GREETER_SERVICE_NAME).public);

        updater = SchemaUpdater::from(schemas);
        updater.add_deployment(
            Some(deployment.id),
            deployment.metadata.clone(),
            vec![greeter_service()],
            true,
        )?;

        let schemas = updater.into_inner();
        assert!(!schemas.assert_service(GREETER_SERVICE_NAME).public);

        Ok(())
    }

    mod change_instance_type {
        use super::*;

        use restate_test_util::assert;
        use test_log::test;

        #[test]
        fn register_new_deployment_fails_changing_instance_type() {
            let mut updater = SchemaUpdater::default();

            let deployment_1 = Deployment::mock_with_uri("http://localhost:9080");
            let deployment_2 = Deployment::mock_with_uri("http://localhost:9081");

            updater
                .add_deployment(
                    Some(deployment_1.id),
                    deployment_1.metadata.clone(),
                    vec![greeter_service()],
                    false,
                )
                .unwrap();
            let schemas = updater.into_inner();

            schemas.assert_service_deployment(GREETER_SERVICE_NAME, deployment_1.id);

            let compute_result = SchemaUpdater::from(schemas).add_deployment(
                Some(deployment_2.id),
                deployment_2.metadata,
                vec![greeter_virtual_object()],
                false,
            );

            assert!(let &SchemaError::Service(
                ServiceError::DifferentType(_)
            ) = compute_result.unwrap_err());
        }
    }

    #[test]
    fn override_existing_deployment_removing_a_service() {
        let mut updater = SchemaUpdater::default();

        let deployment = Deployment::mock();
        updater
            .add_deployment(
                Some(deployment.id),
                deployment.metadata.clone(),
                vec![greeter_service(), another_greeter_service()],
                false,
            )
            .unwrap();

        let schemas = updater.into_inner();
        schemas.assert_service_deployment(GREETER_SERVICE_NAME, deployment.id);
        schemas.assert_service_deployment(ANOTHER_GREETER_SERVICE_NAME, deployment.id);

        updater = SchemaUpdater::from(schemas);
        updater
            .add_deployment(
                Some(deployment.id),
                deployment.metadata.clone(),
                vec![greeter_service()],
                true,
            )
            .unwrap();

        let schemas = updater.into_inner();

        schemas.assert_service_deployment(GREETER_SERVICE_NAME, deployment.id);
        assert!(schemas
            .resolve_latest_service(ANOTHER_GREETER_SERVICE_NAME)
            .is_none());
    }

    #[test]
    fn cannot_override_existing_deployment_endpoint_conflict() {
        let mut updater = SchemaUpdater::default();

        let deployment = Deployment::mock();
        updater
            .add_deployment(
                Some(deployment.id),
                deployment.metadata.clone(),
                vec![greeter_service()],
                false,
            )
            .unwrap();

        assert!(let SchemaError::Override(_) = updater.add_deployment(
            Some(deployment.id),
            deployment.metadata,
            vec![greeter_service()],
            false).unwrap_err()
        );
    }

    #[test]
    fn cannot_override_existing_deployment_existing_id_mismatch() {
        let mut updater = SchemaUpdater::default();

        let deployment = Deployment::mock();
        updater
            .add_deployment(
                Some(deployment.id),
                deployment.metadata.clone(),
                vec![greeter_service()],
                false,
            )
            .unwrap();

        let new_id = DeploymentId::new();

        let rejection = updater
            .add_deployment(
                Some(new_id),
                deployment.metadata,
                vec![greeter_service()],
                false,
            )
            .unwrap_err();
        let_assert!(
            SchemaError::Deployment(DeploymentError::IncorrectId {
                requested,
                existing
            }) = rejection
        );
        assert_eq!(new_id, requested);
        assert_eq!(deployment.id, existing);
    }

    #[test]
    fn register_two_deployments_then_remove_first() {
        let mut updater = SchemaUpdater::default();

        let deployment_1 = Deployment::mock_with_uri("http://localhost:9080");
        let deployment_2 = Deployment::mock_with_uri("http://localhost:9081");

        updater
            .add_deployment(
                Some(deployment_1.id),
                deployment_1.metadata.clone(),
                vec![greeter_service(), another_greeter_service()],
                false,
            )
            .unwrap();
        updater
            .add_deployment(
                Some(deployment_2.id),
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
        updater = schemas.into();
        updater.remove_deployment(deployment_1.id);
        let schemas = updater.into_inner();

        schemas.assert_service_deployment(GREETER_SERVICE_NAME, deployment_2.id);
        schemas.assert_service_revision(GREETER_SERVICE_NAME, 2);
        assert!(version_before_removal < schemas.version());
        assert!(schemas
            .resolve_latest_service(ANOTHER_GREETER_SERVICE_NAME)
            .is_none());
        assert!(schemas.get_deployment(&deployment_1.id).is_none());
    }

    mod remove_method {
        use super::*;

        use restate_test_util::{check, let_assert};
        use test_log::test;

        fn greeter_v1_service() -> endpoint_manifest::Service {
            endpoint_manifest::Service {
                ty: endpoint_manifest::ServiceType::Service,
                name: GREETER_SERVICE_NAME.parse().unwrap(),
                handlers: vec![
                    endpoint_manifest::Handler {
                        name: "greet".parse().unwrap(),
                        ty: None,
                        input: None,
                        output: None,
                    },
                    endpoint_manifest::Handler {
                        name: "doSomething".parse().unwrap(),
                        ty: None,
                        input: None,
                        output: None,
                    },
                ],
            }
        }

        fn greeter_v2_service() -> endpoint_manifest::Service {
            endpoint_manifest::Service {
                ty: endpoint_manifest::ServiceType::Service,
                name: GREETER_SERVICE_NAME.parse().unwrap(),
                handlers: vec![endpoint_manifest::Handler {
                    name: "greet".parse().unwrap(),
                    ty: None,
                    input: None,
                    output: None,
                }],
            }
        }

        #[test]
        fn reject_removing_existing_methods() {
            let mut updater = SchemaUpdater::default();

            let deployment_1 = Deployment::mock_with_uri("http://localhost:9080");
            let deployment_2 = Deployment::mock_with_uri("http://localhost:9081");

            updater
                .add_deployment(
                    Some(deployment_1.id),
                    deployment_1.metadata,
                    vec![greeter_v1_service()],
                    false,
                )
                .unwrap();
            let schemas = updater.into_inner();
            schemas.assert_service_revision(GREETER_SERVICE_NAME, 1);

            updater = schemas.into();
            let rejection = updater
                .add_deployment(
                    Some(deployment_2.id),
                    deployment_2.metadata,
                    vec![greeter_v2_service()],
                    false,
                )
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
}
