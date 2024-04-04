// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use arc_swap::ArcSwap;
use http::{HeaderValue, Uri};
use restate_schema_api::component::{
    ComponentMetadata, ComponentType, HandlerMetadata, HandlerType,
};
use restate_schema_api::deployment::{DeploymentMetadata, DeploymentType};
use restate_schema_api::invocation_target::{
    InputRules, InputValidationRule, OutputContentTypeRule, OutputRules,
};
use restate_schema_api::subscription::{
    EventReceiverComponentType, Sink, Source, Subscription, SubscriptionValidator,
};
use restate_service_protocol::discovery::schema;
use restate_types::identifiers::{ComponentRevision, DeploymentId, SubscriptionId};
use serde::{Deserialize, Serialize};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{info, warn};

mod component;
mod deployment;
mod error;
mod invocation_target;
mod subscriptions;

use crate::component::{
    check_reserved_name, to_component_type, ComponentLocation, ComponentSchemas,
    DiscoveredHandlerMetadata,
};
use crate::deployment::DeploymentSchemas;
pub use error::*;
use restate_types::{Version, Versioned};

/// Immutable view of the schema registry.
///
/// Temporary bridge until users are migrated to directly using the metadata
/// provided schema registry.
#[derive(Debug, Default, Clone)]
pub struct SchemaView(Arc<ArcSwap<SchemaRegistry>>);

impl SchemaView {
    /// Update current view with new schema registry.
    pub fn update(&self, schema_registry: Arc<SchemaRegistry>) {
        self.0.store(schema_registry);
    }
}

/// The schema registry
#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct SchemaRegistry {
    version: Version,
    components: HashMap<String, ComponentSchemas>,
    deployments: HashMap<DeploymentId, DeploymentSchemas>,
    subscriptions: HashMap<SubscriptionId, Subscription>,
}

impl SchemaRegistry {
    pub fn increment_version(&mut self) {
        self.version = self.version.next();
    }

    pub fn add_subscription<V: SubscriptionValidator>(
        &mut self,
        id: Option<SubscriptionId>,
        source: Uri,
        sink: Uri,
        metadata: Option<HashMap<String, String>>,
        validator: &V,
    ) -> Result<SubscriptionId, Error> {
        // generate id if not provided
        let id = id.unwrap_or_default();

        if self.subscriptions.contains_key(&id) {
            return Err(Error::Override);
        }

        // TODO This logic to parse source and sink should be moved elsewhere to abstract over the known source/sink providers
        //  Maybe together with the validator?

        // Parse source
        let source = match source.scheme_str() {
            Some("kafka") => {
                let cluster_name = source
                    .authority()
                    .ok_or_else(|| {
                        Error::Subscription(SubscriptionError::InvalidKafkaSourceAuthority(
                            source.clone(),
                        ))
                    })?
                    .as_str();
                let topic_name = &source.path()[1..];
                Source::Kafka {
                    cluster: cluster_name.to_string(),
                    topic: topic_name.to_string(),
                    ordering_key_format: Default::default(),
                }
            }
            _ => {
                return Err(Error::Subscription(SubscriptionError::InvalidSourceScheme(
                    source,
                )))
            }
        };

        // Parse sink
        let sink = match sink.scheme_str() {
            Some("component") => {
                let component_name = sink
                    .authority()
                    .ok_or_else(|| {
                        Error::Subscription(SubscriptionError::InvalidComponentSinkAuthority(
                            sink.clone(),
                        ))
                    })?
                    .as_str();
                let handler_name = &sink.path()[1..];

                // Retrieve component and handler in the schema registry
                let component_schemas = self.components.get(component_name).ok_or_else(|| {
                    Error::Subscription(SubscriptionError::SinkComponentNotFound(sink.clone()))
                })?;
                if !component_schemas.handlers.contains_key(handler_name) {
                    return Err(Error::Subscription(
                        SubscriptionError::SinkComponentNotFound(sink),
                    ));
                }

                let ty = match component_schemas.ty {
                    ComponentType::VirtualObject => EventReceiverComponentType::VirtualObject {
                        ordering_key_is_key: false,
                    },
                    ComponentType::Service => EventReceiverComponentType::Service,
                };

                Sink::Component {
                    name: component_name.to_owned(),
                    handler: handler_name.to_owned(),
                    ty,
                }
            }
            _ => {
                return Err(Error::Subscription(SubscriptionError::InvalidSinkScheme(
                    sink,
                )))
            }
        };

        let subscription = validator
            .validate(Subscription::new(
                id,
                source,
                sink,
                metadata.unwrap_or_default(),
            ))
            .map_err(|e| Error::Subscription(SubscriptionError::Validation(e.into())))?;

        self.subscriptions.insert(id, subscription);

        Ok(id)
    }

    pub fn remove_subscription(&mut self, subscription_id: SubscriptionId) -> Option<Subscription> {
        self.subscriptions.remove(&subscription_id)
    }

    /// Find existing deployment that knows about a particular endpoint
    fn find_existing_deployment_by_endpoint(
        &self,
        endpoint: &DeploymentType,
    ) -> Option<(&DeploymentId, &DeploymentSchemas)> {
        self.deployments.iter().find(|(_, schemas)| {
            schemas.metadata.ty.protocol_type() == endpoint.protocol_type()
                && schemas.metadata.ty.normalized_address() == endpoint.normalized_address()
        })
    }

    fn find_existing_deployment_by_id(
        &self,
        deployment_id: &DeploymentId,
    ) -> Option<(&DeploymentId, &DeploymentSchemas)> {
        self.deployments.iter().find(|(id, _)| deployment_id == *id)
    }

    pub fn add_deployment(
        &mut self,
        requested_deployment_id: Option<DeploymentId>,
        deployment_metadata: DeploymentMetadata,
        components: Vec<schema::Component>,
        force: bool,
    ) -> Result<DeploymentId, Error> {
        let deployment_id: Option<DeploymentId>;

        let proposed_components: HashMap<_, _> = components
            .into_iter()
            .map(|c| (c.fully_qualified_component_name.to_string(), c))
            .collect();

        // Did we find an existing deployment with same id or with a conflicting endpoint url?
        let found_existing_deployment = requested_deployment_id
            .and_then(|id| self.find_existing_deployment_by_id(&id))
            .or_else(|| self.find_existing_deployment_by_endpoint(&deployment_metadata.ty));

        let mut components_to_remove = Vec::default();

        if let Some((existing_deployment_id, existing_deployment)) = found_existing_deployment {
            if requested_deployment_id.is_some_and(|dp| &dp != existing_deployment_id) {
                // The deployment id is different from the existing one, we don't accept that even
                // if force is used. It means that the user intended to update another deployment.
                return Err(Error::Deployment(DeploymentError::IncorrectId {
                    requested: requested_deployment_id.expect("must be set"),
                    existing: *existing_deployment_id,
                }));
            }

            if force {
                deployment_id = Some(*existing_deployment_id);

                for component in &existing_deployment.components {
                    // If a component is not available anymore in the new deployment, we need to remove it
                    if !proposed_components.contains_key(&component.name) {
                        warn!(
                            restate.deployment.id = %existing_deployment_id,
                            restate.deployment.address = %deployment_metadata.address_display(),
                            "Going to remove component {} due to a forced deployment update",
                            component.name
                        );
                        components_to_remove.push(component.name.clone());
                    }
                }
            } else {
                return Err(Error::Override);
            }
        } else {
            // New deployment. Use the supplied deployment_id if passed, otherwise, generate one.
            deployment_id = requested_deployment_id.or_else(|| Some(DeploymentId::new()));
        }

        // We must have a deployment id by now, either a new or existing one.
        let deployment_id = deployment_id.unwrap();

        let mut components_to_add = HashMap::with_capacity(proposed_components.len());

        // Compute component schemas
        for (component_name, component) in proposed_components {
            check_reserved_name(&component_name)?;
            let component_type = to_component_type(component.component_type);
            let handlers = ComponentSchemas::compute_handlers(
                component_type,
                component
                    .handlers
                    .into_iter()
                    .map(|h| DiscoveredHandlerMetadata::from_schema(component_type, h))
                    .collect::<Result<Vec<_>, _>>()?,
            );

            // For the time being when updating we overwrite existing data
            let component_schema = if let Some(existing_component) =
                self.components.get(&component_name)
            {
                let removed_handlers: Vec<String> = existing_component
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
                            "Going to remove the following methods from component type {} due to a forced deployment update: {:?}.",
                            component.fully_qualified_component_name.as_str(),
                            removed_handlers
                        );
                    } else {
                        return Err(Error::Component(ComponentError::RemovedHandlers(
                            component_name,
                            removed_handlers,
                        )));
                    }
                }

                if existing_component.ty != component_type {
                    if force {
                        warn!(
                            restate.deployment.id = %deployment_id,
                            restate.deployment.address = %deployment_metadata.address_display(),
                            "Going to overwrite component type {} due to a forced deployment update: {:?} != {:?}. This is a potentially dangerous operation, and might result in data loss.",
                            component_name,
                            existing_component.ty,
                            component_type
                        );
                    } else {
                        return Err(Error::Component(ComponentError::DifferentType(
                            component_name,
                        )));
                    }
                }

                info!(
                    rpc.service = component_name,
                    "Overwriting existing component schemas"
                );
                let mut component_schemas = existing_component.clone();
                component_schemas.revision = existing_component.revision.wrapping_add(1);
                component_schemas.ty = component_type;
                component_schemas.handlers = handlers;
                component_schemas.location.latest_deployment = deployment_id;

                component_schemas
            } else {
                ComponentSchemas {
                    revision: 1,
                    handlers,
                    ty: component_type,
                    location: ComponentLocation {
                        latest_deployment: deployment_id,
                        public: true,
                    },
                }
            };

            components_to_add.insert(component_name, component_schema);
        }

        for component_to_remove in components_to_remove {
            self.components.remove(&component_to_remove);
        }

        let components_metadata = components_to_add
            .into_iter()
            .map(|(name, schema)| {
                let metadata = schema.as_component_metadata(name.clone());
                self.components.insert(name, schema);
                metadata
            })
            .collect();

        self.deployments.insert(
            deployment_id,
            DeploymentSchemas {
                components: components_metadata,
                metadata: deployment_metadata,
            },
        );

        Ok(deployment_id)
    }

    pub fn remove_deployment(&mut self, deployment_id: DeploymentId) -> bool {
        if let Some(deployment) = self.deployments.remove(&deployment_id) {
            for component_metadata in deployment.components {
                match self.components.entry(component_metadata.name) {
                    // we need to check for the right revision in the component has been overwritten
                    // by a different deployment.
                    Entry::Occupied(entry)
                        if entry.get().revision == component_metadata.revision =>
                    {
                        entry.remove();
                    }
                    _ => {}
                }
            }
            true
        } else {
            false
        }
    }

    pub fn modify_component(&mut self, component_name: String, public: bool) -> Result<(), Error> {
        if let Some(schemas) = self.components.get_mut(&component_name) {
            // Update the public field
            schemas.location.public = public;
            for h in schemas.handlers.values_mut() {
                h.target_meta.public = public;
            }
        } else {
            return Err(Error::NotFound);
        }

        Ok(())
    }

    pub(crate) fn use_component_schema<F, R>(
        &self,
        component_name: impl AsRef<str>,
        f: F,
    ) -> Option<R>
    where
        F: FnOnce(&ComponentSchemas) -> R,
    {
        self.components.get(component_name.as_ref()).map(f)
    }
}

impl Versioned for SchemaRegistry {
    fn version(&self) -> Version {
        self.version
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use restate_schema_api::component::ComponentMetadataResolver;
    use restate_schema_api::deployment::{Deployment, DeploymentResolver};
    use restate_schema_api::invocation_target::InvocationTargetResolver;
    use restate_test_util::{assert, assert_eq, let_assert};

    use test_log::test;

    impl SchemaRegistry {
        #[track_caller]
        pub(crate) fn assert_component_handler(&self, component_name: &str, handler_name: &str) {
            assert!(self
                .resolve_latest_invocation_target(component_name, handler_name)
                .is_some());
        }

        #[track_caller]
        pub(crate) fn assert_component_revision(
            &self,
            component_name: &str,
            revision: ComponentRevision,
        ) {
            assert_eq!(
                self.resolve_latest_component(component_name)
                    .unwrap()
                    .revision,
                revision
            );
        }

        #[track_caller]
        pub(crate) fn assert_component_deployment(
            &self,
            component_name: &str,
            deployment_id: DeploymentId,
        ) {
            assert_eq!(
                self.resolve_latest_component(component_name)
                    .unwrap()
                    .deployment_id,
                deployment_id
            );
        }

        #[track_caller]
        pub(crate) fn assert_component(&self, component_name: &str) -> ComponentMetadata {
            self.resolve_latest_component(component_name).unwrap()
        }
    }

    const GREETER_SERVICE_NAME: &str = "greeter.Greeter";
    const ANOTHER_GREETER_SERVICE_NAME: &str = "greeter.AnotherGreeter";

    fn greeter_service() -> schema::Component {
        schema::Component {
            component_type: schema::ComponentType::Service,
            fully_qualified_component_name: GREETER_SERVICE_NAME.parse().unwrap(),
            handlers: vec![schema::Handler {
                name: "greet".parse().unwrap(),
                handler_type: None,
                input: None,
                output: None,
            }],
        }
    }

    fn greeter_virtual_object() -> schema::Component {
        schema::Component {
            component_type: schema::ComponentType::VirtualObject,
            fully_qualified_component_name: GREETER_SERVICE_NAME.parse().unwrap(),
            handlers: vec![schema::Handler {
                name: "greet".parse().unwrap(),
                handler_type: None,
                input: None,
                output: None,
            }],
        }
    }

    fn another_greeter_service() -> schema::Component {
        schema::Component {
            component_type: schema::ComponentType::Service,
            fully_qualified_component_name: ANOTHER_GREETER_SERVICE_NAME.parse().unwrap(),
            handlers: vec![schema::Handler {
                name: "another_greeter".parse().unwrap(),
                handler_type: None,
                input: None,
                output: None,
            }],
        }
    }

    #[test]
    fn register_new_deployment() {
        let mut schemas = SchemaRegistry::default();

        let deployment = Deployment::mock();
        let deployment_id = schemas
            .add_deployment(
                Some(deployment.id),
                deployment.metadata.clone(),
                vec![greeter_service()],
                false,
            )
            .unwrap();

        // Ensure we are using the pre-determined id
        assert_eq!(deployment.id, deployment_id);

        schemas.assert_component_revision(GREETER_SERVICE_NAME, 1);
        schemas.assert_component_deployment(GREETER_SERVICE_NAME, deployment_id);
        schemas.assert_component_handler(GREETER_SERVICE_NAME, "greet");
    }

    #[test]
    fn register_new_deployment_add_unregistered_service() {
        let mut schemas = SchemaRegistry::default();

        let deployment_1 = Deployment::mock_with_uri("http://localhost:9080");
        let deployment_2 = Deployment::mock_with_uri("http://localhost:9081");

        // Register first deployment
        schemas
            .add_deployment(
                Some(deployment_1.id),
                deployment_1.metadata.clone(),
                vec![greeter_service()],
                false,
            )
            .unwrap();

        schemas.assert_component_deployment(GREETER_SERVICE_NAME, deployment_1.id);
        assert!(schemas
            .resolve_latest_component(ANOTHER_GREETER_SERVICE_NAME)
            .is_none());

        schemas
            .add_deployment(
                Some(deployment_2.id),
                deployment_2.metadata.clone(),
                vec![greeter_service(), another_greeter_service()],
                false,
            )
            .unwrap();

        schemas.assert_component_deployment(GREETER_SERVICE_NAME, deployment_2.id);
        schemas.assert_component_revision(GREETER_SERVICE_NAME, 2);
        schemas.assert_component_deployment(ANOTHER_GREETER_SERVICE_NAME, deployment_2.id);
        schemas.assert_component_revision(ANOTHER_GREETER_SERVICE_NAME, 1);
    }

    /// This test case ensures that https://github.com/restatedev/restate/issues/1205 works
    #[test]
    fn force_deploy_private_service() -> Result<(), Error> {
        let mut schemas = SchemaRegistry::default();
        let deployment = Deployment::mock();

        schemas.add_deployment(
            Some(deployment.id),
            deployment.metadata.clone(),
            vec![greeter_service()],
            false,
        )?;

        assert!(schemas.assert_component(GREETER_SERVICE_NAME).public);

        schemas.modify_component(GREETER_SERVICE_NAME.to_owned(), false)?;

        assert!(!schemas.assert_component(GREETER_SERVICE_NAME).public);

        schemas.add_deployment(
            Some(deployment.id),
            deployment.metadata.clone(),
            vec![greeter_service()],
            true,
        )?;
        assert!(!schemas.assert_component(GREETER_SERVICE_NAME).public);

        Ok(())
    }

    mod change_instance_type {
        use super::*;

        use restate_test_util::assert;
        use test_log::test;

        #[test]
        fn register_new_deployment_fails_changing_instance_type() {
            let mut schemas = SchemaRegistry::default();

            let deployment_1 = Deployment::mock_with_uri("http://localhost:9080");
            let deployment_2 = Deployment::mock_with_uri("http://localhost:9081");

            schemas
                .add_deployment(
                    Some(deployment_1.id),
                    deployment_1.metadata.clone(),
                    vec![greeter_service()],
                    false,
                )
                .unwrap();

            schemas.assert_component_deployment(GREETER_SERVICE_NAME, deployment_1.id);

            let compute_result = schemas.add_deployment(
                Some(deployment_2.id),
                deployment_2.metadata,
                vec![greeter_virtual_object()],
                false,
            );

            assert!(let &Error::Component(
                ComponentError::DifferentType(_)
            ) = compute_result.unwrap_err());
        }
    }

    #[test]
    fn override_existing_deployment_removing_a_service() {
        let mut schemas = SchemaRegistry::default();

        let deployment = Deployment::mock();
        schemas
            .add_deployment(
                Some(deployment.id),
                deployment.metadata.clone(),
                vec![greeter_service(), another_greeter_service()],
                false,
            )
            .unwrap();

        schemas.assert_component_deployment(GREETER_SERVICE_NAME, deployment.id);
        schemas.assert_component_deployment(ANOTHER_GREETER_SERVICE_NAME, deployment.id);

        schemas
            .add_deployment(
                Some(deployment.id),
                deployment.metadata.clone(),
                vec![greeter_service()],
                true,
            )
            .unwrap();

        schemas.assert_component_deployment(GREETER_SERVICE_NAME, deployment.id);
        assert!(schemas
            .resolve_latest_component(ANOTHER_GREETER_SERVICE_NAME)
            .is_none());
    }

    #[test]
    fn cannot_override_existing_deployment_endpoint_conflict() {
        let mut schemas = SchemaRegistry::default();

        let deployment = Deployment::mock();
        schemas
            .add_deployment(
                Some(deployment.id),
                deployment.metadata.clone(),
                vec![greeter_service()],
                false,
            )
            .unwrap();

        assert!(let Error::Override = schemas.add_deployment(
            Some(deployment.id),
            deployment.metadata,
            vec![greeter_service()],
            false).unwrap_err()
        );
    }

    #[test]
    fn cannot_override_existing_deployment_existing_id_mismatch() {
        let mut schemas = SchemaRegistry::default();

        let deployment = Deployment::mock();
        schemas
            .add_deployment(
                Some(deployment.id),
                deployment.metadata.clone(),
                vec![greeter_service()],
                false,
            )
            .unwrap();

        let new_id = DeploymentId::new();

        let rejection = schemas
            .add_deployment(
                Some(new_id),
                deployment.metadata,
                vec![greeter_service()],
                false,
            )
            .unwrap_err();
        let_assert!(
            Error::Deployment(DeploymentError::IncorrectId {
                requested,
                existing
            }) = rejection
        );
        assert_eq!(new_id, requested);
        assert_eq!(deployment.id, existing);
    }

    #[test]
    fn register_two_deployments_then_remove_first() {
        let mut schemas = SchemaRegistry::default();

        let deployment_1 = Deployment::mock_with_uri("http://localhost:9080");
        let deployment_2 = Deployment::mock_with_uri("http://localhost:9081");

        schemas
            .add_deployment(
                Some(deployment_1.id),
                deployment_1.metadata.clone(),
                vec![greeter_service(), another_greeter_service()],
                false,
            )
            .unwrap();
        schemas
            .add_deployment(
                Some(deployment_2.id),
                deployment_2.metadata.clone(),
                vec![greeter_service()],
                false,
            )
            .unwrap();

        schemas.assert_component_deployment(GREETER_SERVICE_NAME, deployment_2.id);
        schemas.assert_component_revision(GREETER_SERVICE_NAME, 2);
        schemas.assert_component_deployment(ANOTHER_GREETER_SERVICE_NAME, deployment_1.id);
        schemas.assert_component_revision(ANOTHER_GREETER_SERVICE_NAME, 1);

        schemas.remove_deployment(deployment_1.id);

        schemas.assert_component_deployment(GREETER_SERVICE_NAME, deployment_2.id);
        schemas.assert_component_revision(GREETER_SERVICE_NAME, 2);
        assert!(schemas
            .resolve_latest_component(ANOTHER_GREETER_SERVICE_NAME)
            .is_none());
        assert!(schemas.get_deployment(&deployment_1.id).is_none());
    }

    mod remove_method {
        use super::*;

        use restate_test_util::{check, let_assert};
        use test_log::test;

        fn greeter_v1_service() -> schema::Component {
            schema::Component {
                component_type: schema::ComponentType::Service,
                fully_qualified_component_name: GREETER_SERVICE_NAME.parse().unwrap(),
                handlers: vec![
                    schema::Handler {
                        name: "greet".parse().unwrap(),
                        handler_type: None,
                        input: None,
                        output: None,
                    },
                    schema::Handler {
                        name: "doSomething".parse().unwrap(),
                        handler_type: None,
                        input: None,
                        output: None,
                    },
                ],
            }
        }

        fn greeter_v2_service() -> schema::Component {
            schema::Component {
                component_type: schema::ComponentType::Service,
                fully_qualified_component_name: GREETER_SERVICE_NAME.parse().unwrap(),
                handlers: vec![schema::Handler {
                    name: "greet".parse().unwrap(),
                    handler_type: None,
                    input: None,
                    output: None,
                }],
            }
        }

        #[test]
        fn reject_removing_existing_methods() {
            let mut schemas = SchemaRegistry::default();

            let deployment_1 = Deployment::mock_with_uri("http://localhost:9080");
            let deployment_2 = Deployment::mock_with_uri("http://localhost:9081");

            schemas
                .add_deployment(
                    Some(deployment_1.id),
                    deployment_1.metadata,
                    vec![greeter_v1_service()],
                    false,
                )
                .unwrap();

            schemas.assert_component_revision(GREETER_SERVICE_NAME, 1);

            let rejection = schemas
                .add_deployment(
                    Some(deployment_2.id),
                    deployment_2.metadata,
                    vec![greeter_v2_service()],
                    false,
                )
                .unwrap_err();

            schemas.assert_component_revision(GREETER_SERVICE_NAME, 1); // unchanged

            let_assert!(
                Error::Component(ComponentError::RemovedHandlers(service, missing_methods)) =
                    rejection
            );
            check!(service == GREETER_SERVICE_NAME);
            check!(missing_methods == &["doSomething"]);
        }
    }
}
