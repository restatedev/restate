use super::*;

use super::component::{check_reserved_name, to_component_type, ComponentError};
use restate_schema_api::deployment::DeploymentType;
use restate_types::identifiers::DeploymentId;

#[derive(Debug, thiserror::Error, codederror::CodedError)]
pub enum DeploymentError {
    #[error("existing deployment id is different from requested (requested = {requested}, existing = {existing})")]
    #[code(restate_errors::META0004)]
    IncorrectId {
        requested: DeploymentId,
        existing: DeploymentId,
    },
}

impl SchemasInner {
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

    pub(crate) fn compute_new_deployment(
        &self,
        requested_deployment_id: Option<DeploymentId>,
        deployment_metadata: DeploymentMetadata,
        components: Vec<schema::Component>,
        force: bool,
    ) -> Result<Vec<SchemasUpdateCommand>, ErrorKind> {
        let mut result_commands = Vec::with_capacity(1 + components.len());
        let deployment_id: Option<DeploymentId>;

        let proposed_components: HashMap<_, _> = components
            .into_iter()
            .map(|c| (c.fully_qualified_component_name.to_string(), c))
            .collect();

        // Did we find an existing deployment with same id or with a conflicting endpoint url?
        let found_existing_deployment = requested_deployment_id
            .and_then(|id| self.find_existing_deployment_by_id(&id))
            .or_else(|| self.find_existing_deployment_by_endpoint(&deployment_metadata.ty));

        if let Some((existing_deployment_id, existing_deployment)) = found_existing_deployment {
            if requested_deployment_id.is_some_and(|dp| &dp != existing_deployment_id) {
                // The deployment id is different from the existing one, we don't accept that even
                // if force is used. It means that the user intended to update another deployment.
                return Err(ErrorKind::Deployment(DeploymentError::IncorrectId {
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
                        result_commands.push(SchemasUpdateCommand::RemoveComponent {
                            name: component.name.clone(),
                            revision: component.revision,
                        });
                    }
                }
            } else {
                return Err(ErrorKind::Override);
            }
        } else {
            // New deployment. Use the supplied deployment_id if passed, otherwise, generate one.
            deployment_id = requested_deployment_id.or_else(|| Some(DeploymentId::new()));
        }

        // We must have a deployment id by now, either a new or existing one.
        let deployment_id = deployment_id.unwrap();

        // Push the InsertDeployment command
        result_commands.push(SchemasUpdateCommand::InsertDeployment {
            deployment_id,
            metadata: deployment_metadata.clone(),
        });

        // Compute component commands
        for (component_name, component) in proposed_components {
            check_reserved_name(&component_name)?;
            let component_type = to_component_type(component.component_type);

            // For the time being when updating we overwrite existing data
            let revision = if let Some(existing_component) = self.components.get(&component_name) {
                let removed_handlers: Vec<String> = existing_component
                    .handlers
                    .keys()
                    .filter(|name| !component.handlers.iter().any(|h| h.name.as_str() == *name))
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
                        return Err(ErrorKind::Component(ComponentError::RemovedHandlers(
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
                        return Err(ErrorKind::Component(ComponentError::DifferentType(
                            component_name,
                        )));
                    }
                }

                existing_component.revision.wrapping_add(1)
            } else {
                1
            };

            result_commands.push(SchemasUpdateCommand::InsertComponent(
                InsertComponentUpdateCommand {
                    name: component_name,
                    revision,
                    ty: component_type,
                    deployment_id,
                    handlers: component
                        .handlers
                        .into_iter()
                        .map(|h| DiscoveredHandlerMetadata::from_schema(component_type, h))
                        .collect::<Result<Vec<_>, _>>()?,
                },
            ));
        }

        Ok(result_commands)
    }

    pub(crate) fn apply_insert_deployment(
        &mut self,
        deployment_id: DeploymentId,
        metadata: DeploymentMetadata,
    ) {
        info!(
            restate.deployment.id = %deployment_id,
            restate.deployment.address = %metadata.address_display(),
            "Registering deployment"
        );

        self.deployments.insert(
            deployment_id,
            DeploymentSchemas {
                metadata,
                components: vec![],
            },
        );
    }

    pub(crate) fn compute_remove_deployment(
        &self,
        deployment_id: DeploymentId,
    ) -> Result<Vec<SchemasUpdateCommand>, ErrorKind> {
        if !self.deployments.contains_key(&deployment_id) {
            return Err(ErrorKind::NotFound);
        }
        let deployment_schemas = self.deployments.get(&deployment_id).unwrap();

        let mut commands = Vec::with_capacity(1 + deployment_schemas.components.len());
        for component in deployment_schemas.components.clone() {
            commands.push(SchemasUpdateCommand::RemoveComponent {
                name: component.name,
                revision: component.revision,
            });
        }
        commands.push(SchemasUpdateCommand::RemoveDeployment { deployment_id });

        Ok(commands)
    }

    pub(crate) fn apply_remove_deployment(&mut self, deployment_id: DeploymentId) {
        self.deployments.remove(&deployment_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use restate_schema_api::component::ComponentMetadataResolver;
    use restate_schema_api::deployment::{Deployment, DeploymentResolver};
    use restate_test_util::{assert, assert_eq, let_assert};
    use test_log::test;

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
        let schemas = Schemas::default();

        let deployment = Deployment::mock();
        let commands = schemas
            .compute_new_deployment(
                Some(deployment.id),
                deployment.metadata.clone(),
                vec![greeter_service()],
                false,
            )
            .unwrap();

        let_assert!(
            Some(SchemasUpdateCommand::InsertDeployment { deployment_id, .. }) = commands.first()
        );
        // Ensure we are using the pre-determined id
        assert_eq!(&deployment.id, deployment_id);
        let deployment_id = *deployment_id;

        // Ensure the service name is here
        let_assert!(
            Some(SchemasUpdateCommand::InsertComponent(insert_component_cmd)) = commands.get(1)
        );
        assert_eq!(insert_component_cmd.name, GREETER_SERVICE_NAME);

        schemas.apply_updates(commands);

        schemas.assert_component_revision(GREETER_SERVICE_NAME, 1);
        schemas.assert_component_deployment(GREETER_SERVICE_NAME, deployment_id);
        schemas.assert_component_handler(GREETER_SERVICE_NAME, "greet");
    }

    #[test]
    fn register_new_deployment_add_unregistered_service() {
        let schemas = Schemas::default();

        let deployment_1 = Deployment::mock_with_uri("http://localhost:9080");
        let deployment_2 = Deployment::mock_with_uri("http://localhost:9081");

        // Register first deployment
        schemas.apply_updates(
            schemas
                .compute_new_deployment(
                    Some(deployment_1.id),
                    deployment_1.metadata.clone(),
                    vec![greeter_service()],
                    false,
                )
                .unwrap(),
        );

        schemas.assert_component_deployment(GREETER_SERVICE_NAME, deployment_1.id);
        assert!(schemas
            .resolve_latest_component(ANOTHER_GREETER_SERVICE_NAME)
            .is_none());

        schemas.apply_updates(
            schemas
                .compute_new_deployment(
                    Some(deployment_2.id),
                    deployment_2.metadata.clone(),
                    vec![greeter_service(), another_greeter_service()],
                    false,
                )
                .unwrap(),
        );

        schemas.assert_component_deployment(GREETER_SERVICE_NAME, deployment_2.id);
        schemas.assert_component_revision(GREETER_SERVICE_NAME, 2);
        schemas.assert_component_deployment(ANOTHER_GREETER_SERVICE_NAME, deployment_2.id);
        schemas.assert_component_revision(ANOTHER_GREETER_SERVICE_NAME, 1);
    }

    /// This test case ensures that https://github.com/restatedev/restate/issues/1205 works
    #[test]
    fn force_deploy_private_service() -> Result<(), crate::Error> {
        let schemas = Schemas::default();
        let deployment = Deployment::mock();

        schemas.apply_updates(schemas.compute_new_deployment(
            Some(deployment.id),
            deployment.metadata.clone(),
            vec![greeter_service()],
            false,
        )?);
        assert!(schemas.assert_component(GREETER_SERVICE_NAME).public);

        schemas.apply_updates(vec![SchemasUpdateCommand::ModifyComponent {
            name: GREETER_SERVICE_NAME.to_owned(),
            public: false,
        }]);
        assert!(!schemas.assert_component(GREETER_SERVICE_NAME).public);

        schemas.apply_updates(schemas.compute_new_deployment(
            Some(deployment.id),
            deployment.metadata.clone(),
            vec![greeter_service()],
            true,
        )?);
        assert!(!schemas.assert_component(GREETER_SERVICE_NAME).public);

        Ok(())
    }

    mod change_instance_type {
        use super::*;

        use restate_test_util::assert;
        use test_log::test;

        #[test]
        fn register_new_deployment_fails_changing_instance_type() {
            let schemas = Schemas::default();

            let deployment_1 = Deployment::mock_with_uri("http://localhost:9080");
            let deployment_2 = Deployment::mock_with_uri("http://localhost:9081");

            schemas.apply_updates(
                schemas
                    .compute_new_deployment(
                        Some(deployment_1.id),
                        deployment_1.metadata.clone(),
                        vec![greeter_service()],
                        false,
                    )
                    .unwrap(),
            );

            schemas.assert_component_deployment(GREETER_SERVICE_NAME, deployment_1.id);

            let compute_result = schemas.compute_new_deployment(
                Some(deployment_2.id),
                deployment_2.metadata,
                vec![greeter_virtual_object()],
                false,
            );

            assert!(let &ErrorKind::Component(
                ComponentError::DifferentType(_)
            ) = compute_result.unwrap_err().kind());
        }
    }

    #[test]
    fn override_existing_deployment_removing_a_service() {
        let schemas = Schemas::default();

        let deployment = Deployment::mock();
        schemas.apply_updates(
            schemas
                .compute_new_deployment(
                    Some(deployment.id),
                    deployment.metadata.clone(),
                    vec![greeter_service(), another_greeter_service()],
                    false,
                )
                .unwrap(),
        );

        schemas.assert_component_deployment(GREETER_SERVICE_NAME, deployment.id);
        schemas.assert_component_deployment(ANOTHER_GREETER_SERVICE_NAME, deployment.id);

        schemas.apply_updates(
            schemas
                .compute_new_deployment(
                    Some(deployment.id),
                    deployment.metadata.clone(),
                    vec![greeter_service()],
                    true,
                )
                .unwrap(),
        );

        schemas.assert_component_deployment(GREETER_SERVICE_NAME, deployment.id);
        assert!(schemas
            .resolve_latest_component(ANOTHER_GREETER_SERVICE_NAME)
            .is_none());
    }

    #[test]
    fn cannot_override_existing_deployment_endpoint_conflict() {
        let schemas = Schemas::default();

        let deployment = Deployment::mock();
        schemas.apply_updates(
            schemas
                .compute_new_deployment(
                    Some(deployment.id),
                    deployment.metadata.clone(),
                    vec![greeter_service()],
                    false,
                )
                .unwrap(),
        );

        assert!(let ErrorKind::Override = schemas.compute_new_deployment(
            Some(deployment.id),
            deployment.metadata,
            vec![greeter_service()],
            false).unwrap_err().kind()
        );
    }

    #[test]
    fn cannot_override_existing_deployment_existing_id_mismatch() {
        let schemas = Schemas::default();

        let deployment = Deployment::mock();
        schemas.apply_updates(
            schemas
                .compute_new_deployment(
                    Some(deployment.id),
                    deployment.metadata.clone(),
                    vec![greeter_service()],
                    false,
                )
                .unwrap(),
        );

        let new_id = DeploymentId::new();

        let rejection = schemas
            .compute_new_deployment(
                Some(new_id),
                deployment.metadata,
                vec![greeter_service()],
                false,
            )
            .unwrap_err();
        let_assert!(
            &ErrorKind::Deployment(DeploymentError::IncorrectId {
                requested,
                existing
            }) = rejection.kind()
        );
        assert_eq!(new_id, requested);
        assert_eq!(deployment.id, existing);
    }

    #[test]
    fn register_two_deployments_then_remove_first() {
        let schemas = Schemas::default();

        let deployment_1 = Deployment::mock_with_uri("http://localhost:9080");
        let deployment_2 = Deployment::mock_with_uri("http://localhost:9081");

        schemas.apply_updates(
            schemas
                .compute_new_deployment(
                    Some(deployment_1.id),
                    deployment_1.metadata.clone(),
                    vec![greeter_service(), another_greeter_service()],
                    false,
                )
                .unwrap(),
        );
        schemas.apply_updates(
            schemas
                .compute_new_deployment(
                    Some(deployment_2.id),
                    deployment_2.metadata.clone(),
                    vec![greeter_service()],
                    false,
                )
                .unwrap(),
        );

        schemas.assert_component_deployment(GREETER_SERVICE_NAME, deployment_2.id);
        schemas.assert_component_revision(GREETER_SERVICE_NAME, 2);
        schemas.assert_component_deployment(ANOTHER_GREETER_SERVICE_NAME, deployment_1.id);
        schemas.assert_component_revision(ANOTHER_GREETER_SERVICE_NAME, 1);

        let commands = schemas.compute_remove_deployment(deployment_1.id).unwrap();

        assert!(
            let Some(SchemasUpdateCommand::RemoveComponent { .. }) = commands.first()
        );
        assert!(
            let Some(SchemasUpdateCommand::RemoveComponent { .. }) = commands.get(1)
        );
        assert!(
            let Some(SchemasUpdateCommand::RemoveDeployment { .. }) = commands.get(2)
        );

        schemas.apply_updates(commands);

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
            let schemas = Schemas::default();

            let deployment_1 = Deployment::mock_with_uri("http://localhost:9080");
            let deployment_2 = Deployment::mock_with_uri("http://localhost:9081");

            schemas.apply_updates(
                schemas
                    .compute_new_deployment(
                        Some(deployment_1.id),
                        deployment_1.metadata,
                        vec![greeter_v1_service()],
                        false,
                    )
                    .unwrap(),
            );
            schemas.assert_component_revision(GREETER_SERVICE_NAME, 1);

            let rejection = schemas
                .compute_new_deployment(
                    Some(deployment_2.id),
                    deployment_2.metadata,
                    vec![greeter_v2_service()],
                    false,
                )
                .unwrap_err();

            schemas.assert_component_revision(GREETER_SERVICE_NAME, 1); // unchanged

            let_assert!(
                ErrorKind::Component(ComponentError::RemovedHandlers(service, missing_methods)) =
                    rejection.kind()
            );
            check!(service == GREETER_SERVICE_NAME);
            check!(missing_methods == &["doSomething"]);
        }
    }
}
