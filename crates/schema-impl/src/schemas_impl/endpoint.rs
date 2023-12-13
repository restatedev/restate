use super::*;
use crate::schemas_impl::service::check_service_name_reserved;

impl SchemasInner {
    /// When `force` is set, allow incompatible service definition updates to existing services.
    pub(crate) fn compute_new_endpoint(
        &self,
        endpoint_metadata: EndpointMetadata,
        services: Vec<ServiceRegistrationRequest>,
        descriptor_pool: DescriptorPool,
        force: bool,
    ) -> Result<Vec<SchemasUpdateCommand>, RegistrationError> {
        let endpoint_id = endpoint_metadata.id();

        let mut result_commands = Vec::with_capacity(1 + services.len());

        if let Some(existing_endpoint) = self.endpoints.get(&endpoint_id) {
            if force {
                // If we need to overwrite the endpoint we need to remove old services
                for svc in &existing_endpoint.services {
                    warn!(
                        restate.service_endpoint.id = %endpoint_id,
                        restate.service_endpoint.address = %endpoint_metadata.address_display(),
                        "Going to remove service {} due to a forced service endpoint update",
                        svc.name
                    );
                    result_commands.push(SchemasUpdateCommand::RemoveService {
                        name: svc.name.clone(),
                        revision: svc.revision,
                    });
                }
            } else {
                return Err(RegistrationError::OverrideEndpoint(endpoint_id));
            }
        }

        // Compute service revision numbers
        let mut computed_revisions = HashMap::with_capacity(services.len());
        for proposed_service in &services {
            check_service_name_reserved(&proposed_service.name)?;

            let instance_type = InstanceTypeMetadata::from_discovered_metadata(
                proposed_service.instance_type.clone(),
                &proposed_service.methods,
            );

            // For the time being when updating we overwrite existing data
            let revision = if let Some(existing_service) = self.services.get(&proposed_service.name)
            {
                let removed_methods: Vec<String> = existing_service
                    .methods
                    .keys()
                    .filter(|method_name| !proposed_service.methods.contains_key(*method_name))
                    .map(|method_name| method_name.to_string())
                    .collect();

                if !removed_methods.is_empty() {
                    if force {
                        warn!(
                            restate.service_endpoint.id = %endpoint_id,
                            restate.service_endpoint.address = %endpoint_metadata.address_display(),
                            "Going to remove the following methods from service instance type {} due to a forced service endpoint update: {:?}.",
                            proposed_service.name,
                            removed_methods
                        );
                    } else {
                        return Err(RegistrationError::IncompatibleServiceChange(
                            IncompatibleServiceChangeError::RemovedMethods(
                                proposed_service.name.clone(),
                                removed_methods,
                            ),
                        ));
                    }
                }

                if existing_service.instance_type != instance_type {
                    if force {
                        warn!(
                            restate.service_endpoint.id = %endpoint_id,
                            restate.service_endpoint.address = %endpoint_metadata.address_display(),
                            "Going to overwrite service instance type {} due to a forced service endpoint update: {:?} != {:?}. This is a potentially dangerous operation, and might result in data loss.",
                            proposed_service.name,
                            existing_service.instance_type,
                            instance_type
                        );
                    } else {
                        return Err(RegistrationError::IncompatibleServiceChange(
                            IncompatibleServiceChangeError::DifferentServiceInstanceType(
                                proposed_service.name.clone(),
                            ),
                        ));
                    }
                }

                existing_service.revision.wrapping_add(1)
            } else {
                1
            };
            computed_revisions.insert(proposed_service.name.clone(), revision);
        }

        // Create the InsertEndpoint command
        result_commands.push(SchemasUpdateCommand::InsertEndpoint {
            metadata: endpoint_metadata,
            services: services
                .into_iter()
                .map(|request| {
                    let revision = computed_revisions.remove(&request.name).unwrap();

                    InsertServiceUpdateCommand {
                        name: request.name,
                        revision,
                        instance_type: request.instance_type,
                        methods: request.methods,
                    }
                })
                .collect(),
            descriptor_pool,
        });

        Ok(result_commands)
    }

    pub(crate) fn apply_insert_endpoint(
        &mut self,
        metadata: EndpointMetadata,
        services: Vec<InsertServiceUpdateCommand>,
        descriptor_pool: DescriptorPool,
    ) -> Result<(), RegistrationError> {
        let endpoint_id = metadata.id();
        info!(
            restate.service_endpoint.id = %endpoint_id,
            restate.service_endpoint.address = %metadata.address_display(),
            "Registering endpoint"
        );

        let mut endpoint_services = vec![];

        for InsertServiceUpdateCommand {
            name,
            revision,
            instance_type,
            methods,
        } in services
        {
            info!(
                rpc.service = name,
                restate.service_endpoint.address = %metadata.address_display(),
                "Registering service"
            );
            let service_descriptor = descriptor_pool
                .get_service_by_name(&name)
                .ok_or_else(|| RegistrationError::MissingServiceInDescriptor(name.clone()))?;

            if tracing::enabled!(tracing::Level::DEBUG) {
                service_descriptor.methods().for_each(|method| {
                    debug!(
                        rpc.service = name,
                        rpc.method = method.name(),
                        "Registering method"
                    )
                });
            }

            // We need to retain the `public` field from previous registrations
            let service_schemas = self
                .services
                .entry(name.clone())
                .and_modify(|service_schemas| {
                    info!(rpc.service = name, "Overwriting existing service schemas");

                    service_schemas.revision = revision;
                    service_schemas.instance_type = InstanceTypeMetadata::from_discovered_metadata(
                        instance_type.clone(),
                        &methods,
                    );
                    service_schemas.methods =
                        ServiceSchemas::compute_service_methods(&service_descriptor, &methods);
                    if let ServiceLocation::ServiceEndpoint {
                        latest_endpoint, ..
                    } = &mut service_schemas.location
                    {
                        *latest_endpoint = endpoint_id.clone();
                    }

                    // We need to remove the service from the proto_symbols.
                    // We re-insert it later with the new endpoint id
                    self.proto_symbols.remove_service(&service_descriptor);
                })
                .or_insert_with(|| {
                    ServiceSchemas::new(
                        revision,
                        ServiceSchemas::compute_service_methods(&service_descriptor, &methods),
                        InstanceTypeMetadata::from_discovered_metadata(
                            instance_type.clone(),
                            &methods,
                        ),
                        endpoint_id.clone(),
                    )
                });

            self.proto_symbols
                .add_service(&endpoint_id, &service_descriptor);

            endpoint_services.push(
                map_to_service_metadata(&name, service_schemas)
                    .expect("Should not be a built-in service"),
            );
        }

        self.endpoints.insert(
            endpoint_id,
            EndpointSchemas {
                metadata,
                services: endpoint_services,
                descriptor_pool,
            },
        );

        Ok(())
    }

    pub(crate) fn compute_remove_endpoint(
        &self,
        endpoint_id: EndpointId,
    ) -> Result<Vec<SchemasUpdateCommand>, RegistrationError> {
        if !self.endpoints.contains_key(&endpoint_id) {
            return Err(RegistrationError::UnknownEndpoint(endpoint_id));
        }
        let endpoint_schemas = self.endpoints.get(&endpoint_id).unwrap();

        let mut commands = Vec::with_capacity(1 + endpoint_schemas.services.len());
        for svc in endpoint_schemas.services.clone() {
            commands.push(SchemasUpdateCommand::RemoveService {
                name: svc.name,
                revision: svc.revision,
            });
        }
        commands.push(SchemasUpdateCommand::RemoveEndpoint { endpoint_id });

        Ok(commands)
    }

    pub(crate) fn apply_remove_endpoint(
        &mut self,
        endpoint_id: EndpointId,
    ) -> Result<(), RegistrationError> {
        self.endpoints.remove(&endpoint_id);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use restate_pb::mocks;
    use restate_schema_api::endpoint::EndpointMetadataResolver;
    use restate_schema_api::service::ServiceMetadataResolver;
    use restate_test_util::{assert, assert_eq, check, let_assert, test};

    #[test]
    fn register_new_endpoint_empty_registry() {
        let schemas = Schemas::default();

        let endpoint = EndpointMetadata::mock();
        let commands = schemas
            .compute_new_endpoint(
                endpoint.clone(),
                vec![ServiceRegistrationRequest::unkeyed_without_annotations(
                    mocks::GREETER_SERVICE_NAME.to_string(),
                    &["Greet"],
                )],
                mocks::DESCRIPTOR_POOL.clone(),
                false,
            )
            .unwrap();

        let_assert!(Some(SchemasUpdateCommand::InsertEndpoint { services, .. }) = commands.get(0));
        assert_eq!(services.len(), 1);

        schemas.apply_updates(commands).unwrap();

        schemas.assert_service_revision(mocks::GREETER_SERVICE_NAME, 1);
        schemas.assert_resolves_endpoint(mocks::GREETER_SERVICE_NAME, endpoint.id());
        // assert_eq!(schemas.list_services().first().unwrap().methods.len(), 1);
    }

    #[test]
    fn register_new_endpoint_updating_old_service() {
        let schemas = Schemas::default();

        let endpoint_1 = EndpointMetadata::mock_with_uri("http://localhost:9080");
        let endpoint_2 = EndpointMetadata::mock_with_uri("http://localhost:9081");

        let commands = schemas
            .compute_new_endpoint(
                endpoint_1.clone(),
                vec![ServiceRegistrationRequest::unkeyed_without_annotations(
                    mocks::GREETER_SERVICE_NAME.to_string(),
                    &["Greet"],
                )],
                mocks::DESCRIPTOR_POOL.clone(),
                false,
            )
            .unwrap();

        let_assert!(Some(SchemasUpdateCommand::InsertEndpoint { services, .. }) = commands.get(0));
        assert_eq!(services.len(), 1);

        schemas.apply_updates(commands).unwrap();

        schemas.assert_resolves_endpoint(mocks::GREETER_SERVICE_NAME, endpoint_1.id());

        let commands = schemas
            .compute_new_endpoint(
                endpoint_2.clone(),
                vec![
                    ServiceRegistrationRequest::unkeyed_without_annotations(
                        mocks::GREETER_SERVICE_NAME.to_string(),
                        &["Greet"],
                    ),
                    ServiceRegistrationRequest::unkeyed_without_annotations(
                        mocks::ANOTHER_GREETER_SERVICE_NAME.to_string(),
                        &["Greet"],
                    ),
                ],
                mocks::DESCRIPTOR_POOL.clone(),
                false,
            )
            .unwrap();

        let_assert!(Some(SchemasUpdateCommand::InsertEndpoint { services, .. }) = commands.get(0));
        assert_eq!(services.len(), 2);

        schemas.apply_updates(commands).unwrap();

        schemas.assert_resolves_endpoint(mocks::GREETER_SERVICE_NAME, endpoint_2.id());
        schemas.assert_service_revision(mocks::GREETER_SERVICE_NAME, 2);
        schemas.assert_resolves_endpoint(mocks::ANOTHER_GREETER_SERVICE_NAME, endpoint_2.id());
        schemas.assert_service_revision(mocks::ANOTHER_GREETER_SERVICE_NAME, 1);
    }

    #[test]
    fn register_new_endpoint_updating_old_service_fails_with_different_instance_type() {
        let schemas = Schemas::default();

        let endpoint_1 = EndpointMetadata::mock_with_uri("http://localhost:9080");
        let endpoint_2 = EndpointMetadata::mock_with_uri("http://localhost:9081");

        schemas
            .apply_updates(
                schemas
                    .compute_new_endpoint(
                        endpoint_1.clone(),
                        vec![ServiceRegistrationRequest::unkeyed_without_annotations(
                            mocks::GREETER_SERVICE_NAME.to_string(),
                            &["Greet"],
                        )],
                        mocks::DESCRIPTOR_POOL.clone(),
                        false,
                    )
                    .unwrap(),
            )
            .unwrap();

        schemas.assert_resolves_endpoint(mocks::GREETER_SERVICE_NAME, endpoint_1.id());

        let compute_result = schemas.compute_new_endpoint(
            endpoint_2,
            vec![ServiceRegistrationRequest::singleton_without_annotations(
                mocks::GREETER_SERVICE_NAME.to_string(),
                &["Greet"],
            )],
            mocks::DESCRIPTOR_POOL.clone(),
            false,
        );

        assert!(let Err(RegistrationError::IncompatibleServiceChange(IncompatibleServiceChangeError::DifferentServiceInstanceType(_))) = compute_result);
    }

    #[test]
    fn override_existing_endpoint() {
        let schemas = Schemas::default();

        let endpoint = EndpointMetadata::mock();
        let commands = schemas
            .compute_new_endpoint(
                endpoint.clone(),
                vec![
                    ServiceRegistrationRequest::unkeyed_without_annotations(
                        mocks::GREETER_SERVICE_NAME.to_string(),
                        &["Greet"],
                    ),
                    ServiceRegistrationRequest::unkeyed_without_annotations(
                        mocks::ANOTHER_GREETER_SERVICE_NAME.to_string(),
                        &["Greet"],
                    ),
                ],
                mocks::DESCRIPTOR_POOL.clone(),
                false,
            )
            .unwrap();
        schemas.apply_updates(commands).unwrap();

        schemas.assert_resolves_endpoint(mocks::GREETER_SERVICE_NAME, endpoint.id());
        schemas.assert_resolves_endpoint(mocks::ANOTHER_GREETER_SERVICE_NAME, endpoint.id());

        let commands = schemas
            .compute_new_endpoint(
                endpoint.clone(),
                vec![ServiceRegistrationRequest::unkeyed_without_annotations(
                    mocks::GREETER_SERVICE_NAME.to_string(),
                    &["Greet"],
                )],
                mocks::DESCRIPTOR_POOL.clone(),
                true,
            )
            .unwrap();
        schemas.apply_updates(commands).unwrap();

        schemas.assert_resolves_endpoint(mocks::GREETER_SERVICE_NAME, endpoint.id());
        assert!(schemas
            .resolve_latest_endpoint_for_service(mocks::ANOTHER_GREETER_SERVICE_NAME)
            .is_none());
    }

    #[test]
    fn cannot_override_existing_endpoint() {
        let schemas = Schemas::default();

        let endpoint = EndpointMetadata::mock();
        let services = vec![ServiceRegistrationRequest::unkeyed_without_annotations(
            mocks::GREETER_SERVICE_NAME.to_string(),
            &["Greet"],
        )];

        let commands = schemas
            .compute_new_endpoint(
                endpoint.clone(),
                services.clone(),
                mocks::DESCRIPTOR_POOL.clone(),
                false,
            )
            .unwrap();
        schemas.apply_updates(commands).unwrap();

        assert!(let Err(RegistrationError::OverrideEndpoint(_)) = schemas.compute_new_endpoint(endpoint, services, mocks::DESCRIPTOR_POOL.clone(), false));
    }

    #[test]
    fn register_two_endpoints_then_remove_first() {
        let schemas = Schemas::default();

        let endpoint_1 = EndpointMetadata::mock_with_uri("http://localhost:9080");
        let endpoint_2 = EndpointMetadata::mock_with_uri("http://localhost:9081");

        schemas
            .apply_updates(
                schemas
                    .compute_new_endpoint(
                        endpoint_1.clone(),
                        vec![
                            ServiceRegistrationRequest::unkeyed_without_annotations(
                                mocks::GREETER_SERVICE_NAME.to_string(),
                                &["Greet"],
                            ),
                            ServiceRegistrationRequest::unkeyed_without_annotations(
                                mocks::ANOTHER_GREETER_SERVICE_NAME.to_string(),
                                &["Greet"],
                            ),
                        ],
                        mocks::DESCRIPTOR_POOL.clone(),
                        false,
                    )
                    .unwrap(),
            )
            .unwrap();
        schemas
            .apply_updates(
                schemas
                    .compute_new_endpoint(
                        endpoint_2.clone(),
                        vec![ServiceRegistrationRequest::unkeyed_without_annotations(
                            mocks::GREETER_SERVICE_NAME.to_string(),
                            &["Greet"],
                        )],
                        mocks::DESCRIPTOR_POOL.clone(),
                        false,
                    )
                    .unwrap(),
            )
            .unwrap();

        schemas.assert_resolves_endpoint(mocks::GREETER_SERVICE_NAME, endpoint_2.id());
        schemas.assert_service_revision(mocks::GREETER_SERVICE_NAME, 2);
        schemas.assert_resolves_endpoint(mocks::ANOTHER_GREETER_SERVICE_NAME, endpoint_1.id());
        schemas.assert_service_revision(mocks::ANOTHER_GREETER_SERVICE_NAME, 1);

        let commands = schemas.compute_remove_endpoint(endpoint_1.id()).unwrap();

        assert!(
            let Some(SchemasUpdateCommand::RemoveService { .. }) = commands.get(0)
        );
        assert!(
            let Some(SchemasUpdateCommand::RemoveService { .. }) = commands.get(1)
        );
        assert!(
            let Some(SchemasUpdateCommand::RemoveEndpoint { .. }) = commands.get(2)
        );

        schemas.apply_updates(commands).unwrap();

        schemas.assert_resolves_endpoint(mocks::GREETER_SERVICE_NAME, endpoint_2.id());
        schemas.assert_service_revision(mocks::GREETER_SERVICE_NAME, 2);
        assert!(schemas
            .resolve_latest_endpoint_for_service(mocks::ANOTHER_GREETER_SERVICE_NAME)
            .is_none());
        assert!(schemas.get_endpoint(&endpoint_1.id()).is_none());
    }

    // Reproducer for issue where the service name is the same of the method name
    #[test]
    fn register_issue682() {
        let schemas = Schemas::default();
        let svc_name = "greeter.Greeter";

        let endpoint = EndpointMetadata::mock();
        schemas
            .apply_updates(
                schemas
                    .compute_new_endpoint(
                        endpoint.clone(),
                        vec![ServiceRegistrationRequest::unkeyed_without_annotations(
                            svc_name.to_string(),
                            &["Greet"],
                        )],
                        mocks::DESCRIPTOR_POOL.clone(),
                        false,
                    )
                    .unwrap(),
            )
            .unwrap();
        schemas.assert_service_revision(svc_name, 1);

        // Force the update. This should not panic.
        schemas
            .apply_updates(
                schemas
                    .compute_new_endpoint(
                        endpoint,
                        vec![ServiceRegistrationRequest::unkeyed_without_annotations(
                            svc_name.to_string(),
                            &["Greet"],
                        )],
                        mocks::DESCRIPTOR_POOL.clone(),
                        true,
                    )
                    .unwrap(),
            )
            .unwrap();
        schemas.assert_service_revision(svc_name, 2);
    }

    #[test]
    fn compute_updates_only_registers_requested_methods() {
        let schemas = Schemas::default();

        let svc = mocks::DESCRIPTOR_POOL.clone();
        svc.services()
            .filter(|s| s.name() == mocks::GREETER_SERVICE_NAME)
            .for_each(|s| {
                let method_names = s
                    .methods()
                    .map(|m| m.name().to_string())
                    .collect::<Vec<_>>();
                check!(method_names == std::vec!["Greet", "GetCount", "GreetStream"]);
            });

        let commands = schemas
            .compute_new_endpoint(
                EndpointMetadata::mock(),
                vec![ServiceRegistrationRequest::unkeyed_without_annotations(
                    mocks::GREETER_SERVICE_NAME.to_string(),
                    &["Greet"],
                )],
                mocks::DESCRIPTOR_POOL.clone(),
                false,
            )
            .unwrap();

        assert_eq!(commands.len(), 1);
        let_assert!(Some(SchemasUpdateCommand::InsertEndpoint { services, .. }) = commands.get(0));
        let_assert!(Some(InsertServiceUpdateCommand { methods, .. }) = services.get(0));
        check!(methods.keys().collect::<Vec<_>>() == std::vec!["Greet"]);

        schemas.apply_updates(commands).unwrap();
        schemas.assert_service_revision(mocks::GREETER_SERVICE_NAME, 1);

        let registered_methods = schemas
            .resolve_latest_service_metadata(mocks::GREETER_SERVICE_NAME)
            .unwrap()
            .methods
            .iter()
            .map(|m| m.name.clone())
            .collect::<Vec<_>>();

        check!(registered_methods == std::vec!["Greet"]);
    }

    mod remove_method {
        use super::*;

        use restate_test_util::{check, let_assert, test};

        load_mock_descriptor!(REMOVE_METHOD_DESCRIPTOR_V1, "remove_method/v1");
        load_mock_descriptor!(REMOVE_METHOD_DESCRIPTOR_V2, "remove_method/v2");
        const GREETER_SERVICE_NAME: &str = "greeter.Greeter";

        #[test]
        fn reject_removing_existing_methods() {
            let schemas = Schemas::default();

            let endpoint_1 = EndpointMetadata::mock_with_uri("http://localhost:9080");
            let endpoint_2 = EndpointMetadata::mock_with_uri("http://localhost:9081");

            let commands = schemas.compute_new_endpoint(
                endpoint_1,
                vec![ServiceRegistrationRequest::unkeyed_without_annotations(
                    GREETER_SERVICE_NAME.to_string(),
                    &["Greet" /*, "GetCount", "GreetStream"*/],
                )],
                REMOVE_METHOD_DESCRIPTOR_V1.clone(),
                false,
            );
            schemas.apply_updates(commands.unwrap()).unwrap();
            schemas.assert_service_revision(GREETER_SERVICE_NAME, 1);

            let rejection = schemas.compute_new_endpoint(
                endpoint_2,
                vec![ServiceRegistrationRequest::unkeyed_without_annotations(
                    GREETER_SERVICE_NAME.to_string(),
                    &["Greetings" /*, "GetCount", "GreetStream"*/],
                )],
                REMOVE_METHOD_DESCRIPTOR_V2.clone(),
                false,
            );

            schemas.assert_service_revision(GREETER_SERVICE_NAME, 1); // unchanged

            let_assert!(
                Err(RegistrationError::IncompatibleServiceChange(
                    IncompatibleServiceChangeError::RemovedMethods(service, missing_methods)
                )) = rejection
            );
            check!(service == "greeter.Greeter");
            check!(missing_methods == std::vec!["Greet"]);
        }
    }
}
