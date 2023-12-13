use super::*;

use crate::schemas_impl::service::check_service_name_reserved;
use prost::DecodeError;
use prost_reflect::{Cardinality, DescriptorError, ExtensionDescriptor, FieldDescriptor};
use restate_errors::*;

const SERVICE_TYPE_EXT: &str = "dev.restate.ext.service_type";
const FIELD_EXT: &str = "dev.restate.ext.field";

const UNKEYED_SERVICE_EXT: i32 = 0;
const KEYED_SERVICE_EXT: i32 = 1;
const SINGLETON_SERVICE_EXT: i32 = 2;

const KEY_FIELD_EXT: i32 = 0;
const EVENT_PAYLOAD_FIELD_EXT: i32 = 1;
const EVENT_METADATA_FIELD_EXT: i32 = 2;

#[derive(Debug, thiserror::Error, codederror::CodedError)]
pub enum BadDescriptorError {
    // User errors
    #[error("bad uri '{0}'. The uri must contain either `http` or `https` scheme, a valid authority and can contain a path where the service endpoint is exposed.")]
    #[code(unknown)]
    BadUri(String),
    #[error("cannot find the dev.restate.ext.service_type extension in the descriptor of service '{0}'. You must annotate a service using the dev.restate.ext.service_type extension to specify whether your service is KEYED, UNKEYED or SINGLETON")]
    #[code(META0001)]
    MissingServiceTypeExtension(String),
    #[error("the service '{0}' is keyed but has no methods. You must specify at least one method")]
    #[code(META0001)]
    KeyedServiceWithoutMethods(String),
    #[error("the service name '{0}' is reserved. Service name should must not start with 'dev.restate' or 'grpc'")]
    #[code(META0001)]
    ServiceNameReserved(String),
    #[error(
        "error when trying to parse the key of service method '{}' with input type '{}'. No key field found",
        MethodDescriptor::full_name(.0),
        MethodDescriptor::input(.0).full_name()
    )]
    #[code(META0002)]
    MissingKeyField(MethodDescriptor),
    #[error(
        "error when trying to parse the annotation {1:?} of service method '{}' with input type '{}'. More than one field annotated with the same annotation found",
        MethodDescriptor::full_name(.0),
        MethodDescriptor::input(.0).full_name()
    )]
    #[code(META0007)]
    MoreThanOneAnnotatedField(MethodDescriptor, FieldAnnotation),
    #[error(
        "error when trying to parse the key of service method '{}' with input type '{}'. Bad key field type",
        MethodDescriptor::full_name(.0),
        MethodDescriptor::input(.0).full_name()
    )]
    #[code(META0002)]
    BadKeyFieldType(MethodDescriptor),
    #[error(
        "error when trying to parse the key of service method '{}' with input type '{}'. The key type is different from other methods key types",
        MethodDescriptor::full_name(.0),
        MethodDescriptor::input(.0).full_name()
    )]
    #[code(META0002)]
    DifferentKeyTypes(MethodDescriptor),
    #[error(
        "error when parsing the annotation EVENT_PAYLOAD of service method '{}' with input type '{}'. Bad type",
        MethodDescriptor::full_name(.0),
        MethodDescriptor::input(.0).full_name()
    )]
    #[code(META0008)]
    BadEventPayloadFieldType(MethodDescriptor),
    #[error(
        "error when parsing the annotation EVENT_METADATA of service method '{}' with input type '{}'. Bad type",
        MethodDescriptor::full_name(.0),
        MethodDescriptor::input(.0).full_name()
    )]
    #[code(META0008)]
    BadEventMetadataFieldType(MethodDescriptor),

    // Errors most likely related to SDK bugs
    #[error("cannot find service '{0}' in descriptor set. This might be a symptom of an SDK bug, or of the build tool/pipeline used to generate the descriptor")]
    #[code(unknown)]
    ServiceNotFoundInDescriptor(String),
    #[error("received a bad response from the SDK: {0}. This might be a symptom of an SDK bug")]
    #[code(unknown)]
    BadResponse(&'static str),
    #[error("received a bad response from the SDK that cannot be decoded: {0}. This might be a symptom of an SDK bug")]
    #[code(unknown)]
    Decode(#[from] DecodeError),
    #[error("received a bad response from the SDK with a descriptor set that cannot be reconstructed: {0}. This might be a symptom of an SDK bug")]
    #[code(unknown)]
    Descriptor(#[from] DescriptorError),
    #[error("bad or missing Restate dependency in the descriptor pool. This might be a symptom of an SDK bug")]
    #[code(unknown)]
    BadOrMissingRestateDependencyInDescriptor,
}

#[derive(Debug, thiserror::Error, codederror::CodedError)]
#[code(restate_errors::META0006)]
pub enum IncompatibleServiceChangeError {
    #[error("detected a new service {0} revision with a service instance type different from the previous revision")]
    #[code(restate_errors::META0006)]
    DifferentServiceInstanceType(String),
    #[error("the service {0} already exists but the new revision removed the methods {1:?}")]
    #[code(restate_errors::META0006)]
    RemovedMethods(String, Vec<String>),
}

impl SchemasInner {
    /// When `force` is set, allow incompatible service definition updates to existing services.
    pub(crate) fn compute_new_endpoint(
        &self,
        endpoint_metadata: EndpointMetadata,
        services: Vec<String>,
        descriptor_pool: DescriptorPool,
        force: bool,
    ) -> Result<Vec<SchemasUpdateCommand>, SchemasUpdateError> {
        let endpoint_id = endpoint_metadata.id();

        let services: Vec<ServiceRegistrationRequest> =
            ServiceRegistrationRequest::infer_all_services_from_descriptor_pool(
                services,
                &descriptor_pool,
            )?;

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
                return Err(SchemasUpdateError::OverrideEndpoint(endpoint_id));
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
                        return Err(SchemasUpdateError::IncompatibleServiceChange(
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
                        return Err(SchemasUpdateError::IncompatibleServiceChange(
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
    ) -> Result<(), SchemasUpdateError> {
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
                .ok_or_else(|| SchemasUpdateError::MissingServiceInDescriptor(name.clone()))?;

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
    ) -> Result<Vec<SchemasUpdateCommand>, SchemasUpdateError> {
        if !self.endpoints.contains_key(&endpoint_id) {
            return Err(SchemasUpdateError::UnknownEndpoint(endpoint_id));
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
    ) -> Result<(), SchemasUpdateError> {
        self.endpoints.remove(&endpoint_id);

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ServiceRegistrationRequest {
    name: String,
    instance_type: DiscoveredInstanceType,
    methods: HashMap<String, DiscoveredMethodMetadata>,
}

impl ServiceRegistrationRequest {
    fn infer_all_services_from_descriptor_pool(
        services: Vec<String>,
        descriptor_pool: &DescriptorPool,
    ) -> Result<Vec<Self>, SchemasUpdateError> {
        // Find the Restate extensions in the DescriptorPool.
        // If they're not available, the descriptor pool is incomplete/doesn't contain the restate dependencies.
        let restate_service_type_extension = descriptor_pool
            .get_extension_by_name(SERVICE_TYPE_EXT)
            .ok_or(BadDescriptorError::BadOrMissingRestateDependencyInDescriptor)?;
        let restate_key_extension = descriptor_pool
            .get_extension_by_name(FIELD_EXT)
            .ok_or(BadDescriptorError::BadOrMissingRestateDependencyInDescriptor)?;

        // Collect all the service descriptors
        let service_descriptors = services
            .into_iter()
            .map(|svc| {
                descriptor_pool
                    .get_service_by_name(&svc)
                    .ok_or(BadDescriptorError::ServiceNotFoundInDescriptor(svc))
            })
            .collect::<Result<Vec<_>, _>>()?;

        // Compute ServiceRegistrationRequest
        let mut services = Vec::with_capacity(service_descriptors.len());
        for svc_desc in service_descriptors {
            services.push(ServiceRegistrationRequest::from_service_descriptor(
                svc_desc,
                &restate_service_type_extension,
                &restate_key_extension,
            )?);
        }

        Ok(services)
    }

    fn from_service_descriptor(
        svc_desc: ServiceDescriptor,
        restate_service_type_extension: &ExtensionDescriptor,
        restate_key_extension: &ExtensionDescriptor,
    ) -> Result<Self, SchemasUpdateError> {
        check_service_name_reserved(svc_desc.full_name())?;
        let mut methods = HashMap::with_capacity(svc_desc.methods().len());

        let instance_type = infer_service_type(
            &svc_desc,
            restate_service_type_extension,
            restate_key_extension,
            &mut methods,
        )?;

        infer_event_fields_annotations(&svc_desc, restate_key_extension, &mut methods)?;

        Ok(ServiceRegistrationRequest {
            name: svc_desc.full_name().to_owned(),
            instance_type,
            methods,
        })
    }
}

fn infer_service_type(
    desc: &ServiceDescriptor,
    restate_service_type_ext: &ExtensionDescriptor,
    restate_field_ext: &ExtensionDescriptor,
    methods: &mut HashMap<String, DiscoveredMethodMetadata>,
) -> Result<DiscoveredInstanceType, BadDescriptorError> {
    if !desc.options().has_extension(restate_service_type_ext) {
        return Err(BadDescriptorError::MissingServiceTypeExtension(
            desc.full_name().to_string(),
        ));
    }

    let service_instance_type = desc
        .options()
        .get_extension(restate_service_type_ext)
        .as_enum_number()
        // This can happen only if the restate dependency is bad?
        .ok_or_else(|| BadDescriptorError::BadOrMissingRestateDependencyInDescriptor)?;

    match service_instance_type {
        UNKEYED_SERVICE_EXT => Ok(DiscoveredInstanceType::Unkeyed),
        KEYED_SERVICE_EXT => infer_keyed_service_type(desc, restate_field_ext, methods),
        SINGLETON_SERVICE_EXT => Ok(DiscoveredInstanceType::Singleton),
        _ => Err(BadDescriptorError::BadOrMissingRestateDependencyInDescriptor),
    }
}

fn infer_keyed_service_type(
    desc: &ServiceDescriptor,
    restate_field_ext: &ExtensionDescriptor,
    methods: &mut HashMap<String, DiscoveredMethodMetadata>,
) -> Result<DiscoveredInstanceType, BadDescriptorError> {
    if desc.methods().len() == 0 {
        return Err(BadDescriptorError::KeyedServiceWithoutMethods(
            desc.full_name().to_string(),
        ));
    }

    // Parse the key from the first method
    let first_method = desc.methods().next().unwrap();
    let first_key_field_descriptor = resolve_key_field(&first_method, restate_field_ext)?;

    // Generate the KeyStructure out of it
    let key_structure = infer_key_structure(&first_key_field_descriptor);
    methods
        .entry(first_method.name().to_string())
        .or_default()
        .input_fields_annotations
        .insert(FieldAnnotation::Key, first_key_field_descriptor.number());

    // Now parse the next methods
    for method_desc in desc.methods().skip(1) {
        let key_field_descriptor = resolve_key_field(&method_desc, restate_field_ext)?;

        // Validate every method has the same key field type
        if key_field_descriptor.kind() != first_key_field_descriptor.kind() {
            return Err(BadDescriptorError::DifferentKeyTypes(method_desc));
        }

        methods
            .entry(method_desc.name().to_string())
            .or_default()
            .input_fields_annotations
            .insert(FieldAnnotation::Key, key_field_descriptor.number());
    }

    Ok(DiscoveredInstanceType::Keyed(key_structure))
}

fn infer_key_structure(field_descriptor: &FieldDescriptor) -> KeyStructure {
    match field_descriptor.kind() {
        Kind::Message(message_descriptor) => KeyStructure::Nested(
            message_descriptor
                .fields()
                .map(|f| (f.number(), infer_key_structure(&f)))
                .collect(),
        ),
        _ => KeyStructure::Scalar,
    }
}

fn resolve_key_field(
    method_descriptor: &MethodDescriptor,
    restate_field_extension: &ExtensionDescriptor,
) -> Result<FieldDescriptor, BadDescriptorError> {
    let field_descriptor = match get_annotated_field(
        method_descriptor,
        restate_field_extension,
        KEY_FIELD_EXT,
        FieldAnnotation::Key,
    )? {
        Some(f) => f,
        None => {
            return Err(BadDescriptorError::MissingKeyField(
                method_descriptor.clone(),
            ));
        }
    };

    // Validate type
    if field_descriptor.is_map() {
        return Err(BadDescriptorError::BadKeyFieldType(
            method_descriptor.clone(),
        ));
    }
    if field_descriptor.is_list() {
        return Err(BadDescriptorError::BadKeyFieldType(
            method_descriptor.clone(),
        ));
    }

    Ok(field_descriptor)
}

fn infer_event_fields_annotations(
    service_desc: &ServiceDescriptor,
    restate_field_ext: &ExtensionDescriptor,
    methods: &mut HashMap<String, DiscoveredMethodMetadata>,
) -> Result<(), BadDescriptorError> {
    for method_desc in service_desc.methods() {
        let method_meta = methods.entry(method_desc.name().to_string()).or_default();

        // Infer event annotations
        if let Some(field_desc) = resolve_event_payload_field(&method_desc, restate_field_ext)? {
            method_meta
                .input_fields_annotations
                .insert(FieldAnnotation::EventPayload, field_desc.number());
        }
        if let Some(field_desc) = resolve_event_metadata_field(&method_desc, restate_field_ext)? {
            method_meta
                .input_fields_annotations
                .insert(FieldAnnotation::EventMetadata, field_desc.number());
        }
    }
    Ok(())
}

fn resolve_event_payload_field(
    method_descriptor: &MethodDescriptor,
    restate_field_extension: &ExtensionDescriptor,
) -> Result<Option<FieldDescriptor>, BadDescriptorError> {
    let field_descriptor = match get_annotated_field(
        method_descriptor,
        restate_field_extension,
        EVENT_PAYLOAD_FIELD_EXT,
        FieldAnnotation::EventPayload,
    )? {
        Some(f) => f,
        None => return Ok(None),
    };

    // Validate type
    if field_descriptor.kind() != Kind::String && field_descriptor.kind() != Kind::Bytes {
        return Err(BadDescriptorError::BadEventPayloadFieldType(
            method_descriptor.clone(),
        ));
    }

    Ok(Some(field_descriptor))
}

fn resolve_event_metadata_field(
    method_descriptor: &MethodDescriptor,
    restate_field_extension: &ExtensionDescriptor,
) -> Result<Option<FieldDescriptor>, BadDescriptorError> {
    let field_descriptor = match get_annotated_field(
        method_descriptor,
        restate_field_extension,
        EVENT_METADATA_FIELD_EXT,
        FieldAnnotation::EventMetadata,
    )? {
        Some(f) => f,
        None => return Ok(None),
    };

    // Validate type
    if !is_map_with(&field_descriptor, Kind::String, Kind::String) {
        return Err(BadDescriptorError::BadEventMetadataFieldType(
            method_descriptor.clone(),
        ));
    }

    Ok(Some(field_descriptor))
}

fn get_annotated_field(
    method_descriptor: &MethodDescriptor,
    restate_field_extension: &ExtensionDescriptor,
    extension_value: i32,
    field_annotation: FieldAnnotation,
) -> Result<Option<FieldDescriptor>, BadDescriptorError> {
    let message_desc = method_descriptor.input();
    let mut iter = message_desc.fields().filter(|f| {
        f.options().has_extension(restate_field_extension)
            && f.options()
                .get_extension(restate_field_extension)
                .as_enum_number()
                == Some(extension_value)
    });

    let field = iter.next();
    if field.is_none() {
        return Ok(None);
    }

    // Check there is only one
    if iter.next().is_some() {
        return Err(BadDescriptorError::MoreThanOneAnnotatedField(
            method_descriptor.clone(),
            field_annotation,
        ));
    }

    Ok(field)
}

// Expanded version of FieldDescriptor::is_map
fn is_map_with(field_descriptor: &FieldDescriptor, key_kind: Kind, value_kind: Kind) -> bool {
    field_descriptor.cardinality() == Cardinality::Repeated
        && match field_descriptor.kind() {
            Kind::Message(message) => {
                message.is_map_entry()
                    && message.map_entry_key_field().kind() == key_kind
                    && message.map_entry_value_field().kind() == value_kind
            }
            _ => false,
        }
}

#[cfg(test)]
mod tests {
    use super::*;

    use restate_schema_api::endpoint::EndpointMetadataResolver;
    use restate_schema_api::service::ServiceMetadataResolver;
    use restate_test_util::{assert, assert_eq, let_assert, test};

    load_mock_descriptor!(DESCRIPTOR, "generic");
    const GREETER_SERVICE_NAME: &str = "greeter.Greeter";
    const ANOTHER_GREETER_SERVICE_NAME: &str = "greeter.AnotherGreeter";

    #[test]
    fn register_new_endpoint() {
        let schemas = Schemas::default();

        let endpoint = EndpointMetadata::mock();
        let commands = schemas
            .compute_new_endpoint(
                endpoint.clone(),
                vec![GREETER_SERVICE_NAME.to_owned()],
                DESCRIPTOR.clone(),
                false,
            )
            .unwrap();

        let_assert!(Some(SchemasUpdateCommand::InsertEndpoint { services, .. }) = commands.get(0));
        assert_eq!(services.len(), 1);

        schemas.apply_updates(commands).unwrap();

        schemas.assert_service_revision(GREETER_SERVICE_NAME, 1);
        schemas.assert_resolves_endpoint(GREETER_SERVICE_NAME, endpoint.id());
        assert_eq!(schemas.list_services().first().unwrap().methods.len(), 3);
    }

    #[test]
    fn register_new_endpoint_add_unregistered_service() {
        let schemas = Schemas::default();

        let endpoint_1 = EndpointMetadata::mock_with_uri("http://localhost:9080");
        let endpoint_2 = EndpointMetadata::mock_with_uri("http://localhost:9081");

        let commands = schemas
            .compute_new_endpoint(
                endpoint_1.clone(),
                vec![GREETER_SERVICE_NAME.to_owned()],
                DESCRIPTOR.clone(),
                false,
            )
            .unwrap();

        let_assert!(Some(SchemasUpdateCommand::InsertEndpoint { services, .. }) = commands.get(0));
        assert_eq!(services.len(), 1);

        schemas.apply_updates(commands).unwrap();

        schemas.assert_resolves_endpoint(GREETER_SERVICE_NAME, endpoint_1.id());
        assert!(schemas
            .resolve_latest_service_metadata(ANOTHER_GREETER_SERVICE_NAME)
            .is_none());

        let commands = schemas
            .compute_new_endpoint(
                endpoint_2.clone(),
                vec![
                    GREETER_SERVICE_NAME.to_owned(),
                    ANOTHER_GREETER_SERVICE_NAME.to_owned(),
                ],
                DESCRIPTOR.clone(),
                false,
            )
            .unwrap();

        let_assert!(Some(SchemasUpdateCommand::InsertEndpoint { services, .. }) = commands.get(0));
        assert_eq!(services.len(), 2);

        schemas.apply_updates(commands).unwrap();

        schemas.assert_resolves_endpoint(GREETER_SERVICE_NAME, endpoint_2.id());
        schemas.assert_service_revision(GREETER_SERVICE_NAME, 2);
        schemas.assert_resolves_endpoint(ANOTHER_GREETER_SERVICE_NAME, endpoint_2.id());
        schemas.assert_service_revision(ANOTHER_GREETER_SERVICE_NAME, 1);
    }

    mod change_instance_type {
        use super::*;

        use restate_test_util::{assert, test};

        load_mock_descriptor!(
            CHANGE_INSTANCE_TYPE_DESCRIPTOR_V1,
            "change_instance_type/v1"
        );
        load_mock_descriptor!(
            CHANGE_INSTANCE_TYPE_DESCRIPTOR_V2,
            "change_instance_type/v2"
        );

        #[test]
        fn register_new_endpoint_fails_changing_instance_type() {
            let schemas = Schemas::default();

            let endpoint_1 = EndpointMetadata::mock_with_uri("http://localhost:9080");
            let endpoint_2 = EndpointMetadata::mock_with_uri("http://localhost:9081");

            schemas
                .apply_updates(
                    schemas
                        .compute_new_endpoint(
                            endpoint_1.clone(),
                            vec![GREETER_SERVICE_NAME.to_owned()],
                            CHANGE_INSTANCE_TYPE_DESCRIPTOR_V1.clone(),
                            false,
                        )
                        .unwrap(),
                )
                .unwrap();

            schemas.assert_resolves_endpoint(GREETER_SERVICE_NAME, endpoint_1.id());

            let compute_result = schemas.compute_new_endpoint(
                endpoint_2,
                vec![GREETER_SERVICE_NAME.to_owned()],
                CHANGE_INSTANCE_TYPE_DESCRIPTOR_V2.clone(),
                false,
            );

            assert!(let Err(SchemasUpdateError::IncompatibleServiceChange(IncompatibleServiceChangeError::DifferentServiceInstanceType(_))) = compute_result);
        }
    }

    #[test]
    fn override_existing_endpoint_removing_a_service() {
        let schemas = Schemas::default();

        let endpoint = EndpointMetadata::mock();
        let commands = schemas
            .compute_new_endpoint(
                endpoint.clone(),
                vec![
                    GREETER_SERVICE_NAME.to_owned(),
                    ANOTHER_GREETER_SERVICE_NAME.to_owned(),
                ],
                DESCRIPTOR.clone(),
                false,
            )
            .unwrap();
        schemas.apply_updates(commands).unwrap();

        schemas.assert_resolves_endpoint(GREETER_SERVICE_NAME, endpoint.id());
        schemas.assert_resolves_endpoint(ANOTHER_GREETER_SERVICE_NAME, endpoint.id());

        let commands = schemas
            .compute_new_endpoint(
                endpoint.clone(),
                vec![GREETER_SERVICE_NAME.to_owned()],
                DESCRIPTOR.clone(),
                true,
            )
            .unwrap();
        schemas.apply_updates(commands).unwrap();

        schemas.assert_resolves_endpoint(GREETER_SERVICE_NAME, endpoint.id());
        assert!(schemas
            .resolve_latest_endpoint_for_service(ANOTHER_GREETER_SERVICE_NAME)
            .is_none());
    }

    #[test]
    fn cannot_override_existing_endpoint() {
        let schemas = Schemas::default();

        let endpoint = EndpointMetadata::mock();

        let commands = schemas
            .compute_new_endpoint(
                endpoint.clone(),
                vec![GREETER_SERVICE_NAME.to_owned()],
                DESCRIPTOR.clone(),
                false,
            )
            .unwrap();
        schemas.apply_updates(commands).unwrap();

        assert!(let Err(SchemasUpdateError::OverrideEndpoint(_)) = schemas.compute_new_endpoint(
            endpoint,
            vec![GREETER_SERVICE_NAME.to_owned()],
            DESCRIPTOR.clone(),
            false)
        );
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
                            GREETER_SERVICE_NAME.to_owned(),
                            ANOTHER_GREETER_SERVICE_NAME.to_owned(),
                        ],
                        DESCRIPTOR.clone(),
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
                        vec![GREETER_SERVICE_NAME.to_owned()],
                        DESCRIPTOR.clone(),
                        false,
                    )
                    .unwrap(),
            )
            .unwrap();

        schemas.assert_resolves_endpoint(GREETER_SERVICE_NAME, endpoint_2.id());
        schemas.assert_service_revision(GREETER_SERVICE_NAME, 2);
        schemas.assert_resolves_endpoint(ANOTHER_GREETER_SERVICE_NAME, endpoint_1.id());
        schemas.assert_service_revision(ANOTHER_GREETER_SERVICE_NAME, 1);

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

        schemas.assert_resolves_endpoint(GREETER_SERVICE_NAME, endpoint_2.id());
        schemas.assert_service_revision(GREETER_SERVICE_NAME, 2);
        assert!(schemas
            .resolve_latest_endpoint_for_service(ANOTHER_GREETER_SERVICE_NAME)
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
                        vec![GREETER_SERVICE_NAME.to_owned()],
                        DESCRIPTOR.clone(),
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
                        vec![GREETER_SERVICE_NAME.to_owned()],
                        DESCRIPTOR.clone(),
                        true,
                    )
                    .unwrap(),
            )
            .unwrap();
        schemas.assert_service_revision(svc_name, 2);
    }

    mod remove_method {
        use super::*;

        use restate_test_util::{check, let_assert, test};

        load_mock_descriptor!(REMOVE_METHOD_DESCRIPTOR_V1, "remove_method/v1");
        load_mock_descriptor!(REMOVE_METHOD_DESCRIPTOR_V2, "remove_method/v2");

        #[test]
        fn reject_removing_existing_methods() {
            let schemas = Schemas::default();

            let endpoint_1 = EndpointMetadata::mock_with_uri("http://localhost:9080");
            let endpoint_2 = EndpointMetadata::mock_with_uri("http://localhost:9081");

            let commands = schemas.compute_new_endpoint(
                endpoint_1,
                vec![GREETER_SERVICE_NAME.to_owned()],
                REMOVE_METHOD_DESCRIPTOR_V1.clone(),
                false,
            );
            schemas.apply_updates(commands.unwrap()).unwrap();
            schemas.assert_service_revision(GREETER_SERVICE_NAME, 1);

            let rejection = schemas.compute_new_endpoint(
                endpoint_2,
                vec![GREETER_SERVICE_NAME.to_owned()],
                REMOVE_METHOD_DESCRIPTOR_V2.clone(),
                false,
            );

            schemas.assert_service_revision(GREETER_SERVICE_NAME, 1); // unchanged

            let_assert!(
                Err(SchemasUpdateError::IncompatibleServiceChange(
                    IncompatibleServiceChangeError::RemovedMethods(service, missing_methods)
                )) = rejection
            );
            check!(service == "greeter.Greeter");
            check!(missing_methods == std::vec!["Greet"]);
        }
    }
}
