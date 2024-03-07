use super::*;

use prost::DecodeError;
use prost_reflect::{Cardinality, DescriptorError, ExtensionDescriptor, FieldDescriptor};

use restate_errors::*;
use restate_schema_api::deployment::DeploymentType;
use restate_types::identifiers::DeploymentId;

use crate::schemas_impl::component::{check_reserved_name, to_component_type};
use crate::schemas_impl::service::check_service_name_reserved;

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
    #[error("bad uri '{0}'. The uri must contain either `http` or `https` scheme, a valid authority and can contain a path where the deployment is exposed.")]
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
    #[error("detected a new component {0} revision with a component type different from the previous revision")]
    #[code(restate_errors::META0006)]
    DifferentComponentInstanceType(String),
    #[error("the component {0} already exists but the new revision removed the handlers {1:?}")]
    #[code(restate_errors::META0006)]
    RemovedHandlers(String, Vec<String>),
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
    /// When `force` is set, allow incompatible service definition updates to existing services.
    pub(crate) fn old_compute_new_deployment(
        &self,
        requested_deployment_id: Option<DeploymentId>,
        deployment_metadata: DeploymentMetadata,
        services: Vec<String>,
        descriptor_pool: DescriptorPool,
        force: bool,
    ) -> Result<Vec<SchemasUpdateCommand>, SchemasUpdateError> {
        let services: Vec<ServiceRegistrationRequest> =
            ServiceRegistrationRequest::infer_all_services_from_descriptor_pool(
                services,
                &descriptor_pool,
            )?;

        let mut result_commands = Vec::with_capacity(1 + services.len());
        let deployment_id: Option<DeploymentId>;

        // Did we find an existing deployment with same id or with a conflicting endpoint url?
        let found_existing_deployment = requested_deployment_id
            .and_then(|id| self.find_existing_deployment_by_id(&id))
            .or_else(|| self.find_existing_deployment_by_endpoint(&deployment_metadata.ty));

        if let Some((existing_deployment_id, existing_deployment)) = found_existing_deployment {
            if requested_deployment_id.is_some_and(|dp| &dp != existing_deployment_id) {
                // The deployment id is different from the existing one, we don't accept that even
                // if force is used. It means that the user intended to update another deployment.
                return Err(SchemasUpdateError::IncorrectDeploymentId {
                    requested: requested_deployment_id.expect("must be set"),
                    existing: *existing_deployment_id,
                });
            }

            if force {
                deployment_id = Some(*existing_deployment_id);
                // If we need to overwrite the deployment we need to remove old services
                for svc in &existing_deployment.services {
                    warn!(
                        restate.deployment.id = %existing_deployment_id,
                        restate.deployment.address = %deployment_metadata.address_display(),
                        "Going to remove service {} due to a forced deployment update",
                        svc.name
                    );
                    result_commands.push(SchemasUpdateCommand::RemoveService {
                        name: svc.name.clone(),
                        revision: svc.revision,
                    });
                }
            } else {
                return Err(SchemasUpdateError::OverrideDeployment(
                    *existing_deployment_id,
                ));
            }
        } else {
            // New deployment. Use the supplied deployment_id if passed, otherwise, generate one.
            deployment_id = requested_deployment_id.or_else(|| Some(DeploymentId::new()));
        }

        // We must have a deployment id by now, either a new or existing one.
        let deployment_id = deployment_id.unwrap();

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
                            restate.deployment.id = %deployment_id,
                            restate.deployment.address = %deployment_metadata.address_display(),
                            "Going to remove the following methods from service instance type {} due to a forced deployment update: {:?}.",
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
                            restate.deployment.id = %deployment_id,
                            restate.deployment.address = %deployment_metadata.address_display(),
                            "Going to overwrite service instance type {} due to a forced deployment update: {:?} != {:?}. This is a potentially dangerous operation, and might result in data loss.",
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

        // Create the InsertDeployment command
        result_commands.push(SchemasUpdateCommand::OldInsertDeployment {
            deployment_id,
            metadata: deployment_metadata,
            services: services
                .into_iter()
                .map(|request| {
                    let revision = computed_revisions.remove(&request.name).unwrap();

                    OldInsertServiceUpdateCommand {
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

    pub(crate) fn apply_old_insert_deployment(
        &mut self,
        deployment_id: DeploymentId,
        metadata: DeploymentMetadata,
        services: Vec<OldInsertServiceUpdateCommand>,
        descriptor_pool: DescriptorPool,
    ) -> Result<(), SchemasUpdateError> {
        info!(
            restate.deployment.id = %deployment_id,
            restate.deployment.address = %metadata.address_display(),
            "Registering deployment"
        );

        let mut deployment_services = vec![];

        for OldInsertServiceUpdateCommand {
            name,
            revision,
            instance_type,
            methods,
        } in services
        {
            info!(
                rpc.service = name,
                restate.deployment.address = %metadata.address_display(),
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
                    if let ServiceLocation::Deployment {
                        latest_deployment, ..
                    } = &mut service_schemas.location
                    {
                        *latest_deployment = deployment_id;
                    }

                    // We need to remove the service from the proto_symbols.
                    // We re-insert it later with the new deployment id
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
                        deployment_id,
                    )
                });

            self.proto_symbols
                .add_service(&deployment_id, &service_descriptor);

            deployment_services.push(
                map_to_service_metadata(&name, service_schemas)
                    .expect("Should not be a built-in service"),
            );
        }

        self.deployments.insert(
            deployment_id,
            DeploymentSchemas {
                metadata,
                services: deployment_services,
                descriptor_pool,
                components: vec![],
            },
        );

        Ok(())
    }

    pub(crate) fn compute_new_deployment(
        &self,
        requested_deployment_id: Option<DeploymentId>,
        deployment_metadata: DeploymentMetadata,
        components: Vec<schema::Component>,
        force: bool,
    ) -> Result<Vec<SchemasUpdateCommand>, SchemasUpdateError> {
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
                return Err(SchemasUpdateError::IncorrectDeploymentId {
                    requested: requested_deployment_id.expect("must be set"),
                    existing: *existing_deployment_id,
                });
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
                return Err(SchemasUpdateError::OverrideDeployment(
                    *existing_deployment_id,
                ));
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
                        return Err(SchemasUpdateError::IncompatibleServiceChange(
                            IncompatibleServiceChangeError::RemovedHandlers(
                                component_name,
                                removed_handlers,
                            ),
                        ));
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
                        return Err(SchemasUpdateError::IncompatibleServiceChange(
                            IncompatibleServiceChangeError::DifferentComponentInstanceType(
                                component_name,
                            ),
                        ));
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
                        .map(|h| DiscoveredHandlerMetadata {
                            name: h.name.to_string(),
                            input_schema: h.input_schema.map(|v| {
                                serde_json::to_vec(&v)
                                    .expect("Serializing Values must never fail")
                                    .into()
                            }),
                            output_schema: h.output_schema.map(|v| {
                                serde_json::to_vec(&v)
                                    .expect("Serializing Values must never fail")
                                    .into()
                            }),
                        })
                        .collect(),
                },
            ));
        }

        Ok(result_commands)
    }

    pub(crate) fn apply_insert_deployment(
        &mut self,
        deployment_id: DeploymentId,
        metadata: DeploymentMetadata,
    ) -> Result<(), SchemasUpdateError> {
        info!(
            restate.deployment.id = %deployment_id,
            restate.deployment.address = %metadata.address_display(),
            "Registering deployment"
        );

        self.deployments.insert(
            deployment_id,
            DeploymentSchemas {
                metadata,
                services: vec![],
                descriptor_pool: DescriptorPool::new(),
                components: vec![],
            },
        );

        Ok(())
    }

    pub(crate) fn compute_remove_deployment(
        &self,
        deployment_id: DeploymentId,
    ) -> Result<Vec<SchemasUpdateCommand>, SchemasUpdateError> {
        if !self.deployments.contains_key(&deployment_id) {
            return Err(SchemasUpdateError::UnknownDeployment(deployment_id));
        }
        let deployment_schemas = self.deployments.get(&deployment_id).unwrap();

        let mut commands = Vec::with_capacity(
            1 + deployment_schemas.services.len() + deployment_schemas.components.len(),
        );
        for svc in deployment_schemas.services.clone() {
            commands.push(SchemasUpdateCommand::RemoveService {
                name: svc.name,
                revision: svc.revision,
            });
        }
        for component in deployment_schemas.components.clone() {
            commands.push(SchemasUpdateCommand::RemoveComponent {
                name: component.name,
                revision: component.revision,
            });
        }
        commands.push(SchemasUpdateCommand::RemoveDeployment { deployment_id });

        Ok(commands)
    }

    pub(crate) fn apply_remove_deployment(
        &mut self,
        deployment_id: DeploymentId,
    ) -> Result<(), SchemasUpdateError> {
        self.deployments.remove(&deployment_id);

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
    if field_descriptor.kind() != Kind::String {
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
                input_schema: None,
                output_schema: None,
            }],
        }
    }

    fn greeter_virtual_object() -> schema::Component {
        schema::Component {
            component_type: schema::ComponentType::VirtualObject,
            fully_qualified_component_name: GREETER_SERVICE_NAME.parse().unwrap(),
            handlers: vec![schema::Handler {
                name: "greet".parse().unwrap(),
                input_schema: None,
                output_schema: None,
            }],
        }
    }

    fn another_greeter_service() -> schema::Component {
        schema::Component {
            component_type: schema::ComponentType::Service,
            fully_qualified_component_name: ANOTHER_GREETER_SERVICE_NAME.parse().unwrap(),
            handlers: vec![schema::Handler {
                name: "another_greeter".parse().unwrap(),
                input_schema: None,
                output_schema: None,
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

        schemas.apply_updates(commands).unwrap();

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
        schemas
            .apply_updates(
                schemas
                    .compute_new_deployment(
                        Some(deployment_1.id),
                        deployment_1.metadata.clone(),
                        vec![greeter_service()],
                        false,
                    )
                    .unwrap(),
            )
            .unwrap();

        schemas.assert_component_deployment(GREETER_SERVICE_NAME, deployment_1.id);
        assert!(schemas
            .resolve_latest_component(ANOTHER_GREETER_SERVICE_NAME)
            .is_none());

        schemas
            .apply_updates(
                schemas
                    .compute_new_deployment(
                        Some(deployment_2.id),
                        deployment_2.metadata.clone(),
                        vec![greeter_service(), another_greeter_service()],
                        false,
                    )
                    .unwrap(),
            )
            .unwrap();

        schemas.assert_component_deployment(GREETER_SERVICE_NAME, deployment_2.id);
        schemas.assert_component_revision(GREETER_SERVICE_NAME, 2);
        schemas.assert_component_deployment(ANOTHER_GREETER_SERVICE_NAME, deployment_2.id);
        schemas.assert_component_revision(ANOTHER_GREETER_SERVICE_NAME, 1);
    }

    /// This test case ensures that https://github.com/restatedev/restate/issues/1205 works
    #[test]
    fn force_deploy_private_service() -> Result<(), SchemasUpdateError> {
        let schemas = Schemas::default();
        let deployment = Deployment::mock();

        schemas.apply_updates(schemas.compute_new_deployment(
            Some(deployment.id),
            deployment.metadata.clone(),
            vec![greeter_service()],
            false,
        )?)?;
        assert!(schemas.assert_component(GREETER_SERVICE_NAME).public);

        schemas.apply_updates(vec![SchemasUpdateCommand::ModifyComponent {
            name: GREETER_SERVICE_NAME.to_owned(),
            public: false,
        }])?;
        assert!(!schemas.assert_component(GREETER_SERVICE_NAME).public);

        schemas.apply_updates(schemas.compute_new_deployment(
            Some(deployment.id),
            deployment.metadata.clone(),
            vec![greeter_service()],
            true,
        )?)?;
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

            schemas
                .apply_updates(
                    schemas
                        .compute_new_deployment(
                            Some(deployment_1.id),
                            deployment_1.metadata.clone(),
                            vec![greeter_service()],
                            false,
                        )
                        .unwrap(),
                )
                .unwrap();

            schemas.assert_component_deployment(GREETER_SERVICE_NAME, deployment_1.id);

            let compute_result = schemas.compute_new_deployment(
                Some(deployment_2.id),
                deployment_2.metadata,
                vec![greeter_virtual_object()],
                false,
            );

            assert!(let Err(SchemasUpdateError::IncompatibleServiceChange(IncompatibleServiceChangeError::DifferentComponentInstanceType(_))) = compute_result);
        }
    }

    #[test]
    fn override_existing_deployment_removing_a_service() {
        let schemas = Schemas::default();

        let deployment = Deployment::mock();
        schemas
            .apply_updates(
                schemas
                    .compute_new_deployment(
                        Some(deployment.id),
                        deployment.metadata.clone(),
                        vec![greeter_service(), another_greeter_service()],
                        false,
                    )
                    .unwrap(),
            )
            .unwrap();

        schemas.assert_component_deployment(GREETER_SERVICE_NAME, deployment.id);
        schemas.assert_component_deployment(ANOTHER_GREETER_SERVICE_NAME, deployment.id);

        schemas
            .apply_updates(
                schemas
                    .compute_new_deployment(
                        Some(deployment.id),
                        deployment.metadata.clone(),
                        vec![greeter_service()],
                        true,
                    )
                    .unwrap(),
            )
            .unwrap();

        schemas.assert_component_deployment(GREETER_SERVICE_NAME, deployment.id);
        assert!(schemas
            .resolve_latest_component(ANOTHER_GREETER_SERVICE_NAME)
            .is_none());
    }

    #[test]
    fn cannot_override_existing_deployment_endpoint_conflict() {
        let schemas = Schemas::default();

        let deployment = Deployment::mock();
        schemas
            .apply_updates(
                schemas
                    .compute_new_deployment(
                        Some(deployment.id),
                        deployment.metadata.clone(),
                        vec![greeter_service()],
                        false,
                    )
                    .unwrap(),
            )
            .unwrap();

        assert!(let Err(SchemasUpdateError::OverrideDeployment(_)) = schemas.compute_new_deployment(
            Some(deployment.id),
            deployment.metadata,
            vec![greeter_service()],
            false)
        );
    }

    #[test]
    fn cannot_override_existing_deployment_existing_id_mismatch() {
        let schemas = Schemas::default();

        let deployment = Deployment::mock();
        schemas
            .apply_updates(
                schemas
                    .compute_new_deployment(
                        Some(deployment.id),
                        deployment.metadata.clone(),
                        vec![greeter_service()],
                        false,
                    )
                    .unwrap(),
            )
            .unwrap();

        let new_id = DeploymentId::new();

        let_assert!(
            Err(SchemasUpdateError::IncorrectDeploymentId {
                requested,
                existing
            }) = schemas.compute_new_deployment(
                Some(new_id),
                deployment.metadata,
                vec![greeter_service()],
                false
            )
        );
        assert_eq!(new_id, requested);
        assert_eq!(deployment.id, existing);
    }

    #[test]
    fn register_two_deployments_then_remove_first() {
        let schemas = Schemas::default();

        let deployment_1 = Deployment::mock_with_uri("http://localhost:9080");
        let deployment_2 = Deployment::mock_with_uri("http://localhost:9081");

        schemas
            .apply_updates(
                schemas
                    .compute_new_deployment(
                        Some(deployment_1.id),
                        deployment_1.metadata.clone(),
                        vec![greeter_service(), another_greeter_service()],
                        false,
                    )
                    .unwrap(),
            )
            .unwrap();
        schemas
            .apply_updates(
                schemas
                    .compute_new_deployment(
                        Some(deployment_2.id),
                        deployment_2.metadata.clone(),
                        vec![greeter_service()],
                        false,
                    )
                    .unwrap(),
            )
            .unwrap();

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

        schemas.apply_updates(commands).unwrap();

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
                        input_schema: None,
                        output_schema: None,
                    },
                    schema::Handler {
                        name: "doSomething".parse().unwrap(),
                        input_schema: None,
                        output_schema: None,
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
                    input_schema: None,
                    output_schema: None,
                }],
            }
        }

        #[test]
        fn reject_removing_existing_methods() {
            let schemas = Schemas::default();

            let deployment_1 = Deployment::mock_with_uri("http://localhost:9080");
            let deployment_2 = Deployment::mock_with_uri("http://localhost:9081");

            schemas
                .apply_updates(
                    schemas
                        .compute_new_deployment(
                            Some(deployment_1.id),
                            deployment_1.metadata,
                            vec![greeter_v1_service()],
                            false,
                        )
                        .unwrap(),
                )
                .unwrap();
            schemas.assert_component_revision(GREETER_SERVICE_NAME, 1);

            let rejection = schemas.compute_new_deployment(
                Some(deployment_2.id),
                deployment_2.metadata,
                vec![greeter_v2_service()],
                false,
            );

            schemas.assert_component_revision(GREETER_SERVICE_NAME, 1); // unchanged

            let_assert!(
                Err(SchemasUpdateError::IncompatibleServiceChange(
                    IncompatibleServiceChangeError::RemovedHandlers(service, missing_methods)
                )) = rejection
            );
            check!(service == GREETER_SERVICE_NAME);
            check!(missing_methods == std::vec!["doSomething"]);
        }
    }
}
