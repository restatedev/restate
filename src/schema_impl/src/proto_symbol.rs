use super::Schemas;

use crate::RegistrationError;
use bytes::Bytes;
use prost::Message;
use prost_reflect::prost_types::{DescriptorProto, EnumDescriptorProto, FileDescriptorProto};
use prost_reflect::{DescriptorPool, FileDescriptor, ServiceDescriptor};
use restate_schema_api::proto_symbol::ProtoSymbolResolver;
use restate_types::identifiers::EndpointId;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::{debug, trace};

impl ProtoSymbolResolver for Schemas {
    fn list_services(&self) -> Vec<String> {
        let guard = self.0.load();
        guard.services.keys().cloned().collect()
    }

    fn get_file_descriptors_by_symbol_name(&self, symbol: &str) -> Option<Vec<Bytes>> {
        let guard = self.0.load();
        guard.proto_symbols.get_symbol(symbol)
    }

    fn get_file_descriptor(&self, file_name: &str) -> Option<Bytes> {
        let guard = self.0.load();
        guard.proto_symbols.get_file(file_name)
    }
}

#[derive(Debug, Clone)]
pub(super) struct ProtoSymbols {
    // The usize here is used for reference count
    files: HashMap<String, (usize, Bytes)>,

    // Symbols contain the fully qualified names of:
    // * Services
    // * Methods
    // * Message
    // * Enum
    // Values are the names of the files, including dependencies
    symbols_index: HashMap<String, Vec<String>>,
}

impl Default for ProtoSymbols {
    fn default() -> Self {
        let mut symbols = ProtoSymbols {
            files: Default::default(),
            symbols_index: Default::default(),
        };
        symbols
            .register_new_service(
                "self_ingress".to_string(),
                restate_pb::REFLECTION_SERVICE_NAME.to_string(),
                restate_pb::DESCRIPTOR_POOL.clone(),
            )
            .expect("Registering self_ingress in the reflections must not fail");
        symbols
            .register_new_service(
                "self_ingress".to_string(),
                restate_pb::INGRESS_SERVICE_NAME.to_string(),
                restate_pb::DESCRIPTOR_POOL.clone(),
            )
            .expect("Registering self_ingress in the reflections must not fail");

        symbols
    }
}

impl ProtoSymbols {
    pub(super) fn register_new_service(
        &mut self,
        endpoint_id: EndpointId,
        service_name: String,
        descriptor_pool: DescriptorPool,
    ) -> Result<(), RegistrationError> {
        let mut discovered_files = HashMap::new();

        // Let's find the service first
        let service_desc = descriptor_pool
            .get_service_by_name(&service_name)
            .ok_or_else(|| RegistrationError::MissingServiceInDescriptor(service_name.clone()))?;

        // Collect all the files belonging to this service
        let files = collect_service_related_file_descriptors(service_desc);

        let mut service_symbols = HashMap::new();

        // Process files
        for file in &files {
            // We rename files prepending them with the endpoint id
            // to avoid collision between file names of unrelated endpoints
            // TODO with schema checks in place,
            //  should we move this or remove this normalization of the file name?
            let file_name = normalize_file_name(&endpoint_id, file.name());
            let file_arc = discovered_files
                .entry(file_name.clone())
                .or_insert_with(|| Arc::new(file.clone()));

            // Discover all symbols in this file
            let mut file_symbols = HashSet::new();
            process_file(&mut file_symbols, file_arc.as_ref().file_descriptor_proto())?;

            // Copy the file_symbols in service_symbols
            for symbol in file_symbols {
                service_symbols.insert(symbol, vec![file_name.clone()]);
            }
        }

        // Service also needs to include itself in the symbols map. We include it with all the dependencies,
        // so when querying we'll include all the transitive dependencies.
        service_symbols.insert(
            service_name.clone(),
            files
                .iter()
                .map(|fd| normalize_file_name(&endpoint_id, fd.name()))
                .collect::<Vec<_>>(),
        );

        trace!(
            "Symbols associated to service '{}': {:?}",
            service_name,
            service_symbols.keys()
        );

        // Now serialize the discovered files
        let serialized_files = discovered_files
            .into_iter()
            .map(|(file_name, file)| {
                (
                    file_name,
                    Bytes::from(
                        normalize_self_and_dependencies_file_names(
                            &endpoint_id,
                            file.file_descriptor_proto().clone(),
                        )
                        .encode_to_vec(),
                    ),
                )
            })
            .collect::<HashMap<_, _>>();

        // Now finally update the shared state
        self.add_service(service_name, service_symbols, &serialized_files);

        Ok(())
    }

    fn add_service(
        &mut self,
        service_name: String,
        symbols_index: HashMap<String, Vec<String>>,
        maybe_files_to_add: &HashMap<String, Bytes>,
    ) {
        let mut files_to_insert = HashMap::new();
        for (symbol_name, related_files) in &symbols_index {
            for related_file in related_files {
                let maybe_file = self.files.get_mut(related_file);
                if let Some((count, _)) = maybe_file {
                    *count += 1;
                    if symbol_name.starts_with("dev.restate")
                        || symbol_name.starts_with("google.protobuf")
                    {
                        trace!(
                            "Found a type collision when registering service '{}' symbol '{}'. \
                        For dev.restate types and google.protobuf types, \
                        this should be fine as the definition of these types don't change.",
                            service_name,
                            symbol_name
                        );
                    } else {
                        debug!(
                        "Found a type collision when registering service '{}' symbol '{}'. \
                        This might indicate two services are sharing the same type definitions, \
                        which is fine as long as the type definitions are equal/compatible with each other.", service_name, symbol_name);
                    }
                } else {
                    let file_to_insert = maybe_files_to_add
                        .get(related_file)
                        .expect("maybe_files_to_add must be a superset of symbols_index values")
                        .clone();
                    files_to_insert.insert(related_file.clone(), (1, file_to_insert));
                }
            }
        }
        self.files.extend(files_to_insert);
        self.symbols_index.extend(symbols_index);
    }

    #[allow(dead_code)]
    fn remove_service(&mut self, _service_name: String) {
        todo!("Start by looking for the service name, parse the file descriptor and then walk it back to remove all the symbols. Can reuse process_file below");
    }

    fn get_file(&self, file_name: &str) -> Option<Bytes> {
        debug!("Get file {}", file_name);
        self.files.get(file_name).map(|(_, bytes)| bytes.clone())
    }

    fn get_symbol(&self, symbol_name: &str) -> Option<Vec<Bytes>> {
        debug!("Get symbol {}", symbol_name);
        self.symbols_index.get(symbol_name).map(|files| {
            files
                .iter()
                .map(|file_name| self.get_file(file_name).unwrap())
                .collect::<Vec<_>>()
        })
    }
}

fn normalize_file_name(endpoint_id: &str, file_name: &str) -> String {
    // Because file_name is a path (either relative or absolute),
    // prepending endpoint_id/ should always be fine
    format!("{endpoint_id}/{file_name}")
}

fn normalize_self_and_dependencies_file_names(
    endpoint_id: &str,
    mut file_desc_proto: FileDescriptorProto,
) -> FileDescriptorProto {
    file_desc_proto.name = file_desc_proto
        .name
        .map(|name| normalize_file_name(endpoint_id, &name));
    for dep in file_desc_proto.dependency.iter_mut() {
        *dep = normalize_file_name(endpoint_id, dep)
    }
    file_desc_proto
}

fn collect_service_related_file_descriptors(svc_desc: ServiceDescriptor) -> Vec<FileDescriptor> {
    rec_collect_dependencies(svc_desc.parent_file())
}

fn rec_collect_dependencies(file_desc: FileDescriptor) -> Vec<FileDescriptor> {
    let mut result = vec![];
    result.push(file_desc.clone());
    for dependency in file_desc.dependencies() {
        result.extend(rec_collect_dependencies(dependency))
    }

    result
}

// --- Took from tonic-reflection
// I teared off indexing fields and enum variants, as this is not done by the Java grpc implementation
// https://github.com/hyperium/tonic/blob/9990e6ef9d00394b5662719662062cc85e6e4700/tonic-reflection/src/server.rs

fn process_file(
    symbols: &mut HashSet<String>,
    fd: &FileDescriptorProto,
) -> Result<(), RegistrationError> {
    let prefix = &fd.package.clone().unwrap_or_default();

    for msg in &fd.message_type {
        process_message(symbols, prefix, msg)?;
    }

    for en in &fd.enum_type {
        process_enum(symbols, prefix, en)?;
    }

    for service in &fd.service {
        let service_name = extract_name(prefix, "service", service.name.as_ref())?;
        symbols.insert(service_name.clone());

        for method in &service.method {
            let method_name = extract_name(&service_name, "method", method.name.as_ref())?;
            symbols.insert(method_name);
        }
    }

    Ok(())
}

fn process_message(
    symbols: &mut HashSet<String>,
    prefix: &str,
    msg: &DescriptorProto,
) -> Result<(), RegistrationError> {
    let message_name = extract_name(prefix, "message", msg.name.as_ref())?;
    symbols.insert(message_name.clone());

    for nested in &msg.nested_type {
        process_message(symbols, &message_name, nested)?;
    }

    for en in &msg.enum_type {
        process_enum(symbols, &message_name, en)?;
    }

    Ok(())
}

fn process_enum(
    symbols: &mut HashSet<String>,
    prefix: &str,
    en: &EnumDescriptorProto,
) -> Result<(), RegistrationError> {
    let enum_name = extract_name(prefix, "enum", en.name.as_ref())?;
    symbols.insert(enum_name);
    Ok(())
}

fn extract_name(
    prefix: &str,
    name_type: &'static str,
    maybe_name: Option<&String>,
) -> Result<String, RegistrationError> {
    match maybe_name {
        None => Err(RegistrationError::MissingFieldInDescriptor(name_type)),
        Some(name) => {
            if prefix.is_empty() {
                Ok(name.to_string())
            } else {
                Ok(format!("{}.{}", prefix, name))
            }
        }
    }
}
