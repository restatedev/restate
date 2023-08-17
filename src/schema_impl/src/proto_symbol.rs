// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::Schemas;

use bytes::Bytes;
use prost::Message;
use prost_reflect::prost_types::FileDescriptorProto;
use prost_reflect::{EnumDescriptor, FileDescriptor, MessageDescriptor, ServiceDescriptor};
use restate_schema_api::proto_symbol::ProtoSymbolResolver;
use restate_types::identifiers::EndpointId;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::{debug, trace};

impl ProtoSymbolResolver for Schemas {
    fn list_services(&self) -> Vec<String> {
        let guard = self.0.load();
        guard
            .services
            .iter()
            .filter_map(|(service_name, service_schemas)| {
                if service_schemas.location.is_ingress_available() {
                    Some(service_name.clone())
                } else {
                    None
                }
            })
            .collect()
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
struct FileEntry {
    reference_count: usize,
    serialized: Bytes,
    // This is used for removal
    contained_message_or_enum_symbols: HashSet<String>,
}

#[derive(Debug, Clone, Default)]
struct FilesIndex(
    /// Key is the normalized file name
    HashMap<String, FileEntry>,
);

impl FilesIndex {
    fn add(
        &mut self,
        normalized_file_name: String,
        endpoint_id: &EndpointId,
        file_desc: FileDescriptor,
        message_or_enum_symbols: HashSet<String>,
    ) {
        self.0
            .entry(normalized_file_name)
            .and_modify(|file_entry| {
                debug_assert_eq!(
                    file_entry.contained_message_or_enum_symbols, message_or_enum_symbols,
                    "File should have the same symbols set"
                );
                file_entry.reference_count += 1;
            })
            .or_insert_with(|| FileEntry {
                reference_count: 1,
                serialized: normalize_self_and_dependencies_file_names(
                    endpoint_id,
                    file_desc.file_descriptor_proto().clone(),
                )
                .encode_to_vec()
                .into(),
                contained_message_or_enum_symbols: message_or_enum_symbols,
            });
    }

    /// Returns the message_or_enum symbols to remove
    fn remove(&mut self, normalized_file_name: &str) -> Option<HashSet<String>> {
        if let Some(FileEntry {
            reference_count, ..
        }) = self.0.get_mut(normalized_file_name)
        {
            *reference_count -= 1;
            if *reference_count == 0 {
                let symbols_to_remove = self
                    .0
                    .remove(normalized_file_name)
                    .unwrap()
                    .contained_message_or_enum_symbols;
                return Some(symbols_to_remove);
            }
        }
        None
    }

    fn get(&self, normalized_file_name: &str) -> Option<Bytes> {
        self.0
            .get(normalized_file_name)
            .map(|entry| entry.serialized.clone())
    }
}

#[derive(Debug, Clone)]
enum Symbol {
    /// Service and methods have a vector containing all the files defining the service and its dependencies.
    /// When removing a service, all the methods are removed as well.
    /// File names are normalized.
    ServiceOrMethod(Arc<Vec<String>>),

    /// Message or enum have a vector specifying all the files containing this symbol.
    /// The last entry of the vector is the file that should be served when the symbol is requested.
    /// When removing a file, the file should be removed from this vector list as well.
    /// File names are normalized.
    MessageOrEnum(Vec<String>),
}

#[derive(Debug, Clone, Default)]
struct SymbolsIndex(HashMap<String, Symbol>);

impl SymbolsIndex {
    /// `methods` should be the [`prost_reflect::MethodDescriptor::full_name`]
    fn add_service(
        &mut self,
        service_name: String,
        methods: Vec<String>,
        dependencies: Vec<String>,
    ) {
        let rc = Arc::new(dependencies);
        for method in methods {
            self.0
                .insert(method, Symbol::ServiceOrMethod(Arc::clone(&rc)));
        }
        self.0.insert(service_name, Symbol::ServiceOrMethod(rc));
    }

    fn add_message_or_enum(&mut self, symbol_name: String, file: String) {
        let symbol = self
            .0
            .entry(symbol_name.clone())
            .or_insert_with(|| Symbol::MessageOrEnum(Vec::with_capacity(1)));
        if symbol_name.starts_with("dev.restate") || symbol_name.starts_with("google.protobuf") {
            trace!(
                "Found a type collision when registering symbol '{}'. \
                For dev.restate types and google.protobuf types, \
                this should be fine as the definition of these types don't change.",
                symbol_name
            );
        } else {
            debug!(
                "Found a type collision when registering symbol '{}'. \
                This might indicate two services are sharing the same type definitions, \
                which is fine as long as the type definitions are equal/compatible with each other.",
                symbol_name
            );
        }

        match symbol {
            Symbol::MessageOrEnum(files) => {
                files.push(file);
            }
            Symbol::ServiceOrMethod(_) => {
                unreachable!(
                    "Trying to register a message/enum symbol but the symbol is method/service"
                );
            }
        };
    }

    /// `methods` should be the [`prost_reflect::MethodDescriptor::full_name`]
    fn remove_service(
        &mut self,
        service_name: &str,
        methods: impl Iterator<Item = String>,
    ) -> Arc<Vec<String>> {
        let service_symbol = self.0.remove(service_name);
        for method in methods {
            self.0.remove(&method);
        }
        match service_symbol {
            Some(Symbol::ServiceOrMethod(arc)) => arc,
            _ => {
                panic!("The removed symbol should be a ServiceOrMethod!")
            }
        }
    }

    fn remove_message_or_enum(&mut self, symbol_name: &str, file: &str) {
        if let Some(Symbol::MessageOrEnum(files)) = self.0.get_mut(symbol_name) {
            if let Some(pos) = files.iter().position(|x| x.as_str() == file) {
                files.remove(pos);
            }
            if files.is_empty() {
                self.0.remove(symbol_name);
            }
        }
    }

    fn contains(&self, symbol_name: &str) -> bool {
        self.0.contains_key(symbol_name)
    }

    fn get_files_of_symbol(&self, symbol_name: &str) -> Option<impl Iterator<Item = &String>> {
        self.0.get(symbol_name).map(|symbol| match symbol {
            Symbol::ServiceOrMethod(deps) => itertools::Either::Right(deps.iter()),
            Symbol::MessageOrEnum(files) => itertools::Either::Left(files.last().into_iter()),
        })
    }
}

#[derive(Debug, Clone)]
pub(super) struct ProtoSymbols {
    files: FilesIndex,
    symbols: SymbolsIndex,
}

impl Default for ProtoSymbols {
    fn default() -> Self {
        let mut symbols = ProtoSymbols {
            files: Default::default(),
            symbols: Default::default(),
        };
        symbols.add_service(
            &"self_ingress".to_string(),
            &restate_pb::DESCRIPTOR_POOL
                .get_service_by_name(restate_pb::REFLECTION_SERVICE_NAME)
                .expect("Service reflections should exist"),
        );
        symbols.add_service(
            &"self_ingress".to_string(),
            &restate_pb::DESCRIPTOR_POOL
                .get_service_by_name(restate_pb::INGRESS_SERVICE_NAME)
                .expect("Service ingress should exist"),
        );
        symbols.add_service(
            &"self_ingress".to_string(),
            &restate_pb::DESCRIPTOR_POOL
                .get_service_by_name(restate_pb::HEALTH_SERVICE_NAME)
                .expect("Service ingress should exist"),
        );

        symbols
    }
}

impl ProtoSymbols {
    pub(super) fn add_service(
        &mut self,
        endpoint_id: &EndpointId,
        service_desc: &ServiceDescriptor,
    ) {
        debug_assert!(
            !self.symbols.contains(service_desc.full_name()),
            "Cannot add service '{}' because it already exists",
            service_desc.full_name()
        );

        // Collect all the files belonging to this service
        let files: HashMap<String, FileDescriptor> =
            collect_service_related_file_descriptors(service_desc)
                .into_iter()
                .map(|file_desc| {
                    (
                        normalize_file_name(endpoint_id, file_desc.name()),
                        file_desc,
                    )
                })
                .collect();

        // Add service to symbols
        self.symbols.add_service(
            service_desc.full_name().to_string(),
            service_desc
                .methods()
                .map(|m| m.full_name().to_string())
                .collect(),
            files.keys().cloned().collect(),
        );

        // Process files to register symbols and files
        for (file_name, file_desc) in files {
            // Discover all symbols in this file
            let mut message_or_enum_symbols = HashSet::new();
            collect_message_or_enum_symbols(&mut message_or_enum_symbols, &file_desc);

            // Copy the file_symbols in symbols_index
            for symbol in message_or_enum_symbols.clone() {
                self.symbols.add_message_or_enum(symbol, file_name.clone());
            }

            // Add the file descriptor
            self.files
                .add(file_name, endpoint_id, file_desc, message_or_enum_symbols);
        }
    }

    pub(super) fn remove_service(&mut self, service_desc: &ServiceDescriptor) {
        debug_assert!(
            self.symbols.contains(service_desc.full_name()),
            "Cannot remove service '{}' because it doesn't exist",
            service_desc.full_name()
        );

        // Remove the service from the symbols index
        let methods = service_desc.methods();
        let service_related_files = self.symbols.remove_service(
            service_desc.full_name(),
            methods.map(|m| m.full_name().to_string()),
        );

        // For each file related to the service, remove it
        for file_name in service_related_files.iter() {
            // If when removing a file we free it, then we need to remove the related message and symbols as well
            if let Some(message_or_enum_symbols_to_remove) = self.files.remove(file_name) {
                for message_or_enum_symbol in message_or_enum_symbols_to_remove {
                    self.symbols
                        .remove_message_or_enum(&message_or_enum_symbol, file_name);
                }
            }
        }
    }

    fn get_file(&self, file_name: &str) -> Option<Bytes> {
        debug!("Get file {}", file_name);
        self.files.get(file_name)
    }

    fn get_symbol(&self, symbol_name: &str) -> Option<Vec<Bytes>> {
        debug!("Get symbol {}", symbol_name);
        self.symbols.get_files_of_symbol(symbol_name).map(|files| {
            files
                .map(|file_name| self.get_file(file_name).unwrap())
                .collect::<Vec<_>>()
        })
    }
}

// We rename files prepending them with the endpoint id
// to avoid collision between file names of unrelated endpoints
// TODO with schema checks in place,
//  should we move this or remove this normalization of the file name?
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

fn collect_service_related_file_descriptors(svc_desc: &ServiceDescriptor) -> Vec<FileDescriptor> {
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

fn collect_message_or_enum_symbols(symbols: &mut HashSet<String>, fd: &FileDescriptor) {
    for msg_desc in fd.messages() {
        process_message(symbols, msg_desc);
    }

    for enum_desc in fd.enums() {
        process_enum(symbols, enum_desc);
    }
}

fn process_message(symbols: &mut HashSet<String>, msg: MessageDescriptor) {
    symbols.insert(msg.full_name().to_string());

    for nested_msg in msg.child_messages() {
        process_message(symbols, nested_msg);
    }

    for nested_enum in msg.child_enums() {
        process_enum(symbols, nested_enum);
    }
}

fn process_enum(symbols: &mut HashSet<String>, en: EnumDescriptor) {
    symbols.insert(en.full_name().to_string());
}
