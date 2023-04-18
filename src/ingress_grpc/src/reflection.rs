//! This module is very similar to tonic-reflection and in some parts copied,
//! but it differs in the fact that we have interior mutability
//! https://github.com/hyperium/tonic/issues/1328
//!
//! Tonic License MIT: https://github.com/hyperium/tonic

use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};

use arc_swap::{ArcSwap, Guard};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use prost::Message;
use prost_reflect::prost_types::{DescriptorProto, EnumDescriptorProto, FileDescriptorProto};
use prost_reflect::{DescriptorPool, FileDescriptor, ServiceDescriptor};
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, trace};

mod pb {
    #![allow(warnings)]
    #![allow(clippy::all)]
    #![allow(unknown_lints)]
    include!(concat!(env!("OUT_DIR"), "/grpc.reflection.v1alpha.rs"));
}

pub use pb::server_reflection_server::ServerReflection;
pub use pb::server_reflection_server::ServerReflectionServer;

#[derive(Debug, Clone, Default)]
struct ReflectionServiceState {
    service_names: Vec<pb::ServiceResponse>,

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

impl ReflectionServiceState {
    fn add_service(
        &mut self,
        service_name: String,
        symbols_index: HashMap<String, Vec<String>>,
        maybe_files_to_add: &HashMap<String, Bytes>,
    ) {
        match self
            .service_names
            .binary_search_by(|s| service_name.cmp(&s.name))
        {
            Ok(_) => {
                // No need to reinsert
                // TODO We should remove the old symbols first? Or we don't allow this at all?
            }
            Err(insert_index) => {
                // This insert retains the order
                self.service_names.insert(
                    insert_index,
                    pb::ServiceResponse {
                        name: service_name.clone(),
                    },
                );
            }
        }

        let mut files_to_insert = HashMap::new();
        for (symbol_name, related_files) in &symbols_index {
            for related_file in related_files {
                let maybe_file = self.files.get_mut(related_file);
                if let Some((count, _)) = maybe_file {
                    *count += 1;
                    // TODO should this be warn?
                    debug!("Found a type collision when registering service {} symbol {}. \
                    This might indicate two services are sharing the same type definitions, \
                    which is fine as long as the type definitions are equal/compatible with each other.", service_name, symbol_name);
                } else {
                    let file_to_insert = maybe_files_to_add.get(related_file).unwrap().clone();
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

#[derive(Default, Clone)]
pub struct ReflectionRegistry {
    reflection_service_state: Arc<ArcSwap<ReflectionServiceState>>,
}

#[derive(Debug, thiserror::Error)]
pub enum RegistrationError {
    #[error("missing expected field {0} in descriptor")]
    MissingFieldInDescriptor(&'static str),
    #[error("missing service {0} in descriptor")]
    MissingServiceInDescriptor(String),
}

impl ReflectionRegistry {
    pub fn register_new_services(
        &mut self,
        services: Vec<String>,
        descriptor_pool: DescriptorPool,
    ) -> Result<(), RegistrationError> {
        let mut discovered_files = HashMap::new();
        let mut discovered_services = vec![];

        for service_name in services {
            // Let's find the service first
            let service_desc = descriptor_pool
                .get_service_by_name(&service_name)
                .ok_or_else(|| {
                    RegistrationError::MissingServiceInDescriptor(service_name.clone())
                })?;

            // Collect all the files belonging to this service
            let files = collect_service_related_file_descriptors(service_desc);

            let mut service_symbols = HashMap::new();

            // Process files
            for file in &files {
                let file_arc = discovered_files
                    .entry(file.name().to_string())
                    .or_insert_with(|| Arc::new(file.clone()));

                // Discover all symbols in this file
                let mut file_symbols = HashSet::new();
                process_file(&mut file_symbols, file_arc.as_ref().file_descriptor_proto())?;

                // Copy the file_symbols in service_symbols
                for symbol in file_symbols {
                    service_symbols.insert(symbol, vec![file.name().to_string()]);
                }
            }

            // Service also needs to include itself in the symbols map. We include it with all the dependencies,
            // so when querying we'll include all the transitive dependencies.
            service_symbols.insert(
                service_name.clone(),
                files
                    .iter()
                    .map(|fd| fd.name().to_string())
                    .collect::<Vec<_>>(),
            );

            trace!(
                "Symbols associated to service '{}': {:?}",
                service_name,
                service_symbols.keys()
            );
            discovered_services.push((service_name, service_symbols));
        }

        // Now serialize the discovered files
        let serialized_files = discovered_files
            .into_iter()
            .map(|(file_name, file)| {
                (
                    file_name,
                    Bytes::from(file.as_ref().file_descriptor_proto().encode_to_vec()),
                )
            })
            .collect::<HashMap<_, _>>();

        // Now finally update the shared state
        let state = self.reflection_service_state.load();
        let mut new_state = ReflectionServiceState::clone(&state);
        for (service_name, symbols_to_file_names) in discovered_services {
            new_state.add_service(service_name, symbols_to_file_names, &serialized_files)
        }

        self.reflection_service_state.store(Arc::new(new_state));

        Ok(())
    }
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

pub struct ReflectionServiceStream {
    request_stream: Streaming<pb::ServerReflectionRequest>,
    state: Guard<Arc<ReflectionServiceState>>,
}

impl ReflectionServiceStream {
    fn handle_request(
        &self,
        request: &pb::server_reflection_request::MessageRequest,
    ) -> Result<pb::server_reflection_response::MessageResponse, Status> {
        use pb::server_reflection_request::*;
        use pb::server_reflection_response::*;
        use pb::*;

        Ok(match request {
            MessageRequest::FileByFilename(f) => self
                .state
                .get_file(f)
                .map(|file| {
                    MessageResponse::FileDescriptorResponse(FileDescriptorResponse {
                        file_descriptor_proto: vec![file],
                    })
                })
                .ok_or_else(|| Status::not_found(f.to_string()))?,
            MessageRequest::FileContainingSymbol(symbol_name) => self
                .state
                .get_symbol(symbol_name)
                .map(|files| {
                    MessageResponse::FileDescriptorResponse(FileDescriptorResponse {
                        file_descriptor_proto: files,
                    })
                })
                .ok_or_else(|| Status::not_found(symbol_name.to_string()))?,
            MessageRequest::FileContainingExtension(_) => {
                // We don't index extensions, empty response is fine
                MessageResponse::FileDescriptorResponse(FileDescriptorResponse::default())
            }
            MessageRequest::AllExtensionNumbersOfType(_) => {
                // We don't index extensions, empty response is fine
                MessageResponse::AllExtensionNumbersResponse(ExtensionNumberResponse::default())
            }
            MessageRequest::ListServices(_) => {
                debug!("List services");
                MessageResponse::ListServicesResponse(ListServiceResponse {
                    service: self.state.service_names.clone(),
                })
            }
        })
    }
}

impl Stream for ReflectionServiceStream {
    type Item = Result<pb::ServerReflectionResponse, Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use pb::server_reflection_response::*;
        use pb::*;

        let request = match ready!(self.request_stream.poll_next_unpin(cx)) {
            Some(Ok(req)) => req,
            _ => return Poll::Ready(None),
        };

        let message_request = if let Some(req) = request.message_request.as_ref() {
            req
        } else {
            return Poll::Ready(Some(Err(Status::invalid_argument(
                "Unexpected empty MessageRequest",
            ))));
        };

        let message_response = match self.handle_request(message_request) {
            Ok(resp_msg) => resp_msg,
            Err(status) => MessageResponse::ErrorResponse(ErrorResponse {
                error_code: status.code() as i32,
                error_message: status.message().to_string(),
            }),
        };

        Poll::Ready(Some(Ok(ServerReflectionResponse {
            valid_host: request.host.clone(),
            original_request: Some(request),
            message_response: Some(message_response),
        })))
    }
}

#[tonic::async_trait]
impl ServerReflection for ReflectionRegistry {
    type ServerReflectionInfoStream = ReflectionServiceStream;

    async fn server_reflection_info(
        &self,
        request: Request<Streaming<pb::ServerReflectionRequest>>,
    ) -> Result<Response<Self::ServerReflectionInfoStream>, Status> {
        Ok(Response::new(ReflectionServiceStream {
            request_stream: request.into_inner(),
            state: self.reflection_service_state.load(),
        }))
    }
}
