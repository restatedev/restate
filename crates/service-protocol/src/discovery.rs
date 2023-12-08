// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use std::fmt::Display;

use bytes::Bytes;

use hyper::header::{ACCEPT, CONTENT_TYPE};
use hyper::http::response::Parts as ResponseParts;
use hyper::http::uri::PathAndQuery;
use hyper::http::{HeaderName, HeaderValue};
use hyper::{Body, HeaderMap};
use prost::{DecodeError, Message};
use prost_reflect::{
    Cardinality, DescriptorError, DescriptorPool, ExtensionDescriptor, FieldDescriptor, Kind,
    MethodDescriptor, ServiceDescriptor,
};

use codederror::CodedError;
use restate_errors::{warn_it, META0001, META0002, META0003, META0007, META0008};
use restate_schema_api::discovery::{
    DiscoveredInstanceType, DiscoveredMethodMetadata, FieldAnnotation, KeyStructure,
    ServiceRegistrationRequest,
};
use restate_schema_api::endpoint::ProtocolType;
use restate_service_client::{
    Parts, Request, ServiceClient, ServiceClientError, ServiceEndpointAddress,
};

use restate_types::retries::{RetryIter, RetryPolicy};

// Clippy false positive, might be caused by Bytes contained within HeaderValue.
// https://github.com/rust-lang/rust/issues/40543#issuecomment-1212981256
#[allow(clippy::declare_interior_mutable_const)]
const APPLICATION_PROTO: HeaderValue = HeaderValue::from_static("application/proto");

const RESTATE_SERVICE_NAME_PREFIX: &str = "dev.restate.";
const GRPC_SERVICE_NAME_PREFIX: &str = "grpc.";
const SERVICE_TYPE_EXT: &str = "dev.restate.ext.service_type";
const FIELD_EXT: &str = "dev.restate.ext.field";

const UNKEYED_SERVICE_EXT: i32 = 0;
const KEYED_SERVICE_EXT: i32 = 1;
const SINGLETON_SERVICE_EXT: i32 = 2;

const KEY_FIELD_EXT: i32 = 0;
const EVENT_PAYLOAD_FIELD_EXT: i32 = 1;
const EVENT_METADATA_FIELD_EXT: i32 = 2;

const DISCOVER_PATH: &str = "/discover";

mod pb {
    pub use generated_structs::ProtocolMode;
    pub use generated_structs::ServiceDiscoveryRequest;

    use super::*;

    mod generated_structs {
        #![allow(warnings)]
        #![allow(clippy::all)]
        #![allow(unknown_lints)]
        include!(concat!(
            env!("OUT_DIR"),
            "/dev.restate.service.discovery.rs"
        ));
    }

    // We manually define the protobuf struct for the response here because prost-build
    // won't parse extensions in ServiceDiscoveryResponse.files
    // We can simplify this code once https://github.com/tokio-rs/prost/pull/591 is fixed
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct ServiceDiscoveryResponse {
        // This field is different from what is defined in the protobuf schema.
        // google.protobuf.FileDescriptorSet files = 1;
        //
        // Because nested messages are serialized as byte arrays, we simply load the raw byte arrays
        // and parse them below using DescriptorPool::decode() with prost-reflect, which can deserialize extensions.
        #[prost(bytes, tag = "1")]
        pub files: Bytes,
        #[prost(string, repeated, tag = "2")]
        pub services: Vec<String>,
        #[prost(uint32, tag = "3")]
        pub min_protocol_version: u32,
        #[prost(uint32, tag = "4")]
        pub max_protocol_version: u32,
        #[prost(enumeration = "ProtocolMode", tag = "5")]
        pub protocol_mode: i32,
    }
}

#[derive(Debug)]
pub struct ServiceDiscovery {
    retry_policy: RetryPolicy,
    client: ServiceClient,
}

impl ServiceDiscovery {
    pub fn new(retry_policy: RetryPolicy, client: ServiceClient) -> Self {
        Self {
            retry_policy,
            client,
        }
    }
}

#[derive(Clone)]
pub struct DiscoverEndpoint(ServiceEndpointAddress, HashMap<HeaderName, HeaderValue>);

impl DiscoverEndpoint {
    pub fn new(
        address: ServiceEndpointAddress,
        additional_headers: HashMap<HeaderName, HeaderValue>,
    ) -> Self {
        Self(address, additional_headers)
    }

    pub fn into_inner(self) -> (ServiceEndpointAddress, HashMap<HeaderName, HeaderValue>) {
        (self.0, self.1)
    }

    pub fn address(&self) -> &ServiceEndpointAddress {
        &self.0
    }

    fn request(&self) -> Request<Body> {
        let mut headers = HeaderMap::from_iter([
            (CONTENT_TYPE, APPLICATION_PROTO),
            (ACCEPT, APPLICATION_PROTO),
        ]);
        headers.extend(self.1.clone());
        let path = PathAndQuery::from_static(DISCOVER_PATH);
        Request::new(
            Parts::new(self.0.clone(), path, headers),
            Body::from(pb::ServiceDiscoveryRequest {}.encode_to_vec()),
        )
    }
}

#[derive(Debug)]
pub struct DiscoveredEndpointMetadata {
    pub services: Vec<ServiceRegistrationRequest>,
    pub descriptor_pool: DescriptorPool,
    pub protocol_type: ProtocolType,
}

#[derive(Debug, thiserror::Error, CodedError)]
pub enum ServiceDiscoveryError {
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

    // Network related retryable errors
    #[error("retry limit exhausted. Last bad status code: {0}")]
    #[code(META0003)]
    BadStatusCode(u16),
    #[error("retry limit exhausted. Last client error: {0}")]
    #[code(META0003)]
    Client(#[from] ServiceClientError),
}

impl ServiceDiscoveryError {
    pub fn is_user_error(&self) -> bool {
        matches!(
            self,
            ServiceDiscoveryError::BadUri(_)
                | ServiceDiscoveryError::MissingServiceTypeExtension(_)
                | ServiceDiscoveryError::KeyedServiceWithoutMethods(_)
                | ServiceDiscoveryError::MissingKeyField(_)
                | ServiceDiscoveryError::MoreThanOneAnnotatedField(_, _)
                | ServiceDiscoveryError::BadKeyFieldType(_)
                | ServiceDiscoveryError::DifferentKeyTypes(_)
                | ServiceDiscoveryError::BadEventPayloadFieldType(_)
                | ServiceDiscoveryError::BadEventMetadataFieldType(_)
        )
    }
}

impl ServiceDiscovery {
    pub async fn discover(
        &self,
        endpoint: &DiscoverEndpoint,
    ) -> Result<DiscoveredEndpointMetadata, ServiceDiscoveryError> {
        let retry_policy = self.retry_policy.clone().into_iter();
        let (mut parts, body) = Self::invoke_discovery_endpoint(
            &self.client,
            endpoint.address(),
            || endpoint.request(),
            retry_policy,
        )
        .await?;

        // Validate response parts.
        // No need to retry these: if the validation fails, they're sdk bugs.
        let content_type = parts.headers.remove(CONTENT_TYPE);
        match content_type {
            // False positive with Bytes field
            #[allow(clippy::borrow_interior_mutable_const)]
            Some(ct) if ct == APPLICATION_PROTO => {}
            _ => {
                return Err(ServiceDiscoveryError::BadResponse(
                    "Bad content type header",
                ));
            }
        }

        // Build the descriptor pool
        let response: pb::ServiceDiscoveryResponse = pb::ServiceDiscoveryResponse::decode(body)?;
        let descriptor_pool = DescriptorPool::decode(patch_built_in_descriptors(response.files)?)?;

        // Find the Restate extensions in the DescriptorPool.
        // If they're not available, the descriptor pool is incomplete/doesn't contain the restate dependencies.
        let restate_service_type_extension = descriptor_pool
            .get_extension_by_name(SERVICE_TYPE_EXT)
            .ok_or(ServiceDiscoveryError::BadOrMissingRestateDependencyInDescriptor)?;
        let restate_key_extension = descriptor_pool
            .get_extension_by_name(FIELD_EXT)
            .ok_or(ServiceDiscoveryError::BadOrMissingRestateDependencyInDescriptor)?;

        // Collect all the service descriptors
        let service_descriptors = response
            .services
            .into_iter()
            .map(|svc| {
                descriptor_pool
                    .get_service_by_name(&svc)
                    .ok_or(ServiceDiscoveryError::ServiceNotFoundInDescriptor(svc))
            })
            .collect::<Result<Vec<_>, _>>()?;

        // Infer service instance type
        let mut services = Vec::with_capacity(service_descriptors.len());
        for svc_desc in service_descriptors {
            let svc_name = svc_desc.full_name();
            if svc_name.starts_with(RESTATE_SERVICE_NAME_PREFIX)
                || svc_name.starts_with(GRPC_SERVICE_NAME_PREFIX)
            {
                return Err(ServiceDiscoveryError::ServiceNameReserved(
                    svc_desc.full_name().to_string(),
                ));
            }
            let mut methods = HashMap::with_capacity(svc_desc.methods().len());

            let instance_type = infer_service_type(
                &svc_desc,
                &restate_service_type_extension,
                &restate_key_extension,
                &mut methods,
            )?;

            infer_event_fields_annotations(&svc_desc, &restate_key_extension, &mut methods)?;

            services.push(ServiceRegistrationRequest {
                name: svc_name.to_string(),
                instance_type,
                methods,
            })
        }

        Ok(DiscoveredEndpointMetadata {
            services,
            descriptor_pool,
            protocol_type: match pb::ProtocolMode::try_from(response.protocol_mode) {
                Ok(pb::ProtocolMode::BidiStream) => ProtocolType::BidiStream,
                Ok(pb::ProtocolMode::RequestResponse) => ProtocolType::RequestResponse,
                Err(_) => {
                    return Err(ServiceDiscoveryError::BadResponse(
                        "cannot decode protocol_mode",
                    ));
                }
            },
        })
    }

    async fn invoke_discovery_endpoint(
        client: &ServiceClient,
        address: impl Display,
        build_request: impl Fn() -> Request<Body>,
        mut retry_iter: RetryIter,
    ) -> Result<(ResponseParts, Bytes), ServiceDiscoveryError> {
        loop {
            let response_fut = client.call(build_request());
            let response = async {
                let (parts, body) = response_fut
                    .await
                    .map_err(Into::<ServiceDiscoveryError>::into)?
                    .into_parts();

                if !parts.status.is_success() {
                    return Err(ServiceDiscoveryError::BadStatusCode(parts.status.as_u16()));
                }

                Ok((
                    parts,
                    hyper::body::to_bytes(body).await.map_err(|err| {
                        ServiceDiscoveryError::Client(ServiceClientError::Http(err.into()))
                    })?,
                ))
            };

            let e = match response.await {
                Ok(response) => {
                    // Discovery succeeded
                    return Ok(response);
                }
                Err(e) => e,
            };

            // Discovery failed
            if let Some(next_retry_interval) = retry_iter.next() {
                warn_it!(
                    e,
                    "Error when discovering service endpoint at address '{}'. Retrying in {} seconds",
                    address,
                    next_retry_interval.as_secs()
                );
                tokio::time::sleep(next_retry_interval).await;
            } else {
                warn_it!(e, "Error when discovering service endpoint '{}'", address);
                return Err(e);
            }
        }
    }
}

pub fn infer_service_type(
    desc: &ServiceDescriptor,
    restate_service_type_ext: &ExtensionDescriptor,
    restate_field_ext: &ExtensionDescriptor,
    methods: &mut HashMap<String, DiscoveredMethodMetadata>,
) -> Result<DiscoveredInstanceType, ServiceDiscoveryError> {
    if !desc.options().has_extension(restate_service_type_ext) {
        return Err(ServiceDiscoveryError::MissingServiceTypeExtension(
            desc.full_name().to_string(),
        ));
    }

    let service_instance_type = desc
        .options()
        .get_extension(restate_service_type_ext)
        .as_enum_number()
        // This can happen only if the restate dependency is bad?
        .ok_or_else(|| ServiceDiscoveryError::BadOrMissingRestateDependencyInDescriptor)?;

    match service_instance_type {
        UNKEYED_SERVICE_EXT => Ok(DiscoveredInstanceType::Unkeyed),
        KEYED_SERVICE_EXT => infer_keyed_service_type(desc, restate_field_ext, methods),
        SINGLETON_SERVICE_EXT => Ok(DiscoveredInstanceType::Singleton),
        _ => Err(ServiceDiscoveryError::BadOrMissingRestateDependencyInDescriptor),
    }
}

pub fn infer_keyed_service_type(
    desc: &ServiceDescriptor,
    restate_field_ext: &ExtensionDescriptor,
    methods: &mut HashMap<String, DiscoveredMethodMetadata>,
) -> Result<DiscoveredInstanceType, ServiceDiscoveryError> {
    if desc.methods().len() == 0 {
        return Err(ServiceDiscoveryError::KeyedServiceWithoutMethods(
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
            return Err(ServiceDiscoveryError::DifferentKeyTypes(method_desc));
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
) -> Result<FieldDescriptor, ServiceDiscoveryError> {
    let field_descriptor = match get_annotated_field(
        method_descriptor,
        restate_field_extension,
        KEY_FIELD_EXT,
        FieldAnnotation::Key,
    )? {
        Some(f) => f,
        None => {
            return Err(ServiceDiscoveryError::MissingKeyField(
                method_descriptor.clone(),
            ));
        }
    };

    if field_descriptor.kind() != Kind::String {
        return Err(ServiceDiscoveryError::BadKeyFieldType(
            method_descriptor.clone(),
        ));
    }

    Ok(field_descriptor)
}

fn infer_event_fields_annotations(
    service_desc: &ServiceDescriptor,
    restate_field_ext: &ExtensionDescriptor,
    methods: &mut HashMap<String, DiscoveredMethodMetadata>,
) -> Result<(), ServiceDiscoveryError> {
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
) -> Result<Option<FieldDescriptor>, ServiceDiscoveryError> {
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
        return Err(ServiceDiscoveryError::BadEventPayloadFieldType(
            method_descriptor.clone(),
        ));
    }

    Ok(Some(field_descriptor))
}

fn resolve_event_metadata_field(
    method_descriptor: &MethodDescriptor,
    restate_field_extension: &ExtensionDescriptor,
) -> Result<Option<FieldDescriptor>, ServiceDiscoveryError> {
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
        return Err(ServiceDiscoveryError::BadEventMetadataFieldType(
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
) -> Result<Option<FieldDescriptor>, ServiceDiscoveryError> {
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
        return Err(ServiceDiscoveryError::MoreThanOneAnnotatedField(
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

// This function patches the built-in descriptors, to fix https://github.com/restatedev/restate/issues/687
// We can remove it once https://github.com/restatedev/sdk-typescript/issues/155 is properly fixed.
fn patch_built_in_descriptors(mut files: Bytes) -> Result<Bytes, ServiceDiscoveryError> {
    // We need the prost_reflect_types to preserve extension :(
    // See above comments in ServiceDiscoveryResponse
    let mut files = prost_reflect_types::FileDescriptorSet::decode(&mut files)?;

    // Let's patch the file google/protobuf/struct.proto
    for file in &mut files.file {
        if file.name() == "google/protobuf/struct.proto" {
            // Let's take the descriptor we need from the DescriptorPool::global()
            let file_desc = DescriptorPool::global()
                .get_file_by_name("google/protobuf/struct.proto")
                .expect("The global descriptor pool must contain struct.proto")
                .encode_to_vec();

            // Let's apply it
            *file = prost_reflect_types::FileDescriptorProto::decode(&*file_desc)
                .expect("This deserialization should not fail!");
        }
    }

    Ok(Bytes::from(files.encode_to_vec()))
}

mod prost_reflect_types {
    // Copy pasted from https://github.com/andrewhickman/prost-reflect/blob/03935865c101da33d2c347d4175ef5833ab34997/prost-reflect/src/descriptor/types.rs
    // License Apache License, Version 2.0

    use std::fmt;

    use prost::{
        bytes::{Buf, BufMut},
        encoding::{encode_key, skip_field, DecodeContext, WireType},
        DecodeError, Message,
    };
    pub(crate) use prost_types::{
        enum_descriptor_proto, field_descriptor_proto, EnumOptions, EnumValueOptions,
        ExtensionRangeOptions, FieldOptions, FileOptions, MessageOptions, MethodOptions,
        OneofOptions, ServiceOptions, SourceCodeInfo,
    };

    #[derive(Clone, PartialEq, Message)]
    pub(crate) struct FileDescriptorSet {
        #[prost(message, repeated, tag = "1")]
        pub file: Vec<FileDescriptorProto>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub(crate) struct FileDescriptorProto {
        #[prost(string, optional, tag = "1")]
        pub name: Option<String>,
        #[prost(string, optional, tag = "2")]
        pub package: Option<String>,
        #[prost(string, repeated, tag = "3")]
        pub dependency: Vec<String>,
        #[prost(int32, repeated, packed = "false", tag = "10")]
        pub public_dependency: Vec<i32>,
        #[prost(int32, repeated, packed = "false", tag = "11")]
        pub weak_dependency: Vec<i32>,
        #[prost(message, repeated, tag = "4")]
        pub message_type: Vec<DescriptorProto>,
        #[prost(message, repeated, tag = "5")]
        pub(crate) enum_type: Vec<EnumDescriptorProto>,
        #[prost(message, repeated, tag = "6")]
        pub service: Vec<ServiceDescriptorProto>,
        #[prost(message, repeated, tag = "7")]
        pub extension: Vec<FieldDescriptorProto>,
        #[prost(message, optional, tag = "8")]
        pub options: Option<Options<FileOptions>>,
        #[prost(message, optional, tag = "9")]
        pub source_code_info: Option<SourceCodeInfo>,
        #[prost(string, optional, tag = "12")]
        pub syntax: Option<String>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub(crate) struct DescriptorProto {
        #[prost(string, optional, tag = "1")]
        pub name: Option<String>,
        #[prost(message, repeated, tag = "2")]
        pub field: Vec<FieldDescriptorProto>,
        #[prost(message, repeated, tag = "6")]
        pub extension: Vec<FieldDescriptorProto>,
        #[prost(message, repeated, tag = "3")]
        pub nested_type: Vec<DescriptorProto>,
        #[prost(message, repeated, tag = "4")]
        pub(crate) enum_type: Vec<EnumDescriptorProto>,
        #[prost(message, repeated, tag = "5")]
        pub extension_range: Vec<descriptor_proto::ExtensionRange>,
        #[prost(message, repeated, tag = "8")]
        pub oneof_decl: Vec<OneofDescriptorProto>,
        #[prost(message, optional, tag = "7")]
        pub options: Option<Options<MessageOptions>>,
        #[prost(message, repeated, tag = "9")]
        pub reserved_range: Vec<descriptor_proto::ReservedRange>,
        #[prost(string, repeated, tag = "10")]
        pub reserved_name: Vec<String>,
    }

    pub(crate) mod descriptor_proto {
        pub(crate) use prost_types::descriptor_proto::ReservedRange;

        use super::*;

        #[derive(Clone, PartialEq, Message)]
        pub(crate) struct ExtensionRange {
            #[prost(int32, optional, tag = "1")]
            pub start: Option<i32>,
            #[prost(int32, optional, tag = "2")]
            pub end: Option<i32>,
            #[prost(message, optional, tag = "3")]
            pub options: Option<Options<ExtensionRangeOptions>>,
        }
    }

    #[derive(Clone, PartialEq, Message)]
    pub(crate) struct FieldDescriptorProto {
        #[prost(string, optional, tag = "1")]
        pub name: Option<String>,
        #[prost(int32, optional, tag = "3")]
        pub number: Option<i32>,
        #[prost(enumeration = "field_descriptor_proto::Label", optional, tag = "4")]
        pub label: Option<i32>,
        #[prost(enumeration = "field_descriptor_proto::Type", optional, tag = "5")]
        pub r#type: Option<i32>,
        #[prost(string, optional, tag = "6")]
        pub type_name: Option<String>,
        #[prost(string, optional, tag = "2")]
        pub extendee: Option<String>,
        #[prost(string, optional, tag = "7")]
        pub default_value: Option<String>,
        #[prost(int32, optional, tag = "9")]
        pub oneof_index: Option<i32>,
        #[prost(string, optional, tag = "10")]
        pub json_name: Option<String>,
        #[prost(message, optional, tag = "8")]
        pub options: Option<Options<FieldOptions>>,
        #[prost(bool, optional, tag = "17")]
        pub proto3_optional: Option<bool>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub(crate) struct OneofDescriptorProto {
        #[prost(string, optional, tag = "1")]
        pub name: Option<String>,
        #[prost(message, optional, tag = "2")]
        pub options: Option<Options<OneofOptions>>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub(crate) struct EnumDescriptorProto {
        #[prost(string, optional, tag = "1")]
        pub name: Option<String>,
        #[prost(message, repeated, tag = "2")]
        pub value: Vec<EnumValueDescriptorProto>,
        #[prost(message, optional, tag = "3")]
        pub options: Option<Options<EnumOptions>>,
        #[prost(message, repeated, tag = "4")]
        pub reserved_range: Vec<enum_descriptor_proto::EnumReservedRange>,
        #[prost(string, repeated, tag = "5")]
        pub reserved_name: Vec<String>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub(crate) struct EnumValueDescriptorProto {
        #[prost(string, optional, tag = "1")]
        pub name: Option<String>,
        #[prost(int32, optional, tag = "2")]
        pub number: Option<i32>,
        #[prost(message, optional, tag = "3")]
        pub options: Option<Options<EnumValueOptions>>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub(crate) struct ServiceDescriptorProto {
        #[prost(string, optional, tag = "1")]
        pub name: Option<String>,
        #[prost(message, repeated, tag = "2")]
        pub method: Vec<MethodDescriptorProto>,
        #[prost(message, optional, tag = "3")]
        pub options: Option<Options<ServiceOptions>>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub(crate) struct MethodDescriptorProto {
        #[prost(string, optional, tag = "1")]
        pub name: Option<String>,
        #[prost(string, optional, tag = "2")]
        pub input_type: Option<String>,
        #[prost(string, optional, tag = "3")]
        pub output_type: Option<String>,
        #[prost(message, optional, tag = "4")]
        pub options: Option<Options<MethodOptions>>,
        #[prost(bool, optional, tag = "5", default = "false")]
        pub client_streaming: Option<bool>,
        #[prost(bool, optional, tag = "6", default = "false")]
        pub server_streaming: Option<bool>,
    }

    #[derive(Clone, Default, PartialEq)]
    pub(crate) struct Options<T> {
        pub(crate) encoded: Vec<u8>,
        pub(crate) value: T,
    }

    impl<T> fmt::Debug for Options<T>
    where
        T: fmt::Debug,
    {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            self.value.fmt(f)
        }
    }

    impl<T> Message for Options<T>
    where
        T: Message + Default,
    {
        fn encode_raw<B>(&self, buf: &mut B)
        where
            B: BufMut,
            Self: Sized,
        {
            buf.put(self.encoded.as_slice());
        }

        fn merge_field<B>(
            &mut self,
            tag: u32,
            wire_type: WireType,
            buf: &mut B,
            ctx: DecodeContext,
        ) -> Result<(), DecodeError>
        where
            B: Buf,
            Self: Sized,
        {
            struct CopyBufAdapter<'a, B> {
                dest: &'a mut Vec<u8>,
                src: &'a mut B,
            }

            impl<'a, B> Buf for CopyBufAdapter<'a, B>
            where
                B: Buf,
            {
                fn advance(&mut self, cnt: usize) {
                    self.dest.put((&mut self.src).take(cnt));
                }

                fn chunk(&self) -> &[u8] {
                    self.src.chunk()
                }

                fn remaining(&self) -> usize {
                    self.src.remaining()
                }
            }

            encode_key(tag, wire_type, &mut self.encoded);
            let start = self.encoded.len();
            skip_field(
                wire_type,
                tag,
                &mut CopyBufAdapter {
                    dest: &mut self.encoded,
                    src: buf,
                },
                ctx.clone(),
            )?;
            self.value
                .merge_field(tag, wire_type, &mut &self.encoded[start..], ctx)?;

            Ok(())
        }

        fn encoded_len(&self) -> usize {
            self.encoded.len()
        }

        fn clear(&mut self) {
            self.encoded.clear();
            self.value.clear();
        }
    }
}
