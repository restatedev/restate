use std::collections::HashMap;

use bytes::Bytes;
use codederror::CodedError;
use hyper::header::{ACCEPT, CONTENT_TYPE};
use hyper::http::{HeaderName, HeaderValue};
use hyper::{Body, Client, Method, Request, Uri};
use hyper_rustls::HttpsConnectorBuilder;
use prost::{DecodeError, Message};
use prost_reflect::{
    DescriptorError, DescriptorPool, ExtensionDescriptor, FieldDescriptor, Kind, MethodDescriptor,
    ServiceDescriptor,
};
use restate_errors::{META0001, META0002, META0003};
use restate_hyper_util::proxy_connector::{Proxy, ProxyConnector};
use restate_service_key_extractor::{KeyStructure, ServiceInstanceType};
use restate_types::retries::RetryPolicy;
use restate_types::service_endpoint::ProtocolType;

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

const DISCOVER_PATH: &str = "/discover";

mod pb {
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
    pub use generated_structs::ProtocolMode;
    pub use generated_structs::ServiceDiscoveryRequest;

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

#[derive(Debug, Default)]
pub struct ServiceDiscovery {
    retry_policy: RetryPolicy,
    proxy: Option<Proxy>,
}

impl ServiceDiscovery {
    pub fn new(retry_policy: RetryPolicy, proxy: Option<Proxy>) -> Self {
        Self {
            retry_policy,
            proxy,
        }
    }
}

#[derive(Debug)]
pub struct DiscoveredEndpointMetadata {
    pub services: Vec<(String, ServiceInstanceType)>,
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
        "error when trying to parse the key of service method '{}' with input type '{}'. More than one key field found",
        MethodDescriptor::full_name(.0),
        MethodDescriptor::input(.0).full_name()
    )]
    #[code(META0002)]
    MoreThanOneKeyField(MethodDescriptor),
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
    #[error("retry limit exhausted. Last hyper error: {0}")]
    #[code(META0003)]
    Hyper(#[from] hyper::Error),
}

impl ServiceDiscoveryError {
    pub fn is_user_error(&self) -> bool {
        matches!(
            self,
            ServiceDiscoveryError::BadUri(_)
                | ServiceDiscoveryError::MissingServiceTypeExtension(_)
                | ServiceDiscoveryError::KeyedServiceWithoutMethods(_)
                | ServiceDiscoveryError::MissingKeyField(_)
                | ServiceDiscoveryError::MoreThanOneKeyField(_)
                | ServiceDiscoveryError::BadKeyFieldType(_)
                | ServiceDiscoveryError::DifferentKeyTypes(_)
        )
    }
}

impl ServiceDiscovery {
    pub async fn discover(
        &self,
        uri: &Uri,
        additional_headers: &HashMap<HeaderName, HeaderValue>,
    ) -> Result<DiscoveredEndpointMetadata, ServiceDiscoveryError> {
        let connector = HttpsConnectorBuilder::new()
            .with_native_roots()
            .https_or_http()
            .enable_http2()
            .build();
        let client = Client::builder()
            .http2_only(true)
            .build::<_, Body>(ProxyConnector::new(self.proxy.clone(), connector));
        let uri = append_discover(uri)?;

        let (mut parts, body) = self
            .retry_policy
            .clone()
            .retry_operation(move || {
                let client = client.clone();

                let mut request_builder = Request::builder()
                    .method(Method::POST)
                    .uri(uri.clone())
                    .header(CONTENT_TYPE, APPLICATION_PROTO)
                    .header(ACCEPT, APPLICATION_PROTO);
                request_builder
                    .headers_mut()
                    .unwrap()
                    .extend(additional_headers.clone().into_iter());

                let request = request_builder
                    .body(Body::from(pb::ServiceDiscoveryRequest {}.encode_to_vec()))
                    .expect("Building the request is not supposed to fail");

                async move {
                    let response = client.request(request).await?;
                    let (parts, body) = response.into_parts();

                    if !parts.status.is_success() {
                        return Err(ServiceDiscoveryError::BadStatusCode(parts.status.as_u16()));
                    }

                    Ok((parts, hyper::body::to_bytes(body).await?))
                }
            })
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
                ))
            }
        }

        // Build the descriptor pool
        let response: pb::ServiceDiscoveryResponse = pb::ServiceDiscoveryResponse::decode(body)?;
        let descriptor_pool = DescriptorPool::decode(response.files)?;

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
            let service_type = infer_service_type(
                &svc_desc,
                &restate_service_type_extension,
                &restate_key_extension,
            )?;
            services.push((svc_name.to_string(), service_type))
        }

        Ok(DiscoveredEndpointMetadata {
            services,
            descriptor_pool,
            protocol_type: match pb::ProtocolMode::from_i32(response.protocol_mode) {
                Some(pb::ProtocolMode::BidiStream) => ProtocolType::BidiStream,
                Some(pb::ProtocolMode::RequestResponse) => ProtocolType::RequestResponse,
                None => {
                    return Err(ServiceDiscoveryError::BadResponse(
                        "cannot decode protocol_mode",
                    ))
                }
            },
        })
    }
}

pub fn infer_service_type(
    desc: &ServiceDescriptor,
    restate_service_type_extension: &ExtensionDescriptor,
    restate_key_extension: &ExtensionDescriptor,
) -> Result<ServiceInstanceType, ServiceDiscoveryError> {
    if !desc.options().has_extension(restate_service_type_extension) {
        return Err(ServiceDiscoveryError::MissingServiceTypeExtension(
            desc.full_name().to_string(),
        ));
    }

    let service_instance_type = desc
        .options()
        .get_extension(restate_service_type_extension)
        .as_enum_number()
        // This can happen only if the restate dependency is bad?
        .ok_or_else(|| ServiceDiscoveryError::BadOrMissingRestateDependencyInDescriptor)?;

    match service_instance_type {
        UNKEYED_SERVICE_EXT => Ok(ServiceInstanceType::Unkeyed),
        KEYED_SERVICE_EXT => infer_keyed_service_type(desc, restate_key_extension),
        SINGLETON_SERVICE_EXT => Ok(ServiceInstanceType::Singleton),
        _ => Err(ServiceDiscoveryError::BadOrMissingRestateDependencyInDescriptor),
    }
}

pub fn infer_keyed_service_type(
    desc: &ServiceDescriptor,
    restate_key_extension: &ExtensionDescriptor,
) -> Result<ServiceInstanceType, ServiceDiscoveryError> {
    if desc.methods().len() == 0 {
        return Err(ServiceDiscoveryError::KeyedServiceWithoutMethods(
            desc.full_name().to_string(),
        ));
    }

    // Service is keyed, we need to make sure the key type is always the same
    let mut service_methods_key_field_root_number = HashMap::with_capacity(desc.methods().len());

    // Parse the key from the first method
    let first_method = desc.methods().next().unwrap();
    let first_key_field_descriptor = resolve_key_field(&first_method, restate_key_extension)?;

    // Generate the KeyStructure out of it
    let key_structure = infer_key_structure(&first_key_field_descriptor);
    service_methods_key_field_root_number.insert(
        first_method.name().to_string(),
        first_key_field_descriptor.number(),
    );

    // Now parse the next methods
    for method_desc in desc.methods().skip(1) {
        let key_field_descriptor = resolve_key_field(&method_desc, restate_key_extension)?;

        // Validate every method has the same key field type
        if key_field_descriptor.kind() != first_key_field_descriptor.kind() {
            return Err(ServiceDiscoveryError::DifferentKeyTypes(method_desc));
        }

        service_methods_key_field_root_number.insert(
            method_desc.name().to_string(),
            key_field_descriptor.number(),
        );
    }

    Ok(ServiceInstanceType::Keyed {
        key_structure,
        service_methods_key_field_root_number,
    })
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
    restate_key_extension: &ExtensionDescriptor,
) -> Result<FieldDescriptor, ServiceDiscoveryError> {
    let mut key_fields = method_descriptor
        .input()
        .fields()
        .filter(|f| f.options().has_extension(restate_key_extension))
        .collect::<Vec<_>>();
    if key_fields.is_empty() {
        return Err(ServiceDiscoveryError::MissingKeyField(
            method_descriptor.clone(),
        ));
    }
    if key_fields.len() != 1 {
        return Err(ServiceDiscoveryError::MoreThanOneKeyField(
            method_descriptor.clone(),
        ));
    }

    let field_descriptor = key_fields.remove(0);

    // Validate type
    if field_descriptor.is_map() {
        return Err(ServiceDiscoveryError::BadKeyFieldType(
            method_descriptor.clone(),
        ));
    }
    if field_descriptor.is_list() {
        return Err(ServiceDiscoveryError::BadKeyFieldType(
            method_descriptor.clone(),
        ));
    }

    Ok(field_descriptor)
}

fn append_discover(uri: &Uri) -> Result<Uri, ServiceDiscoveryError> {
    let p = format!(
        "{}{}",
        match uri.path().strip_suffix('/') {
            None => uri.path(),
            Some(s) => s,
        },
        DISCOVER_PATH
    );

    Ok(Uri::builder()
        .authority(
            uri.authority()
                .ok_or_else(|| ServiceDiscoveryError::BadUri(uri.to_string()))?
                .clone(),
        )
        .scheme(
            uri.scheme()
                .ok_or_else(|| ServiceDiscoveryError::BadUri(uri.to_string()))?
                .clone(),
        )
        .path_and_query(p)
        .build()
        .unwrap())
}
