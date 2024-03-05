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
use hyper::{Body, HeaderMap, StatusCode};
use prost::{DecodeError, Message};
use prost_reflect::{DescriptorError, DescriptorPool};
use tracing::warn;

use codederror::CodedError;
use restate_errors::META0003;
use restate_schema_api::deployment::ProtocolType;
use restate_service_client::{Endpoint, Parts, Request, ServiceClient, ServiceClientError};

use restate_types::retries::{RetryIter, RetryPolicy};

// Clippy false positive, might be caused by Bytes contained within HeaderValue.
// https://github.com/rust-lang/rust/issues/40543#issuecomment-1212981256
#[allow(clippy::declare_interior_mutable_const)]
const APPLICATION_PROTO: HeaderValue = HeaderValue::from_static("application/proto");

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
pub struct DiscoverEndpoint(Endpoint, HashMap<HeaderName, HeaderValue>);

impl DiscoverEndpoint {
    pub fn new(address: Endpoint, additional_headers: HashMap<HeaderName, HeaderValue>) -> Self {
        Self(address, additional_headers)
    }

    pub fn into_inner(self) -> (Endpoint, HashMap<HeaderName, HeaderValue>) {
        (self.0, self.1)
    }

    pub fn address(&self) -> &Endpoint {
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
    pub services: Vec<String>,
    pub descriptor_pool: DescriptorPool,
    pub protocol_type: ProtocolType,
}

#[derive(Debug, thiserror::Error, CodedError)]
pub enum ServiceDiscoveryError {
    // Errors most likely related to SDK bugs
    #[error("received a bad response from the SDK: {0}. This might be a symptom of an SDK bug")]
    #[code(unknown)]
    BadResponse(&'static str),
    #[error("received a bad response from the SDK that cannot be decoded: {0}. This might be a symptom of an SDK bug")]
    #[code(unknown)]
    Decode(#[from] DecodeError),
    #[error("received a bad response from the SDK with a descriptor set that cannot be reconstructed: {0}. This might be a symptom of an SDK bug")]
    #[code(unknown)]
    Descriptor(#[from] DescriptorError),

    // Network related retryable errors
    #[error("bad status code: {0}")]
    #[code(META0003)]
    BadStatusCode(u16),
    #[error("client error: {0}")]
    #[code(META0003)]
    Client(#[from] ServiceClientError),
}

impl ServiceDiscoveryError {
    /// Retryable errors are those which can be caused by transient faults and where
    /// retrying can succeed.
    pub fn is_retryable(&self) -> bool {
        match self {
            ServiceDiscoveryError::BadStatusCode(status) => matches!(
                StatusCode::from_u16(*status).expect("should be valid status code"),
                StatusCode::REQUEST_TIMEOUT
                    | StatusCode::TOO_MANY_REQUESTS
                    | StatusCode::INTERNAL_SERVER_ERROR
                    | StatusCode::BAD_GATEWAY
                    | StatusCode::SERVICE_UNAVAILABLE
                    | StatusCode::GATEWAY_TIMEOUT
            ),
            ServiceDiscoveryError::Client(client_error) => client_error.is_retryable(),
            ServiceDiscoveryError::BadResponse(_)
            | ServiceDiscoveryError::Decode(_)
            | ServiceDiscoveryError::Descriptor(_) => false,
        }
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

        Ok(DiscoveredEndpointMetadata {
            services: response.services,
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
            if e.is_retryable() {
                if let Some(next_retry_interval) = retry_iter.next() {
                    warn!(
                        "Error when discovering deployment at address '{}'. Retrying in {} seconds: {}",
                        address,
                        next_retry_interval.as_secs(),
                        e
                    );
                    tokio::time::sleep(next_retry_interval).await;
                    continue;
                }
            }

            return Err(e);
        }
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

        // Temporary fix to make the TS SDK work with grpc service reflection. It seems that the
        // TS SDK is not able to send the proper FileDescriptors for the below files. This can be
        // removed once we no longer rely on receiving gRPC file descriptors from the SDK.
        if file.name() == "google/protobuf/descriptor.proto" {
            // Let's take the descriptor we need from the DescriptorPool::global()
            let file_desc = DescriptorPool::global()
                .get_file_by_name("google/protobuf/descriptor.proto")
                .expect("The global descriptor pool must contain descriptor.proto")
                .encode_to_vec();

            // Let's apply it
            *file = prost_reflect_types::FileDescriptorProto::decode(&*file_desc)
                .expect("This deserialization should not fail!");
        }

        if file.name() == "dev/restate/ext.proto" {
            // Let's take the descriptor we need from the restate_pb descriptor pool
            let file_desc = restate_pb::DESCRIPTOR_POOL
                .get_file_by_name("dev/restate/ext.proto")
                .expect("The restate_pb descriptor pool must contain ext.proto")
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
