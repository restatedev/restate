// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod dispatcher;
mod handler;
mod options;
mod protocol;
mod reflection;
mod server;

pub use dispatcher::{IngressDispatcherLoop, IngressDispatcherLoopError};
pub use options::{Options, OptionsBuilder, OptionsBuilderError};
pub use server::{HyperServerIngress, IngressServerError, StartSignal};

use bytes::Bytes;
use opentelemetry::Context;
use restate_futures_util::command::*;
use restate_types::errors::InvocationError;
use restate_types::identifiers::{FullInvocationId, IngressId, PeerId};
use restate_types::invocation::{InvocationResponse, ResponseResult, ServiceInvocation};
use restate_types::message::{AckKind, MessageIndex};
use tokio::sync::mpsc;
use tonic::Status;

// --- Data model used by handlers and protocol

#[derive(Debug, Clone)]
pub struct IngressRequestHeaders {
    service_name: String,
    method_name: String,
    tracing_context: Context,
}

impl IngressRequestHeaders {
    pub fn new(service_name: String, method_name: String, tracing_context: Context) -> Self {
        Self {
            service_name,
            method_name,
            tracing_context,
        }
    }
}

type IngressResult = Result<IngressResponse, IngressError>;

pub type IngressRequest = (IngressRequestHeaders, Bytes);
pub type IngressResponse = Bytes;
pub type IngressError = InvocationError;

// --- Input and output messages to interact with ingress

#[derive(Debug, Clone)]
pub enum InvocationOrResponse {
    Invocation(ServiceInvocation),
    Response(InvocationResponse),
}

#[derive(Debug, Clone)]
pub struct IngressResponseMessage {
    pub full_invocation_id: FullInvocationId,
    pub result: Result<IngressResponse, IngressError>,
    pub ack_target: AckTarget,
}

#[derive(Debug, Clone)]
pub struct AckTarget {
    pub shuffle_target: PeerId,
    pub msg_index: MessageIndex,
}

impl AckTarget {
    pub fn new(shuffle_target: PeerId, msg_index: MessageIndex) -> Self {
        Self {
            shuffle_target,
            msg_index,
        }
    }

    fn acknowledge(self) -> AckResponse {
        AckResponse {
            shuffle_target: self.shuffle_target,
            kind: AckKind::Acknowledge(self.msg_index),
        }
    }
}

#[derive(Debug)]
pub enum IngressInput {
    Response(IngressResponseMessage),
    MessageAck(MessageIndex),
}

impl IngressInput {
    pub fn message_ack(seq_number: MessageIndex) -> Self {
        IngressInput::MessageAck(seq_number)
    }

    pub fn response(response: IngressResponseMessage) -> Self {
        IngressInput::Response(response)
    }
}

#[derive(Debug)]
pub enum IngressOutput {
    Invocation {
        service_invocation: ServiceInvocation,
        ingress_id: IngressId,
        msg_index: MessageIndex,
    },
    AwakeableCompletion {
        response: InvocationResponse,
        ingress_id: IngressId,
        msg_index: MessageIndex,
    },
    Ack(AckResponse),
}

#[derive(Debug)]
pub struct AckResponse {
    pub shuffle_target: PeerId,
    pub kind: AckKind,
}

impl IngressOutput {
    pub fn service_invocation(
        service_invocation: ServiceInvocation,
        ingress_id: IngressId,
        msg_index: MessageIndex,
    ) -> Self {
        Self::Invocation {
            service_invocation,
            ingress_id,
            msg_index,
        }
    }

    pub fn awakeable_completion(
        response: InvocationResponse,
        ingress_id: IngressId,
        msg_index: MessageIndex,
    ) -> Self {
        Self::AwakeableCompletion {
            response,
            ingress_id,
            msg_index,
        }
    }

    pub fn shuffle_ack(ack_response: AckResponse) -> Self {
        Self::Ack(ack_response)
    }
}

// --- Channels

pub type DispatcherCommandSender = UnboundedCommandSender<InvocationOrResponse, IngressResult>;
pub type IngressInputReceiver = mpsc::Receiver<IngressInput>;
pub type IngressInputSender = mpsc::Sender<IngressInput>;

// Contains some mocks we use in unit tests in this crate
#[cfg(test)]
mod mocks {
    use restate_schema_api::endpoint::{DeliveryOptions, EndpointMetadata, ProtocolType};
    use restate_schema_api::key::ServiceInstanceType;
    use restate_schema_impl::{Schemas, ServiceRegistrationRequest};

    pub(super) fn test_schemas() -> Schemas {
        let schemas = Schemas::default();

        schemas
            .apply_updates(
                schemas
                    .compute_new_endpoint_updates(
                        EndpointMetadata::new(
                            "http://localhost:8080".parse().unwrap(),
                            ProtocolType::BidiStream,
                            DeliveryOptions::default(),
                        ),
                        vec![ServiceRegistrationRequest::new(
                            "greeter.Greeter".to_string(),
                            ServiceInstanceType::Singleton,
                        )],
                        restate_pb::mocks::DESCRIPTOR_POOL.clone(),
                        false,
                    )
                    .unwrap(),
            )
            .unwrap();

        schemas
    }
}
