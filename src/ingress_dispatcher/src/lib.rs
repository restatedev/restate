// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::Bytes;
use bytestring::ByteString;
use restate_types::errors::InvocationError;
use restate_types::identifiers::{FullInvocationId, IngressId, PeerId};
use restate_types::invocation::{
    InvocationResponse, ServiceInvocation, ServiceInvocationSpanContext, SpanRelation,
};
use restate_types::message::{AckKind, MessageIndex};
use tokio::sync::{mpsc, oneshot};

// -- Re-export dispatcher service

mod service;
pub use service::Error as ServiceError;
pub use service::Service;

// -- Types used by the ingress to interact with the dispatcher

pub type IngressRequestSender = mpsc::UnboundedSender<IngressRequest>;
pub type IngressRequestReceiver = mpsc::UnboundedReceiver<IngressRequest>;
pub type IngressResponseSender = oneshot::Sender<IngressResponse>;
pub type IngressResponseReceiver = oneshot::Receiver<IngressResponse>;
pub type AckSender = oneshot::Sender<()>;
pub type AckReceiver = oneshot::Receiver<()>;

#[derive(Debug)]
pub struct IngressRequest(IngressRequestInner);

pub type IngressResponse = Result<Bytes, InvocationError>;

/// Trimmed down version of [`ServiceInvocation`] without the destination.
#[derive(Debug, Clone, PartialEq, Eq)]
struct IngressServiceInvocation {
    fid: FullInvocationId,
    method_name: ByteString,
    argument: Bytes,
    span_context: ServiceInvocationSpanContext,
}

impl IngressServiceInvocation {
    fn new(
        fid: FullInvocationId,
        method_name: impl Into<ByteString>,
        argument: impl Into<Bytes>,
        related_span: SpanRelation,
    ) -> Self {
        let span_context = ServiceInvocationSpanContext::start(&fid, related_span);
        Self {
            fid,
            method_name: method_name.into(),
            argument: argument.into(),
            span_context,
        }
    }
}

#[derive(Debug)]
enum IngressRequestInner {
    Invocation(IngressServiceInvocation, ResponseOrAckSender),
    Response(InvocationResponse, AckSender),
}

#[derive(Debug)]
enum ResponseOrAckSender {
    Response(IngressResponseSender),
    Ack(AckSender),
}

impl IngressRequest {
    pub fn response(invocation_response: InvocationResponse) -> (AckReceiver, Self) {
        let (ack_tx, ack_rx) = oneshot::channel();

        (
            ack_rx,
            IngressRequest(IngressRequestInner::Response(invocation_response, ack_tx)),
        )
    }

    pub fn invocation(
        fid: FullInvocationId,
        method_name: impl Into<ByteString>,
        argument: impl Into<Bytes>,
        related_span: SpanRelation,
    ) -> (IngressResponseReceiver, Self) {
        let (result_tx, result_rx) = oneshot::channel();

        (
            result_rx,
            IngressRequest(IngressRequestInner::Invocation(
                IngressServiceInvocation::new(fid, method_name, argument, related_span),
                ResponseOrAckSender::Response(result_tx),
            )),
        )
    }

    pub fn background_invocation(
        fid: FullInvocationId,
        method_name: impl Into<ByteString>,
        argument: impl Into<Bytes>,
        related_span: SpanRelation,
    ) -> (AckReceiver, Self) {
        let (ack_tx, ack_rx) = oneshot::channel();

        (
            ack_rx,
            IngressRequest(IngressRequestInner::Invocation(
                IngressServiceInvocation::new(fid, method_name, argument, related_span),
                ResponseOrAckSender::Ack(ack_tx),
            )),
        )
    }
}

// -- Types used by the network to interact with the ingress dispatcher service

pub type IngressDispatcherInputReceiver = mpsc::Receiver<IngressDispatcherInput>;
pub type IngressDispatcherInputSender = mpsc::Sender<IngressDispatcherInput>;

#[derive(Debug, Clone)]
pub struct IngressResponseMessage {
    pub full_invocation_id: FullInvocationId,
    pub result: IngressResponse,
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
pub enum IngressDispatcherInput {
    Response(IngressResponseMessage),
    MessageAck(MessageIndex),
}

impl IngressDispatcherInput {
    pub fn message_ack(seq_number: MessageIndex) -> Self {
        IngressDispatcherInput::MessageAck(seq_number)
    }

    pub fn response(response: IngressResponseMessage) -> Self {
        IngressDispatcherInput::Response(response)
    }
}

#[derive(Debug)]
pub enum IngressDispatcherOutput {
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

impl IngressDispatcherOutput {
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

#[cfg(feature = "mocks")]
pub mod mocks {
    use super::*;

    use restate_test_util::let_assert;

    impl IngressRequest {
        pub fn expect_invocation(
            self,
        ) -> (
            FullInvocationId,
            ByteString,
            Bytes,
            ServiceInvocationSpanContext,
            IngressResponseSender,
        ) {
            let_assert!(
                IngressRequestInner::Invocation(
                    IngressServiceInvocation {
                        fid,
                        method_name,
                        argument,
                        span_context,
                    },
                    ResponseOrAckSender::Response(ingress_response_sender)
                ) = self.0
            );
            (
                fid,
                method_name,
                argument,
                span_context,
                ingress_response_sender,
            )
        }

        pub fn expect_background_invocation(
            self,
        ) -> (
            FullInvocationId,
            ByteString,
            Bytes,
            ServiceInvocationSpanContext,
            AckSender,
        ) {
            let_assert!(
                IngressRequestInner::Invocation(
                    IngressServiceInvocation {
                        fid,
                        method_name,
                        argument,
                        span_context,
                    },
                    ResponseOrAckSender::Ack(ack_sender)
                ) = self.0
            );
            (fid, method_name, argument, span_context, ack_sender)
        }

        pub fn expect_response(self) -> (InvocationResponse, AckSender) {
            let_assert!(IngressRequestInner::Response(invocation_response, ack_sender) = self.0);
            (invocation_response, ack_sender)
        }
    }
}
