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
use restate_types::identifiers::{FullInvocationId, IngressDispatcherId, PeerId};
use restate_types::invocation::{ServiceInvocation, ServiceInvocationSpanContext, SpanRelation};
use restate_types::message::{AckKind, MessageIndex};
use std::time::Duration;
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

#[derive(Debug, PartialEq, Eq)]
pub enum IdempotencyMode {
    Key(Bytes, Option<Duration>),
    None,
}

impl IdempotencyMode {
    pub fn key(key: impl Into<Bytes>, retention_period: Option<Duration>) -> Self {
        Self::Key(key.into(), retention_period)
    }
}

#[derive(Debug)]
pub struct IngressRequest {
    fid: FullInvocationId,
    method_name: ByteString,
    argument: Bytes,
    span_context: ServiceInvocationSpanContext,
    request_mode: IngressRequestMode,
    idempotency: IdempotencyMode,
}

#[derive(Debug, Clone)]
pub struct IngressResponse {
    idempotency_expiry_time: Option<String>,
    result: Result<Bytes, InvocationError>,
}

impl IngressResponse {
    pub fn idempotency_expire_time(&self) -> Option<&String> {
        self.idempotency_expiry_time.as_ref()
    }
}

impl From<IngressResponse> for Result<Bytes, InvocationError> {
    fn from(value: IngressResponse) -> Self {
        value.result
    }
}

impl From<Result<Bytes, InvocationError>> for IngressResponse {
    fn from(value: Result<Bytes, InvocationError>) -> Self {
        Self {
            idempotency_expiry_time: None,
            result: value,
        }
    }
}

pub type IngressDeduplicationId = (String, MessageIndex);

#[derive(Debug)]
enum IngressRequestMode {
    RequestResponse(IngressResponseSender),
    DedupFireAndForget(IngressDeduplicationId, AckSender),
    FireAndForget(AckSender),
}

impl IngressRequest {
    pub fn invocation(
        fid: FullInvocationId,
        method_name: impl Into<ByteString>,
        argument: impl Into<Bytes>,
        related_span: SpanRelation,
        idempotency: IdempotencyMode,
    ) -> (Self, IngressResponseReceiver) {
        let span_context = ServiceInvocationSpanContext::start(&fid, related_span);
        let (result_tx, result_rx) = oneshot::channel();

        (
            IngressRequest {
                fid,
                method_name: method_name.into(),
                argument: argument.into(),
                request_mode: IngressRequestMode::RequestResponse(result_tx),
                span_context,
                idempotency,
            },
            result_rx,
        )
    }

    pub fn background_invocation(
        fid: FullInvocationId,
        method_name: impl Into<ByteString>,
        argument: impl Into<Bytes>,
        related_span: SpanRelation,
        ingress_deduplication_id: Option<IngressDeduplicationId>,
    ) -> (Self, AckReceiver) {
        let span_context = ServiceInvocationSpanContext::start(&fid, related_span);
        let (ack_tx, ack_rx) = oneshot::channel();

        (
            IngressRequest {
                fid,
                method_name: method_name.into(),
                argument: argument.into(),
                span_context,
                request_mode: match ingress_deduplication_id {
                    None => IngressRequestMode::FireAndForget(ack_tx),
                    Some(dedup_id) => IngressRequestMode::DedupFireAndForget(dedup_id, ack_tx),
                },
                idempotency: IdempotencyMode::None,
            },
            ack_rx,
        )
    }
}

// -- Types used by the network to interact with the ingress dispatcher service

pub type IngressDispatcherInputReceiver = mpsc::Receiver<IngressDispatcherInput>;
pub type IngressDispatcherInputSender = mpsc::Sender<IngressDispatcherInput>;

#[derive(Debug, Clone)]
pub struct IngressResponseMessage {
    pub full_invocation_id: FullInvocationId,
    pub result: Result<Bytes, InvocationError>,
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
    DedupMessageAck(String, MessageIndex),
}

impl IngressDispatcherInput {
    pub fn message_ack(seq_number: MessageIndex) -> Self {
        IngressDispatcherInput::MessageAck(seq_number)
    }

    pub fn dedup_message_ack(dedup_name: String, seq_number: MessageIndex) -> Self {
        IngressDispatcherInput::DedupMessageAck(dedup_name, seq_number)
    }

    pub fn response(response: IngressResponseMessage) -> Self {
        IngressDispatcherInput::Response(response)
    }
}

#[derive(Debug)]
pub enum IngressDispatcherOutput {
    Invocation {
        service_invocation: ServiceInvocation,
        ingress_dispatcher_id: IngressDispatcherId,
        deduplication_source: Option<String>,
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
        ingress_dispatcher_id: IngressDispatcherId,
        deduplication_source: Option<String>,
        msg_index: MessageIndex,
    ) -> Self {
        Self::Invocation {
            service_invocation,
            ingress_dispatcher_id,
            deduplication_source,
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
            IdempotencyMode,
            IngressResponseSender,
        ) {
            let_assert!(
                IngressRequest {
                    fid,
                    method_name,
                    argument,
                    span_context,
                    request_mode: IngressRequestMode::RequestResponse(ingress_response_sender),
                    idempotency,
                    ..
                } = self
            );
            (
                fid,
                method_name,
                argument,
                span_context,
                idempotency,
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
                IngressRequest {
                    fid,
                    method_name,
                    argument,
                    span_context,
                    request_mode: IngressRequestMode::FireAndForget(ack_sender),
                    ..
                } = self
            );
            (fid, method_name, argument, span_context, ack_sender)
        }

        pub fn expect_dedupable_background_invocation(
            self,
        ) -> (
            FullInvocationId,
            ByteString,
            Bytes,
            ServiceInvocationSpanContext,
            IngressDeduplicationId,
            AckSender,
        ) {
            let_assert!(
                IngressRequest {
                    fid,
                    method_name,
                    argument,
                    span_context,
                    request_mode: IngressRequestMode::DedupFireAndForget(dedup_id, ack_sender),
                    ..
                } = self
            );
            (
                fid,
                method_name,
                argument,
                span_context,
                dedup_id,
                ack_sender,
            )
        }
    }
}
