// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use bytestring::ByteString;
use prost::Message;
use restate_pb::restate::Event;
use restate_schema_api::subscription::{EventReceiverServiceInstanceType, Sink, Subscription};
use restate_types::errors::InvocationError;
use restate_types::identifiers::{FullInvocationId, IngressDispatcherId, InvocationUuid, PeerId};
use restate_types::invocation::{ServiceInvocation, ServiceInvocationSpanContext, SpanRelation};
use restate_types::message::{AckKind, MessageIndex};
use std::fmt::Display;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

// -- Re-export dispatcher service

mod event_remapping;
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

pub trait DeduplicationId: Display {
    fn requires_proxying(subscription: &Subscription) -> bool;
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

    pub fn event<D: DeduplicationId>(
        subscription: &Subscription,
        mut event: Event,
        related_span: SpanRelation,
        deduplication: Option<(D, MessageIndex)>,
    ) -> (Self, AckReceiver) {
        let (ack_tx, ack_rx) = oneshot::channel();

        // Check if we need to proxy or not
        let (proxying_key, request_mode) = if let Some((dedup_id, dedup_index)) = deduplication {
            let dedup_id = dedup_id.to_string();
            (
                if D::requires_proxying(subscription) {
                    Some(dedup_id.clone())
                } else {
                    None
                },
                IngressRequestMode::DedupFireAndForget((dedup_id, dedup_index), ack_tx),
            )
        } else {
            (None, IngressRequestMode::FireAndForget(ack_tx))
        };

        let Sink::Service {
            ref name,
            ref method,
            ref input_event_remap,
            ref instance_type,
        } = subscription.sink();

        // Generate fid
        let target_fid = FullInvocationId::generate(
            &**name,
            // TODO This should probably live somewhere and be unified with the rest of the key extraction logic
            match instance_type {
                EventReceiverServiceInstanceType::Keyed {
                    ordering_key_is_key,
                } => generate_restate_key(if *ordering_key_is_key {
                    event.ordering_key.clone()
                } else {
                    event.key.clone()
                }),
                EventReceiverServiceInstanceType::Unkeyed => {
                    Bytes::copy_from_slice(InvocationUuid::now_v7().as_bytes())
                }
                EventReceiverServiceInstanceType::Singleton => Bytes::new(),
            },
        );

        // Generate span context
        let span_context = ServiceInvocationSpanContext::start(&target_fid, related_span);

        // Perform event remapping
        let argument = Bytes::from(if let Some(event_remap) = input_event_remap.as_ref() {
            event_remapping::MappedEvent(&mut event, event_remap).encode_to_vec()
        } else {
            event.encode_to_vec()
        });

        if let Some(proxying_key) = proxying_key {
            // For keyed events, we dispatch them through the Proxy service, to avoid scattering the offset info throughout all the partitions
            let proxy_fid =
                FullInvocationId::generate(restate_pb::PROXY_SERVICE_NAME, proxying_key);

            (
                IngressRequest {
                    fid: proxy_fid,
                    method_name: ByteString::from_static(
                        restate_pb::PROXY_PROXY_THROUGH_METHOD_NAME,
                    ),
                    argument: restate_pb::restate::internal::ProxyThroughRequest {
                        target_service: target_fid.service_id.service_name.to_string(),
                        target_method: method.to_string(),
                        target_key: target_fid.service_id.key,
                        target_invocation_uuid: Bytes::copy_from_slice(
                            target_fid.invocation_uuid.as_bytes(),
                        ),
                        input: argument,
                    }
                    .encode_to_vec()
                    .into(),
                    span_context,
                    request_mode,
                    idempotency: IdempotencyMode::None,
                },
                ack_rx,
            )
        } else {
            (
                IngressRequest {
                    fid: target_fid,
                    method_name: ByteString::from(&**method),
                    argument,
                    span_context,
                    request_mode,
                    idempotency: IdempotencyMode::None,
                },
                ack_rx,
            )
        }
    }
}

fn generate_restate_key(key: impl Buf) -> Bytes {
    // Because this needs to be a valid Restate key, we need to prepend it with its length to make it
    // look like it was extracted using the RestateKeyExtractor
    // This is done to ensure all the other operations on the key will work correctly (e.g. key to json)
    let key_len = key.remaining();
    let mut buf =
        BytesMut::with_capacity(prost::encoding::encoded_len_varint(key_len as u64) + key_len);
    prost::encoding::encode_varint(key_len as u64, &mut buf);
    buf.put(key);
    buf.freeze()
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
