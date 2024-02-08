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
use prost::Message;
use restate_pb::restate::Event;
use restate_schema_api::subscription::{EventReceiverServiceInstanceType, Sink, Subscription};
use restate_types::errors::InvocationError;
use restate_types::identifiers::{FullInvocationId, InvocationUuid, ServiceId, WithPartitionKey};
use restate_types::invocation::{ServiceInvocation, ServiceInvocationSpanContext, SpanRelation};
use restate_types::message::MessageIndex;
use restate_types::GenerationalNodeId;
use std::fmt::Display;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

// -- Re-export dispatcher service

mod event_remapping;
mod service;

pub use event_remapping::Error as EventError;
use restate_core::metadata;
use restate_types::ingress::IngressResponse;
use restate_wal_protocol::{AckMode, Command, Destination, Envelope, Header, Source};
pub use service::Error as ServiceError;
pub use service::Service;

// -- Types used by the ingress to interact with the dispatcher

pub type IngressRequestSender = mpsc::UnboundedSender<IngressRequest>;
pub type IngressRequestReceiver = mpsc::UnboundedReceiver<IngressRequest>;
pub type IngressResponseSender = oneshot::Sender<ExpiringIngressResponse>;
pub type IngressResponseReceiver = oneshot::Receiver<ExpiringIngressResponse>;
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
pub struct ExpiringIngressResponse {
    idempotency_expiry_time: Option<String>,
    result: Result<Bytes, InvocationError>,
}

impl ExpiringIngressResponse {
    pub fn idempotency_expire_time(&self) -> Option<&String> {
        self.idempotency_expiry_time.as_ref()
    }
}

impl From<ExpiringIngressResponse> for Result<Bytes, InvocationError> {
    fn from(value: ExpiringIngressResponse) -> Self {
        value.result
    }
}

impl From<Result<Bytes, InvocationError>> for ExpiringIngressResponse {
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
    ) -> Result<(Self, AckReceiver), EventError> {
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
        let target_fid = FullInvocationId::generate(ServiceId::new(
            &**name,
            // TODO This should probably live somewhere and be unified with the rest of the key extraction logic
            match instance_type {
                EventReceiverServiceInstanceType::Keyed {
                    ordering_key_is_key,
                } => Bytes::from(if *ordering_key_is_key {
                    event.ordering_key.clone()
                } else {
                    std::str::from_utf8(&event.key)
                        .map_err(|e| EventError {
                            field_name: "key",
                            tag: 2,
                            reason: e,
                        })?
                        .to_owned()
                }),
                EventReceiverServiceInstanceType::Unkeyed => {
                    Bytes::from(InvocationUuid::new().to_string())
                }
                EventReceiverServiceInstanceType::Singleton => Bytes::new(),
            },
        ));

        // Generate span context
        let span_context = ServiceInvocationSpanContext::start(&target_fid, related_span);

        // Perform event remapping
        let argument = Bytes::from(if let Some(event_remap) = input_event_remap.as_ref() {
            event_remapping::MappedEvent::new(&mut event, event_remap)?.encode_to_vec()
        } else {
            event.encode_to_vec()
        });

        Ok(if let Some(proxying_key) = proxying_key {
            // For keyed events, we dispatch them through the Proxy service, to avoid scattering the offset info throughout all the partitions
            let proxy_fid = FullInvocationId::generate(ServiceId::new(
                restate_pb::PROXY_SERVICE_NAME,
                proxying_key,
            ));

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
                        target_invocation_uuid: target_fid.invocation_uuid.into(),
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
        })
    }
}

// -- Types used by the network to interact with the ingress dispatcher service

pub type IngressDispatcherInputReceiver = mpsc::Receiver<IngressDispatcherInput>;
pub type IngressDispatcherInputSender = mpsc::Sender<IngressDispatcherInput>;

#[derive(Debug)]
pub enum IngressDispatcherInput {
    Response(IngressResponse),
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

    pub fn response(response: IngressResponse) -> Self {
        IngressDispatcherInput::Response(response)
    }
}

pub fn wrap_service_invocation_in_envelope(
    service_invocation: ServiceInvocation,
    from_node_id: GenerationalNodeId,
    deduplication_source: Option<String>,
    msg_index: MessageIndex,
) -> Envelope {
    let ack_mode = if deduplication_source.is_some() {
        AckMode::Dedup
    } else {
        AckMode::Ack
    };

    let header = Header {
        source: Source::Ingress {
            node_id: from_node_id,
            sequence_number: msg_index,
            dedup_key: deduplication_source,
            nodes_config_version: metadata().nodes_config_version(),
        },
        dest: Destination::Processor {
            partition_key: service_invocation.fid.partition_key(),
        },
        ack_mode,
    };

    Envelope::new(header, Command::Invoke(service_invocation))
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
