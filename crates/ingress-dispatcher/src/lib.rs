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
use restate_pb::restate::internal::Event;
use restate_schema_api::subscription::{EventReceiverComponentType, Sink, Subscription};
use restate_types::errors::InvocationError;
use restate_types::identifiers::{FullInvocationId, ServiceId, WithPartitionKey};
use restate_types::invocation::{ServiceInvocation, ServiceInvocationSpanContext, SpanRelation};
use restate_types::message::MessageIndex;
use restate_types::GenerationalNodeId;
use std::fmt::Display;
use std::time::Duration;
use tokio::sync::oneshot;

mod dispatcher;
pub mod error;

pub use dispatcher::{DispatchIngressRequest, IngressDispatcher};

use restate_core::metadata;
use restate_types::dedup::DedupInformation;
use restate_wal_protocol::{Command, Destination, Envelope, Header, Source};

// -- Types used by the ingress to interact with the dispatcher
pub type IngressResponseSender = oneshot::Sender<ExpiringIngressResponse>;
pub type IngressResponseReceiver = oneshot::Receiver<ExpiringIngressResponse>;

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
    headers: Vec<restate_types::invocation::Header>,
}

#[derive(Debug, Clone)]
pub struct ExpiringIngressResponse {
    idempotency_expiry_time: Option<String>,
    result: Result<Bytes, InvocationError>,
}

impl ExpiringIngressResponse {
    pub fn idempotency_expiry_time(&self) -> Option<&String> {
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
    DedupFireAndForget(IngressDeduplicationId),
    FireAndForget,
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
        headers: Vec<restate_types::invocation::Header>,
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
                headers,
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
        headers: Vec<restate_types::invocation::Header>,
    ) -> Self {
        let span_context = ServiceInvocationSpanContext::start(&fid, related_span);
        IngressRequest {
            fid,
            method_name: method_name.into(),
            argument: argument.into(),
            span_context,
            request_mode: match ingress_deduplication_id {
                None => IngressRequestMode::FireAndForget,
                Some(dedup_id) => IngressRequestMode::DedupFireAndForget(dedup_id),
            },
            idempotency: IdempotencyMode::None,
            headers,
        }
    }

    pub fn event<D: DeduplicationId>(
        subscription: &Subscription,
        event: Event,
        related_span: SpanRelation,
        deduplication: Option<(D, MessageIndex)>,
        headers: Vec<restate_types::invocation::Header>,
    ) -> Result<Self, anyhow::Error> {
        // Check if we need to proxy or not
        let (proxying_key, request_mode) = if let Some((dedup_id, dedup_index)) = deduplication {
            let dedup_id = dedup_id.to_string();
            (
                if D::requires_proxying(subscription) {
                    Some(dedup_id.clone())
                } else {
                    None
                },
                IngressRequestMode::DedupFireAndForget((dedup_id, dedup_index)),
            )
        } else {
            (None, IngressRequestMode::FireAndForget)
        };
        let (target_fid, handler, argument) = match subscription.sink() {
            Sink::Component {
                ref name,
                ref handler,
                ty,
            } => {
                let target_fid = FullInvocationId::generate(match ty {
                    EventReceiverComponentType::VirtualObject {
                        ordering_key_is_key,
                    } => ServiceId::new(
                        &**name,
                        if *ordering_key_is_key {
                            event.ordering_key.clone()
                        } else {
                            std::str::from_utf8(&event.key)
                                .map_err(|e| anyhow::anyhow!("The key must be valid UTF-8: {e}"))?
                                .to_owned()
                        },
                    ),
                    EventReceiverComponentType::Service => ServiceId::unkeyed(&**name),
                });

                (target_fid, handler, event.payload.clone())
            }
        };

        // Generate span context
        let span_context = ServiceInvocationSpanContext::start(&target_fid, related_span);

        Ok(if let Some(proxying_key) = proxying_key {
            // For keyed events, we dispatch them through the Proxy service, to avoid scattering the offset info throughout all the partitions
            let proxy_fid = FullInvocationId::generate(ServiceId::new(
                restate_pb::PROXY_SERVICE_NAME,
                proxying_key,
            ));

            IngressRequest {
                fid: proxy_fid,
                method_name: ByteString::from_static(restate_pb::PROXY_PROXY_THROUGH_METHOD_NAME),
                argument: restate_pb::restate::internal::ProxyThroughRequest {
                    target_service: target_fid.service_id.service_name.to_string(),
                    target_method: handler.to_owned(),
                    target_key: target_fid.service_id.key,
                    target_invocation_uuid: target_fid.invocation_uuid.into(),
                    input: argument,
                }
                .encode_to_vec()
                .into(),
                span_context,
                request_mode,
                idempotency: IdempotencyMode::None,
                headers,
            }
        } else {
            IngressRequest {
                fid: target_fid,
                method_name: ByteString::from(&**handler),
                argument,
                span_context,
                request_mode,
                idempotency: IdempotencyMode::None,
                headers,
            }
        })
    }
}

pub fn wrap_service_invocation_in_envelope(
    service_invocation: ServiceInvocation,
    from_node_id: GenerationalNodeId,
    deduplication_source: Option<String>,
    msg_index: MessageIndex,
) -> Envelope {
    let header = Header {
        source: Source::Ingress {
            node_id: from_node_id,
            nodes_config_version: metadata().nodes_config_version(),
        },
        dest: Destination::Processor {
            partition_key: service_invocation.fid.partition_key(),
            dedup: deduplication_source.map(|src| DedupInformation::ingress(src, msg_index)),
        },
    };

    Envelope::new(header, Command::Invoke(service_invocation))
}

#[cfg(feature = "mocks")]
pub mod mocks {
    use super::*;

    use tokio::sync::mpsc;

    use restate_test_util::let_assert;
    use restate_types::identifiers::InvocationId;

    use self::error::IngressDispatchError;

    #[derive(Clone)]
    pub struct MockDispatcher {
        sender: mpsc::UnboundedSender<IngressRequest>,
    }

    impl MockDispatcher {
        pub fn new(sender: mpsc::UnboundedSender<IngressRequest>) -> Self {
            Self { sender }
        }
    }

    impl DispatchIngressRequest for MockDispatcher {
        fn evict_pending_response(&self, _invocation_id: &InvocationId) {}
        async fn dispatch_ingress_request(
            &self,
            ingress_request: IngressRequest,
        ) -> Result<(), IngressDispatchError> {
            let _ = self.sender.send(ingress_request);
            Ok(())
        }
    }

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
            Vec<restate_types::invocation::Header>,
        ) {
            let_assert!(
                IngressRequest {
                    fid,
                    method_name,
                    argument,
                    span_context,
                    request_mode: IngressRequestMode::RequestResponse(ingress_response_sender),
                    idempotency,
                    headers,
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
                headers,
            )
        }

        pub fn expect_background_invocation(
            self,
        ) -> (
            FullInvocationId,
            ByteString,
            Bytes,
            ServiceInvocationSpanContext,
        ) {
            let_assert!(
                IngressRequest {
                    fid,
                    method_name,
                    argument,
                    span_context,
                    request_mode: IngressRequestMode::FireAndForget,
                    ..
                } = self
            );
            (fid, method_name, argument, span_context)
        }

        pub fn expect_dedupable_background_invocation(
            self,
        ) -> (
            FullInvocationId,
            ByteString,
            Bytes,
            ServiceInvocationSpanContext,
            IngressDeduplicationId,
        ) {
            let_assert!(
                IngressRequest {
                    fid,
                    method_name,
                    argument,
                    span_context,
                    request_mode: IngressRequestMode::DedupFireAndForget(dedup_id),
                    ..
                } = self
            );
            (fid, method_name, argument, span_context, dedup_id)
        }
    }
}
