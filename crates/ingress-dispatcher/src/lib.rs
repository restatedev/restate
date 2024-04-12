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
use restate_core::metadata;
use restate_pb::restate::internal::Event;
use restate_schema_api::subscription::{EventReceiverComponentType, Sink, Subscription};
use restate_types::dedup::DedupInformation;
use restate_types::identifiers::{
    FullInvocationId, IdempotencyId, InvocationId, ServiceId, WithPartitionKey,
};
use restate_types::invocation::{
    HandlerType, Idempotency, InvocationTarget, ResponseResult, ServiceInvocation,
    ServiceInvocationSpanContext, SpanRelation,
};
use restate_types::message::MessageIndex;
use restate_types::GenerationalNodeId;
use restate_wal_protocol::{Command, Destination, Envelope, Header, Source};
use std::fmt::Display;
use std::time::SystemTime;
use tokio::sync::oneshot;

mod dispatcher;
pub mod error;

pub use dispatcher::{DispatchIngressRequest, IngressDispatcher};
use restate_types::time::MillisSinceEpoch;

// -- Types used by the ingress to interact with the dispatcher
pub type IngressResponseSender = oneshot::Sender<IngressDispatcherResponse>;
pub type IngressResponseReceiver = oneshot::Receiver<IngressDispatcherResponse>;

// TODO we could eventually remove this type and replace it with something simpler once
//  https://github.com/restatedev/restate/issues/1329 is in place
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum IngressCorrelationId {
    InvocationId(InvocationId),
    IdempotencyId(IdempotencyId),
}

#[derive(Debug)]
pub struct IngressDispatcherRequest {
    pub correlation_id: IngressCorrelationId,
    fid: FullInvocationId,
    invocation_target: InvocationTarget,
    argument: Bytes,
    span_context: ServiceInvocationSpanContext,
    request_mode: IngressRequestMode,
    idempotency: Option<Idempotency>,
    headers: Vec<restate_types::invocation::Header>,
    execution_time: Option<MillisSinceEpoch>,
}

#[derive(Debug, Clone)]
pub struct IngressDispatcherResponse {
    pub idempotency_expiry_time: Option<String>,
    pub result: ResponseResult,
}

impl From<ResponseResult> for IngressDispatcherResponse {
    fn from(result: ResponseResult) -> Self {
        Self {
            idempotency_expiry_time: None,
            result,
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

impl IngressDispatcherRequest {
    pub fn invocation(
        fid: FullInvocationId,
        invocation_target: InvocationTarget,
        argument: impl Into<Bytes>,
        related_span: SpanRelation,
        idempotency: Option<Idempotency>,
        headers: Vec<restate_types::invocation::Header>,
    ) -> (Self, IngressResponseReceiver) {
        let invocation_id = InvocationId::from(&fid);
        let span_context = ServiceInvocationSpanContext::start(&invocation_id, related_span);
        let (result_tx, result_rx) = oneshot::channel();

        let correlation_id =
            ingress_correlation_id(&fid, invocation_target.handler_name(), idempotency.as_ref());

        (
            IngressDispatcherRequest {
                correlation_id,
                fid,
                argument: argument.into(),
                request_mode: IngressRequestMode::RequestResponse(result_tx),
                span_context,
                idempotency,
                headers,
                execution_time: None,
                invocation_target,
            },
            result_rx,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn background_invocation(
        fid: FullInvocationId,
        invocation_target: InvocationTarget,
        argument: impl Into<Bytes>,
        related_span: SpanRelation,
        ingress_deduplication_id: Option<IngressDeduplicationId>,
        idempotency: Option<Idempotency>,
        headers: Vec<restate_types::invocation::Header>,
        execution_time: Option<SystemTime>,
    ) -> Self {
        let invocation_id = InvocationId::from(&fid);
        let span_context = ServiceInvocationSpanContext::start(&invocation_id, related_span);

        let correlation_id =
            ingress_correlation_id(&fid, invocation_target.handler_name(), idempotency.as_ref());

        IngressDispatcherRequest {
            correlation_id,
            fid,
            invocation_target,
            argument: argument.into(),
            span_context,
            request_mode: match ingress_deduplication_id {
                None => IngressRequestMode::FireAndForget,
                Some(dedup_id) => IngressRequestMode::DedupFireAndForget(dedup_id),
            },
            idempotency,
            headers,
            execution_time: execution_time.map(Into::into),
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
        let invocation_id = InvocationId::from(&target_fid);
        let span_context = ServiceInvocationSpanContext::start(&invocation_id, related_span);

        Ok(if let Some(proxying_key) = proxying_key {
            // For keyed events, we dispatch them through the Proxy service, to avoid scattering the offset info throughout all the partitions
            let proxy_fid = FullInvocationId::generate(ServiceId::new(
                restate_pb::PROXY_SERVICE_NAME,
                proxying_key,
            ));

            IngressDispatcherRequest {
                correlation_id: IngressCorrelationId::InvocationId(InvocationId::from(&proxy_fid)),
                fid: proxy_fid,
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
                idempotency: None,
                headers,
                execution_time: None,
                invocation_target: InvocationTarget::Service {
                    name: ByteString::from_static(restate_pb::PROXY_SERVICE_NAME),
                    handler: ByteString::from_static(restate_pb::PROXY_PROXY_THROUGH_METHOD_NAME),
                },
            }
        } else {
            IngressDispatcherRequest {
                correlation_id: IngressCorrelationId::InvocationId(InvocationId::from(&target_fid)),
                invocation_target: InvocationTarget::VirtualObject {
                    name: target_fid.service_id.service_name.clone(),
                    key: ByteString::try_from(target_fid.service_id.key.clone())
                        .map_err(|e| anyhow::anyhow!("The key must be valid UTF-8: {e}"))?,
                    handler: ByteString::from(handler.clone()),
                    handler_ty: HandlerType::Exclusive,
                },
                fid: target_fid,
                argument,
                span_context,
                request_mode,
                idempotency: None,
                headers,
                execution_time: None,
            }
        })
    }
}

pub fn ingress_correlation_id(
    fid: &FullInvocationId,
    handler_name: &ByteString,
    idempotency: Option<&Idempotency>,
) -> IngressCorrelationId {
    if let Some(idempotency) = idempotency {
        IngressCorrelationId::IdempotencyId(IdempotencyId::combine(
            fid.service_id.clone(),
            handler_name.clone(),
            idempotency.key.clone(),
        ))
    } else {
        IngressCorrelationId::InvocationId(InvocationId::from(fid))
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

    use crate::error::IngressDispatchError;
    use restate_test_util::let_assert;
    use tokio::sync::mpsc;

    #[derive(Clone)]
    pub struct MockDispatcher {
        sender: mpsc::UnboundedSender<IngressDispatcherRequest>,
    }

    impl MockDispatcher {
        pub fn new(sender: mpsc::UnboundedSender<IngressDispatcherRequest>) -> Self {
            Self { sender }
        }
    }

    impl DispatchIngressRequest for MockDispatcher {
        fn evict_pending_response(&self, _invocation_id: &IngressCorrelationId) {}
        async fn dispatch_ingress_request(
            &self,
            ingress_request: IngressDispatcherRequest,
        ) -> Result<(), IngressDispatchError> {
            let _ = self.sender.send(ingress_request);
            Ok(())
        }
    }

    impl IngressDispatcherRequest {
        pub fn expect_invocation(
            self,
        ) -> (
            InvocationId,
            InvocationTarget,
            Bytes,
            ServiceInvocationSpanContext,
            Option<Idempotency>,
            IngressResponseSender,
            Vec<restate_types::invocation::Header>,
        ) {
            let_assert!(
                IngressDispatcherRequest {
                    fid,
                    invocation_target,
                    argument,
                    span_context,
                    request_mode: IngressRequestMode::RequestResponse(ingress_response_sender),
                    idempotency,
                    headers,
                    ..
                } = self
            );
            (
                InvocationId::from(fid),
                invocation_target,
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
            InvocationId,
            InvocationTarget,
            Bytes,
            ServiceInvocationSpanContext,
            Option<MillisSinceEpoch>,
        ) {
            let_assert!(
                IngressDispatcherRequest {
                    fid,
                    invocation_target,
                    argument,
                    span_context,
                    request_mode: IngressRequestMode::FireAndForget,
                    execution_time,
                    ..
                } = self
            );
            (
                InvocationId::from(fid),
                invocation_target,
                argument,
                span_context,
                execution_time,
            )
        }

        pub fn expect_dedupable_background_invocation(
            self,
        ) -> (
            InvocationId,
            InvocationTarget,
            Bytes,
            ServiceInvocationSpanContext,
            IngressDeduplicationId,
        ) {
            let_assert!(
                IngressDispatcherRequest {
                    fid,
                    invocation_target,
                    argument,
                    span_context,
                    request_mode: IngressRequestMode::DedupFireAndForget(dedup_id),
                    ..
                } = self
            );
            (
                InvocationId::from(fid),
                invocation_target,
                argument,
                span_context,
                dedup_id,
            )
        }
    }
}
