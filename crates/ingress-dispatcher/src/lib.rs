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
use restate_core::metadata;
use restate_schema_api::subscription::{EventReceiverServiceType, Sink, Subscription};
use restate_types::identifiers::{
    partitioner, IdempotencyId, InvocationId, PartitionKey, WithPartitionKey,
};
use restate_types::invocation::{
    InvocationResponse, InvocationTarget, ResponseResult, ServiceInvocation,
    ServiceInvocationResponseSink, SpanRelation, VirtualObjectHandlerType, WorkflowHandlerType,
};
use restate_types::message::MessageIndex;
use std::fmt::Display;
use std::hash::Hash;
use tokio::sync::oneshot;

mod dispatcher;
pub mod error;

// -- Types used by the ingress to interact with the dispatcher
pub use dispatcher::{DispatchIngressRequest, IngressDispatcher};
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
enum IngressDispatcherRequestInner {
    Invoke(ServiceInvocation),
    ProxyThrough(ServiceInvocation),
    InvocationResponse(InvocationResponse),
}

impl WithPartitionKey for IngressDispatcherRequestInner {
    fn partition_key(&self) -> PartitionKey {
        match self {
            IngressDispatcherRequestInner::Invoke(si) => si.partition_key(),
            IngressDispatcherRequestInner::ProxyThrough(si) => si.partition_key(),
            IngressDispatcherRequestInner::InvocationResponse(ir) => ir.partition_key(),
        }
    }
}

#[derive(Debug)]
pub struct IngressDispatcherRequest {
    inner: IngressDispatcherRequestInner,
    request_mode: IngressRequestMode,
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
    RequestResponse(IngressCorrelationId, IngressResponseSender),
    DedupFireAndForget {
        deduplication_id: IngressDeduplicationId,
        proxying_partition_key: Option<PartitionKey>,
    },
    FireAndForget,
}

pub trait DeduplicationId: Display + Hash {
    fn requires_proxying(subscription: &Subscription) -> bool;
}

impl IngressDispatcherRequest {
    pub fn invocation(
        mut service_invocation: ServiceInvocation,
    ) -> (Self, IngressCorrelationId, IngressResponseReceiver) {
        let (result_tx, result_rx) = oneshot::channel();

        let correlation_id = ingress_correlation_id(
            &service_invocation.invocation_id,
            &service_invocation.invocation_target,
            service_invocation.idempotency_key.as_ref(),
        );

        let my_node_id = metadata().my_node_id();
        service_invocation.response_sink = Some(ServiceInvocationResponseSink::Ingress(my_node_id));

        (
            IngressDispatcherRequest {
                request_mode: IngressRequestMode::RequestResponse(
                    correlation_id.clone(),
                    result_tx,
                ),
                inner: IngressDispatcherRequestInner::Invoke(service_invocation),
            },
            correlation_id,
            result_rx,
        )
    }

    pub fn one_way_invocation(service_invocation: ServiceInvocation) -> Self {
        IngressDispatcherRequest {
            request_mode: IngressRequestMode::FireAndForget,
            inner: IngressDispatcherRequestInner::Invoke(service_invocation),
        }
    }

    pub fn event<D: DeduplicationId>(
        subscription: &Subscription,
        key: Bytes,
        payload: Bytes,
        related_span: SpanRelation,
        deduplication: Option<(D, MessageIndex)>,
        headers: Vec<restate_types::invocation::Header>,
    ) -> Result<Self, anyhow::Error> {
        // Check if we need to proxy or not
        let (should_proxy, request_mode) = if let Some((dedup_id, dedup_index)) = deduplication {
            let proxying_partition_key = if D::requires_proxying(subscription) {
                Some(partitioner::HashPartitioner::compute_partition_key(
                    &dedup_id,
                ))
            } else {
                None
            };
            (
                true,
                IngressRequestMode::DedupFireAndForget {
                    deduplication_id: (dedup_id.to_string(), dedup_index),
                    proxying_partition_key,
                },
            )
        } else {
            (false, IngressRequestMode::FireAndForget)
        };
        let (invocation_target, argument) = match subscription.sink() {
            Sink::Service {
                ref name,
                ref handler,
                ty,
            } => {
                let target_invocation_target = match ty {
                    EventReceiverServiceType::VirtualObject => InvocationTarget::virtual_object(
                        &**name,
                        std::str::from_utf8(&key)
                            .map_err(|e| anyhow::anyhow!("The key must be valid UTF-8: {e}"))?
                            .to_owned(),
                        &**handler,
                        VirtualObjectHandlerType::Exclusive,
                    ),
                    EventReceiverServiceType::Workflow => InvocationTarget::workflow(
                        &**name,
                        std::str::from_utf8(&key)
                            .map_err(|e| anyhow::anyhow!("The key must be valid UTF-8: {e}"))?
                            .to_owned(),
                        &**handler,
                        WorkflowHandlerType::Workflow,
                    ),
                    EventReceiverServiceType::Service => {
                        InvocationTarget::service(&**name, &**handler)
                    }
                };

                (target_invocation_target, payload.clone())
            }
        };

        // Generate service invocation
        let invocation_id = InvocationId::generate(&invocation_target);
        let mut service_invocation = ServiceInvocation::initialize(
            invocation_id,
            invocation_target,
            restate_types::invocation::Source::Ingress,
        );
        service_invocation.with_related_span(related_span);
        service_invocation.argument = argument;
        service_invocation.headers = headers;

        Ok(IngressDispatcherRequest {
            inner: if should_proxy {
                IngressDispatcherRequestInner::ProxyThrough(service_invocation)
            } else {
                IngressDispatcherRequestInner::Invoke(service_invocation)
            },
            request_mode,
        })
    }

    pub fn completion(invocation_response: InvocationResponse) -> Self {
        IngressDispatcherRequest {
            request_mode: IngressRequestMode::FireAndForget,
            inner: IngressDispatcherRequestInner::InvocationResponse(invocation_response),
        }
    }
}

pub fn ingress_correlation_id(
    id: &InvocationId,
    invocation_target: &InvocationTarget,
    idempotency: Option<&ByteString>,
) -> IngressCorrelationId {
    if let Some(idempotency) = idempotency {
        IngressCorrelationId::IdempotencyId(IdempotencyId::combine(
            *id,
            invocation_target,
            idempotency.clone(),
        ))
    } else {
        IngressCorrelationId::InvocationId(*id)
    }
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
            ServiceInvocation,
            IngressCorrelationId,
            IngressResponseSender,
        ) {
            let_assert!(
                IngressDispatcherRequest {
                    inner: IngressDispatcherRequestInner::Invoke(service_invocation),
                    request_mode: IngressRequestMode::RequestResponse(
                        ingress_correlation_id,
                        ingress_response_sender
                    ),
                } = self
            );
            (
                service_invocation,
                ingress_correlation_id,
                ingress_response_sender,
            )
        }

        pub fn expect_one_way_invocation(self) -> ServiceInvocation {
            let_assert!(
                IngressDispatcherRequest {
                    inner: IngressDispatcherRequestInner::Invoke(service_invocation),
                    request_mode: IngressRequestMode::FireAndForget,
                } = self
            );
            service_invocation
        }

        pub fn expect_completion(self) -> InvocationResponse {
            let_assert!(
                IngressDispatcherRequest {
                    inner: IngressDispatcherRequestInner::InvocationResponse(ir),
                    request_mode: IngressRequestMode::FireAndForget,
                } = self
            );
            ir
        }

        pub fn expect_event(self) -> (ServiceInvocation, IngressDeduplicationId) {
            let_assert!(
                IngressDispatcherRequest {
                    inner: IngressDispatcherRequestInner::Invoke(service_invocation),
                    request_mode: IngressRequestMode::DedupFireAndForget {
                        deduplication_id,
                        proxying_partition_key: None
                    },
                    ..
                } = self
            );
            (service_invocation, deduplication_id)
        }

        pub fn expect_event_with_proxy_through(
            self,
        ) -> (ServiceInvocation, IngressDeduplicationId) {
            let_assert!(
                IngressDispatcherRequest {
                    inner: IngressDispatcherRequestInner::ProxyThrough(service_invocation),
                    request_mode: IngressRequestMode::DedupFireAndForget {
                        deduplication_id,
                        proxying_partition_key: Some(_)
                    },
                    ..
                } = self
            );
            (service_invocation, deduplication_id)
        }
    }
}
