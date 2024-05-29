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
use restate_core::metadata;
use restate_schema_api::subscription::{EventReceiverServiceType, Sink, Subscription};
use restate_types::identifiers::{
    partitioner, IngressRequestId, InvocationId, PartitionKey, WithPartitionKey,
};
use restate_types::ingress::IngressResponseResult;
use restate_types::invocation::{
    AttachInvocationRequest, InvocationQuery, InvocationResponse, InvocationTarget,
    InvocationTargetType, ServiceInvocation, ServiceInvocationResponseSink, SpanRelation,
    SubmitNotificationSink, VirtualObjectHandlerType, WorkflowHandlerType,
};
use restate_types::message::MessageIndex;
use std::fmt::Display;
use std::future::Future;
use std::hash::Hash;
use tokio::sync::oneshot;

mod dispatcher;
pub mod error;

// -- Types used by the ingress to interact with the dispatcher
pub use dispatcher::{DispatchIngressRequest, IngressDispatcher};

pub type IngressInvocationResponseSender = oneshot::Sender<IngressInvocationResponse>;
pub type IngressInvocationResponseReceiver = oneshot::Receiver<IngressInvocationResponse>;
pub type IngressGetInvocationOutputResponseSender = oneshot::Sender<IngressInvocationResponse>;
pub type IngressGetInvocationOutputResponseReceiver = oneshot::Receiver<IngressInvocationResponse>;
pub type IngressSubmittedInvocationNotificationSender =
    oneshot::Sender<SubmittedInvocationNotification>;
pub type IngressSubmittedInvocationNotificationReceiver =
    oneshot::Receiver<SubmittedInvocationNotification>;

#[derive(Debug)]
enum IngressDispatcherRequestInner {
    Invoke(ServiceInvocation),
    ProxyThrough(ServiceInvocation),
    InvocationResponse(InvocationResponse),
    Attach(AttachInvocationRequest),
}

impl WithPartitionKey for IngressDispatcherRequestInner {
    fn partition_key(&self) -> PartitionKey {
        match self {
            IngressDispatcherRequestInner::Invoke(si) => si.invocation_id.partition_key(),
            IngressDispatcherRequestInner::ProxyThrough(si) => si.invocation_id.partition_key(),
            IngressDispatcherRequestInner::InvocationResponse(ir) => ir.id.partition_key(),
            IngressDispatcherRequestInner::Attach(iq) => iq.partition_key(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SubmittedInvocationNotification {
    pub invocation_id: InvocationId,
}

#[derive(Debug)]
pub struct IngressDispatcherRequest {
    inner: IngressDispatcherRequestInner,
    request_mode: IngressRequestMode,
}

#[derive(Debug, Clone)]
pub struct IngressInvocationResponse {
    pub idempotency_expiry_time: Option<String>,
    pub result: IngressResponseResult,
    pub invocation_id: Option<InvocationId>,
}

pub type IngressDeduplicationId = (String, MessageIndex);

#[derive(Debug)]
enum IngressRequestMode {
    RequestResponse(IngressRequestId, IngressInvocationResponseSender),
    DedupFireAndForget {
        deduplication_id: IngressDeduplicationId,
        proxying_partition_key: Option<PartitionKey>,
    },
    WaitSubmitNotification(
        IngressRequestId,
        IngressSubmittedInvocationNotificationSender,
    ),
    FireAndForget,
}

pub trait DeduplicationId: Display + Hash {
    fn requires_proxying(subscription: &Subscription) -> bool;
}

impl IngressDispatcherRequest {
    pub fn invocation(
        mut service_invocation: ServiceInvocation,
    ) -> (Self, IngressRequestId, IngressInvocationResponseReceiver) {
        let (result_tx, result_rx) = oneshot::channel();

        let node_id = metadata().my_node_id();
        let request_id = IngressRequestId::default();
        service_invocation.response_sink = Some(ServiceInvocationResponseSink::Ingress {
            node_id,
            request_id,
        });

        (
            IngressDispatcherRequest {
                request_mode: IngressRequestMode::RequestResponse(request_id, result_tx),
                inner: IngressDispatcherRequestInner::Invoke(service_invocation),
            },
            request_id,
            result_rx,
        )
    }

    pub fn attach(
        invocation_query: InvocationQuery,
    ) -> (Self, IngressRequestId, IngressInvocationResponseReceiver) {
        let (result_tx, result_rx) = oneshot::channel();

        let node_id = metadata().my_node_id();
        let request_id = IngressRequestId::default();

        (
            IngressDispatcherRequest {
                request_mode: IngressRequestMode::RequestResponse(request_id, result_tx),
                inner: IngressDispatcherRequestInner::Attach(AttachInvocationRequest {
                    invocation_query,
                    response_sink: ServiceInvocationResponseSink::Ingress {
                        node_id,
                        request_id,
                    },
                }),
            },
            request_id,
            result_rx,
        )
    }

    pub fn one_way_invocation(
        mut service_invocation: ServiceInvocation,
    ) -> (
        Self,
        IngressRequestId,
        impl Future<Output = Result<SubmittedInvocationNotification, oneshot::error::RecvError>>,
    ) {
        if service_invocation.idempotency_key.is_some()
            || service_invocation.invocation_target.invocation_target_ty()
                == InvocationTargetType::Workflow(WorkflowHandlerType::Workflow)
        {
            let node_id = metadata().my_node_id();
            let request_id = IngressRequestId::default();
            service_invocation.submit_notification_sink = Some(SubmitNotificationSink::Ingress {
                node_id,
                request_id,
            });

            let (tx, rx) = oneshot::channel();

            (
                IngressDispatcherRequest {
                    request_mode: IngressRequestMode::WaitSubmitNotification(request_id, tx),
                    inner: IngressDispatcherRequestInner::Invoke(service_invocation),
                },
                request_id,
                futures::future::Either::Left(rx),
            )
        } else {
            let invocation_id = service_invocation.invocation_id;
            (
                IngressDispatcherRequest {
                    request_mode: IngressRequestMode::FireAndForget,
                    inner: IngressDispatcherRequestInner::Invoke(service_invocation),
                },
                IngressRequestId::default(),
                futures::future::Either::Right(std::future::ready(Ok(
                    SubmittedInvocationNotification { invocation_id },
                ))),
            )
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
        fn evict_pending_response(&self, _req_id: IngressRequestId) {}

        fn evict_pending_submit_notification(&self, _req_id: IngressRequestId) {}

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
            IngressRequestId,
            IngressInvocationResponseSender,
        ) {
            let_assert!(
                IngressDispatcherRequest {
                    inner: IngressDispatcherRequestInner::Invoke(service_invocation),
                    request_mode: IngressRequestMode::RequestResponse(
                        ingress_response_key,
                        ingress_response_sender
                    ),
                } = self
            );
            (
                service_invocation,
                ingress_response_key,
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

        pub fn expect_one_way_invocation_with_submit_notification(
            self,
        ) -> (
            ServiceInvocation,
            IngressSubmittedInvocationNotificationSender,
        ) {
            let_assert!(
                IngressDispatcherRequest {
                    inner: IngressDispatcherRequestInner::Invoke(service_invocation),
                    request_mode: IngressRequestMode::WaitSubmitNotification(_, tx),
                } = self
            );
            (service_invocation, tx)
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

        pub fn expect_attach(
            self,
        ) -> (
            InvocationQuery,
            IngressRequestId,
            IngressInvocationResponseSender,
        ) {
            let_assert!(
                IngressDispatcherRequest {
                    inner: IngressDispatcherRequestInner::Attach(AttachInvocationRequest {
                        invocation_query,
                        response_sink
                    }),
                    request_mode: IngressRequestMode::RequestResponse(
                        ingress_response_key,
                        ingress_response_sender
                    ),
                } = self
            );

            let_assert!(ServiceInvocationResponseSink::Ingress { request_id, .. } = response_sink);
            assert_eq!(request_id, ingress_response_key);

            (
                invocation_query,
                ingress_response_key,
                ingress_response_sender,
            )
        }
    }
}
