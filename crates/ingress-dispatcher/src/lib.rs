// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Display;
use std::hash::Hash;

use bytes::Bytes;

use restate_types::identifiers::{partitioner, InvocationId, PartitionKey, WithPartitionKey};
use restate_types::invocation::{
    InvocationTarget, ServiceInvocation, SpanRelation, VirtualObjectHandlerType,
    WorkflowHandlerType,
};
use restate_types::message::MessageIndex;
use restate_types::schema::subscriptions::{EventReceiverServiceType, Sink, Subscription};

mod dispatcher;
pub mod error;

// -- Types used by the ingress to interact with the dispatcher
pub use dispatcher::{DispatchIngressRequest, IngressDispatcher};

#[derive(Debug)]
enum IngressDispatcherRequestInner {
    Invoke(ServiceInvocation),
    ProxyThrough(ServiceInvocation),
}

impl WithPartitionKey for IngressDispatcherRequestInner {
    fn partition_key(&self) -> PartitionKey {
        match self {
            IngressDispatcherRequestInner::Invoke(si) => si.invocation_id.partition_key(),
            IngressDispatcherRequestInner::ProxyThrough(si) => si.invocation_id.partition_key(),
        }
    }
}

#[derive(Debug)]
pub struct IngressDispatcherRequest {
    inner: IngressDispatcherRequestInner,
    request_mode: IngressRequestMode,
}

pub type IngressDeduplicationId = (String, MessageIndex);

#[derive(Debug)]
enum IngressRequestMode {
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
        let invocation_id = InvocationId::generate(&invocation_target, None);
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
}

#[cfg(feature = "test-util")]
pub mod test_util {
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
        async fn dispatch_ingress_request(
            &self,
            ingress_request: IngressDispatcherRequest,
        ) -> Result<(), IngressDispatchError> {
            let _ = self.sender.send(ingress_request);
            Ok(())
        }
    }

    impl IngressDispatcherRequest {
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
