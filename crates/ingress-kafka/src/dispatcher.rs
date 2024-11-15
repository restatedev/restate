// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::consumer_task::KafkaDeduplicationId;
use bytes::Bytes;
use restate_bifrost::Bifrost;
use restate_core::metadata;
use restate_storage_api::deduplication_table::DedupInformation;
use restate_types::identifiers::{
    partitioner, InvocationId, PartitionKey, PartitionProcessorRpcRequestId, WithPartitionKey,
};
use restate_types::invocation::{
    InvocationTarget, ServiceInvocation, SpanRelation, VirtualObjectHandlerType,
    WorkflowHandlerType,
};
use restate_types::message::MessageIndex;
use restate_types::partition_table::PartitionTableError;
use restate_types::schema::subscriptions::{EventReceiverServiceType, Sink, Subscription};
use restate_types::GenerationalNodeId;
use restate_wal_protocol::{
    append_envelope_to_bifrost, Command, Destination, Envelope, Header, Source,
};
use std::sync::Arc;
use tracing::debug;

#[derive(Debug)]
pub struct KafkaIngressEvent {
    service_invocation: ServiceInvocation,
    deduplication_id: KafkaDeduplicationId,
    deduplication_index: MessageIndex,
    proxying_partition_key: Option<PartitionKey>,
}

impl KafkaIngressEvent {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        subscription: &Subscription,
        key: Bytes,
        payload: Bytes,
        related_span: SpanRelation,
        deduplication_id: KafkaDeduplicationId,
        deduplication_index: MessageIndex,
        headers: Vec<restate_types::invocation::Header>,
        experimental_feature_kafka_ingress_next: bool,
    ) -> Result<Self, anyhow::Error> {
        // Check if we need to proxy or not
        let proxying_partition_key = if KafkaDeduplicationId::requires_proxying(subscription) {
            Some(partitioner::HashPartitioner::compute_partition_key(
                &deduplication_id,
            ))
        } else {
            None
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
            if experimental_feature_kafka_ingress_next {
                restate_types::invocation::Source::Subscription(subscription.id())
            } else {
                restate_types::invocation::Source::Ingress(PartitionProcessorRpcRequestId::new())
            },
        );
        service_invocation.with_related_span(related_span);
        service_invocation.argument = argument;
        service_invocation.headers = headers;

        Ok(KafkaIngressEvent {
            service_invocation,
            deduplication_id,
            deduplication_index,
            proxying_partition_key,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum IngressDispatchError {
    #[error("bifrost error: {0}")]
    WalProtocol(#[from] restate_wal_protocol::Error),
    #[error("partition routing error: {0}")]
    PartitionRoutingError(#[from] PartitionTableError),
}

/// Dispatches a request from kafka ingress to bifrost
pub trait DispatchKafkaEvent {
    fn dispatch_kafka_event(
        &self,
        event: KafkaIngressEvent,
    ) -> impl std::future::Future<Output = Result<(), IngressDispatchError>> + Send;
}

#[derive(Clone)]
pub(crate) struct KafkaIngressDispatcher {
    bifrost: Bifrost,
}

impl KafkaIngressDispatcher {
    pub(crate) fn new(bifrost: Bifrost) -> Self {
        Self { bifrost }
    }
}

impl DispatchKafkaEvent for KafkaIngressDispatcher {
    async fn dispatch_kafka_event(
        &self,
        ingress_request: KafkaIngressEvent,
    ) -> Result<(), IngressDispatchError> {
        let KafkaIngressEvent {
            service_invocation: inner,
            deduplication_id,
            deduplication_index,
            proxying_partition_key,
        } = ingress_request;

        let partition_key = proxying_partition_key.unwrap_or_else(|| inner.partition_key());

        let envelope = wrap_service_invocation_in_envelope(
            partition_key,
            inner,
            metadata().my_node_id(),
            deduplication_id.to_string(),
            deduplication_index,
        );
        let (log_id, lsn) = append_envelope_to_bifrost(&self.bifrost, Arc::new(envelope)).await?;

        debug!(
            log_id = %log_id,
            lsn = %lsn,
            "Ingress request written to bifrost"
        );
        Ok(())
    }
}

fn wrap_service_invocation_in_envelope(
    partition_key: PartitionKey,
    service_invocation: ServiceInvocation,
    from_node_id: GenerationalNodeId,
    deduplication_source: String,
    deduplication_index: MessageIndex,
) -> Envelope {
    let header = Header {
        source: Source::Ingress {
            node_id: from_node_id,
            nodes_config_version: metadata().nodes_config_version(),
        },
        dest: Destination::Processor {
            partition_key,
            dedup: Some(DedupInformation::ingress(
                deduplication_source,
                deduplication_index,
            )),
        },
    };

    Envelope::new(header, Command::ProxyThrough(service_invocation))
}
