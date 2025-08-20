// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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
use opentelemetry::propagation::{Extractor, TextMapPropagator};
use opentelemetry::trace::{Span, SpanContext, TraceContextExt};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use restate_bifrost::Bifrost;
use restate_core::{Metadata, my_node_id};
use restate_storage_api::deduplication_table::DedupInformation;
use restate_types::identifiers::{InvocationId, PartitionKey, WithPartitionKey, partitioner};
use restate_types::invocation::{InvocationTarget, ServiceInvocation, SpanRelation};
use restate_types::message::MessageIndex;
use restate_types::partition_table::PartitionTableError;
use restate_types::schema::Schema;
use restate_types::schema::invocation_target::InvocationTargetResolver;
use restate_types::schema::subscriptions::{EventInvocationTargetTemplate, Sink, Subscription};
use restate_types::{GenerationalNodeId, live};
use restate_wal_protocol::{Command, Destination, Envelope, Header, Source};
use std::borrow::Borrow;
use std::sync::Arc;
use tracing::debug;

#[derive(Debug)]
pub struct KafkaIngressEvent {
    service_invocation: Box<ServiceInvocation>,
    deduplication_id: KafkaDeduplicationId,
    deduplication_index: MessageIndex,
    proxying_partition_key: Option<PartitionKey>,
}

impl KafkaIngressEvent {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        subscription: &Subscription,
        schema: live::Pinned<Schema>,
        key: Bytes,
        payload: Bytes,
        deduplication_id: KafkaDeduplicationId,
        deduplication_index: MessageIndex,
        headers: Vec<restate_types::invocation::Header>,
        consumer_group_id: &str,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<Self, anyhow::Error> {
        // Check if we need to proxy or not
        let proxying_partition_key = if KafkaDeduplicationId::requires_proxying(subscription) {
            Some(partitioner::HashPartitioner::compute_partition_key(
                &deduplication_id,
            ))
        } else {
            None
        };

        let invocation_target = match subscription.sink() {
            Sink::Invocation {
                event_invocation_target_template,
            } => match event_invocation_target_template {
                EventInvocationTargetTemplate::Service { name, handler } => {
                    InvocationTarget::service(name.clone(), handler.clone())
                }
                EventInvocationTargetTemplate::VirtualObject {
                    name,
                    handler,
                    handler_ty,
                } => InvocationTarget::virtual_object(
                    name.clone(),
                    std::str::from_utf8(&key)
                        .map_err(|e| {
                            anyhow::anyhow!("The Kafka record key must be valid UTF-8: {e}")
                        })?
                        .to_owned(),
                    handler.clone(),
                    *handler_ty,
                ),
                EventInvocationTargetTemplate::Workflow {
                    name,
                    handler,
                    handler_ty,
                } => InvocationTarget::workflow(
                    name.clone(),
                    std::str::from_utf8(&key)
                        .map_err(|e| {
                            anyhow::anyhow!("The Kafka record key must be valid UTF-8: {e}")
                        })?
                        .to_owned(),
                    handler.clone(),
                    *handler_ty,
                ),
            },
        };

        // Compute the retention values
        let invocation_retention = schema
            .resolve_latest_invocation_target(
                invocation_target.service_name(),
                invocation_target.handler_name(),
            )
            .ok_or_else(|| anyhow::anyhow!("Service and handler are not registered"))?
            .compute_retention(false);

        // Time to generate invocation id
        let invocation_id = InvocationId::generate(&invocation_target, None);

        // Figure out tracing span
        let ingress_span_context = prepare_tracing_span(
            &invocation_id,
            &invocation_target,
            &headers,
            consumer_group_id,
            topic,
            partition as i64,
            offset,
        );

        // Finally generate service invocation
        let mut service_invocation = Box::new(ServiceInvocation::initialize(
            invocation_id,
            invocation_target,
            restate_types::invocation::Source::Subscription(subscription.id()),
        ));
        service_invocation.with_related_span(SpanRelation::parent(ingress_span_context));
        service_invocation.argument = payload;
        service_invocation.headers = headers;
        service_invocation.with_retention(invocation_retention);

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
    WalProtocol(#[from] restate_bifrost::AppendError),
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
            my_node_id(),
            deduplication_id.to_string(),
            deduplication_index,
        );
        let (log_id, lsn) =
            restate_bifrost::append_to_bifrost(&self.bifrost, Arc::new(envelope)).await?;

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
    service_invocation: Box<ServiceInvocation>,
    from_node_id: GenerationalNodeId,
    deduplication_source: String,
    deduplication_index: MessageIndex,
) -> Envelope {
    let header = Header {
        source: Source::Ingress {
            node_id: from_node_id,
            nodes_config_version: Metadata::with_current(|m| m.nodes_config_version()),
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

#[allow(clippy::too_many_arguments)]
pub(crate) fn prepare_tracing_span(
    invocation_id: &InvocationId,
    invocation_target: &InvocationTarget,
    headers: &[restate_types::invocation::Header],
    consumer_group_name: &str,
    topic: &str,
    partition: i64,
    offset: i64,
) -> SpanContext {
    let tracing_context = TraceContextPropagator::new().extract(&HeaderExtractor(headers));
    let inbound_span = tracing_context.span();

    let relation = if inbound_span.span_context().is_valid() {
        SpanRelation::parent(inbound_span.span_context())
    } else {
        SpanRelation::None
    };

    let span = restate_tracing_instrumentation::info_invocation_span!(
        relation = relation,
        prefix = "ingress_kafka",
        id = invocation_id,
        target = invocation_target,
        tags = (
            messaging.system = "kafka",
            messaging.consumer.group.name = consumer_group_name.to_owned(),
            messaging.operation.type = "process",
            messaging.kafka.offset = offset,
            messaging.source.partition.id = partition,
            messaging.source.name = topic.to_owned()
        )
    );

    span.span_context().clone()
}

struct HeaderExtractor<'a>(pub &'a [restate_types::invocation::Header]);

impl Extractor for HeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0
            .iter()
            .find(|h| h.name.eq_ignore_ascii_case(key))
            .map(|value| value.value.borrow())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.iter().map(|h| h.name.borrow()).collect::<Vec<_>>()
    }
}
