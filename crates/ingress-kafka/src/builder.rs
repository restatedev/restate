// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Borrow;

use anyhow::bail;
use base64::Engine;
use bytes::Bytes;
use opentelemetry::propagation::{Extractor, TextMapPropagator};
use opentelemetry::trace::{Span, SpanContext, TraceContextExt};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use rdkafka::Message;
use rdkafka::message::BorrowedMessage;
use tracing::{info_span, trace};

use restate_storage_api::deduplication_table::DedupInformation;
use restate_types::identifiers::{InvocationId, WithPartitionKey, partitioner};
use restate_types::invocation::{Header, InvocationTarget, ServiceInvocation, SpanRelation};
use restate_types::live::Live;
use restate_types::schema::Schema;
use restate_types::schema::invocation_target::{DeploymentStatus, InvocationTargetResolver};
use restate_types::schema::subscriptions::{EventInvocationTargetTemplate, Sink, Subscription};
use restate_wal_protocol::{Command, Destination, Envelope, Source};

use crate::Error;

#[derive(Clone)]
pub struct EnvelopeBuilder {
    subscription: Subscription,
    schema: Live<Schema>,
    // avoids creating a new string for each invocation
    subscription_id: String,
}

impl EnvelopeBuilder {
    pub fn new(subscription: Subscription, schema: Live<Schema>) -> Self {
        Self {
            subscription_id: subscription.id().to_string(),
            subscription,
            schema,
        }
    }

    pub fn subscription(&self) -> &Subscription {
        &self.subscription
    }

    pub fn build(
        &mut self,
        producer_id: u128,
        consumer_group_id: &str,
        msg: BorrowedMessage<'_>,
    ) -> Result<Envelope, Error> {
        // Prepare ingress span
        let ingress_span = info_span!(
            "kafka_ingress_consume",
            otel.name = "kafka_ingress_consume",
            messaging.system = "kafka",
            messaging.operation = "receive",
            messaging.source.name = msg.topic(),
            messaging.source.partition = msg.partition(),
            messaging.destination.name = %self.subscription.sink(),
            restate.subscription.id = %self.subscription.id(),
            messaging.consumer.group.name = consumer_group_id
        );

        trace!(parent: &ingress_span, "Building Kafka ingress request");

        let key = if let Some(k) = msg.key() {
            Bytes::copy_from_slice(k)
        } else {
            Bytes::default()
        };
        let payload = if let Some(p) = msg.payload() {
            Bytes::copy_from_slice(p)
        } else {
            Bytes::default()
        };

        let headers = Self::generate_events_attributes(&msg, &self.subscription_id);
        let dedup = DedupInformation::producer(producer_id, msg.offset() as u64);

        let invocation = InvocationBuilder::create(
            &self.subscription,
            producer_id,
            self.schema.live_load(),
            key,
            payload,
            headers,
            consumer_group_id,
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )
        .map_err(|cause| Error::Event {
            subscription: self.subscription_id.clone(),
            topic: msg.topic().to_string(),
            partition: msg.partition(),
            offset: msg.offset(),
            cause,
        })?;

        Ok(self.wrap_service_invocation_in_envelope(invocation, dedup))
    }

    fn wrap_service_invocation_in_envelope(
        &self,
        service_invocation: Box<ServiceInvocation>,
        dedup_information: DedupInformation,
    ) -> Envelope {
        let header = restate_wal_protocol::Header {
            source: Source::Ingress {},
            dest: Destination::Processor {
                partition_key: service_invocation.partition_key(),
                dedup: Some(dedup_information),
            },
        };

        Envelope::new(header, Command::Invoke(service_invocation))
    }

    fn generate_events_attributes(msg: &impl Message, subscription_id: &str) -> Vec<Header> {
        let mut headers = Vec::with_capacity(6);
        headers.push(Header::new("kafka.offset", msg.offset().to_string()));
        headers.push(Header::new("kafka.topic", msg.topic()));
        headers.push(Header::new("kafka.partition", msg.partition().to_string()));
        if let Some(timestamp) = msg.timestamp().to_millis() {
            headers.push(Header::new("kafka.timestamp", timestamp.to_string()));
        }
        headers.push(Header::new("restate.subscription.id", subscription_id));

        if let Some(key) = msg.key() {
            headers.push(Header::new(
                "kafka.key",
                &*base64::prelude::BASE64_URL_SAFE.encode(key),
            ));
        }

        headers
    }
}

#[derive(Debug)]
pub struct InvocationBuilder;

impl InvocationBuilder {
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        subscription: &Subscription,
        producer_id: u128,
        schema: &Schema,
        key: Bytes,
        payload: Bytes,
        headers: Vec<restate_types::invocation::Header>,
        consumer_group_id: &str,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<Box<ServiceInvocation>, anyhow::Error> {
        let Sink::Invocation {
            event_invocation_target_template,
        } = subscription.sink();

        let invocation_target = match event_invocation_target_template {
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
                    .map_err(|e| anyhow::anyhow!("The Kafka record key must be valid UTF-8: {e}"))?
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
                    .map_err(|e| anyhow::anyhow!("The Kafka record key must be valid UTF-8: {e}"))?
                    .to_owned(),
                handler.clone(),
                *handler_ty,
            ),
        };

        // Compute the retention values
        let target = schema
            .resolve_latest_invocation_target(
                invocation_target.service_name(),
                invocation_target.handler_name(),
            )
            .ok_or_else(|| anyhow::anyhow!("Service and handler are not registered"))?;

        if let DeploymentStatus::Deprecated(dp_id) = target.deployment_status {
            bail!(
                "the service {} is exposed by the deprecated deployment {dp_id}, please upgrade the SDK.",
                invocation_target.service_name()
            )
        }

        let invocation_retention = target.compute_retention(false);

        let seed = KafkaPartitionKeySeed {
            producer: &producer_id,
            offset: &offset,
        };

        let invocation_id = InvocationId::generate_or_else(&invocation_target, None, || {
            partitioner::HashPartitioner::compute_partition_key(seed)
        });

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

        Ok(service_invocation)
    }
}

#[derive(Hash)]
/// Hashable seed that yields a deterministic partition key for service invocations, keeping
/// identical invocations on the same partition for deduplication.
struct KafkaPartitionKeySeed<'a> {
    producer: &'a u128,
    offset: &'a i64,
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
