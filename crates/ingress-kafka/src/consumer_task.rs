// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use base64::Engine;
use bytes::Bytes;
use opentelemetry::trace::TraceContextExt;
use rdkafka::consumer::{Consumer, DefaultConsumerContext, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::message::BorrowedMessage;
use rdkafka::{ClientConfig, Message};
use restate_ingress_dispatcher::{
    DeduplicationId, DispatchIngressRequest, IngressDispatcher, IngressDispatcherRequest,
};
use restate_schema_api::subscription::{EventReceiverServiceType, Sink, Subscription};
use restate_types::identifiers::SubscriptionId;
use restate_types::invocation::{Header, SpanRelation};
use restate_types::message::MessageIndex;
use std::fmt;
use tokio::sync::oneshot;
use tracing::{debug, info, info_span, Instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Kafka(#[from] KafkaError),
    #[error(
        "error processing message topic {topic} partition {partition} offset {offset}: {cause}"
    )]
    Event {
        topic: String,
        partition: i32,
        offset: i64,
        #[source]
        cause: anyhow::Error,
    },
    #[error("ingress dispatcher channel is closed")]
    IngressDispatcherClosed,
}

type MessageConsumer = StreamConsumer<DefaultConsumerContext>;

#[derive(Debug, Hash)]
pub struct KafkaDeduplicationId(String, String, i32);

impl fmt::Display for KafkaDeduplicationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl DeduplicationId for KafkaDeduplicationId {
    fn requires_proxying(subscription: &Subscription) -> bool {
        matches!(
            subscription.sink(),
            Sink::Service {
                ty: EventReceiverServiceType::Service,
                ..
            },
        )
    }
}

#[derive(Clone)]
pub struct MessageSender {
    subscription: Subscription,
    dispatcher: IngressDispatcher,
}

impl MessageSender {
    pub fn new(subscription: Subscription, dispatcher: IngressDispatcher) -> Self {
        Self {
            subscription,
            dispatcher,
        }
    }

    async fn send(
        &mut self,
        consumer_group_id: &str,
        msg: &BorrowedMessage<'_>,
    ) -> Result<(), Error> {
        // Prepare ingress span
        let ingress_span = info_span!(
            "kafka_ingress_consume",
            otel.name = "kafka_ingress_consume",
            messaging.system = "kafka",
            messaging.operation = "receive",
            messaging.source.name = msg.topic(),
            messaging.destination.name = %self.subscription.sink()
        );
        info!(parent: &ingress_span, "Processing Kafka ingress request");
        let ingress_span_context = ingress_span.context().span().span_context().clone();

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
        let headers = Self::generate_events_attributes(msg, self.subscription.id());

        let req = IngressDispatcherRequest::event(
            &self.subscription,
            key,
            payload,
            SpanRelation::Parent(ingress_span_context),
            Some(Self::generate_deduplication_id(consumer_group_id, msg)),
            headers,
        )
        .map_err(|cause| Error::Event {
            topic: msg.topic().to_string(),
            partition: msg.partition(),
            offset: msg.offset(),
            cause,
        })?;

        self.dispatcher
            .dispatch_ingress_request(req)
            .instrument(ingress_span)
            .await
            .map_err(|_| Error::IngressDispatcherClosed)?;
        Ok(())
    }

    fn generate_events_attributes(
        msg: &impl Message,
        subscription_id: SubscriptionId,
    ) -> Vec<Header> {
        let mut headers = Vec::with_capacity(6);
        headers.push(Header::new("kafka.offset", msg.offset().to_string()));
        headers.push(Header::new("kafka.topic", msg.topic()));
        headers.push(Header::new("kafka.partition", msg.partition().to_string()));
        if let Some(timestamp) = msg.timestamp().to_millis() {
            headers.push(Header::new("kafka.timestamp", timestamp.to_string()));
        }
        headers.push(Header::new(
            "restate.subscription.id".to_string(),
            subscription_id.to_string(),
        ));

        if let Some(key) = msg.key() {
            headers.push(Header::new(
                "kafka.key",
                &*base64::prelude::BASE64_URL_SAFE.encode(key),
            ));
        }

        headers
    }

    fn generate_deduplication_id(
        consumer_group: &str,
        msg: &impl Message,
    ) -> (KafkaDeduplicationId, MessageIndex) {
        (
            KafkaDeduplicationId(
                consumer_group.to_owned(),
                msg.topic().to_owned(),
                msg.partition(),
            ),
            msg.offset() as u64,
        )
    }
}

#[derive(Clone)]
pub struct ConsumerTask {
    client_config: ClientConfig,
    topics: Vec<String>,
    sender: MessageSender,
}

impl ConsumerTask {
    pub fn new(client_config: ClientConfig, topics: Vec<String>, sender: MessageSender) -> Self {
        Self {
            client_config,
            topics,
            sender,
        }
    }

    pub async fn run(mut self, mut rx: oneshot::Receiver<()>) -> Result<(), Error> {
        // Create the consumer and subscribe to the topic
        let consumer_group_id = self
            .client_config
            .get("group.id")
            .expect("group.id must be set")
            .to_string();
        debug!(
            "Starting consumer for topics {:?} with configuration {:?}",
            self.topics, self.client_config
        );

        let consumer: MessageConsumer = self.client_config.create()?;
        let topics: Vec<&str> = self.topics.iter().map(|x| &**x).collect();
        consumer.subscribe(&topics)?;

        loop {
            tokio::select! {
                res = consumer.recv() => {
                    let msg = res?;
                    self.sender.send(&consumer_group_id, &msg).await?;
                    // This method tells rdkafka that we have processed this message,
                    // so its offset can be safely committed.
                    // rdkafka periodically commits these offsets asynchronously, with a period configurable
                    // with auto.commit.interval.ms
                    consumer.store_offset_from_message(&msg)?;
                }
                _ = &mut rx => {
                    return Ok(());
                }
            }
        }
    }
}
