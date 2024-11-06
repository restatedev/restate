// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use base64::Engine;
use bytes::Bytes;
use metrics::counter;
use opentelemetry::trace::TraceContextExt;
use rdkafka::consumer::stream_consumer::StreamPartitionQueue;
use rdkafka::consumer::{Consumer, DefaultConsumerContext, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::message::BorrowedMessage;
use rdkafka::{ClientConfig, Message};
use tokio::sync::oneshot;
use tracing::{debug, info, info_span, Instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::dispatcher::{DispatchKafkaEvent, KafkaIngressDispatcher, KafkaIngressEvent};
use crate::metric_definitions::KAFKA_INGRESS_REQUESTS;
use restate_core::{cancellation_watcher, TaskCenter, TaskId, TaskKind};
use restate_types::invocation::{Header, SpanRelation};
use restate_types::message::MessageIndex;
use restate_types::schema::subscriptions::{EventReceiverServiceType, Sink, Subscription};

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
    #[error("topic {0} partition {1} queue split didn't succeed")]
    TopicPartitionSplit(String, i32),
}

type MessageConsumer = StreamConsumer<DefaultConsumerContext>;

#[derive(Debug, Hash)]
pub struct KafkaDeduplicationId {
    consumer_group: String,
    topic: String,
    partition: i32,
}

impl fmt::Display for KafkaDeduplicationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}-{}-{}",
            self.consumer_group, self.topic, self.partition
        )
    }
}

impl KafkaDeduplicationId {
    pub(crate) fn requires_proxying(subscription: &Subscription) -> bool {
        // Service event receiver requires proxying because we don't want to scatter deduplication ids (kafka topic/partition offsets) in all the Restate partitions.
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
    dispatcher: KafkaIngressDispatcher,

    subscription_id: String,
    ingress_request_counter: metrics::Counter,
}

impl MessageSender {
    pub fn new(subscription: Subscription, dispatcher: KafkaIngressDispatcher) -> Self {
        Self {
            subscription_id: subscription.id().to_string(),
            ingress_request_counter: counter!(
                KAFKA_INGRESS_REQUESTS,
                "subscription" => subscription.id().to_string()
            ),
            subscription,
            dispatcher,
        }
    }

    async fn send(&self, consumer_group_id: &str, msg: BorrowedMessage<'_>) -> Result<(), Error> {
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
        let headers = Self::generate_events_attributes(&msg, &self.subscription_id);

        let (deduplication_id, deduplication_index) =
            Self::generate_deduplication_id(consumer_group_id, &msg);
        let req = KafkaIngressEvent::new(
            &self.subscription,
            key,
            payload,
            SpanRelation::Parent(ingress_span_context),
            deduplication_id,
            deduplication_index,
            headers,
        )
        .map_err(|cause| Error::Event {
            topic: msg.topic().to_string(),
            partition: msg.partition(),
            offset: msg.offset(),
            cause,
        })?;

        self.ingress_request_counter.increment(1);

        self.dispatcher
            .dispatch_kafka_event(req)
            .instrument(ingress_span)
            .await
            .map_err(|_| Error::IngressDispatcherClosed)?;
        Ok(())
    }

    fn generate_events_attributes(msg: &impl Message, subscription_id: &str) -> Vec<Header> {
        let mut headers = Vec::with_capacity(6);
        headers.push(Header::new("kafka.offset", msg.offset().to_string()));
        headers.push(Header::new("kafka.topic", msg.topic()));
        headers.push(Header::new("kafka.partition", msg.partition().to_string()));
        if let Some(timestamp) = msg.timestamp().to_millis() {
            headers.push(Header::new("kafka.timestamp", timestamp.to_string()));
        }
        headers.push(Header::new(
            "restate.subscription.id".to_string(),
            subscription_id,
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
            KafkaDeduplicationId {
                consumer_group: consumer_group.to_owned(),
                topic: msg.topic().to_owned(),
                partition: msg.partition(),
            },
            msg.offset() as u64,
        )
    }
}

#[derive(Clone)]
pub struct ConsumerTask {
    task_center: TaskCenter,
    client_config: ClientConfig,
    topics: Vec<String>,
    sender: MessageSender,
}

impl ConsumerTask {
    pub fn new(
        task_center: TaskCenter,
        client_config: ClientConfig,
        topics: Vec<String>,
        sender: MessageSender,
    ) -> Self {
        Self {
            task_center,
            client_config,
            topics,
            sender,
        }
    }

    pub async fn run(self, mut rx: oneshot::Receiver<()>) -> Result<(), Error> {
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

        let consumer: Arc<MessageConsumer> = Arc::new(self.client_config.create()?);
        let topics: Vec<&str> = self.topics.iter().map(|x| &**x).collect();
        consumer.subscribe(&topics)?;

        let mut topic_partition_tasks: HashMap<(String, i32), TaskId> = Default::default();

        let result = loop {
            tokio::select! {
                res = consumer.recv() => {
                    let msg = match res {
                       Ok(msg) => msg,
                        Err(e) => break Err(e.into())
                    };
                    let topic = msg.topic().to_owned();
                    let partition = msg.partition();
                    let offset = msg.offset();

                    // If we didn't split the queue, let's do it and start the topic partition consumer
                     if let Entry::Vacant(e) = topic_partition_tasks.entry((topic.clone(), partition)) {
                        let topic_partition_consumer = match consumer
                            .split_partition_queue(&topic, partition) {
                            Some(q) => q,
                            None => break Err(Error::TopicPartitionSplit(topic.clone(), partition))
                        };

                        let task = topic_partition_queue_consumption_loop(
                            self.sender.clone(),
                            topic.clone(), partition,
                            topic_partition_consumer,
                            Arc::clone(&consumer),
                            consumer_group_id.clone()
                        );

                        if let Ok(task_id) = self.task_center.spawn_child(TaskKind::Ingress, "partition-queue", None, task) {
                            e.insert(task_id);
                        } else {
                            break Ok(());
                        }
                    }

                    // We got this message, let's send it through
                    if let Err(e) = self.sender.send(&consumer_group_id, msg).await {
                        break Err(e)
                    }

                    // This method tells rdkafka that we have processed this message,
                    // so its offset can be safely committed.
                    // rdkafka periodically commits these offsets asynchronously, with a period configurable
                    // with auto.commit.interval.ms
                    if let Err(e) = consumer.store_offset(&topic, partition, offset) {
                        break Err(e.into())
                    }
                }
                _ = &mut rx => {
                    break Ok(());
                }
            }
        };
        for task_id in topic_partition_tasks.into_values() {
            self.task_center.cancel_task(task_id);
        }
        result
    }
}

async fn topic_partition_queue_consumption_loop(
    sender: MessageSender,
    topic: String,
    partition: i32,
    topic_partition_consumer: StreamPartitionQueue<DefaultConsumerContext>,
    consumer: Arc<MessageConsumer>,
    consumer_group_id: String,
) -> Result<(), anyhow::Error> {
    let mut shutdown = std::pin::pin!(cancellation_watcher());

    loop {
        tokio::select! {
            res = topic_partition_consumer.recv() => {
                let msg = res?;
                let offset = msg.offset();
                sender.send(&consumer_group_id, msg).await?;
                consumer.store_offset(&topic, partition, offset)?;
            }
            _ = &mut shutdown => {
                return Ok(())
            }
        }
    }
}
