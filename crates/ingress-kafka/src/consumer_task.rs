// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::fmt::{self, Display};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use base64::Engine;
use bytes::Bytes;
use metrics::counter;
use opentelemetry::trace::TraceContextExt;
use rdkafka::consumer::stream_consumer::StreamPartitionQueue;
use rdkafka::consumer::{Consumer, ConsumerContext, Rebalance, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::message::BorrowedMessage;
use rdkafka::topic_partition_list::TopicPartitionListElem;
use rdkafka::{ClientConfig, ClientContext, Message};
use tokio::sync::oneshot;
use tracing::{debug, info, info_span, warn, Instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use restate_core::{TaskCenter, TaskId, TaskKind};
use restate_ingress_dispatcher::{
    DeduplicationId, DispatchIngressRequest, IngressDispatcher, IngressDispatcherRequest,
};
use restate_types::invocation::{Header, SpanRelation};
use restate_types::message::MessageIndex;
use restate_types::schema::subscriptions::{EventReceiverServiceType, Sink, Subscription};

use crate::metric_definitions::KAFKA_INGRESS_REQUESTS;

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
    #[error("received a message on the main partition queue for topic {0} partition {1} despite partitioned queues")]
    UnexpectedMainQueueMessage(String, i32),
}

type MessageConsumer = StreamConsumer<RebalanceContext>;

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

    subscription_id: String,
    ingress_request_counter: metrics::Counter,
}

impl MessageSender {
    pub fn new(subscription: Subscription, dispatcher: IngressDispatcher) -> Self {
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

        let req = IngressDispatcherRequest::event(
            &self.subscription,
            key,
            payload,
            SpanRelation::Parent(ingress_span_context),
            Some(Self::generate_deduplication_id(consumer_group_id, &msg)),
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
            .dispatch_ingress_request(req)
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

        let rebalance_context = RebalanceContext {
            task_center: self.task_center.clone(),
            consumer: OnceLock::new(),
            topic_partition_tasks: Mutex::new(HashMap::new()),
            sender: self.sender.clone(),
            consumer_group_id,
        };
        let consumer: Arc<MessageConsumer> =
            Arc::new(self.client_config.create_with_context(rebalance_context)?);
        _ = consumer.context().consumer.set(Arc::downgrade(&consumer));
        let consumer = ConsumerDrop(consumer);

        let topics: Vec<&str> = self.topics.iter().map(|x| &**x).collect();
        consumer.subscribe(&topics)?;

        // we have to poll the main consumer for callbacks to be processed, but we expect to only see messages on the partitioned queues
        loop {
            tokio::select! {
                res = consumer.recv() => {
                    break match res {
                        // We shouldn't see any messages on the main consumer loop
                        Ok(msg) => Err(Error::UnexpectedMainQueueMessage(msg.topic().into(), msg.partition())),
                        Err(e) => Err(e.into()),
                    };
                }
                _ = &mut rx => {
                    break Ok(());
                }
            }
        }
    }
}

struct ConsumerDrop(Arc<MessageConsumer>);

impl std::ops::Deref for ConsumerDrop {
    type Target = Arc<MessageConsumer>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Drop for ConsumerDrop {
    fn drop(&mut self) {
        debug!(
            "Stopping consumer with id {}",
            self.context().consumer_group_id
        );

        let task_center = &self.context().task_center;
        self
            .context()
            .topic_partition_tasks
            .lock()
            .unwrap()
            .drain()
            .for_each(|(partition, task_id)| {
                debug!("Stopping partitioned consumer for partition {partition} due to the consumer stopping");
                task_center
                    .cancel_task(task_id)
                    .map(|handle| handle.abort());
            });
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct TopicPartition(String, i32);

impl<'a> From<TopicPartitionListElem<'a>> for TopicPartition {
    fn from(value: TopicPartitionListElem<'a>) -> Self {
        Self(value.topic().into(), value.partition())
    }
}

impl Display for TopicPartition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.0, self.1)
    }
}

struct RebalanceContext {
    task_center: TaskCenter,
    consumer: OnceLock<Weak<MessageConsumer>>,
    topic_partition_tasks: Mutex<HashMap<TopicPartition, TaskId>>,
    sender: MessageSender,
    consumer_group_id: String,
}

impl ClientContext for RebalanceContext {}

// This callback is called synchronously with the poll of the main queue, so we don't want to block here.
// Once the pre balance steps finish assign() will be called. If we have not split at this point,
// then queues will be created defaulting to forward to the main loop - which we don't want.
// However, if we have split the partition before assign is called, the queue will be created
// with a flag RD_KAFKA_Q_F_FWD_APP and this flag will ensure that the queue will not be sent to the
// main loop. Therefore its critical that the splits happen synchronously before the pre_rebalance ends.
//
// On non-cooperative rebalance during assign all the existing partitions are revoked,
// and their queues are destroyed. Split partition queues will stop working in this case. We should ensure
// that they are not polled again after the assign. Then there will be a further rebalance callback after the revoke
// and we will set up new split partition streams before the assign.
impl ConsumerContext for RebalanceContext {
    fn pre_rebalance<'a>(&self, rebalance: &Rebalance<'a>) {
        let mut topic_partition_tasks = self.topic_partition_tasks.lock().unwrap();
        let consumer = self
            .consumer
            .get()
            .expect("consumer must have been set in context at rebalance time");

        // if the consumer has been dropped, we don't need to maintain tasks any more
        let Some(consumer) = consumer.upgrade() else {
            return;
        };

        match rebalance {
            Rebalance::Assign(partitions) => {
                for partition in partitions.elements() {
                    let partition: TopicPartition = partition.into();

                    if let Some(task_id) = topic_partition_tasks.remove(&partition) {
                        warn!("Kafka informed us of an assigned partition {partition} which we already consider assigned, cancelling the existing partitioned consumer");
                        self.task_center
                            .cancel_task(task_id)
                            .map(|handle| handle.abort());
                    }

                    match consumer.split_partition_queue(&partition.0, partition.1) {
                        Some(queue) => {
                            let task = topic_partition_queue_consumption_loop(
                                self.sender.clone(),
                                partition.clone(),
                                queue,
                                Arc::clone(&consumer),
                                self.consumer_group_id.clone(),
                            );

                            if let Ok(task_id) = self.task_center.spawn_child(
                                TaskKind::Ingress,
                                "partition-queue",
                                None,
                                task,
                            ) {
                                topic_partition_tasks.insert(partition, task_id);
                            } else {
                                // shutting down
                                return;
                            }
                        }
                        None => {
                            warn!("Invalid partition {partition} given to us in rebalance, ignoring it");
                            continue;
                        }
                    }
                }
            }
            Rebalance::Revoke(partitions) => {
                for partition in partitions.elements() {
                    let partition = partition.into();
                    match topic_partition_tasks.remove(&partition)
                {
                    Some(task_id) => {
                        debug!("Stopping partitioned consumer for partition {partition} due to rebalance");
                        // The partitioned queue will not be polled again.
                        // It might be mid-poll right now, but if so its result will not be sent anywhere.
                        self.task_center.cancel_task(task_id).map(|handle| handle.abort());
                    }
                    None => warn!("Kafka informed us of a revoked partition {partition} which we had no consumer task for"),
                }
                }
            }
            Rebalance::Error(_) => {}
        }
    }
}

async fn topic_partition_queue_consumption_loop(
    sender: MessageSender,
    partition: TopicPartition,
    topic_partition_consumer: StreamPartitionQueue<impl ConsumerContext>,
    consumer: Arc<MessageConsumer>,
    consumer_group_id: String,
) -> Result<(), anyhow::Error> {
    debug!("Starting partitioned consumer for partition {partition}");

    // this future will be aborted when the partition is no longer needed
    loop {
        tokio::select! {
            res = topic_partition_consumer.recv() => {
                let msg = res?;
                let offset = msg.offset();
                sender.send(&consumer_group_id, msg).await?;
                consumer.store_offset(&partition.0, partition.1, offset)?;
            }
        }
    }
}
