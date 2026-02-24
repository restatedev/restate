// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::fmt;
use std::hash::Hash;
use std::sync::{Arc, OnceLock, Weak};
use std::time::Duration;

use crate::Error;
use crate::legacy::dispatcher::{DispatchKafkaEvent, KafkaIngressDispatcher, KafkaIngressEvent};
use crate::legacy::metric_definitions::{KAFKA_INGRESS_CONSUMER_LAG, KAFKA_INGRESS_REQUESTS};
use anyhow::Context;
use base64::Engine;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use metrics::{counter, gauge};
use rdkafka::consumer::stream_consumer::StreamPartitionQueue;
use rdkafka::consumer::{
    BaseConsumer, CommitMode, Consumer, ConsumerContext, Rebalance, StreamConsumer,
};
use rdkafka::error::KafkaError;
use rdkafka::message::BorrowedMessage;
use rdkafka::topic_partition_list::TopicPartitionListElem;
use rdkafka::types::RDKafkaErrorCode;
use rdkafka::{ClientConfig, ClientContext, Message, Statistics};
use restate_core::network::{
    NetworkSender, Networking, RpcError, RpcReplyError, Swimlane, TransportConnect,
};
use restate_core::partitions::PartitionRouting;
use restate_core::{Metadata, TaskCenter, TaskHandle, TaskKind, task_center};
use restate_types::identifiers::{PartitionId, SubscriptionId};
use restate_types::invocation::Header;
use restate_types::live::Live;
use restate_types::message::MessageIndex;
use restate_types::net::ingest::{DedupSequenceNrQueryRequest, ProducerId, ResponseStatus};
use restate_types::retries::RetryPolicy;
use restate_types::schema::Schema;
use restate_types::schema::subscriptions::{EventInvocationTargetTemplate, Sink, Subscription};
use tokio::sync::{mpsc, oneshot};
use tracing::{Instrument, debug, info_span, trace, warn};

type MessageConsumer<T> = StreamConsumer<RebalanceContext<T>>;

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
            Sink::Invocation {
                event_invocation_target_template: EventInvocationTargetTemplate::Service { .. }
            },
        )
    }
}

#[derive(Clone)]
pub struct MessageSender {
    subscription: Subscription,
    dispatcher: KafkaIngressDispatcher,
    schema: Live<Schema>,

    subscription_id: String,
    ingress_request_counter: metrics::Counter,
}

impl MessageSender {
    pub fn new(
        subscription: Subscription,
        dispatcher: KafkaIngressDispatcher,
        schema: Live<Schema>,
    ) -> Self {
        Self {
            subscription_id: subscription.id().to_string(),
            ingress_request_counter: counter!(
                KAFKA_INGRESS_REQUESTS,
                "subscription" => subscription.id().to_string()
            ),
            subscription,
            dispatcher,
            schema,
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
            messaging.destination.name = %self.subscription.sink(),
            restate.subscription.id = %self.subscription.id(),
            messaging.consumer.group.name = consumer_group_id
        );
        trace!(parent: &ingress_span, "Processing Kafka ingress request");

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
            self.schema.pinned(),
            key,
            payload,
            deduplication_id,
            deduplication_index,
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

        self.ingress_request_counter.increment(1);

        self.dispatcher
            .dispatch_kafka_event(req)
            .instrument(ingress_span)
            .await
            .map_err(|err| Error::IngestionClosed(err.into()))?;
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
        headers.push(Header::new("restate.subscription.id", subscription_id));

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

    fn update_consumer_stats(&self, stats: Statistics) {
        for topic in stats.topics {
            for partition in topic.1.partitions {
                let lag = partition.1.consumer_lag as f64;
                gauge!(
                    KAFKA_INGRESS_CONSUMER_LAG,
                     "subscription" => self.subscription.id().to_string(),
                     "topic" => topic.0.to_string(),
                     "partition" =>  partition.0.to_string()
                )
                .set(lag);
            }
        }
    }
}

#[derive(Clone)]
pub struct ConsumerTask<T> {
    networking: Networking<T>,
    partition_routing: PartitionRouting,
    client_config: ClientConfig,
    topics: Vec<String>,
    sender: MessageSender,
}

impl<T> ConsumerTask<T>
where
    T: TransportConnect,
{
    pub fn new(
        networking: Networking<T>,
        partition_routing: PartitionRouting,
        client_config: ClientConfig,
        topics: Vec<String>,
        sender: MessageSender,
    ) -> Self {
        Self {
            networking,
            partition_routing,
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
            restate.subscription.id = %self.sender.subscription.id(),
            messaging.consumer.group.name = consumer_group_id,
            "Starting consumer for topics {:?} with configuration {:?}",
            self.topics, self.client_config
        );

        let (failures_tx, failures_rx) = mpsc::unbounded_channel();

        let rebalance_context = RebalanceContext {
            networking: self.networking.clone(),
            partition_routing: self.partition_routing.clone(),
            task_center_handle: TaskCenter::current(),
            consumer: OnceLock::new(),
            topic_partition_tasks: parking_lot::Mutex::new(HashMap::new()),
            failures_tx,
            sender: self.sender.clone(),
            consumer_group_id,
        };
        let consumer: Arc<MessageConsumer<T>> =
            Arc::new(self.client_config.create_with_context(rebalance_context)?);
        // this OnceLock<Weak> dance is needed because the rebalance callbacks don't get a handle on the consumer,
        // which is strange because practically everything you'd want to do with them involves the consumer.
        _ = consumer.context().consumer.set(Arc::downgrade(&consumer));

        // ensure partitioned tasks are cancelled when this function exits/stops being polled
        let consumer = ConsumerDrop(consumer);

        let topics: Vec<&str> = self.topics.iter().map(|x| &**x).collect();
        consumer.subscribe(&topics)?;

        let mut failures_rx = std::pin::pin!(failures_rx);

        tokio::select! {
            // we have to poll the main consumer for callbacks to be processed, but we expect to only see messages on the partitioned queues
            res = consumer.recv() => {
                match res {
                    // We shouldn't see any messages on the main consumer loop, because we split the queues into partitioned queues before they
                    // are ever assigned. Messages here should be treated as a bug in our assumptions.
                    Ok(msg) => Err(Error::UnexpectedMainQueueMessage(msg.topic().into(), msg.partition())),
                    Err(e) => Err(e.into()),
                }
            }
            // watch for errors in the partitioned consumers - they should only ever abort, not return errors
            Some(err) = failures_rx.recv() => {
                Err(err)
            }
            _ = &mut rx => {
                 Ok(())
            }
        }
    }
}

#[derive(derive_more::Deref)]
struct ConsumerDrop<T>(Arc<MessageConsumer<T>>)
where
    T: TransportConnect;

impl<T> Drop for ConsumerDrop<T>
where
    T: TransportConnect,
{
    fn drop(&mut self) {
        debug!(
            "Stopping consumer with id {}",
            self.context().consumer_group_id
        );

        // we have to clear this because the partitioned tasks themselves hold a reference to MessageConsumer
        self.context().topic_partition_tasks.lock().clear();
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct TopicPartition(String, i32);

impl<'a> From<TopicPartitionListElem<'a>> for TopicPartition {
    fn from(value: TopicPartitionListElem<'a>) -> Self {
        Self(value.topic().into(), value.partition())
    }
}

impl fmt::Display for TopicPartition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.0, self.1)
    }
}

struct RebalanceContext<T>
where
    T: TransportConnect,
{
    networking: Networking<T>,
    partition_routing: PartitionRouting,
    task_center_handle: task_center::Handle,
    consumer: OnceLock<Weak<MessageConsumer<T>>>,
    topic_partition_tasks: parking_lot::Mutex<HashMap<TopicPartition, AbortOnDrop>>,
    failures_tx: mpsc::UnboundedSender<Error>,
    sender: MessageSender,
    consumer_group_id: String,
}

impl<T> ClientContext for RebalanceContext<T>
where
    T: TransportConnect,
{
    fn stats(&self, statistics: Statistics) {
        self.sender.update_consumer_stats(statistics);
    }
}

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
impl<T> ConsumerContext for RebalanceContext<T>
where
    T: TransportConnect,
{
    fn pre_rebalance(&self, _base_consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        let mut topic_partition_tasks = self.topic_partition_tasks.lock();
        let consumer = self
            .consumer
            .get()
            .expect("consumer must have been set in context at rebalance time");

        let Some(consumer) = consumer.upgrade() else {
            // if the consumer has been dropped, we don't need to maintain tasks any more
            return;
        };

        match rebalance {
            Rebalance::Assign(partitions) if partitions.count() > 0 => {
                for partition in partitions.elements() {
                    let partition: TopicPartition = partition.into();

                    if let Some(task_id) = topic_partition_tasks.remove(&partition) {
                        // This probably implies a problem in our assumptions, because librdkafka shouldn't be assigning us a partition again without having revoked it.
                        // However its fair to assume that the existing partitioned consumer is now invalid.
                        warn!(
                            "Kafka informed us of an assigned partition {partition} which we already consider assigned, cancelling the existing partitioned consumer"
                        );
                        drop(task_id);
                    }

                    match consumer.split_partition_queue(&partition.0, partition.1) {
                        Some(queue) => {
                            let task = topic_partition_queue_consumption_loop(
                                self.networking.clone(),
                                self.partition_routing.clone(),
                                self.sender.clone(),
                                partition.clone(),
                                queue,
                                Arc::clone(&consumer),
                                self.consumer_group_id.clone(),
                                self.failures_tx.clone(),
                            );

                            if let Ok(task_handle) = self.task_center_handle.spawn_unmanaged(
                                TaskKind::Ingress,
                                "kafka-partition-ingest",
                                task,
                            ) {
                                topic_partition_tasks.insert(partition, AbortOnDrop(task_handle));
                            } else {
                                // shutting down
                                return;
                            }
                        }
                        None => {
                            warn!(
                                "Invalid partition {partition} given to us in rebalance, ignoring it"
                            );
                            continue;
                        }
                    }
                }
            }
            Rebalance::Revoke(partitions) if partitions.count() > 0 => {
                for partition in partitions.elements() {
                    let partition = partition.into();
                    match topic_partition_tasks.remove(&partition) {
                        Some(task_id) => {
                            debug!(
                                "Stopping partitioned consumer for partition {partition} due to rebalance"
                            );
                            // The partitioned queue will not be polled again.
                            // It might be mid-poll right now, but if so its result will not be sent anywhere.
                            drop(task_id);
                        }
                        None => warn!(
                            "Kafka informed us of a revoked partition {partition} which we had no consumer task for"
                        ),
                    }
                }

                match consumer.commit_consumer_state(CommitMode::Async) {
                    Ok(_) | Err(KafkaError::ConsumerCommit(RDKafkaErrorCode::NoOffset)) => {
                        // Success
                    }
                    Err(error) => warn!("Failed to commit the current consumer state: {error}"),
                }
            }
            // called with empty partitions; important to not call .elements() as this panics apparently.
            // unclear why we are called with no partitions
            Rebalance::Assign(_) | Rebalance::Revoke(_) => {}
            Rebalance::Error(_) => {}
        }
    }
}

struct AbortOnDrop(TaskHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[allow(clippy::too_many_arguments)]
async fn topic_partition_queue_consumption_loop<T: TransportConnect>(
    networking: Networking<T>,
    partition_routing: PartitionRouting,
    sender: MessageSender,
    topic_partition: TopicPartition,
    topic_partition_consumer: StreamPartitionQueue<impl ConsumerContext>,
    consumer: Arc<MessageConsumer<T>>,
    consumer_group_id: String,
    failed: mpsc::UnboundedSender<Error>,
) {
    let producer_id = new_style_dedup_producer_id(
        sender.subscription.id(),
        &consumer_group_id,
        &topic_partition.0,
        topic_partition.1,
    );

    let dedup_offset = query_dedup_offset(networking, partition_routing, producer_id).await;

    debug!(
        topic=%topic_partition.0,
        kafka_partition=%topic_partition.1,
        consumer_group=%consumer_group_id,
        "Forward compatibility dedup offset {dedup_offset:?}"
    );

    debug!(
        restate.subscription.id = %sender.subscription.id(),
        messaging.consumer.group.name = consumer_group_id,
        "Starting topic '{}' partition '{}' consumption loop",
        topic_partition.0,
        topic_partition.1
    );
    // this future will be aborted when the partition is no longer needed, so any exit is a failure
    let err = loop {
        let res = topic_partition_consumer.recv().await;
        let msg = match res {
            Ok(msg) => msg,
            Err(err) => break err.into(),
        };
        let offset = msg.offset();

        if dedup_offset.is_some_and(|dedup_offset| offset as u64 <= dedup_offset) {
            debug!(
                topic=%topic_partition.0,
                kafka_partition=%topic_partition.1,
                offset=%offset,
                consumer_group=%consumer_group_id,
                "skipping kafka message (dedup)"
            );

            if let Err(err) = consumer.store_offset(&topic_partition.0, topic_partition.1, offset) {
                break err.into();
            }

            continue;
        }

        if let Err(err) = sender.send(&consumer_group_id, msg).await {
            break err;
        }

        if let Err(err) = consumer.store_offset(&topic_partition.0, topic_partition.1, offset) {
            break err.into();
        }
    };

    _ = failed.send(err);
}

fn new_style_dedup_producer_id(
    subscription: SubscriptionId,
    consumer_group: &str,
    topic: &str,
    partition: i32,
) -> u128 {
    let mut hasher = xxhash_rust::xxh3::Xxh3::new();

    subscription.hash(&mut hasher);
    '\0'.hash(&mut hasher);
    consumer_group.hash(&mut hasher);
    '\0'.hash(&mut hasher);
    topic.hash(&mut hasher);
    '\0'.hash(&mut hasher);
    partition.hash(&mut hasher);

    hasher.digest128()
}

async fn query_dedup_offset<T: TransportConnect>(
    networking: Networking<T>,
    partition_routing: PartitionRouting,
    producer_id: u128,
) -> Option<u64> {
    // now we need to scatter-gather this producer id to all
    // partitions. Then take the 'max' as the last processed
    // message offset.

    let partition_table = Metadata::with_current(|m| m.partition_table_snapshot());
    let mut fut = FuturesUnordered::new();

    for partition_id in partition_table.iter_ids() {
        fut.push(query_partition_dedup_offset(
            networking.clone(),
            partition_routing.clone(),
            producer_id,
            *partition_id,
            None,
        ));
    }

    let retry_policy = RetryPolicy::exponential(
        Duration::from_millis(100),
        2.0,
        None,
        Some(Duration::from_secs(1)),
    );

    let mut backoff = HashMap::new();

    let mut max_offset = None;
    while let Some((partition_id, result)) = fut.next().await {
        let (err_count, retry_iter) = backoff
            .entry(partition_id)
            .or_insert_with(|| (0, retry_policy.clone().into_iter()));

        let offset = match result {
            Ok(offset) => offset,
            Err(err) => {
                *err_count += 1;
                if *err_count >= 10 {
                    warn!(
                        "Error while looking up latest dedup info for {partition_id}: {err} .. retrying"
                    );
                } else {
                    debug!(
                        "Error while looking up latest dedup info for {partition_id}: {err} .. retrying"
                    );
                }

                fut.push(query_partition_dedup_offset(
                    networking.clone(),
                    partition_routing.clone(),
                    producer_id,
                    partition_id,
                    retry_iter.next(),
                ));

                continue;
            }
        };

        max_offset = max_offset.max(offset)
    }

    max_offset
}

async fn query_partition_dedup_offset<T: TransportConnect>(
    networking: Networking<T>,
    partition_routing: PartitionRouting,
    producer_id: u128,
    partition_id: PartitionId,
    delay: Option<Duration>,
) -> (PartitionId, anyhow::Result<Option<u64>>) {
    if let Some(delay) = delay {
        tokio::time::sleep(delay).await;
    }

    let result = async {
        let node_id = partition_routing
            .get_node_by_partition(partition_id)
            .with_context(|| format!("cannot find node for partition {partition_id}"))?;

        let result = networking
            .call_rpc(
                node_id,
                Swimlane::General,
                DedupSequenceNrQueryRequest {
                    producer_id: ProducerId::Numeric(producer_id),
                },
                Some(partition_id.into()),
                None,
            )
            .await;

        let result = match result {
            Ok(result) => result,
            Err(RpcError::Receive(RpcReplyError::MessageUnrecognized)) => {
                // MessageUnrecognized indicates a mixed v1.5/v1.6 deployment where some nodes lack the query endpoint.
                // Returning None is fine because legacy ingestion remains enabled by default.
                // Once the cluster finishes rolling to v1.6 or higher, the endpoint should exist and this branch should not trigger.
                return Ok(None);
            }
            Err(err) => anyhow::bail!(err),
        };

        match result.status {
            ResponseStatus::Ack => Ok(result.sequence_number),
            status => anyhow::bail!(
                "failed to query latest dedup sequence number for partition \
            {partition_id} and producer '{producer_id}' on node {node_id}: {status:?}"
            ),
        }
    }
    .await;

    (partition_id, result)
}
