// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::{Arc, OnceLock, Weak};

use crate::Error;
use crate::builder::EnvelopeBuilder;
use crate::metric_definitions::{KAFKA_INGRESS_CONSUMER_LAG, KAFKA_INGRESS_REQUESTS};
use futures::future::OptionFuture;
use metrics::{counter, gauge};
use rdkafka::consumer::stream_consumer::StreamPartitionQueue;
use rdkafka::consumer::{
    BaseConsumer, CommitMode, Consumer, ConsumerContext, Rebalance, StreamConsumer,
};
use rdkafka::error::KafkaError;
use rdkafka::topic_partition_list::TopicPartitionListElem;
use rdkafka::types::RDKafkaErrorCode;
use rdkafka::{ClientConfig, ClientContext, Message, Statistics};
use restate_core::network::TransportConnect;
use restate_core::{TaskCenter, TaskHandle, TaskKind, task_center};
use restate_ingress_client::{CommitError, IngestionError, IngressClient, RecordCommit};
use restate_types::identifiers::WithPartitionKey;
use restate_types::logs::HasRecordKeys;
use restate_types::net::ingress::IngestRecord;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info, trace, warn};

impl From<IngestionError> for Error {
    fn from(value: IngestionError) -> Self {
        match value {
            IngestionError::Closed => Self::IngressClosed,
            IngestionError::PartitionTableError(err) => Self::PartitionTableError(err),
        }
    }
}
type MessageConsumer<T> = StreamConsumer<RebalanceContext<T>>;

#[derive(Clone)]
pub struct ConsumerTask<T> {
    client_config: ClientConfig,
    topics: Vec<String>,
    ingress: IngressClient<T>,
    builder: EnvelopeBuilder,
}

impl<T> ConsumerTask<T>
where
    T: TransportConnect,
{
    pub fn new(
        client_config: ClientConfig,
        topics: Vec<String>,
        ingress: IngressClient<T>,
        builder: EnvelopeBuilder,
    ) -> Self {
        Self {
            client_config,
            topics,
            ingress,
            builder,
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
            restate.subscription.id = %self.builder.subscription().id(),
            messaging.consumer.group.name = consumer_group_id,
            "Starting consumer for topics {:?} with configuration {:?}",
            self.topics, self.client_config
        );

        let (failures_tx, failures_rx) = mpsc::unbounded_channel();

        let rebalance_context = RebalanceContext {
            task_center_handle: TaskCenter::current(),
            consumer: OnceLock::new(),
            topic_partition_tasks: parking_lot::Mutex::new(HashMap::new()),
            failures_tx,
            ingress: self.ingress.clone(),
            builder: self.builder.clone(),
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
struct ConsumerDrop<T: TransportConnect>(Arc<MessageConsumer<T>>);

impl<T: TransportConnect> Drop for ConsumerDrop<T> {
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

struct RebalanceContext<T: TransportConnect> {
    task_center_handle: task_center::Handle,
    consumer: OnceLock<Weak<MessageConsumer<T>>>,
    topic_partition_tasks: parking_lot::Mutex<HashMap<TopicPartition, AbortOnDrop>>,
    failures_tx: mpsc::UnboundedSender<Error>,
    ingress: IngressClient<T>,
    builder: EnvelopeBuilder,
    consumer_group_id: String,
}

impl<T> ClientContext for RebalanceContext<T>
where
    T: TransportConnect,
{
    fn stats(&self, statistics: Statistics) {
        for topic in statistics.topics {
            for partition in topic.1.partitions {
                let lag = partition.1.consumer_lag as f64;
                gauge!(
                    KAFKA_INGRESS_CONSUMER_LAG,
                     "subscription" => self.builder.subscription().id().to_string(),
                     "topic" => topic.0.to_string(),
                     "partition" =>  partition.0.to_string()
                )
                .set(lag);
            }
        }
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
                    info!("Assigned kafka partition: {partition}");
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
                            let task = TopicConsumptionTask::new(
                                self.ingress.clone(),
                                self.builder.clone(),
                                partition.clone(),
                                queue,
                                Arc::clone(&consumer),
                                self.consumer_group_id.clone(),
                                self.failures_tx.clone(),
                            );

                            if let Ok(task_handle) = self.task_center_handle.spawn_unmanaged(
                                TaskKind::Ingress,
                                "kafka-partition-ingest",
                                task.run(),
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
                    info!("Revoked kafka partition: {partition}");
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

struct TopicConsumptionTask<T, C>
where
    T: TransportConnect,
    C: ConsumerContext,
{
    ingress: IngressClient<T>,
    builder: EnvelopeBuilder,
    topic_partition: TopicPartition,
    topic_partition_consumer: StreamPartitionQueue<C>,
    consumer: Arc<MessageConsumer<T>>,
    consumer_group_id: String,
    failed: mpsc::UnboundedSender<Error>,
}

impl<T, C> TopicConsumptionTask<T, C>
where
    T: TransportConnect,
    C: ConsumerContext,
{
    fn new(
        ingress: IngressClient<T>,
        builder: EnvelopeBuilder,
        topic_partition: TopicPartition,
        topic_partition_consumer: StreamPartitionQueue<C>,
        consumer: Arc<MessageConsumer<T>>,
        consumer_group_id: String,
        failed: mpsc::UnboundedSender<Error>,
    ) -> Self {
        Self {
            ingress,
            builder,
            topic_partition,
            topic_partition_consumer,
            consumer,
            consumer_group_id,
            failed,
        }
    }

    async fn run(mut self) {
        // this future will be aborted when the partition is no longer needed, so any exit is a failure
        if let Err(err) = self.inner().await {
            _ = self.failed.send(err);
        }
    }

    async fn inner(&mut self) -> Result<(), Error> {
        debug!(
            restate.subscription.id = %self.builder.subscription().id(),
            messaging.consumer.group.name = self.consumer_group_id,
            "Starting topic '{}' partition '{}' consumption loop",
            self.topic_partition.0,
            self.topic_partition.1
        );

        let producer_id = dedup_producer_id(
            &self.consumer_group_id,
            &self.topic_partition.0,
            self.topic_partition.1,
        );

        let ingress_request_counter = counter!(
            KAFKA_INGRESS_REQUESTS,
            "subscription" => self.builder.subscription().id().to_string(),
            "topic" => self.topic_partition.0.to_string(),
            "partition" => self.topic_partition.1.to_string(),
        );

        let mut inflight = VecDeque::new();

        loop {
            tokio::select! {
                biased;
                Some(committed) = Self::head_committed(&mut inflight) => {
                    let head = inflight.pop_front().expect("to exist");
                    if let Err(CommitError::Cancelled) = committed {
                        return Err(Error::IngressClosed);
                    }
                    ingress_request_counter.increment(1);
                    trace!(
                        topic=%self.topic_partition.0, kafka_partition=%self.topic_partition.1, offset=%head.offset,
                        "store kafka offset",
                    );
                    self.consumer.store_offset(&self.topic_partition.0, self.topic_partition.1, head.offset)?;
                },
                received = self.topic_partition_consumer.recv() => {
                    let msg = received?;

                    let offset = msg.offset();
                    // to check: if this blocks because there are no more permits
                    let envelope = self.builder.build(producer_id, &self.consumer_group_id, msg)?;

                    // get a permit but also
                    // continue process committed records.
                    let permit = loop {
                        tokio::select! {
                            biased;
                            Some(committed) = Self::head_committed(&mut inflight) => {
                                let head = inflight.pop_front().expect("to exist");
                                if let Err(CommitError::Cancelled) = committed {
                                    return Err(Error::IngressClosed);
                                }
                                ingress_request_counter.increment(1);
                                trace!(
                                    topic=%self.topic_partition.0, kafka_partition=%self.topic_partition.1, offset=%head.offset,
                                    "store kafka offset",
                                );
                                self.consumer.store_offset(&self.topic_partition.0, self.topic_partition.1, head.offset)?;
                            },
                            perm = self.ingress.reserve() => {
                                break perm?;
                            }
                        }
                    };

                    let token = permit.ingest(envelope.partition_key(),
                        IngestRecord::from_parts(envelope.record_keys(), envelope)
                    )?;
                    inflight.push_back(InflightMessage{offset, token});
                }
            }
        }
    }

    #[inline]
    pub fn head_committed(
        inflight: &mut VecDeque<InflightMessage>,
    ) -> OptionFuture<&mut RecordCommit> {
        OptionFuture::from(
            inflight
                .front_mut()
                .map(|i: &mut InflightMessage| &mut i.token),
        )
    }
}

fn dedup_producer_id(consumer_group: &str, topic: &str, partition: i32) -> u128 {
    use std::io::Write;

    let mut hasher = xxhash_rust::xxh3::Xxh3::new();
    // todo(azmy): add cluster name to the hash?
    write!(hasher, "{consumer_group}:{topic}:{partition}").unwrap();

    hasher.digest128()
}

struct InflightMessage {
    offset: i64,
    token: RecordCommit,
}
