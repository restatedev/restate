// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::{Arc, OnceLock, Weak};
use std::time::Duration;

use anyhow::Context;
use futures::StreamExt;
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

use restate_core::network::{NetworkSender, Swimlane, TransportConnect};
use restate_core::{Metadata, TaskCenter, TaskHandle, TaskKind, task_center};
use restate_ingestion_client::{IngestionClient, IngestionError, RecordCommit};
use restate_types::identifiers::partitioner::HashPartitioner;
use restate_types::identifiers::{SubscriptionId, WithPartitionKey};
use restate_types::net::ingest::{DedupSequenceNrQueryRequest, ProducerId, ResponseStatus};
use restate_types::partitions::FindPartition;
use restate_types::retries::RetryPolicy;
use restate_types::schema::subscriptions::{EventInvocationTargetTemplate, Sink};
use restate_wal_protocol::Envelope;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, instrument, trace, warn};

use crate::Error;
use crate::builder::EnvelopeBuilder;
use crate::metric_definitions::{KAFKA_INGRESS_CONSUMER_LAG, KAFKA_INGRESS_REQUESTS};

/// The concrete Kafka consumer type for a given transport `T` and OAUTHBEARER
/// policy `O`. The policy is baked into the type because librdkafka reads
/// [`ClientContext::ENABLE_REFRESH_OAUTH_TOKEN`] — an associated *const* — to
/// decide whether to invoke our custom token callback, so it must be fixed at
/// compile time for each context instantiation.
type MessageConsumer<T, O> = StreamConsumer<RebalanceContext<T, O>>;

#[derive(Clone)]
pub struct ConsumerTask<T> {
    client_config: ClientConfig,
    topics: Vec<String>,
    ingestion: IngestionClient<T, Envelope>,
    builder: EnvelopeBuilder,
}

impl<T> ConsumerTask<T>
where
    T: TransportConnect,
{
    pub fn new(
        client_config: ClientConfig,
        topics: Vec<String>,
        ingestion: IngestionClient<T, Envelope>,
        builder: EnvelopeBuilder,
    ) -> Self {
        Self {
            client_config,
            topics,
            ingestion,
            builder,
        }
    }

    pub async fn run(self, rx: oneshot::Receiver<()>) -> Result<(), Error> {
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

        // Decide which OAUTHBEARER policy this subscription needs, based on the
        // `sasl.oauthbearer.config`. If it selects the MSK IAM provider we install
        // Restate's custom token callback (`MskOAuth`); otherwise we leave
        // librdkafka's built-in OAUTHBEARER handling untouched (`PlainOAuth`),
        // so Confluent OIDC and friends keep working. The policy is baked into
        // the consumer type because `ClientContext::ENABLE_REFRESH_OAUTH_TOKEN`
        // is an associated const, hence the branch on distinct concrete types.
        let oauthbearer_config = self.client_config.get("sasl.oauthbearer.config");
        let kind = if crate::oauth::is_msk_iam_config(oauthbearer_config.as_deref()) {
            ConsumerKind::MskIam
        } else {
            ConsumerKind::Plain
        };
        debug!(
            restate.subscription.id = %self.builder.subscription().id(),
            "Selected {kind:?} OAUTHBEARER policy for consumer"
        );

        match kind {
            ConsumerKind::MskIam => {
                self.run_with_context::<MskOAuth>(consumer_group_id, rx)
                    .await
            }
            ConsumerKind::Plain => {
                self.run_with_context::<PlainOAuth>(consumer_group_id, rx)
                    .await
            }
        }
    }

    /// Create a consumer with the OAUTHBEARER policy `O`, subscribe, and run the
    /// main poll/select loop. Shared by both `ConsumerKind` branches so the loop
    /// logic is written once and monomorphised per policy.
    async fn run_with_context<O: OAuthMode>(
        self,
        consumer_group_id: String,
        mut rx: oneshot::Receiver<()>,
    ) -> Result<(), Error> {
        let (failures_tx, failures_rx) = mpsc::unbounded_channel();

        let rebalance_context = RebalanceContext {
            task_center_handle: TaskCenter::current(),
            consumer: OnceLock::new(),
            topic_partition_tasks: parking_lot::Mutex::new(HashMap::new()),
            failures_tx,
            ingestion: self.ingestion.clone(),
            builder: self.builder.clone(),
            consumer_group_id,
            _oauth: PhantomData,
        };
        let consumer: Arc<MessageConsumer<T, O>> =
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

/// Runtime selection of the OAUTHBEARER policy for a consumer. Chosen from the
/// subscription's `sasl.oauthbearer.config` and mapped to a concrete
/// [`OAuthMode`] marker (`MskOAuth` / `PlainOAuth`) in [`ConsumerTask::run`].
#[derive(Debug, Clone, Copy)]
enum ConsumerKind {
    /// AWS MSK IAM — install Restate's custom OAUTHBEARER token callback.
    MskIam,
    /// Everything else — leave librdkafka's built-in OAUTHBEARER handling in place.
    Plain,
}

#[derive(derive_more::Deref)]
struct ConsumerDrop<T: TransportConnect, O: OAuthMode>(Arc<MessageConsumer<T, O>>);

impl<T: TransportConnect, O: OAuthMode> Drop for ConsumerDrop<T, O> {
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

/// OAUTHBEARER policy for a Kafka consumer, selected at runtime per subscription.
/// A single generic [`ClientContext`] impl reads these off the policy, so
/// [`MskOAuth`] and [`PlainOAuth`] yield two distinct `ClientContext` behaviours.
trait OAuthMode: Send + Sync + 'static {
    /// `true` makes librdkafka install our token-refresh callback; `false` leaves
    /// its built-in OAUTHBEARER handling in place (e.g. Confluent OIDC).
    const ENABLE_REFRESH_OAUTH_TOKEN: bool;

    /// Only invoked by librdkafka when `ENABLE_REFRESH_OAUTH_TOKEN` is `true`, so
    /// pass-through policies inherit this never-called default.
    fn generate_oauth_token(
        _oauthbearer_config: Option<&str>,
    ) -> Result<rdkafka::client::OAuthToken, Box<dyn std::error::Error + 'static>> {
        Err("OAUTHBEARER token callback invoked for a pass-through OAuth policy".into())
    }
}

/// AWS MSK IAM OAUTHBEARER policy — installs Restate's custom token callback.
struct MskOAuth;
impl OAuthMode for MskOAuth {
    const ENABLE_REFRESH_OAUTH_TOKEN: bool = true;

    fn generate_oauth_token(
        oauthbearer_config: Option<&str>,
    ) -> Result<rdkafka::client::OAuthToken, Box<dyn std::error::Error + 'static>> {
        crate::oauth::generate_oauth_token(oauthbearer_config)
    }
}

/// Pass-through OAUTHBEARER policy — leaves librdkafka's built-in handling in place.
struct PlainOAuth;
impl OAuthMode for PlainOAuth {
    const ENABLE_REFRESH_OAUTH_TOKEN: bool = false;
}

struct RebalanceContext<T: TransportConnect, O: OAuthMode> {
    task_center_handle: task_center::Handle,
    consumer: OnceLock<Weak<MessageConsumer<T, O>>>,
    topic_partition_tasks: parking_lot::Mutex<HashMap<TopicPartition, AbortOnDrop>>,
    failures_tx: mpsc::UnboundedSender<Error>,
    ingestion: IngestionClient<T, Envelope>,
    builder: EnvelopeBuilder,
    consumer_group_id: String,
    _oauth: PhantomData<fn() -> O>,
}

impl<T, O> RebalanceContext<T, O>
where
    T: TransportConnect,
    O: OAuthMode,
{
    fn report_stats(&self, statistics: Statistics) {
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

// Must be a single generic impl, not two concrete ones: `RebalanceContext` holds a
// `Weak<StreamConsumer<Self>>` and `StreamConsumer<C>` requires `C: ConsumerContext`
// (hence `C: ClientContext`), so `ClientContext` must be provable for every `O: OAuthMode`.
impl<T, O> ClientContext for RebalanceContext<T, O>
where
    T: TransportConnect,
    O: OAuthMode,
{
    const ENABLE_REFRESH_OAUTH_TOKEN: bool = O::ENABLE_REFRESH_OAUTH_TOKEN;

    fn stats(&self, statistics: Statistics) {
        self.report_stats(statistics);
    }

    fn generate_oauth_token(
        &self,
        oauthbearer_config: Option<&str>,
    ) -> Result<rdkafka::client::OAuthToken, Box<dyn std::error::Error + 'static>> {
        O::generate_oauth_token(oauthbearer_config)
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
impl<T, O> ConsumerContext for RebalanceContext<T, O>
where
    T: TransportConnect,
    O: OAuthMode,
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
                    debug!(
                        subscription = %self.builder.subscription().id(),
                        topic = %partition.0,
                        partition = %partition.1,
                        "Assigned kafka partition"
                    );

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
                            let task = TopicPartitionConsumptionTask::new(
                                self.ingestion.clone(),
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
                    let partition: TopicPartition = partition.into();
                    debug!(
                        subscription = %self.builder.subscription().id(),
                        topic = %partition.0,
                        partition = %partition.1,
                        "Revoked kafka partition"
                    );

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

struct TopicPartitionConsumptionTask<T, O>
where
    T: TransportConnect,
    O: OAuthMode,
{
    ingestion: IngestionClient<T, Envelope>,
    builder: EnvelopeBuilder,
    topic_partition: TopicPartition,
    topic_partition_consumer: StreamPartitionQueue<RebalanceContext<T, O>>,
    consumer: Arc<MessageConsumer<T, O>>,
    consumer_group_id: String,
    failed: mpsc::UnboundedSender<Error>,
}

impl<T, O> TopicPartitionConsumptionTask<T, O>
where
    T: TransportConnect,
    O: OAuthMode,
{
    fn new(
        ingestion: IngestionClient<T, Envelope>,
        builder: EnvelopeBuilder,
        topic_partition: TopicPartition,
        topic_partition_consumer: StreamPartitionQueue<RebalanceContext<T, O>>,
        consumer: Arc<MessageConsumer<T, O>>,
        consumer_group_id: String,
        failed: mpsc::UnboundedSender<Error>,
    ) -> Self {
        Self {
            ingestion,
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
        let err = match self.run_inner().await {
            Err(err) => err,
            Ok(_) => Error::UnexpectedConsumptionTaskExited {
                subscription: self.builder.subscription().id().to_string(),
                topic: self.topic_partition.0,
                partition: self.topic_partition.1,
            },
        };

        _ = self.failed.send(err);
    }

    /// query the legacy dedup information for this consumption task.
    async fn legacy_dedup_offset(&self) -> Option<u64> {
        if !matches!(
            self.builder.subscription().sink(),
            Sink::Invocation {
                event_invocation_target_template: EventInvocationTargetTemplate::Service { .. }
            }
        ) {
            // legacy dedup is only valid for services which used to
            // be proxied
            return None;
        }

        #[derive(Hash)]
        pub struct LegacyKafkaDeduplicationId<'a> {
            consumer_group: &'a str,
            topic: &'a str,
            partition: i32,
        }

        // producer id constructed as {consumer-group}-{topic}-{kafka-partition}
        let legacy_producer_id = format!(
            "{}-{}-{}",
            self.consumer_group_id, self.topic_partition.0, self.topic_partition.1
        );

        let proxy_partition_key =
            HashPartitioner::compute_partition_key(&LegacyKafkaDeduplicationId {
                consumer_group: &self.consumer_group_id,
                topic: &self.topic_partition.0,
                partition: self.topic_partition.1,
            });

        RetryPolicy::exponential(
            Duration::from_millis(50),
            2.0,
            None,
            Some(Duration::from_secs(1)),
        )
        .retry_with_inspect(
            || async {
                let partition_id = Metadata::with_current(|m| {
                    m.partition_table_ref()
                        .find_partition_id(proxy_partition_key)
                })?;

                let node_id = self
                    .ingestion
                    .partition_routing()
                    .get_node_by_partition(partition_id)
                    .with_context(|| {
                        format!("cannot lookup node id for partition id {partition_id}")
                    })?;

                // we use long timeout of 5 seconds in case partition processor is catching up
                let response = self
                    .ingestion
                    .networking()
                    .call_rpc(
                        node_id,
                        Swimlane::General,
                        DedupSequenceNrQueryRequest {
                            producer_id: ProducerId::String(legacy_producer_id.clone()),
                        },
                        Some(partition_id.into()),
                        Some(Duration::from_secs(5)),
                    )
                    .await
                    .with_context(|| {
                        format!(
                            "failed to query legacy dedup \
                            sequence number for producer '{legacy_producer_id}' \
                            from node {node_id}"
                        )
                    })?;

                match response.status {
                    ResponseStatus::Ack => Ok(response.sequence_number),
                    status => Err(anyhow::anyhow!(
                        "failed to query latest dedup \
                        sequence number from node {node_id} for '{legacy_producer_id}': {status:?}"
                    )),
                }
            },
            |attempts, err| {
                if attempts >= 10 {
                    warn!("Failed to query legacy dedup information: {err:#} .. retrying");
                } else {
                    debug!("Failed to query legacy dedup information: {err:#} .. retrying");
                }
            },
        )
        .await
        .expect("tries forever")
    }

    #[instrument(skip(self), fields(
        restate.subscription.id = %self.builder.subscription().id(),
        topic=%self.topic_partition.0,
        kafka_partition=%self.topic_partition.1,
        consumer_group=%self.consumer_group_id)
    )]
    async fn run_inner(&mut self) -> Result<(), Error> {
        debug!("Starting topic consumption loop");

        let legacy_dedup_offset = self.legacy_dedup_offset().await;
        debug!("Legacy dedup offset: {legacy_dedup_offset:?}",);

        let producer_id = dedup_producer_id(
            &self.builder.subscription().id(),
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

        let mut consumer_stream = self.topic_partition_consumer.stream();
        loop {
            tokio::select! {
                biased;
                Some(committed) = Self::head_committed(&mut inflight) => {
                    _ = inflight.pop_front().expect("to exist");
                    let offset = committed.map_err(|_| Error::IngestionError(IngestionError::Closed("commit cancelled")))?;

                    ingress_request_counter.increment(1);
                    trace!(
                        offset=%offset,
                        "Store kafka offset",
                    );

                    self.consumer.store_offset(&self.topic_partition.0, self.topic_partition.1, offset)?;
                },
                Some(received) = consumer_stream.next() => {
                    let msg = received?;
                    let offset = msg.offset();
                    if legacy_dedup_offset.is_some_and(|dedup_offset| offset as u64 <= dedup_offset) {
                        // skip duplicated messages. Any gap should be small.
                        debug!(
                            offset=%offset,
                            "Skipping kafka message (dedup)"
                        );
                        self.consumer.store_offset(&self.topic_partition.0, self.topic_partition.1, offset)?;
                        continue;
                    }

                    trace!(
                        offset=%offset,
                        "Ingesting kafka message"
                    );

                    let envelope = self.builder.build(producer_id, &self.consumer_group_id, msg)?;

                    let commit_token = self
                        .ingestion
                        .ingest(envelope.partition_key(), envelope)
                        .await?
                        .map(|_| offset);

                    inflight.push_back(commit_token);
                }
            }
        }
    }

    #[inline]
    pub fn head_committed(
        inflight: &mut VecDeque<RecordCommit<i64>>,
    ) -> OptionFuture<&mut RecordCommit<i64>> {
        OptionFuture::from(inflight.front_mut())
    }
}

// Do not change. Changing this hasher will create new producer-id which can
// cause duplicates
fn dedup_producer_id(
    subscription: &SubscriptionId,
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
