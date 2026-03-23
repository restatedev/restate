// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use anyhow::Context;
use restate_wal_protocol::Envelope;
use tokio::sync::mpsc;
use tracing::warn;

use restate_core::cancellation_watcher;
use restate_core::network::TransportConnect;
use restate_ingestion_client::IngestionClient;
use restate_types::identifiers::SubscriptionId;
use restate_types::live::Live;
use restate_types::retries::RetryPolicy;
use restate_types::schema::Schema;
use restate_types::schema::kafka::KafkaCluster;
use restate_types::schema::subscriptions::{Source, Subscription};

use super::*;
use crate::builder::EnvelopeBuilder;
use crate::subscription_controller::task_orchestrator::TaskOrchestrator;

// For simplicity of the current implementation, this currently lives in this module
// In future versions, we should either pull this out in a separate process, or generify it and move it to the worker, or an ad-hoc module
pub struct Service<T> {
    ingestion: IngestionClient<T, Envelope>,
    schema: Live<Schema>,

    commands_tx: SubscriptionCommandSender,
    commands_rx: SubscriptionCommandReceiver,
}

impl<T> Service<T>
where
    T: TransportConnect,
{
    pub fn new(ingestion: IngestionClient<T, Envelope>, schema: Live<Schema>) -> Self {
        metric_definitions::describe_metrics();
        let (commands_tx, commands_rx) = mpsc::channel(10);

        Service {
            ingestion,
            schema,
            commands_tx,
            commands_rx,
        }
    }

    pub fn create_command_sender(&self) -> SubscriptionCommandSender {
        self.commands_tx.clone()
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        let shutdown = cancellation_watcher();
        tokio::pin!(shutdown);

        let mut task_orchestrator = TaskOrchestrator::new(RetryPolicy::exponential(
            Duration::from_millis(200),
            2.0,
            None,
            Some(Duration::from_secs(10)),
        ));

        loop {
            tokio::select! {
                Some(cmd) = self.commands_rx.recv() => {
                    match cmd {
                        Command::UpdateSubscriptions(kafka_clusters, subscriptions) => if let Err(e) = self.handle_update_subscriptions(kafka_clusters, subscriptions, &mut task_orchestrator) {
                            warn!("Error when updating subscriptions: {e:?}");
                            break;
                        },
                    }
                }
                _ = task_orchestrator.poll(), if !task_orchestrator.is_empty() => {},
                _ = &mut shutdown => {
                    break;
                }
            }
        }

        // Wait for consumers to shutdown
        task_orchestrator.shutdown().await;
        Ok(())
    }

    fn handle_start_subscription(
        &mut self,
        kafka_cluster: KafkaCluster,
        subscription: Subscription,
        task_orchestrator: &mut TaskOrchestrator<T>,
    ) -> anyhow::Result<()> {
        let mut client_config = rdkafka::ClientConfig::new();
        // enabling probing for the ca certificates if the user does not specify anything else
        client_config.set("https.ca.location", "probe");

        let Source::Kafka { topic, .. } = subscription.source();

        // Copy cluster options and subscription metadata into client_config
        let cluster_properties = kafka_cluster.properties.clone();

        for (k, v) in cluster_properties.clone() {
            client_config.set(k, v);
        }
        for (k, v) in subscription.metadata() {
            client_config.set(k, v);
        }

        // Options required by the business logic of our consumer,
        // see ConsumerTask::run
        client_config.set("enable.auto.commit", "true");
        client_config.set("enable.auto.offset.store", "false");

        let subscription_id = subscription.id();

        // Create the consumer task
        let consumer_task = consumer_task::ConsumerTask::new(
            client_config,
            vec![topic.to_string()],
            self.ingestion.clone(),
            EnvelopeBuilder::new(subscription.clone(), self.schema.clone()),
        );

        task_orchestrator.start(subscription_id, consumer_task, kafka_cluster, subscription);

        Ok(())
    }

    fn handle_stop_subscription(
        &mut self,
        subscription_id: SubscriptionId,
        task_orchestrator: &mut TaskOrchestrator<T>,
    ) {
        task_orchestrator.stop(subscription_id);
    }

    fn handle_update_subscriptions(
        &mut self,
        kafka_clusters: Vec<KafkaCluster>,
        subscriptions: Vec<Subscription>,
        task_orchestrator: &mut TaskOrchestrator<T>,
    ) -> anyhow::Result<()> {
        // Build a map from cluster name to KafkaCluster for quick lookup
        let cluster_map: HashMap<&str, &KafkaCluster> = kafka_clusters
            .iter()
            .map(|cluster| (cluster.name(), cluster))
            .collect();

        // Track which running subscriptions we've seen in the new configuration
        let mut running_subscriptions: HashSet<_> =
            task_orchestrator.running_subscriptions().cloned().collect();

        for subscription in subscriptions {
            let subscription_id = subscription.id();

            // Find the KafkaCluster for this subscription
            let Source::Kafka { cluster, .. } = subscription.source();
            let kafka_cluster = cluster_map.get(cluster.as_str()).cloned().with_context(|| {
                format!(
                    "KafkaCluster '{}' not found for subscription {}. This might happen if you registered a subscription with a cluster name, but this cluster is not available anymore in the configuration. Configured Kafka clusters: {:?}",
                    cluster, subscription_id, cluster_map.keys().collect::<Vec<_>>()
                )
            })?;

            if let Some((running_cluster, running_subscription)) =
                task_orchestrator.get_running_config(&subscription_id)
            {
                // Subscription is already running - check if configuration changed
                let config_changed = running_subscription != &subscription
                    || running_cluster.properties != kafka_cluster.properties;

                if config_changed {
                    // Configuration changed -> restart the subscription
                    self.handle_stop_subscription(subscription_id, task_orchestrator);
                    self.handle_start_subscription(
                        kafka_cluster.clone(),
                        subscription,
                        task_orchestrator,
                    )?;
                }
                // We're good with this subscription
                running_subscriptions.remove(&subscription_id);
            } else {
                // New subscription -> start it
                self.handle_start_subscription(
                    kafka_cluster.clone(),
                    subscription,
                    task_orchestrator,
                )?;
            }
        }

        // Stop any subscriptions that are no longer in the configuration
        for subscription_id in running_subscriptions {
            self.handle_stop_subscription(subscription_id, task_orchestrator);
        }

        Ok(())
    }
}

mod task_orchestrator {
    use crate::consumer_task;
    use restate_core::network::TransportConnect;
    use restate_core::{TaskCenterFutureExt, TaskKind};
    use restate_timer_queue::TimerQueue;
    use restate_types::identifiers::SubscriptionId;
    use restate_types::retries::{RetryIter, RetryPolicy};
    use restate_types::schema::kafka::KafkaCluster;
    use restate_types::schema::subscriptions::Subscription;
    use std::collections::HashMap;
    use std::time::SystemTime;
    use tokio::sync::oneshot;
    use tokio::task;
    use tokio::task::{JoinError, JoinSet};
    use tracing::{debug, warn};

    struct TaskState<T> {
        // We use this to restart the consumer task in case of a failure
        consumer_task_clone: consumer_task::ConsumerTask<T>,
        task_state_inner: TaskStateInner,
        retry_iter: RetryIter<'static>,
        // Store the KafkaCluster and Subscription to detect configuration changes
        kafka_cluster: KafkaCluster,
        subscription: Subscription,
    }

    enum TaskStateInner {
        Running {
            task_id: task::Id,
            _close_ch: oneshot::Sender<()>,
        },
        WaitingRetryTimer,
    }

    pub(super) struct TaskOrchestrator<T> {
        retry_policy: RetryPolicy,
        running_tasks_to_subscriptions: HashMap<task::Id, SubscriptionId>,
        subscription_id_to_task_state: HashMap<SubscriptionId, TaskState<T>>,
        tasks: JoinSet<Result<(), crate::Error>>,
        timer_queue: TimerQueue<SubscriptionId>,
    }

    impl<T> TaskOrchestrator<T>
    where
        T: TransportConnect,
    {
        pub(super) fn new(retry_policy: RetryPolicy) -> Self {
            Self {
                retry_policy,
                running_tasks_to_subscriptions: HashMap::default(),
                subscription_id_to_task_state: HashMap::default(),
                tasks: JoinSet::default(),
                timer_queue: TimerQueue::default(),
            }
        }

        pub(super) async fn poll(&mut self) {
            tokio::select! {
                Some(res) = self.tasks.join_next_with_id(), if !self.tasks.is_empty() => {
                    self.handle_task_closed(res);
                },
                timer = self.timer_queue.await_timer(), if !self.timer_queue.is_empty() => {
                    self.handle_timer_fired(timer.into_inner());
                }
            }
        }

        pub(super) fn is_empty(&self) -> bool {
            self.tasks.is_empty() && self.timer_queue.is_empty()
        }

        fn handle_task_closed(
            &mut self,
            result: Result<(task::Id, Result<(), crate::Error>), JoinError>,
        ) {
            let task_id = match result {
                Ok((id, _)) => id,
                Err(ref err) => err.id(),
            };

            let subscription_id = if let Some(subscription_id) =
                self.running_tasks_to_subscriptions.remove(&task_id)
            {
                subscription_id
            } else {
                match result {
                    Ok((_, Ok(_))) => {} // the normal case; a removed subscription should exit cleanly
                    Ok((_, Err(e))) => {
                        warn!(
                            "Consumer task for removed subscription unexpectedly returned error: {e}"
                        );
                    }
                    Err(e) => {
                        warn!("Consumer task for removed subscription unexpectedly panicked: {e}");
                    }
                }
                // no need to retry a subscription we don't care about any more
                return;
            };

            match result {
                Ok((_, Ok(_))) => {
                    warn!("Consumer task for subscription {subscription_id} unexpectedly closed");
                }
                Ok((_, Err(e))) => {
                    warn!(
                        "Consumer task for subscription {subscription_id} unexpectedly returned error: {e}"
                    );
                }
                Err(e) => {
                    warn!(
                        "Consumer task for subscription {subscription_id} unexpectedly panicked: {e}"
                    );
                }
            };

            let task_state = self
                .subscription_id_to_task_state
                .get_mut(&subscription_id)
                .expect("There must be a task state to start the retry timer");
            task_state.task_state_inner = TaskStateInner::WaitingRetryTimer;
            if let Some(next_timer) = task_state.retry_iter.next() {
                self.timer_queue
                    .sleep_until(SystemTime::now() + next_timer, subscription_id);
            } else {
                warn!(
                    "Not going to retry consumer task for subscription {subscription_id} because retry limit exhausted."
                );
                self.subscription_id_to_task_state.remove(&subscription_id);
            }
        }

        fn handle_timer_fired(&mut self, subscription_id: SubscriptionId) {
            match self.subscription_id_to_task_state.get(&subscription_id) {
                Some(TaskState {
                    task_state_inner: TaskStateInner::Running { .. },
                    ..
                }) => {
                    // Timer fired for a subscription task that is already running
                    return;
                }
                None => {
                    // Timer fired for a subscription that was removed
                    return;
                }
                _ => {}
            };

            let TaskState {
                consumer_task_clone,
                kafka_cluster,
                subscription,
                ..
            } = self
                .subscription_id_to_task_state
                .remove(&subscription_id)
                .expect("Checked in the previous match statement");
            self.start(
                subscription_id,
                consumer_task_clone,
                kafka_cluster,
                subscription,
            );
        }

        pub(super) fn start(
            &mut self,
            subscription_id: SubscriptionId,
            consumer_task_clone: consumer_task::ConsumerTask<T>,
            kafka_cluster: KafkaCluster,
            subscription: Subscription,
        ) {
            // Shutdown old task, if any
            if let Some(task_state) = self.subscription_id_to_task_state.remove(&subscription_id) {
                // Shutdown the old task
                if let TaskStateInner::Running { task_id, .. } = task_state.task_state_inner {
                    self.running_tasks_to_subscriptions.remove(&task_id);
                }
            }

            // Prepare shutdown channel
            let (tx, rx) = oneshot::channel();

            debug!(
                "Spawning the consumer task for subscription id {}",
                subscription_id
            );
            let task_id = self
                .tasks
                .build_task()
                .name("kafka-consumer")
                .spawn({
                    let consumer_task_clone = consumer_task_clone.clone();
                    consumer_task_clone
                        .run(rx)
                        .in_current_tc_as_task(TaskKind::Kafka, "kafka-consumer-task")
                })
                .expect("to spawn kafka consumer task")
                .id();

            self.running_tasks_to_subscriptions
                .insert(task_id, subscription_id);
            self.subscription_id_to_task_state.insert(
                subscription_id,
                TaskState {
                    consumer_task_clone: consumer_task_clone.clone(),
                    task_state_inner: TaskStateInner::Running {
                        task_id,
                        _close_ch: tx,
                    },
                    retry_iter: self.retry_policy.clone().into_iter(),
                    kafka_cluster,
                    subscription,
                },
            );
        }

        pub(super) fn stop(&mut self, subscription_id: SubscriptionId) {
            if let Some(TaskState {
                task_state_inner: TaskStateInner::Running { task_id, .. },
                ..
            }) = self.subscription_id_to_task_state.remove(&subscription_id)
            {
                self.running_tasks_to_subscriptions.remove(&task_id);
            }
        }

        pub(super) async fn shutdown(&mut self) {
            self.subscription_id_to_task_state.clear();
            // This will close all the channels
            self.running_tasks_to_subscriptions.clear();
            self.tasks.shutdown().await;
        }

        pub(super) fn running_subscriptions(&self) -> impl Iterator<Item = &SubscriptionId> {
            self.subscription_id_to_task_state.keys()
        }

        pub(super) fn get_running_config(
            &self,
            subscription_id: &SubscriptionId,
        ) -> Option<(&KafkaCluster, &Subscription)> {
            self.subscription_id_to_task_state
                .get(subscription_id)
                .map(|state| (&state.kafka_cluster, &state.subscription))
        }
    }
}
