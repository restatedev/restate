// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::consumer_task::{MessageDispatcherType, MessageSender};
use super::options::Options;
use super::*;

use crate::subscription_controller::task_orchestrator::TaskOrchestrator;
use rdkafka::error::KafkaError;
use restate_ingress_dispatcher::IngressRequestSender;
use restate_schema_api::subscription::{Sink, Source, Subscription};
use restate_types::retries::RetryPolicy;
use std::time::Duration;
use tokio::sync::mpsc;

#[derive(Debug)]
pub enum Command {
    StartSubscription(Subscription),
    StopSubscription(String),
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Kafka(#[from] KafkaError),
}

// For simplicity of the current implementation, this currently lives in this module
// In future versions, we should either pull this out in a separate process, or generify it and move it to the worker, or an ad-hoc module
pub struct Service {
    options: Options,
    ingress_tx: IngressRequestSender,

    commands_tx: SubscriptionCommandSender,
    commands_rx: SubscriptionCommandReceiver,
}

impl Service {
    pub(crate) fn new(options: Options, ingress_tx: IngressRequestSender) -> Service {
        let (commands_tx, commands_rx) = mpsc::channel(10);

        Service {
            options,
            ingress_tx,
            commands_tx,
            commands_rx,
        }
    }

    pub fn create_command_sender(&self) -> SubscriptionCommandSender {
        self.commands_tx.clone()
    }

    pub async fn run(mut self, drain: drain::Watch) {
        let shutdown = drain.signaled();
        tokio::pin!(shutdown);

        let mut task_orchestrator = TaskOrchestrator::new(RetryPolicy::exponential(
            Duration::from_millis(200),
            2.0,
            usize::MAX,
            Some(Duration::from_secs(10)),
        ));

        loop {
            tokio::select! {
                Some(cmd) = self.commands_rx.recv() => {
                    match cmd {
                        Command::StartSubscription(sub) => self.handle_start_subscription(sub, &mut task_orchestrator),
                        Command::StopSubscription(sub_id) => self.handle_stop_subscription(&sub_id, &mut task_orchestrator)
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
    }

    fn handle_start_subscription(
        &mut self,
        subscription: Subscription,
        task_orchestrator: &mut TaskOrchestrator,
    ) {
        let mut client_config = rdkafka::ClientConfig::new();

        let Source::Kafka { cluster, topic } = subscription.source();

        // Copy cluster options and subscription metadata into client_config
        let cluster_options = self
            .options
            .clusters
            .get(cluster)
            .unwrap_or_else(|| panic!("KafkaOptions should contain the cluster '{}'", cluster));

        client_config.set("metadata.broker.list", cluster_options.servers.clone());
        for (k, v) in cluster_options.additional_options.clone() {
            client_config.set(k, v);
        }
        for (k, v) in subscription.metadata() {
            client_config.set(k, v);
        }

        // Options required by the business logic of our consumer,
        // see ConsumerTask::run
        client_config.set("enable.auto.commit", "false");
        client_config.set("enable.auto.offset.store", "false");

        // Infer message_dispatcher_type
        let Sink::Service {
            name,
            method,
            is_input_type_keyed,
        } = subscription.sink();
        let message_dispatcher_type = if *is_input_type_keyed {
            MessageDispatcherType::DispatchKeyedEvent
        } else {
            MessageDispatcherType::DispatchEvent
        };

        // Create the consumer task
        let consumer_task = consumer_task::ConsumerTask::new(
            client_config,
            vec![topic.to_string()],
            MessageSender::new(
                name.to_string(),
                method.to_string(),
                subscription.id().to_string(),
                message_dispatcher_type,
                self.ingress_tx.clone(),
            ),
        );

        task_orchestrator.start(subscription.id().to_string(), consumer_task);
    }

    fn handle_stop_subscription(
        &mut self,
        subscription_id: &str,
        task_orchestrator: &mut TaskOrchestrator,
    ) {
        task_orchestrator.stop(subscription_id);
    }
}

mod task_orchestrator {
    use crate::consumer_task;
    use restate_timer_queue::TimerQueue;
    use restate_types::retries::{RetryIter, RetryPolicy};
    use std::collections::HashMap;
    use std::time::{Duration, SystemTime};
    use tokio::sync::oneshot;
    use tokio::task;
    use tokio::task::{JoinError, JoinSet};
    use tracing::{debug, warn};

    struct TaskState {
        // We use this to restart the consumer task in case of a failure
        consumer_task_clone: consumer_task::ConsumerTask,
        task_state_inner: TaskStateInner,
        retry_iter: RetryIter,
    }

    enum TaskStateInner {
        Running {
            task_id: task::Id,
            _close_ch: oneshot::Sender<()>,
        },
        WaitingRetryTimer,
    }

    pub(super) struct TaskOrchestrator {
        retry_policy: RetryPolicy,
        running_tasks_to_subscriptions: HashMap<task::Id, String>,
        subscription_id_to_task_state: HashMap<String, TaskState>,
        tasks: JoinSet<Result<(), consumer_task::Error>>,
        timer_queue: TimerQueue<String>,
    }

    impl TaskOrchestrator {
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
            result: Result<(task::Id, Result<(), consumer_task::Error>), JoinError>,
        ) {
            match result {
                Ok((id, Ok(_))) => {
                    warn!("Consumer unexpectedly closed");
                    self.start_retry_timer(id);
                }
                Ok((id, Err(e))) => {
                    warn!("Consumer unexpectedly closed with reason: {e}");
                    self.start_retry_timer(id);
                }
                Err(e) => {
                    warn!("Consumer unexpectedly panicked with reason: {e}");
                    self.start_retry_timer(e.id());
                }
            };
        }

        fn start_retry_timer(&mut self, task_id: task::Id) {
            let subscription_id = if let Some(subscription_id) =
                self.running_tasks_to_subscriptions.remove(&task_id)
            {
                subscription_id
            } else {
                // No need to do anything, as it's a correct closure
                return;
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
                warn!("Not going to retry because retry limit exhausted.");
                self.subscription_id_to_task_state.remove(&subscription_id);
            }
        }

        fn handle_timer_fired(&mut self, subscription_id: String) {
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
                ..
            } = self
                .subscription_id_to_task_state
                .remove(&subscription_id)
                .expect("Checked in the previous match statement");
            self.start(subscription_id, consumer_task_clone);
        }

        pub(super) fn start(
            &mut self,
            subscription_id: String,
            consumer_task_clone: consumer_task::ConsumerTask,
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
            let task_id = self.tasks.spawn(consumer_task_clone.clone().run(rx)).id();

            self.running_tasks_to_subscriptions
                .insert(task_id, subscription_id.clone());
            self.subscription_id_to_task_state.insert(
                subscription_id,
                TaskState {
                    consumer_task_clone: consumer_task_clone.clone(),
                    task_state_inner: TaskStateInner::Running {
                        task_id,
                        _close_ch: tx,
                    },
                    retry_iter: self.retry_policy.clone().into_iter(),
                },
            );
        }

        pub(super) fn stop(&mut self, subscription_id: &str) {
            if let Some(TaskState {
                task_state_inner: TaskStateInner::Running { task_id, .. },
                ..
            }) = self.subscription_id_to_task_state.remove(subscription_id)
            {
                self.running_tasks_to_subscriptions.remove(&task_id);
            }
        }

        pub(super) async fn shutdown(&mut self) {
            // This will close all the channels
            warn!("Shutdown!");
            self.subscription_id_to_task_state.clear();
            self.running_tasks_to_subscriptions.clear();

            let timeout = tokio::time::sleep(Duration::from_secs(5));
            tokio::pin!(timeout);

            loop {
                tokio::select! {
                    v = self.tasks.join_next() => {
                        if v.is_none() {
                              warn!("Shutdown done!");
                            return
                        }
                    },
                    _ = timeout.as_mut() => {
                        break;
                    }
                }
            }
            warn!("Shutdown forced!");
            self.tasks.shutdown().await;
        }
    }
}
