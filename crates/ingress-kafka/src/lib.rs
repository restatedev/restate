// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod builder;
mod consumer_task;
mod metric_definitions;
mod subscription_controller;

use rdkafka::error::KafkaError;
use tokio::sync::mpsc;

use restate_types::schema::kafka::KafkaCluster;
use restate_types::{partitions::PartitionTableError, schema::subscriptions::Subscription};

#[derive(Debug)]
pub enum Command {
    UpdateSubscriptions(Vec<KafkaCluster>, Vec<Subscription>),
}

pub type SubscriptionCommandSender = mpsc::Sender<Command>;
pub type SubscriptionCommandReceiver = mpsc::Receiver<Command>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Kafka(#[from] KafkaError),
    #[error(
        "Error processing message subscription {subscription} topic {topic} partition {partition} offset {offset}: {cause}"
    )]
    Event {
        subscription: String,
        topic: String,
        partition: i32,
        offset: i64,
        #[source]
        cause: anyhow::Error,
    },
    #[error("Ingress stream is closed: {0}")]
    IngestionClosed(Box<dyn std::error::Error + Send + Sync>),
    #[error(transparent)]
    PartitionTableError(#[from] PartitionTableError),
    #[error(
        "Received a message on the main partition queue for topic {0} partition {1} despite partitioned queues"
    )]
    UnexpectedMainQueueMessage(String, i32),
    #[error(
        "Consumption task exited unexpectedly for subscription '{subscription}', topic: {topic} and partition: {partition}"
    )]
    UnexpectedConsumptionTaskExited {
        subscription: String,
        topic: String,
        partition: i32,
    },
}

pub use subscription_controller::Service;
