// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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
use restate_types::partitions::PartitionTableError;
use tokio::sync::mpsc;

pub use subscription_controller::{Command, Service};

pub type SubscriptionCommandSender = mpsc::Sender<Command>;
pub type SubscriptionCommandReceiver = mpsc::Receiver<Command>;

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
    #[error("ingress stream is closed")]
    IngressClosed,
    #[error(transparent)]
    PartitionTableError(PartitionTableError),
    #[error(
        "received a message on the main partition queue for topic {0} partition {1} despite partitioned queues"
    )]
    UnexpectedMainQueueMessage(String, i32),
}
