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
mod legacy;
mod metric_definitions;
mod subscription_controller;

use rdkafka::error::KafkaError;
use tokio::sync::mpsc;

use restate_bifrost::Bifrost;
use restate_core::network::TransportConnect;
use restate_ingestion_client::IngestionClient;
use restate_types::schema::kafka::KafkaCluster;
use restate_types::{
    config::Configuration,
    live::Live,
    partitions::PartitionTableError,
    schema::{Schema, subscriptions::Subscription},
};
use restate_wal_protocol::Envelope;
use tracing::debug;

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

enum ServiceInner<T> {
    Legacy(legacy::Service<T>),
    IngestionClient(subscription_controller::Service<T>),
}

pub struct Service<T> {
    inner: ServiceInner<T>,
}

impl<T> Service<T>
where
    T: TransportConnect,
{
    pub fn new(
        bifrost: Bifrost,
        ingestion: IngestionClient<T, Envelope>,
        schema: Live<Schema>,
    ) -> Self {
        let batch_ingestion = Configuration::pinned()
            .common
            .experimental_kafka_batch_ingestion;

        let inner = if batch_ingestion {
            debug!("Using kafka experimental batch ingestion mechanism");
            ServiceInner::IngestionClient(subscription_controller::Service::new(ingestion, schema))
        } else {
            debug!("Using kafka legacy ingestion mechanism");
            ServiceInner::Legacy(legacy::Service::new(
                ingestion.networking().clone(),
                ingestion.partition_routing().clone(),
                bifrost,
                schema,
            ))
        };

        Self { inner }
    }

    pub fn create_command_sender(&self) -> SubscriptionCommandSender {
        match &self.inner {
            ServiceInner::Legacy(svc) => svc.create_command_sender(),
            ServiceInner::IngestionClient(svc) => svc.create_command_sender(),
        }
    }

    pub async fn run(self) -> anyhow::Result<()> {
        match self.inner {
            ServiceInner::Legacy(svc) => svc.run().await,
            ServiceInner::IngestionClient(svc) => svc.run().await,
        }
    }
}
