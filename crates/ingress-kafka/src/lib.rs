// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#[cfg(feature = "legacy-ingestion")]
mod legacy;

#[cfg(not(feature = "legacy-ingestion"))]
mod builder;
#[cfg(not(feature = "legacy-ingestion"))]
mod consumer_task;
#[cfg(not(feature = "legacy-ingestion"))]
mod metric_definitions;
#[cfg(not(feature = "legacy-ingestion"))]
mod subscription_controller;

use rdkafka::error::KafkaError;
use restate_types::{
    identifiers::SubscriptionId, partitions::PartitionTableError,
    schema::subscriptions::Subscription,
};
use tokio::sync::mpsc;

#[derive(Debug)]
pub enum Command {
    StartSubscription(Subscription),
    StopSubscription(SubscriptionId),
    UpdateSubscriptions(Vec<Subscription>),
}

pub type SubscriptionCommandSender = mpsc::Sender<Command>;
pub type SubscriptionCommandReceiver = mpsc::Receiver<Command>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Kafka(#[from] KafkaError),
    #[error(
        "error processing message subscription {subscription} topic {topic} partition {partition} offset {offset}: {cause}"
    )]
    Event {
        subscription: String,
        topic: String,
        partition: i32,
        offset: i64,
        #[source]
        cause: anyhow::Error,
    },
    #[error("ingress stream is closed")]
    IngestionClosed,
    #[error(transparent)]
    PartitionTableError(#[from] PartitionTableError),
    #[error(
        "received a message on the main partition queue for topic {0} partition {1} despite partitioned queues"
    )]
    UnexpectedMainQueueMessage(String, i32),
}

#[cfg(feature = "legacy-ingestion")]
pub struct Service<T> {
    inner: legacy::Service<T>,
}

#[cfg(not(feature = "legacy-ingestion"))]
pub struct Service<T> {
    inner: subscription_controller::Service<T>,
}

#[cfg(not(feature = "legacy-ingestion"))]
mod service_impl {
    use restate_bifrost::Bifrost;
    use restate_core::network::TransportConnect;
    use restate_ingestion_client::IngestionClient;
    use restate_types::{
        config::IngressOptions,
        live::{Live, LiveLoad},
        schema::Schema,
    };
    use restate_wal_protocol::Envelope;

    use crate::{SubscriptionCommandSender, subscription_controller};

    use super::Service;

    impl<T> Service<T>
    where
        T: TransportConnect,
    {
        pub fn new(
            _bifrost: Bifrost,
            ingestion: IngestionClient<T, Envelope>,
            schema: Live<Schema>,
        ) -> Self {
            Self {
                inner: subscription_controller::Service::new(ingestion, schema),
            }
        }

        pub fn create_command_sender(&self) -> SubscriptionCommandSender {
            self.inner.create_command_sender()
        }

        pub fn run(
            self,
            updateable_config: impl LiveLoad<Live = IngressOptions>,
        ) -> impl Future<Output = anyhow::Result<()>> {
            self.inner.run(updateable_config)
        }
    }
}

#[cfg(feature = "legacy-ingestion")]
mod service_impl {

    use restate_bifrost::Bifrost;
    use restate_core::network::TransportConnect;
    use restate_ingestion_client::IngestionClient;
    use restate_types::{
        config::IngressOptions,
        live::{Live, LiveLoad},
        schema::Schema,
    };
    use restate_wal_protocol::Envelope;

    use crate::{SubscriptionCommandSender, legacy};

    use super::Service;

    impl<T> Service<T>
    where
        T: TransportConnect,
    {
        pub fn new(
            bifrost: Bifrost,
            ingestion: IngestionClient<T, Envelope>,
            schema: Live<Schema>,
        ) -> Self {
            Self {
                inner: legacy::Service::new(
                    ingestion.networking().clone(),
                    ingestion.partition_routing().clone(),
                    bifrost,
                    schema,
                ),
            }
        }

        pub fn create_command_sender(&self) -> SubscriptionCommandSender {
            self.inner.create_command_sender()
        }

        pub fn run(
            self,
            updateable_config: impl LiveLoad<Live = IngressOptions>,
        ) -> impl Future<Output = anyhow::Result<()>> {
            self.inner.run(updateable_config)
        }
    }
}
