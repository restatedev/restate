// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

extern crate core;

mod error;
mod handle;
mod invoker_integration;
mod metric_definitions;
mod partition;
mod partition_processor_manager;
mod subscription_controller;
mod subscription_integration;

use std::sync::Arc;

use codederror::CodedError;
use restate_core::network::Swimlane;
use restate_ingestion_client::SessionOptions;
use restate_wal_protocol::Envelope;
use tracing::info;

use restate_bifrost::Bifrost;
use restate_core::MetadataKind;
use restate_core::cancellation_watcher;
use restate_core::network::MessageRouterBuilder;
use restate_core::network::Networking;
use restate_core::network::TransportConnect;
use restate_core::partitions::PartitionRouting;
use restate_core::worker_api::ProcessorsManagerHandle;
use restate_core::{Metadata, TaskKind};
use restate_core::{MetadataWriter, TaskCenter};
use restate_ingestion_client::IngestionClient;
use restate_ingress_kafka::Service as IngressKafkaService;
use restate_invoker_impl::InvokerHandle as InvokerChannelServiceHandle;
use restate_partition_store::snapshots::SnapshotRepository;
use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_storage_query_datafusion::context::{QueryContext, SelectPartitionsFromMetadata};
use restate_storage_query_datafusion::remote_query_scanner_client::create_remote_scanner_service;
use restate_storage_query_datafusion::remote_query_scanner_manager::{
    RemoteScannerManager, create_partition_locator,
};
use restate_storage_query_datafusion::remote_query_scanner_server::RemoteQueryScannerServer;
use restate_types::Version;
use restate_types::Versioned;
use restate_types::config::Configuration;
use restate_types::health::HealthStatus;
use restate_types::partitions::state::PartitionReplicaSetStates;
use restate_types::protobuf::common::WorkerStatus;
use restate_types::schema::Redaction;
use restate_types::schema::kafka::KafkaClusterResolver;
use restate_types::schema::subscriptions::SubscriptionResolver;

use crate::partition::invoker_storage_reader::InvokerStorageReader;
use crate::partition_processor_manager::PartitionProcessorManager;

pub use self::error::*;
pub use self::handle::*;
pub use crate::subscription_controller::SubscriptionController;
pub use crate::subscription_integration::SubscriptionControllerHandle;

type PartitionProcessorBuilder = partition::PartitionProcessorBuilder<
    InvokerChannelServiceHandle<InvokerStorageReader<PartitionStore>>,
>;

#[derive(Debug, thiserror::Error, CodedError)]
#[error("failed creating worker: {0}")]
pub enum BuildError {
    DataFusion(
        #[from]
        #[code]
        restate_storage_query_datafusion::BuildError,
    ),
    #[error("failed creating worker: {0}")]
    PartitionStore(
        #[from]
        #[code]
        restate_partition_store::BuildError,
    ),
    #[error("failed opening partition store: {0}")]
    RocksDb(
        #[from]
        #[code]
        restate_rocksdb::RocksError,
    ),
    #[code(unknown)]
    Invoker(#[from] restate_invoker_impl::BuildError),
    #[error("failed constructing partition snapshot repository: {0}")]
    #[code(unknown)]
    SnapshotRepository(#[from] anyhow::Error),
}

pub struct Worker<T> {
    storage_query_context: QueryContext,
    datafusion_remote_scanner: RemoteQueryScannerServer,
    ingress_kafka: IngressKafkaService<T>,
    subscription_controller_handle: SubscriptionControllerHandle,
    partition_processor_manager: PartitionProcessorManager<T>,
}

impl<T> Worker<T>
where
    T: TransportConnect,
{
    #[allow(clippy::too_many_arguments)]
    pub async fn create(
        health_status: HealthStatus<WorkerStatus>,
        replica_set_states: PartitionReplicaSetStates,
        partition_store_manager: Arc<PartitionStoreManager>,
        networking: Networking<T>,
        bifrost: Bifrost,
        ingestion_client: IngestionClient<T, Envelope>,
        router_builder: &mut MessageRouterBuilder,
        metadata_writer: MetadataWriter,
    ) -> Result<Self, BuildError> {
        metric_definitions::describe_metrics();
        restate_vqueues::describe_metrics();
        health_status.update(WorkerStatus::StartingUp);

        let partition_routing =
            PartitionRouting::new(replica_set_states.clone(), TaskCenter::current());

        let config = Configuration::pinned();
        let metadata = Metadata::current();

        let schema = metadata.updateable_schema();

        // ingress_kafka
        let ingress_kafka =
            IngressKafkaService::new(bifrost.clone(), ingestion_client.clone(), schema.clone());

        let subscription_controller_handle =
            SubscriptionControllerHandle::new(ingress_kafka.create_command_sender());

        let snapshots_options = &config.worker.snapshots;
        if (snapshots_options.snapshot_interval.is_some()
            || snapshots_options.snapshot_interval_num_records.is_some())
            && snapshots_options.destination.is_none()
        {
            return Err(BuildError::SnapshotRepository(anyhow::anyhow!(
                "Periodic snapshot interval set without a specified snapshot destination"
            )));
        }

        // A dedicated ingestion client for PPM that uses
        // BifrostData swimlane
        let ppm_ingestion_client = IngestionClient::new(
            networking.clone(),
            Metadata::with_current(|m| m.updateable_partition_table()),
            partition_routing.clone(),
            config
                .worker
                .shuffle
                .inflight_memory_budget
                .as_non_zero_usize(),
            Some(SessionOptions {
                batch_size: config.worker.shuffle.request_batch_size.as_usize(),
                connection_retry_policy: config.worker.shuffle.connection_retry_policy.clone(),
                swimlane: Swimlane::BifrostData,
            }),
        );

        let partition_processor_manager = PartitionProcessorManager::new(
            health_status,
            Configuration::live(),
            metadata_writer,
            partition_store_manager.clone(),
            replica_set_states,
            router_builder,
            bifrost,
            SnapshotRepository::create_if_configured(
                snapshots_options,
                config.worker.storage.snapshots_staging_dir(),
            )
            .await
            .map_err(BuildError::SnapshotRepository)?,
            ppm_ingestion_client,
        );

        let remote_scanner_manager = RemoteScannerManager::new(
            create_remote_scanner_service(networking),
            create_partition_locator(partition_routing, metadata),
        );
        let storage_query_context = QueryContext::with_user_tables(
            &config.admin.query_engine,
            SelectPartitionsFromMetadata,
            partition_store_manager,
            Some(partition_processor_manager.invokers_status_reader()),
            schema,
            remote_scanner_manager.clone(),
        )
        .await?;

        let datafusion_remote_scanner =
            RemoteQueryScannerServer::new(remote_scanner_manager, router_builder);

        Ok(Self {
            storage_query_context,
            datafusion_remote_scanner,
            ingress_kafka,
            subscription_controller_handle,
            partition_processor_manager,
        })
    }

    pub fn storage_query_context(&self) -> &QueryContext {
        &self.storage_query_context
    }

    pub fn partition_processor_manager_handle(&self) -> ProcessorsManagerHandle {
        self.partition_processor_manager.handle()
    }

    pub async fn run(self) -> anyhow::Result<()> {
        TaskCenter::spawn_child(
            TaskKind::MetadataBackgroundSync,
            "subscription_controller",
            Self::watch_subscriptions(self.subscription_controller_handle.clone()),
        )?;

        // Datafusion remote scanner
        TaskCenter::spawn_child(
            TaskKind::SystemService,
            "datafusion-scan-server",
            self.datafusion_remote_scanner.run(),
        )?;

        // Kafka Ingress
        TaskCenter::spawn_child(
            TaskKind::SystemService,
            "kafka-ingress",
            self.ingress_kafka.run(),
        )?;

        self.partition_processor_manager.run().await?;
        info!("Worker role has stopped");

        Ok(())
    }

    async fn watch_subscriptions<SC>(subscription_controller: SC) -> anyhow::Result<()>
    where
        SC: SubscriptionController + Clone + Send + Sync,
    {
        let metadata = Metadata::current();
        let mut updateable_schema = metadata.updateable_schema();
        let mut next_version = Version::MIN;
        let mut cancellation_watcher = std::pin::pin!(cancellation_watcher());

        loop {
            tokio::select! {
                _ = &mut cancellation_watcher => {
                    break;
                },
                version = metadata.wait_for_version(MetadataKind::Schema, next_version) => {
                    let _ = version?;
                    let schema = updateable_schema.live_load();
                    let kafka_clusters = schema.list_kafka_clusters(Redaction::No);
                    let subscriptions = schema.list_subscriptions(&[], Redaction::No);
                    subscription_controller
                        .update_subscriptions(kafka_clusters, subscriptions)
                        .await?;

                    next_version = schema.version().next();
                }
            }
        }

        Ok(())
    }
}
