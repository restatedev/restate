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

use codederror::CodedError;
use std::time::Duration;

use restate_bifrost::Bifrost;
use restate_core::network::MessageRouterBuilder;
use restate_core::network::Networking;
use restate_core::network::TransportConnect;
use restate_core::partitions::PartitionRouting;
use restate_core::worker_api::ProcessorsManagerHandle;
use restate_core::{task_center, Metadata, TaskKind};
use restate_ingress_kafka::Service as IngressKafkaService;
use restate_invoker_impl::InvokerHandle as InvokerChannelServiceHandle;
use restate_metadata_store::MetadataStoreClient;
use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_storage_query_datafusion::context::{QueryContext, SelectPartitionsFromMetadata};
use restate_storage_query_datafusion::remote_query_scanner_client::create_remote_scanner_service;
use restate_storage_query_datafusion::remote_query_scanner_manager::{
    create_partition_locator, RemoteScannerManager,
};
use restate_storage_query_datafusion::remote_query_scanner_server::RemoteQueryScannerServer;
use restate_storage_query_postgres::service::PostgresQueryService;
use restate_types::config::Configuration;
use restate_types::health::HealthStatus;
use restate_types::live::Live;
use restate_types::protobuf::common::WorkerStatus;

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
    Datafusion(
        #[from]
        #[code]
        restate_storage_query_datafusion::BuildError,
    ),
    #[error("failed creating worker: {0}")]
    RocksDB(
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
}

#[derive(Debug, thiserror::Error, CodedError)]
pub enum Error {
    #[error("thread '{thread}' panicked: {cause}")]
    #[code(unknown)]
    ThreadPanic {
        thread: &'static str,
        cause: restate_types::errors::ThreadJoinError,
    },
}

pub struct Worker {
    updateable_config: Live<Configuration>,
    storage_query_context: QueryContext,
    storage_query_postgres: PostgresQueryService,
    datafusion_remote_scanner: RemoteQueryScannerServer,
    ingress_kafka: IngressKafkaService,
    subscription_controller_handle: SubscriptionControllerHandle,
    partition_processor_manager: PartitionProcessorManager,
}

impl Worker {
    #[allow(clippy::too_many_arguments)]
    pub async fn create<T: TransportConnect>(
        updateable_config: Live<Configuration>,
        health_status: HealthStatus<WorkerStatus>,
        metadata: Metadata,
        partition_routing: PartitionRouting,
        networking: Networking<T>,
        bifrost: Bifrost,
        router_builder: &mut MessageRouterBuilder,
        metadata_store_client: MetadataStoreClient,
    ) -> Result<Self, BuildError> {
        metric_definitions::describe_metrics();
        health_status.update(WorkerStatus::StartingUp);

        let config = updateable_config.pinned();

        // ingress_kafka
        let ingress_kafka = IngressKafkaService::new(bifrost.clone());
        let subscription_controller_handle = SubscriptionControllerHandle::new(
            config.ingress.clone(),
            ingress_kafka.create_command_sender(),
        );

        let partition_store_manager = PartitionStoreManager::create(
            updateable_config.clone().map(|c| &c.worker.storage),
            updateable_config
                .clone()
                .map(|c| &c.worker.storage.rocksdb)
                .boxed(),
            &[],
        )
        .await?;

        let partition_processor_manager = PartitionProcessorManager::new(
            task_center(),
            health_status,
            updateable_config.clone(),
            metadata.clone(),
            metadata_store_client,
            partition_store_manager.clone(),
            router_builder,
            bifrost,
        );

        // handle RPCs
        router_builder.add_message_handler(partition_processor_manager.message_handler());

        let remote_scanner_manager = RemoteScannerManager::new(
            create_remote_scanner_service(networking, task_center(), router_builder),
            create_partition_locator(partition_routing, metadata.clone()),
        );
        let schema = metadata.updateable_schema();
        let storage_query_context = QueryContext::create(
            &config.admin.query_engine,
            SelectPartitionsFromMetadata::new(metadata),
            Some(partition_store_manager.clone()),
            Some(partition_processor_manager.invokers_status_reader()),
            schema,
            remote_scanner_manager,
        )
        .await?;

        let storage_query_postgres = PostgresQueryService::from_options(
            &config.admin.query_engine,
            storage_query_context.clone(),
        );

        let datafusion_remote_scanner = RemoteQueryScannerServer::new(
            Duration::from_secs(60),
            storage_query_context.clone(),
            router_builder,
        );

        Ok(Self {
            updateable_config,
            storage_query_context,
            storage_query_postgres,
            datafusion_remote_scanner,
            ingress_kafka,
            subscription_controller_handle,
            partition_processor_manager,
        })
    }

    pub fn subscription_controller_handle(&self) -> SubscriptionControllerHandle {
        self.subscription_controller_handle.clone()
    }

    pub fn storage_query_context(&self) -> &QueryContext {
        &self.storage_query_context
    }

    pub fn partition_processor_manager_handle(&self) -> ProcessorsManagerHandle {
        self.partition_processor_manager.handle()
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let tc = task_center();

        // Postgres external server
        tc.spawn_child(
            TaskKind::RpcServer,
            "postgres-query-server",
            None,
            self.storage_query_postgres.run(),
        )?;

        // Datafusion remote scanner
        tc.spawn_child(
            TaskKind::SystemService,
            "datafusion-scan-server",
            None,
            self.datafusion_remote_scanner.run(),
        )?;

        // Kafka Ingress
        tc.spawn_child(
            TaskKind::SystemService,
            "kafka-ingress",
            None,
            self.ingress_kafka
                .run(self.updateable_config.clone().map(|c| &c.ingress)),
        )?;

        tc.spawn_child(
            TaskKind::SystemService,
            "partition-processor-manager",
            None,
            self.partition_processor_manager.run(),
        )?;

        Ok(())
    }
}
