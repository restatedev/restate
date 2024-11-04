// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
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
mod ingress_integration;
mod invoker_integration;
mod metric_definitions;
mod partition;
mod partition_processor_manager;
mod subscription_controller;
mod subscription_integration;

use codederror::CodedError;
use std::time::Duration;

use restate_bifrost::Bifrost;
use restate_core::network::partition_processor_rpc_client::PartitionProcessorRpcClient;
use restate_core::network::rpc_router::ConnectionAwareRpcRouter;
use restate_core::network::MessageRouterBuilder;
use restate_core::network::Networking;
use restate_core::network::TransportConnect;
use restate_core::routing_info::PartitionRoutingRefresher;
use restate_core::worker_api::ProcessorsManagerHandle;
use restate_core::{task_center, Metadata, TaskKind};
use restate_ingress_dispatcher::IngressDispatcher;
use restate_ingress_http::HyperServerIngress;
use restate_ingress_kafka::Service as IngressKafkaService;
use restate_invoker_impl::InvokerHandle as InvokerChannelServiceHandle;
use restate_metadata_store::MetadataStoreClient;
use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_storage_query_datafusion::context::{QueryContext, SelectPartitionsFromMetadata};
use restate_storage_query_datafusion::remote_query_scanner_client::create_remote_scanner_service;
use restate_storage_query_datafusion::remote_query_scanner_server::RemoteQueryScannerServer;
use restate_storage_query_postgres::service::PostgresQueryService;
use restate_types::config::Configuration;
use restate_types::health::HealthStatus;
use restate_types::live::Live;
use restate_types::protobuf::common::WorkerStatus;
use restate_types::schema::Schema;

use crate::ingress_integration::RpcRequestDispatcher;
use crate::partition::invoker_storage_reader::InvokerStorageReader;
use crate::partition_processor_manager::PartitionProcessorManager;

pub use self::error::*;
pub use self::handle::*;
pub use crate::subscription_controller::SubscriptionController;
pub use crate::subscription_integration::SubscriptionControllerHandle;

type PartitionProcessorBuilder = partition::PartitionProcessorBuilder<
    InvokerChannelServiceHandle<InvokerStorageReader<PartitionStore>>,
>;

type ExternalClientIngress<T> = HyperServerIngress<Schema, RpcRequestDispatcher<T>>;

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

pub struct Worker<T> {
    updateable_config: Live<Configuration>,
    storage_query_context: QueryContext,
    storage_query_postgres: PostgresQueryService,
    datafusion_remote_scanner: RemoteQueryScannerServer,
    external_client_ingress: ExternalClientIngress<T>,
    ingress_kafka: IngressKafkaService,
    subscription_controller_handle: SubscriptionControllerHandle,
    partition_processor_manager: PartitionProcessorManager<T>,
}

impl<T: TransportConnect> Worker<T> {
    #[allow(clippy::too_many_arguments)]
    pub async fn create(
        updateable_config: Live<Configuration>,
        health_status: HealthStatus<WorkerStatus>,
        metadata: Metadata,
        networking: Networking<T>,
        bifrost: Bifrost,
        router_builder: &mut MessageRouterBuilder,
        schema: Live<Schema>,
        metadata_store_client: MetadataStoreClient,
    ) -> Result<Self, BuildError> {
        metric_definitions::describe_metrics();
        health_status.update(WorkerStatus::StartingUp);

        let config = updateable_config.pinned();

        // ingress_kafka
        let ingress_kafka = IngressKafkaService::new(IngressDispatcher::new(bifrost.clone()));
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

        // TODO sort this out!
        //  it's on https://github.com/restatedev/restate/pull/2172
        let partition_routing_refresher =
            PartitionRoutingRefresher::new(metadata_store_client.clone());

        let rpc_router = ConnectionAwareRpcRouter::new(router_builder);
        let partition_table = metadata.updateable_partition_table();
        // http ingress
        let ingress_http = HyperServerIngress::from_options(
            &config.ingress,
            RpcRequestDispatcher::new(PartitionProcessorRpcClient::new(
                networking.clone(),
                rpc_router,
                partition_table,
                partition_routing_refresher.partition_routing(),
            )),
            schema.clone(),
        );

        let partition_processor_manager = PartitionProcessorManager::new(
            task_center(),
            health_status,
            updateable_config.clone(),
            metadata.clone(),
            metadata_store_client,
            partition_store_manager.clone(),
            router_builder,
            networking.clone(),
            bifrost,
        );

        // handle RPCs
        router_builder.add_message_handler(partition_processor_manager.message_handler());

        let storage_query_context = QueryContext::create(
            &config.admin.query_engine,
            SelectPartitionsFromMetadata::new(metadata),
            Some(partition_store_manager.clone()),
            partition_processor_manager.invokers_status_reader(),
            schema.clone(),
            create_remote_scanner_service(networking, task_center(), router_builder),
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
            external_client_ingress: ingress_http,
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

    pub fn parition_processor_manager_handle(&self) -> ProcessorsManagerHandle {
        self.partition_processor_manager.handle()
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let tc = task_center();

        // Ingress RPC server
        tc.spawn_child(
            TaskKind::IngressServer,
            "ingress-rpc-server",
            None,
            self.external_client_ingress.run(),
        )?;

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
