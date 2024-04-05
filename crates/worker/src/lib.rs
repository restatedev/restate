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
mod invoker_integration;
mod metric_definitions;
mod partition;
mod partition_processor_manager;
mod subscription_controller;
mod subscription_integration;

pub use error::*;
pub use handle::*;
use restate_types::arc_util::ArcSwapExt;
use restate_types::config::UpdateableConfiguration;
pub use subscription_controller::SubscriptionController;
pub use subscription_integration::SubscriptionControllerHandle;

use codederror::CodedError;
use restate_bifrost::Bifrost;
use restate_core::network::MessageRouterBuilder;
use restate_core::{cancellation_watcher, metadata, task_center, TaskKind};
use restate_ingress_dispatcher::IngressDispatcher;
use restate_ingress_http::HyperServerIngress;
use restate_ingress_kafka::Service as IngressKafkaService;
use restate_invoker_impl::{
    ChannelServiceHandle as InvokerChannelServiceHandle, Service as InvokerService,
};
use restate_metadata_store::MetadataStoreClient;
use restate_network::Networking;
use restate_schema_impl::Schemas;
use restate_service_protocol::codec::ProtobufRawEntryCodec;
use restate_storage_query_datafusion::context::QueryContext;
use restate_storage_query_postgres::service::PostgresQueryService;
use restate_storage_rocksdb::{RocksDBStorage, RocksDBWriter};
use tracing::debug;

use crate::invoker_integration::EntryEnricher;
use crate::partition::storage::invoker::InvokerStorageReader;
use crate::partition_processor_manager::{
    Action, PartitionProcessorManager, PartitionProcessorPlan, Role,
};
use restate_types::Version;

type PartitionProcessor =
    partition::PartitionProcessor<ProtobufRawEntryCodec, InvokerChannelServiceHandle>;

type ExternalClientIngress = HyperServerIngress<Schemas, IngressDispatcher>;

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
        restate_storage_rocksdb::BuildError,
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
    #[error("rocksdb writer failed: {0}")]
    #[code(unknown)]
    RocksDBWriter(#[from] anyhow::Error),
}

impl Error {
    fn thread_panic(thread: &'static str, cause: restate_types::errors::ThreadJoinError) -> Self {
        Error::ThreadPanic { thread, cause }
    }
}

pub struct Worker {
    updateable_config: UpdateableConfiguration,
    networking: Networking,
    metadata_store_client: MetadataStoreClient,
    storage_query_context: QueryContext,
    storage_query_postgres: PostgresQueryService,
    #[allow(clippy::type_complexity)]
    invoker: InvokerService<
        InvokerStorageReader<RocksDBStorage>,
        InvokerStorageReader<RocksDBStorage>,
        EntryEnricher<Schemas, ProtobufRawEntryCodec>,
        Schemas,
    >,
    external_client_ingress: ExternalClientIngress,
    ingress_kafka: IngressKafkaService,
    subscription_controller_handle: SubscriptionControllerHandle,
    rocksdb_writer: RocksDBWriter,
    rocksdb_storage: RocksDBStorage,
}

impl Worker {
    pub fn from_options(
        updateable_config: UpdateableConfiguration,
        networking: Networking,
        bifrost: Bifrost,
        router_builder: &mut MessageRouterBuilder,
        schemas: Schemas,
        metadata_store_client: MetadataStoreClient,
    ) -> Result<Worker, BuildError> {
        metric_definitions::describe_metrics();
        Worker::new(
            updateable_config,
            networking,
            bifrost,
            router_builder,
            schemas,
            metadata_store_client,
        )
    }

    pub fn new(
        updateable_config: UpdateableConfiguration,
        networking: Networking,
        bifrost: Bifrost,
        router_builder: &mut MessageRouterBuilder,
        schemas: Schemas,
        metadata_store_client: MetadataStoreClient,
    ) -> Result<Self, BuildError> {
        let ingress_dispatcher = IngressDispatcher::new(bifrost);
        router_builder.add_message_handler(ingress_dispatcher.clone());

        let config = updateable_config.pinned();
        // http ingress
        let ingress_http = HyperServerIngress::from_options(
            &config.ingress,
            ingress_dispatcher.clone(),
            schemas.clone(),
        );

        // ingress_kafka
        let ingress_kafka = IngressKafkaService::new(ingress_dispatcher.clone());
        let subscription_controller_handle =
            subscription_integration::SubscriptionControllerHandle::new(
                config.ingress.clone(),
                ingress_kafka.create_command_sender(),
            );

        let (rocksdb_storage, rocksdb_writer) = RocksDBStorage::new(
            updateable_config
                .clone()
                .map_as_updateable_owned(|c| &c.worker),
        )?;

        let invoker_storage_reader = InvokerStorageReader::new(rocksdb_storage.clone());
        let invoker = InvokerService::from_options(
            &config.common.service_client,
            &config.worker.invoker,
            invoker_storage_reader.clone(),
            invoker_storage_reader,
            EntryEnricher::new(schemas.clone()),
            schemas.clone(),
        )?;

        let storage_query_context = QueryContext::from_options(
            &config.admin.query_engine,
            rocksdb_storage.clone(),
            invoker.status_reader(),
            schemas.clone(),
        )?;
        let storage_query_postgres = PostgresQueryService::from_options(
            &config.admin.query_engine,
            storage_query_context.clone(),
        );

        Ok(Self {
            updateable_config,
            networking,
            storage_query_context,
            storage_query_postgres,
            invoker,
            external_client_ingress: ingress_http,
            ingress_kafka,
            subscription_controller_handle,
            rocksdb_writer,
            rocksdb_storage,
            metadata_store_client,
        })
    }

    pub fn subscription_controller_handle(&self) -> SubscriptionControllerHandle {
        self.subscription_controller_handle.clone()
    }

    pub fn storage_query_context(&self) -> &QueryContext {
        &self.storage_query_context
    }

    pub fn rocksdb_storage(&self) -> &RocksDBStorage {
        &self.rocksdb_storage
    }

    pub async fn run(self, bifrost: Bifrost) -> anyhow::Result<()> {
        let tc = task_center();
        let (shutdown_signal, shutdown_watch) = drain::channel();

        // RocksDB Writer
        tc.spawn_child(TaskKind::SystemService, "rocksdb-writer", None, async {
            let handle = self.rocksdb_writer.run(shutdown_watch);
            Ok(handle
                .await
                .map_err(|err| Error::thread_panic("rocksdb writer", err))?
                .map_err(Error::RocksDBWriter)?)
        })?;

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

        // Kafka Ingress
        tc.spawn_child(
            TaskKind::SystemService,
            "kafka-ingress",
            None,
            self.ingress_kafka.run(
                self.updateable_config
                    .clone()
                    .map_as_updateable_owned(|c| &c.ingress),
            ),
        )?;

        let invoker_handle = self.invoker.handle();

        // Invoker service
        tc.spawn_child(
            TaskKind::SystemService,
            "invoker",
            None,
            self.invoker.run(
                self.updateable_config
                    .clone()
                    .map_as_updateable_owned(|c| &c.worker.invoker),
            ),
        )?;

        let shutdown = cancellation_watcher();

        let mut partition_processor_manager = PartitionProcessorManager::new(
            self.updateable_config.clone(),
            metadata().my_node_id(),
            self.metadata_store_client,
            self.rocksdb_storage,
            self.networking,
            bifrost,
            invoker_handle,
        );

        let partition_table = metadata().wait_for_partition_table(Version::MIN).await?;
        let plan = PartitionProcessorPlan::new(
            partition_table.version(),
            partition_table
                .partitioner()
                .map(|(partition_id, _)| (partition_id, Action::Start(Role::Leader)))
                .collect(),
        );
        partition_processor_manager.apply_plan(plan).await?;

        tokio::select! {
            _ = shutdown => {
                debug!("Initiating shutdown of worker");

                // This will only shutdown rocksdb writer thread. Everything else will respond to
                // the cancellation signal independently.
                shutdown_signal.drain().await;
            },
        }

        Ok(())
    }
}
