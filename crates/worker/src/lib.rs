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

use crate::invoker_integration::EntryEnricher;
use crate::partition::storage::invoker::InvokerStorageReader;
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
use restate_network::Networking;
use restate_schema_impl::Schemas;
use restate_service_protocol::codec::ProtobufRawEntryCodec;
use restate_storage_query_datafusion::context::QueryContext;
use restate_storage_query_postgres::service::PostgresQueryService;
use restate_storage_rocksdb::{RocksDBStorage, RocksDBWriter};
use std::path::Path;
use tracing::debug;

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

pub use restate_ingress_http::{
    Options as IngressOptions, OptionsBuilder as IngressOptionsBuilder,
    OptionsBuilderError as IngressOptionsBuilderError,
};
pub use restate_ingress_kafka::{
    Options as KafkaIngressOptions, OptionsBuilder as KafkaIngressOptionsBuilder,
    OptionsBuilderError as KafkaIngressOptionsBuilderError,
};
pub use restate_invoker_impl::{
    Options as InvokerOptions, OptionsBuilder as InvokerOptionsBuilder,
    OptionsBuilderError as InvokerOptionsBuilderError,
};
use restate_metadata_store::MetadataStoreClient;
pub use subscription_controller::SubscriptionController;

pub use restate_storage_rocksdb::{
    Options as RocksdbOptions, OptionsBuilder as RocksdbOptionsBuilder,
    OptionsBuilderError as RocksdbOptionsBuilderError,
};
pub use restate_timer::{
    Options as TimerOptions, OptionsBuilder as TimerOptionsBuilder,
    OptionsBuilderError as TimerOptionsBuilderError,
};

pub use restate_storage_query_datafusion::{
    Options as StorageQueryDatafusionOptions,
    OptionsBuilder as StorageQueryDatafusionOptionsBuilder,
    OptionsBuilderError as StorageQueryDatafusionOptionsBuilderError,
};

use crate::partition_processor_manager::{
    Action, PartitionProcessorManager, PartitionProcessorPlan, Role,
};
pub use crate::subscription_integration::SubscriptionControllerHandle;
pub use restate_storage_query_postgres::{
    Options as StorageQueryPostgresOptions, OptionsBuilder as StorageQueryPostgresOptionsBuilder,
    OptionsBuilderError as StorageQueryPostgresOptionsBuilderError,
};
use restate_types::Version;

type PartitionProcessor =
    partition::PartitionProcessor<ProtobufRawEntryCodec, InvokerChannelServiceHandle>;
type ExternalClientIngress = HyperServerIngress<Schemas, IngressDispatcher>;

/// # Worker options
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
#[cfg_attr(
    feature = "options_schema",
    schemars(rename = "WorkerOptions", default)
)]
#[builder(default)]
pub struct Options {
    /// # Bounded channel size
    channel_size: usize,
    timers: TimerOptions,
    storage_query_datafusion: StorageQueryDatafusionOptions,
    storage_query_postgres: StorageQueryPostgresOptions,
    storage_rocksdb: RocksdbOptions,
    ingress: IngressOptions,
    invoker: InvokerOptions,

    /// # Partitions
    ///
    /// Number of partitions to be used to process messages.
    ///
    /// Note: This config entry **will be removed** in future Restate releases,
    /// as the partitions number will be dynamically configured depending on the load.
    ///
    /// Cannot be higher than `4611686018427387903` (You should almost never need as many partitions anyway)
    pub partitions: u64,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            channel_size: 64,
            timers: Default::default(),
            storage_query_datafusion: Default::default(),
            storage_query_postgres: Default::default(),
            storage_rocksdb: Default::default(),
            ingress: Default::default(),
            invoker: Default::default(),
            partitions: 64,
        }
    }
}

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

impl Options {
    pub fn storage_path(&self) -> &Path {
        self.storage_rocksdb.path.as_path()
    }
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
    options: Options,
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
        options: Options,
        kafka_options: KafkaIngressOptions,
        networking: Networking,
        bifrost: Bifrost,
        router_builder: &mut MessageRouterBuilder,
        schemas: Schemas,
        metadata_store_client: MetadataStoreClient,
    ) -> Result<Worker, BuildError> {
        metric_definitions::describe_metrics();
        Worker::new(
            options,
            kafka_options,
            networking,
            bifrost,
            router_builder,
            schemas,
            metadata_store_client,
        )
    }

    pub fn new(
        opts: Options,
        kafka_options: KafkaIngressOptions,
        networking: Networking,
        bifrost: Bifrost,
        router_builder: &mut MessageRouterBuilder,
        schemas: Schemas,
        metadata_store_client: MetadataStoreClient,
    ) -> Result<Self, BuildError> {
        let options = opts.clone();

        let Options {
            ingress,
            storage_query_datafusion,
            storage_query_postgres,
            storage_rocksdb,
            ..
        } = opts;

        let ingress_dispatcher = IngressDispatcher::new(bifrost);
        router_builder.add_message_handler(ingress_dispatcher.clone());

        // http ingress
        let ingress_http = ingress.build(ingress_dispatcher.clone(), schemas.clone());

        // ingress_kafka
        let kafka_config_clone = kafka_options.clone();
        let ingress_kafka = kafka_options.build(ingress_dispatcher.clone());
        let subscription_controller_handle =
            subscription_integration::SubscriptionControllerHandle::new(
                kafka_config_clone,
                ingress_kafka.create_command_sender(),
            );

        let (rocksdb_storage, rocksdb_writer) = storage_rocksdb.build()?;

        let invoker_storage_reader = InvokerStorageReader::new(rocksdb_storage.clone());
        let invoker = InvokerService::from_options(
            opts.invoker,
            invoker_storage_reader.clone(),
            invoker_storage_reader,
            EntryEnricher::new(schemas.clone()),
            schemas.clone(),
        )?;

        let storage_query_context = storage_query_datafusion.build(
            rocksdb_storage.clone(),
            invoker.status_reader(),
            schemas.clone(),
        )?;
        let storage_query_postgres = storage_query_postgres.build(storage_query_context.clone());

        Ok(Self {
            options,
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
            self.ingress_kafka.run(),
        )?;

        let invoker_handle = self.invoker.handle();

        // Invoker service
        tc.spawn_child(TaskKind::SystemService, "invoker", None, self.invoker.run())?;

        let shutdown = cancellation_watcher();

        let mut partition_processor_manager = PartitionProcessorManager::new(
            metadata().my_node_id(),
            self.options,
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
