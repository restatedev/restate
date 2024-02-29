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
use crate::partitioning_scheme::FixedConsecutivePartitions;
use crate::services::Services;
use codederror::CodedError;
use partition::shuffle;
use restate_consensus::Consensus;
use restate_core::{cancellation_watcher, task_center, TaskKind};
use restate_ingress_dispatcher::{
    IngressDispatcherInputSender, Service as IngressDispatcherService,
};
use restate_ingress_grpc::HyperServerIngress;
use restate_ingress_kafka::Service as IngressKafkaService;
use restate_invoker_impl::{
    ChannelServiceHandle as InvokerChannelServiceHandle, Service as InvokerService,
};
use restate_network::{Networking, PartitionProcessorSender, UnboundedNetworkHandle};
use restate_schema_impl::Schemas;
use restate_service_protocol::codec::ProtobufRawEntryCodec;
use restate_storage_query_datafusion::context::QueryContext;
use restate_storage_query_postgres::service::PostgresQueryService;
use restate_storage_rocksdb::{RocksDBStorage, RocksDBWriter};
use restate_types::identifiers::{PartitionKey, PeerId};
use restate_types::message::PartitionTarget;
use std::ops::RangeInclusive;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::debug;
use util::IdentitySender;

mod invoker_integration;
mod metric_definitions;
mod network_integration;
mod partition;
mod partitioning_scheme;
mod services;
mod subscription_integration;
mod util;

pub use restate_ingress_grpc::{
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

pub use crate::subscription_integration::SubscriptionControllerHandle;
pub use restate_storage_query_postgres::{
    Options as StorageQueryPostgresOptions, OptionsBuilder as StorageQueryPostgresOptionsBuilder,
    OptionsBuilderError as StorageQueryPostgresOptionsBuilderError,
};
use restate_wal_protocol::Envelope;
pub use services::WorkerCommandSender;

type PartitionProcessorCommand = Envelope;
type ConsensusCommand = restate_consensus::Command<PartitionProcessorCommand>;
type ConsensusMsg = PartitionTarget<PartitionProcessorCommand>;
type PartitionProcessor = partition::PartitionProcessor<
    ProtobufRawEntryCodec,
    InvokerChannelServiceHandle,
    UnboundedNetworkHandle<shuffle::ShuffleInput, Envelope>,
>;
type ExternalClientIngress = HyperServerIngress<Schemas>;

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
    ingress_grpc: IngressOptions,
    pub kafka: KafkaIngressOptions,
    invoker: InvokerOptions,
    partition_processor: partition::Options,

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
            ingress_grpc: Default::default(),
            kafka: Default::default(),
            invoker: Default::default(),
            partition_processor: Default::default(),
            partitions: 1024,
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
}

impl Options {
    pub fn storage_path(&self) -> &str {
        &self.storage_rocksdb.path
    }

    pub fn build(
        self,
        networking: Arc<Networking>,
        schemas: Schemas,
    ) -> Result<Worker, BuildError> {
        metric_definitions::describe_metrics();
        Worker::new(self, networking, schemas)
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
    #[error("worker services failed: {0}")]
    #[code(unknown)]
    Services(#[from] services::Error),
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
    consensus: Consensus<PartitionProcessorCommand>,
    processors: Vec<PartitionProcessor>,
    networking: Arc<Networking>,
    network: network_integration::Network,
    storage_query_context: QueryContext,
    storage_query_postgres: PostgresQueryService,
    #[allow(clippy::type_complexity)]
    invoker: InvokerService<
        InvokerStorageReader<RocksDBStorage>,
        InvokerStorageReader<RocksDBStorage>,
        EntryEnricher<Schemas, ProtobufRawEntryCodec>,
        Schemas,
    >,
    ingress_dispatcher_service: IngressDispatcherService,
    external_client_ingress: ExternalClientIngress,
    network_ingress_sender: mpsc::Sender<Envelope>,
    ingress_kafka: IngressKafkaService,
    services: Services<FixedConsecutivePartitions>,
    rocksdb_writer: RocksDBWriter,
    rocksdb_storage: RocksDBStorage,
}

impl Worker {
    pub fn new(
        opts: Options,
        networking: Arc<Networking>,
        schemas: Schemas,
    ) -> Result<Self, BuildError> {
        let Options {
            channel_size,
            ingress_grpc,
            kafka,
            timers,
            storage_query_datafusion,
            storage_query_postgres,
            storage_rocksdb,
            partition_processor: partition_processor_options,
            ..
        } = opts;

        let num_partition_processors = opts.partitions;
        let (raft_in_tx, raft_in_rx) = mpsc::channel(channel_size);

        let ingress_dispatcher_service = IngressDispatcherService::new(channel_size);

        // ingress_grpc
        let external_client_ingress = ingress_grpc.build(
            ingress_dispatcher_service.create_ingress_request_sender(),
            schemas.clone(),
        );

        // ingress_kafka
        let kafka_config_clone = kafka.clone();
        let ingress_kafka = kafka.build(ingress_dispatcher_service.create_ingress_request_sender());
        let subscription_controller_handle =
            subscription_integration::SubscriptionControllerHandle::new(
                kafka_config_clone,
                ingress_kafka.create_command_sender(),
            );

        let partition_table = FixedConsecutivePartitions::new(num_partition_processors);

        let network = network_integration::Network::new(
            raft_in_tx,
            ingress_dispatcher_service.create_ingress_dispatcher_input_sender(),
            partition_table.clone(),
            channel_size,
        );
        let network_ingress_sender = network.create_ingress_sender();

        let mut consensus =
            Consensus::new(raft_in_rx, network.create_consensus_sender(), channel_size);

        let network_handle = network.create_network_handle();

        let (rocksdb_storage, rocksdb_writer) = storage_rocksdb.build()?;

        let invoker_storage_reader = InvokerStorageReader::new(rocksdb_storage.clone());
        let invoker = opts.invoker.build(
            invoker_storage_reader.clone(),
            invoker_storage_reader,
            EntryEnricher::new(schemas.clone()),
            schemas.clone(),
        );

        let storage_query_context = storage_query_datafusion.build(
            rocksdb_storage.clone(),
            invoker.status_reader(),
            schemas.clone(),
        )?;
        let storage_query_postgres = storage_query_postgres.build(storage_query_context.clone());

        let partitioner = partition_table.partitioner();

        let (command_senders, processors): (Vec<_>, Vec<_>) = partitioner
            .map(|(idx, partition_range)| {
                let proposal_sender = consensus.create_proposal_sender();
                let invoker_sender = invoker.handle();

                Self::create_partition_processor(
                    idx,
                    partition_range,
                    timers.clone(),
                    channel_size,
                    proposal_sender,
                    invoker_sender,
                    network_handle.clone(),
                    network.create_partition_processor_sender(),
                    rocksdb_storage.clone(),
                    schemas.clone(),
                    partition_processor_options.clone(),
                    ingress_dispatcher_service.create_ingress_dispatcher_input_sender(),
                )
            })
            .unzip();

        consensus.register_state_machines(command_senders);

        let services = Services::new(
            consensus.create_proposal_sender(),
            subscription_controller_handle,
            partition_table,
            channel_size,
        );

        Ok(Self {
            consensus,
            processors,
            networking,
            network,
            storage_query_context,
            storage_query_postgres,
            invoker,
            ingress_dispatcher_service,
            external_client_ingress,
            network_ingress_sender,
            ingress_kafka,
            services,
            rocksdb_writer,
            rocksdb_storage,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn create_partition_processor(
        peer_id: PeerId,
        partition_key_range: RangeInclusive<PartitionKey>,
        timer_service_options: restate_timer::Options,
        channel_size: usize,
        proposal_sender: mpsc::Sender<ConsensusMsg>,
        invoker_sender: InvokerChannelServiceHandle,
        network_handle: UnboundedNetworkHandle<shuffle::ShuffleInput, Envelope>,
        ack_sender: PartitionProcessorSender<partition::types::AckResponse>,
        rocksdb_storage: RocksDBStorage,
        schemas: Schemas,
        partition_processor_options: partition::Options,
        ingress_tx: IngressDispatcherInputSender,
    ) -> ((PeerId, mpsc::Sender<ConsensusCommand>), PartitionProcessor) {
        let (command_tx, command_rx) = mpsc::channel(channel_size);
        let processor = PartitionProcessor::new(
            peer_id,
            peer_id,
            partition_key_range,
            timer_service_options,
            channel_size,
            command_rx,
            IdentitySender::new(peer_id, proposal_sender),
            invoker_sender,
            network_handle,
            ack_sender,
            rocksdb_storage,
            schemas,
            partition_processor_options,
            ingress_tx,
        );

        ((peer_id, command_tx), processor)
    }

    pub fn worker_command_tx(&self) -> WorkerCommandSender {
        self.services.worker_command_tx()
    }

    pub fn subscription_controller_handle(&self) -> SubscriptionControllerHandle {
        self.services.subscription_controller_handler()
    }

    pub fn storage_query_context(&self) -> &QueryContext {
        &self.storage_query_context
    }

    pub fn rocksdb_storage(&self) -> &RocksDBStorage {
        &self.rocksdb_storage
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let tc = task_center();
        let shutdown = cancellation_watcher();
        let (shutdown_signal, shutdown_watch) = drain::channel();

        // RocksDB Writer
        tc.spawn_child(TaskKind::SystemService, "rocksdb-writer", None, async {
            let handle = self.rocksdb_writer.run(shutdown_watch);
            Ok(handle
                .await
                .map_err(|err| Error::thread_panic("rocksdb writer", err))?
                .map_err(Error::RocksDBWriter)?)
        })?;

        // Ingress dispatcher
        tc.spawn_child(
            TaskKind::SystemService,
            "ingress-dispatcher",
            None,
            self.ingress_dispatcher_service
                .run(self.network_ingress_sender),
        )?;

        // Ingress RPC server
        tc.spawn_child(
            TaskKind::RpcServer,
            "ingress-rpc-server",
            None,
            self.external_client_ingress.run(),
        )?;

        // Invoker service
        tc.spawn_child(TaskKind::SystemService, "invoker", None, self.invoker.run())?;

        // Networking
        tc.spawn_child(
            TaskKind::SystemService,
            "networking-legacy",
            None,
            self.network.run(),
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

        // Consensus Service
        tc.spawn_child(
            TaskKind::SystemService,
            "consensus",
            None,
            self.consensus.run(),
        )?;

        // Create partition processors
        for processor in self.processors {
            let networking = self.networking.clone();
            tc.spawn_child(
                TaskKind::PartitionProcessor,
                "partition-processor",
                Some(processor.partition_id),
                processor.run(networking),
            )?;
        }

        // Create worker operations service
        tc.spawn_child(
            TaskKind::SystemService,
            "worker-ops-service",
            None,
            self.services.run(),
        )?;

        tokio::select! {
            _ = shutdown => {
                debug!("Initiating shutdown of worker");

                // This will only shutdown rocksdb writer thread. Everything else will respond to
                // the cancellation signal independently.
                shutdown_signal.drain().await;
            }
        }

        Ok(())
    }
}
