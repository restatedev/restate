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

use crate::ingress_integration::{ExternalClientIngressRunner, IngressIntegrationError};
use crate::invoker_integration::EntryEnricher;
use crate::partition::storage::invoker::InvokerStorageReader;
use crate::partitioning_scheme::FixedConsecutivePartitions;
use crate::services::Services;
use codederror::CodedError;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use partition::ack::AckCommand;
use partition::shuffle;
use restate_consensus::Consensus;
use restate_invoker_impl::{
    ChannelServiceHandle as InvokerChannelServiceHandle, Service as InvokerService,
};
use restate_network::{PartitionProcessorSender, UnboundedNetworkHandle};
use restate_schema_impl::Schemas;
use restate_service_protocol::codec::ProtobufRawEntryCodec;
use restate_storage_query::service::PostgresQueryService;
use restate_storage_rocksdb::RocksDBStorage;
use restate_types::identifiers::{IngressId, PartitionKey, PeerId};
use restate_types::message::PeerTarget;
use std::ops::RangeInclusive;
use tokio::join;
use tokio::sync::mpsc;
use tracing::debug;
use util::IdentitySender;

mod ingress_integration;
mod invoker_integration;
mod network_integration;
mod partition;
mod partitioning_scheme;
mod services;
mod util;

pub use restate_ingress_grpc::{
    Options as IngressOptions, OptionsBuilder as IngressOptionsBuilder,
    OptionsBuilderError as IngressOptionsBuilderError,
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

pub use restate_storage_query::{
    Options as StorageQueryOptions, OptionsBuilder as StorageQueryOptionsBuilder,
    OptionsBuilderError as StorageQueryOptionsBuilderError,
};

type PartitionProcessorCommand = AckCommand;
type ConsensusCommand = restate_consensus::Command<PartitionProcessorCommand>;
type ConsensusMsg = PeerTarget<PartitionProcessorCommand>;
type PartitionProcessor = partition::PartitionProcessor<
    ProtobufRawEntryCodec,
    InvokerChannelServiceHandle,
    UnboundedNetworkHandle<shuffle::ShuffleInput, shuffle::ShuffleOutput>,
    Schemas,
>;

/// # Worker options
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "options_schema", schemars(rename = "WorkerOptions"))]
#[builder(default)]
pub struct Options {
    /// # Bounded channel size
    #[cfg_attr(
        feature = "options_schema",
        schemars(default = "Options::default_channel_size")
    )]
    channel_size: usize,
    #[cfg_attr(feature = "options_schema", schemars(default))]
    timers: TimerOptions,
    #[cfg_attr(feature = "options_schema", schemars(default))]
    storage_query: StorageQueryOptions,
    #[cfg_attr(feature = "options_schema", schemars(default))]
    storage_rocksdb: RocksdbOptions,
    #[cfg_attr(feature = "options_schema", schemars(default))]
    ingress_grpc: IngressOptions,
    #[cfg_attr(feature = "options_schema", schemars(default))]
    invoker: InvokerOptions,
    /// # Partitions
    ///
    /// Number of partitions to be used to process messages.
    ///
    /// Note: This config entry **will be removed** in future Restate releases,
    /// as the partitions number will be dynamically configured depending on the load.
    #[cfg_attr(
        feature = "options_schema",
        schemars(default = "Options::default_partitions")
    )]
    partitions: u64,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            channel_size: Options::default_channel_size(),
            timers: Default::default(),
            storage_query: Default::default(),
            storage_rocksdb: Default::default(),
            ingress_grpc: Default::default(),
            invoker: Default::default(),
            partitions: Options::default_partitions(),
        }
    }
}

#[derive(Debug, thiserror::Error, CodedError)]
#[error("failed creating worker: {cause}")]
pub struct BuildError {
    #[from]
    #[code]
    cause: restate_storage_rocksdb::BuildError,
}

impl Options {
    fn default_channel_size() -> usize {
        64
    }

    fn default_partitions() -> u64 {
        1024
    }

    pub fn storage_path(&self) -> &str {
        &self.storage_rocksdb.path
    }

    pub fn build(self, schemas: Schemas) -> Result<Worker, BuildError> {
        Worker::new(self, schemas)
    }
}

#[derive(Debug, thiserror::Error, CodedError)]
pub enum Error {
    #[error("component '{component}' panicked: {cause}")]
    #[code(unknown)]
    ComponentPanic {
        component: &'static str,
        cause: tokio::task::JoinError,
    },
    #[error("network failed: {0}")]
    #[code(unknown)]
    Network(#[from] restate_network::RoutingError),
    #[error("storage query failed: {0}")]
    #[code(unknown)]
    StorageQuery(#[from] restate_storage_query::Error),
    #[error("consensus failed: {0}")]
    #[code(unknown)]
    Consensus(anyhow::Error),
    #[error("external client ingress failed: {0}")]
    ExternalClientIngress(
        #[from]
        #[code]
        IngressIntegrationError,
    ),
    #[error("no partition processor is running")]
    #[code(unknown)]
    NoPartitionProcessorRunning,
    #[error("partition processor failed: {0}")]
    #[code(unknown)]
    PartitionProcessor(anyhow::Error),
    #[error("worker services failed: {0}")]
    #[code(unknown)]
    Services(#[from] services::Error),
}

impl Error {
    fn component_panic(component: &'static str, cause: tokio::task::JoinError) -> Self {
        Error::ComponentPanic { component, cause }
    }
}

pub struct Worker {
    consensus: Consensus<PartitionProcessorCommand>,
    processors: Vec<PartitionProcessor>,
    network: network_integration::Network,
    storage_query: PostgresQueryService,
    invoker: InvokerService<
        InvokerStorageReader<RocksDBStorage>,
        InvokerStorageReader<RocksDBStorage>,
        EntryEnricher<Schemas, ProtobufRawEntryCodec>,
        Schemas,
    >,
    external_client_ingress_runner: ExternalClientIngressRunner,
    services: Services<FixedConsecutivePartitions>,
}

impl Worker {
    pub fn new(opts: Options, schemas: Schemas) -> Result<Self, BuildError> {
        let Options {
            channel_size,
            ingress_grpc,
            timers,
            storage_query,
            storage_rocksdb,
            ..
        } = opts;

        let num_partition_processors = opts.partitions;
        let (raft_in_tx, raft_in_rx) = mpsc::channel(channel_size);

        let external_client_ingress_id = IngressId(
            "127.0.0.1:0"
                .parse()
                .expect("Loopback address needs to be valid."),
        );

        let (ingress_dispatcher_loop, external_client_ingress) = ingress_grpc.build(
            // TODO replace with proper network address once we have a distributed runtime
            external_client_ingress_id,
            schemas.clone(),
            channel_size,
        );

        let partition_table = FixedConsecutivePartitions::new(num_partition_processors);

        let network = network_integration::Network::new(
            raft_in_tx,
            ingress_dispatcher_loop.create_response_sender(),
            partition_table.clone(),
            channel_size,
        );
        let network_ingress_sender = network.create_ingress_sender();

        let mut consensus =
            Consensus::new(raft_in_rx, network.create_consensus_sender(), channel_size);

        let network_handle = network.create_network_handle();

        let rocksdb = storage_rocksdb.build()?;

        let storage_query = storage_query.build(rocksdb.clone());

        let invoker_storage_reader = InvokerStorageReader::new(rocksdb.clone());
        let invoker = opts.invoker.build(
            invoker_storage_reader.clone(),
            invoker_storage_reader,
            EntryEnricher::new(schemas.clone()),
            schemas.clone(),
        );

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
                    rocksdb.clone(),
                    schemas.clone(),
                )
            })
            .unzip();

        consensus.register_state_machines(command_senders);

        let services = Services::new(
            consensus.create_proposal_sender(),
            partition_table,
            channel_size,
        );

        Ok(Self {
            consensus,
            processors,
            network,
            storage_query,
            invoker,
            external_client_ingress_runner: ExternalClientIngressRunner::new(
                external_client_ingress,
                ingress_dispatcher_loop,
                network_ingress_sender,
            ),
            services,
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
        network_handle: UnboundedNetworkHandle<shuffle::ShuffleInput, shuffle::ShuffleOutput>,
        ack_sender: PartitionProcessorSender<partition::AckResponse>,
        rocksdb_storage: RocksDBStorage,
        schemas: Schemas,
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
        );

        ((peer_id, command_tx), processor)
    }

    pub fn worker_command_tx(&self) -> impl restate_worker_api::Handle + Send + Sync {
        self.services.worker_command_tx()
    }

    pub async fn run(self, drain: drain::Watch) -> Result<(), Error> {
        let (shutdown_signal, shutdown_watch) = drain::channel();

        let mut external_client_ingress_handle = tokio::spawn(
            self.external_client_ingress_runner
                .run(shutdown_watch.clone()),
        );
        let mut invoker_handle = tokio::spawn(self.invoker.run(shutdown_watch.clone()));
        let mut network_handle = tokio::spawn(self.network.run(shutdown_watch.clone()));
        let mut storage_query_handle = tokio::spawn(self.storage_query.run(shutdown_watch.clone()));
        let mut consensus_handle = tokio::spawn(self.consensus.run());
        let mut processors_handles: FuturesUnordered<_> = self
            .processors
            .into_iter()
            .map(|partition_processor| tokio::spawn(partition_processor.run()))
            .collect();
        let mut services_handle = tokio::spawn(self.services.run(shutdown_watch));

        let shutdown = drain.signaled();

        tokio::select! {
            _ = shutdown => {
                debug!("Initiating shutdown of worker");

                // first we shut down the network which shuts down the consensus which shuts
                // down the partition processors transitively
                shutdown_signal.drain().await;

                // ignored because we are shutting down
                let _ = join!(
                    network_handle,
                    storage_query_handle,
                    consensus_handle,
                    processors_handles.collect::<Vec<_>>(),
                    invoker_handle,
                    external_client_ingress_handle,
                    services_handle);

                debug!("Completed shutdown of worker");
            },
            invoker_result = &mut invoker_handle => {
                invoker_result.map_err(|err| Error::component_panic("invoker", err))?;
                panic!("Unexpected termination of invoker.");
            },
            network_result = &mut network_handle => {
                network_result.map_err(|err| Error::component_panic("network", err))??;
                panic!("Unexpected termination of network.");
            },
            storage_query_result = &mut storage_query_handle => {
                storage_query_result.map_err(|err| Error::component_panic("storage query", err))??;
                panic!("Unexpected termination of storage query.");
            },
            consensus_result = &mut consensus_handle => {
                consensus_result
                .map_err(|err| Error::component_panic("consensus", err))?
                .map_err(Error::Consensus)?;
                panic!("Unexpected termination of consensus.");
            },
            processor_result = processors_handles.next() => {
                processor_result
                .ok_or(Error::NoPartitionProcessorRunning)?
                .map_err(|err| Error::component_panic("partition processor", err))?
                .map_err(Error::PartitionProcessor)?;
                panic!("Unexpected termination of one of the partition processors.");
            },
            external_client_ingress_result = &mut external_client_ingress_handle => {
                external_client_ingress_result.map_err(|err| Error::component_panic("external client ingress", err))??;
                panic!("Unexpected termination of external client ingress.");
            },
            services_result = &mut services_handle => {
                services_result.map_err(|err| Error::component_panic("worker services", err))??;
                panic!("Unexpected termination of worker services.");
            },
        }

        Ok(())
    }
}
