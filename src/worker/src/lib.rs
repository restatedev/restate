extern crate core;

use crate::ingress_integration::{ExternalClientIngressRunner, IngressIntegrationError};
use crate::network_integration::FixedPartitionTable;
use crate::partition::storage::journal_reader::JournalReader;
use crate::range_partitioner::RangePartitioner;
use crate::service_invocation_factory::DefaultServiceInvocationFactory;
use crate::services::Services;
use codederror::CodedError;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use partition::ack::AckCommand;
use partition::shuffle;
use restate_common::types::{IngressId, PartitionKey, PeerId, PeerTarget};
use restate_common::worker_command::WorkerCommandSender;
use restate_consensus::Consensus;
use restate_ingress_grpc::ReflectionRegistry;
use restate_invoker::{Invoker, UnboundedInvokerInputSender};
use restate_network::{PartitionProcessorSender, UnboundedNetworkHandle};
use restate_service_key_extractor::KeyExtractorsRegistry;
use restate_service_metadata::{InMemoryMethodDescriptorRegistry, InMemoryServiceEndpointRegistry};
use restate_service_protocol::codec::ProtobufRawEntryCodec;
use restate_storage_grpc::StorageService;
use restate_storage_rocksdb::RocksDBStorage;
use std::ops::RangeInclusive;
use tokio::join;
use tokio::sync::mpsc;
use tracing::debug;
use util::IdentitySender;

mod ingress_integration;
mod network_integration;
mod partition;
mod range_partitioner;
mod service_invocation_factory;
mod services;
mod util;

type PartitionProcessorCommand = AckCommand;
type ConsensusCommand = restate_consensus::Command<PartitionProcessorCommand>;
type ConsensusMsg = PeerTarget<PartitionProcessorCommand>;
type PartitionProcessor = partition::PartitionProcessor<
    ProtobufRawEntryCodec,
    UnboundedInvokerInputSender,
    UnboundedNetworkHandle<shuffle::ShuffleInput, shuffle::ShuffleOutput>,
    KeyExtractorsRegistry,
>;

/// # Worker options
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "options_schema", schemars(rename = "WorkerOptions"))]
pub struct Options {
    /// # Bounded channel size
    #[cfg_attr(
        feature = "options_schema",
        schemars(default = "Options::default_channel_size")
    )]
    channel_size: usize,
    #[cfg_attr(feature = "options_schema", schemars(default))]
    timers: restate_timer::Options,
    #[cfg_attr(feature = "options_schema", schemars(default))]
    storage_grpc: restate_storage_grpc::Options,
    #[cfg_attr(feature = "options_schema", schemars(default))]
    storage_rocksdb: restate_storage_rocksdb::Options,
    #[cfg_attr(feature = "options_schema", schemars(default))]
    ingress_grpc: restate_ingress_grpc::Options,
    #[cfg_attr(feature = "options_schema", schemars(default))]
    invoker: restate_invoker::Options,
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
    partitions: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            channel_size: Options::default_channel_size(),
            timers: Default::default(),
            storage_grpc: Default::default(),
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

    fn default_partitions() -> usize {
        1024
    }

    pub fn storage_path(&self) -> &str {
        &self.storage_rocksdb.path
    }

    pub fn build(
        self,
        method_descriptor_registry: InMemoryMethodDescriptorRegistry,
        key_extractor_registry: KeyExtractorsRegistry,
        reflections_registry: ReflectionRegistry,
        service_endpoint_registry: InMemoryServiceEndpointRegistry,
    ) -> Result<Worker, BuildError> {
        Worker::new(
            self,
            method_descriptor_registry,
            key_extractor_registry,
            reflections_registry,
            service_endpoint_registry,
        )
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
    #[error("storage grpc failed: {0}")]
    StorageGrpc(
        #[from]
        #[code]
        restate_storage_grpc::Error,
    ),
    #[error("consensus failed: {0}")]
    #[code(unknown)]
    Consensus(#[from] anyhow::Error),
    #[error("external client ingress failed: {0}")]
    ExternalClientIngress(
        #[from]
        #[code]
        IngressIntegrationError,
    ),
    #[error("no partition processor is running")]
    #[code(unknown)]
    NoPartitionProcessorRunning,
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
    storage_grpc: StorageService,
    invoker: Invoker<
        ProtobufRawEntryCodec,
        JournalReader<RocksDBStorage>,
        InMemoryServiceEndpointRegistry,
    >,
    external_client_ingress_runner: ExternalClientIngressRunner,
    services: Services,
}

impl Worker {
    pub fn new(
        opts: Options,
        method_descriptor_registry: InMemoryMethodDescriptorRegistry,
        key_extractor_registry: KeyExtractorsRegistry,
        reflections_registry: ReflectionRegistry,
        service_endpoint_registry: InMemoryServiceEndpointRegistry,
    ) -> Result<Self, BuildError> {
        let Options {
            channel_size,
            ingress_grpc,
            timers,
            storage_grpc,
            storage_rocksdb,
            ..
        } = opts;

        let num_partition_processors = opts.partitions as u64;
        let (raft_in_tx, raft_in_rx) = mpsc::channel(channel_size);

        let external_client_ingress_id = IngressId(
            "127.0.0.1:0"
                .parse()
                .expect("Loopback address needs to be valid."),
        );

        let invocation_factory =
            DefaultServiceInvocationFactory::new(key_extractor_registry.clone());

        let (ingress_dispatcher_loop, external_client_ingress) = ingress_grpc.build(
            // TODO replace with proper network address once we have a distributed runtime
            external_client_ingress_id,
            method_descriptor_registry,
            invocation_factory,
            reflections_registry,
            channel_size,
        );

        let partition_table = FixedPartitionTable::new(num_partition_processors);

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

        let storage_grpc = storage_grpc.build(rocksdb.clone());

        let invoker = opts.invoker.build(
            JournalReader::new(rocksdb.clone()),
            service_endpoint_registry,
        );

        let range_partitioner = RangePartitioner::new(num_partition_processors);

        let (command_senders, processors): (Vec<_>, Vec<_>) = range_partitioner
            .map(|(idx, partition_range)| {
                let proposal_sender = consensus.create_proposal_sender();
                let invoker_sender = invoker.create_sender();

                Self::create_partition_processor(
                    idx,
                    partition_range,
                    timers.clone(),
                    channel_size,
                    proposal_sender,
                    invoker_sender,
                    network_handle.clone(),
                    network.create_partition_processor_sender(),
                    key_extractor_registry.clone(),
                    rocksdb.clone(),
                )
            })
            .unzip();

        consensus.register_state_machines(command_senders);

        let services = Services::new(consensus.create_proposal_sender(), partition_table);

        Ok(Self {
            consensus,
            processors,
            network,
            storage_grpc,
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
        invoker_sender: UnboundedInvokerInputSender,
        network_handle: UnboundedNetworkHandle<shuffle::ShuffleInput, shuffle::ShuffleOutput>,
        ack_sender: PartitionProcessorSender<partition::AckResponse>,
        key_extractor: KeyExtractorsRegistry,
        rocksdb_storage: RocksDBStorage,
    ) -> ((PeerId, mpsc::Sender<ConsensusCommand>), PartitionProcessor) {
        let (command_tx, command_rx) = mpsc::channel(1);
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
            key_extractor,
            rocksdb_storage,
        );

        ((peer_id, command_tx), processor)
    }

    pub fn worker_command_tx(&self) -> WorkerCommandSender {
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
        let mut storage_grpc_handle = tokio::spawn(self.storage_grpc.run(shutdown_watch.clone()));
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
                    storage_grpc_handle,
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
            storage_grpc_result = &mut storage_grpc_handle => {
                storage_grpc_result.map_err(|err| Error::component_panic("storage grpc", err))??;
                panic!("Unexpected termination of storage grpc.");
            },
            consensus_result = &mut consensus_handle => {
                consensus_result.map_err(|err| Error::component_panic("consensus", err))??;
                panic!("Unexpected termination of consensus.");
            },
            processor_result = processors_handles.next() => {
                processor_result
                .ok_or(Error::NoPartitionProcessorRunning)?
                .map_err(|err| Error::component_panic("partition processor", err))??;
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
