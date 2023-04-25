extern crate core;

use crate::ingress_integration::ExternalClientIngressRunner;
use crate::network_integration::FixedPartitionTable;
use crate::partition::storage::journal_reader::JournalReader;
use crate::range_partitioner::RangePartitioner;
use crate::service_invocation_factory::DefaultServiceInvocationFactory;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use partition::ack::AckCommand;
use partition::shuffle;
use restate_common::types::{IngressId, PartitionKey, PeerId, PeerTarget};
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
        }
    }
}

impl Options {
    fn default_channel_size() -> usize {
        64
    }

    pub fn build(
        self,
        method_descriptor_registry: InMemoryMethodDescriptorRegistry,
        key_extractor_registry: KeyExtractorsRegistry,
        reflections_registry: ReflectionRegistry,
        service_endpoint_registry: InMemoryServiceEndpointRegistry,
    ) -> Worker {
        Worker::new(
            self,
            method_descriptor_registry,
            key_extractor_registry,
            reflections_registry,
            service_endpoint_registry,
        )
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
}

impl Worker {
    pub fn new(
        opts: Options,
        method_descriptor_registry: InMemoryMethodDescriptorRegistry,
        key_extractor_registry: KeyExtractorsRegistry,
        reflections_registry: ReflectionRegistry,
        service_endpoint_registry: InMemoryServiceEndpointRegistry,
    ) -> Self {
        let Options {
            channel_size,
            ingress_grpc,
            timers,
            storage_grpc,
            storage_rocksdb,
            ..
        } = opts;

        let num_partition_processors = 10;
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
        );

        let network = network_integration::Network::new(
            raft_in_tx,
            ingress_dispatcher_loop.create_response_sender(),
            FixedPartitionTable::new(num_partition_processors),
            channel_size,
        );
        let network_ingress_sender = network.create_ingress_sender();

        let mut consensus =
            Consensus::new(raft_in_rx, network.create_consensus_sender(), channel_size);

        let network_handle = network.create_network_handle();

        let rocksdb = storage_rocksdb.build();

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

        Self {
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
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn create_partition_processor(
        peer_id: PeerId,
        partition_key_range: RangeInclusive<PartitionKey>,
        timer_service_options: restate_timer::Options,
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

    pub async fn run(self, drain: drain::Watch) {
        let (shutdown_signal, shutdown_watch) = drain::channel();

        let mut external_client_ingress_handle = tokio::spawn(
            self.external_client_ingress_runner
                .run(shutdown_watch.clone()),
        );
        let mut invoker_handle = tokio::spawn(self.invoker.run(shutdown_watch.clone()));
        let mut network_handle = tokio::spawn(self.network.run(shutdown_watch.clone()));
        let mut storage_grpc_handle = tokio::spawn(self.storage_grpc.run(shutdown_watch));
        let mut consensus_handle = tokio::spawn(self.consensus.run());
        let mut processors_handles: FuturesUnordered<_> = self
            .processors
            .into_iter()
            .map(|partition_processor| tokio::spawn(partition_processor.run()))
            .collect();

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
                    external_client_ingress_handle);

                debug!("Completed shutdown of worker");
            },
            invoker_result = &mut invoker_handle => {
                panic!("Invoker stopped running: {invoker_result:?}");
            },
            network_result = &mut network_handle => {
                panic!("Network stopped running: {network_result:?}");
            },
            storage_grpc_result = &mut storage_grpc_handle => {
                panic!("Storage GRPC stopped running: {storage_grpc_result:?}");
            },
            consensus_result = &mut consensus_handle => {
                panic!("Consensus stopped running: {consensus_result:?}");
            },
            processor_result = processors_handles.next() => {
                panic!("One partition processor stopped running: {processor_result:?}");
            },
            external_client_ingress_result = &mut external_client_ingress_handle => {
                panic!("External client ingress stopped running: {external_client_ingress_result:?}");
            },
        }
    }
}
