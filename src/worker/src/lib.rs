extern crate core;

use crate::ingress_integration::ExternalClientIngressRunner;
use crate::network_integration::FixedPartitionTable;
use crate::partition::storage::memory::InMemoryJournalReader;
use crate::partition::storage::InMemoryPartitionStorage;
use crate::service_invocation_factory::DefaultServiceInvocationFactory;
use common::retry_policy::RetryPolicy;
use common::types::{IngressId, PeerId, PeerTarget};
use consensus::Consensus;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use invoker::{Invoker, UnboundedInvokerInputSender};
use network::{PartitionProcessorSender, UnboundedNetworkHandle};
use partition::ack::AckableCommand;
use partition::shuffle;
use service_key_extractor::KeyExtractorsRegistry;
use service_metadata::{InMemoryMethodDescriptorRegistry, InMemoryServiceEndpointRegistry};
use service_protocol::codec::ProtobufRawEntryCodec;
use tokio::join;
use tokio::sync::mpsc;
use tracing::debug;
use util::IdentitySender;

mod ingress_integration;
mod network_integration;
mod partition;
mod service_invocation_factory;
mod util;

type PartitionProcessorCommand = AckableCommand;
type ConsensusCommand = consensus::Command<PartitionProcessorCommand>;
type ConsensusMsg = PeerTarget<PartitionProcessorCommand>;
type PartitionProcessor = partition::PartitionProcessor<
    ProtobufRawEntryCodec,
    UnboundedInvokerInputSender,
    UnboundedNetworkHandle<shuffle::ShuffleInput, shuffle::ShuffleOutput>,
    KeyExtractorsRegistry,
>;

#[derive(Debug, clap::Parser)]
#[group(skip)]
pub struct Options {
    /// Bounded channel size
    #[arg(
        long = "worker-channel-size",
        env = "WORKER_CHANNEL_SIZE",
        default_value = "64"
    )]
    channel_size: usize,

    #[command(flatten)]
    timer_service: timer::Options,

    #[command(flatten)]
    storage_rocksdb: storage_rocksdb::Options,

    #[command(flatten)]
    external_client_ingress: ingress_grpc::Options,
}

pub struct Worker {
    consensus: Consensus<PartitionProcessorCommand>,
    processors: Vec<PartitionProcessor>,
    network: network_integration::Network,
    invoker: Invoker<ProtobufRawEntryCodec, InMemoryJournalReader, InMemoryServiceEndpointRegistry>,
    external_client_ingress_runner: ExternalClientIngressRunner,
}

impl Options {
    pub fn build(
        self,
        method_descriptor_registry: InMemoryMethodDescriptorRegistry,
        key_extractor_registry: KeyExtractorsRegistry,
        service_endpoint_registry: InMemoryServiceEndpointRegistry,
    ) -> Worker {
        Worker::new(
            self,
            method_descriptor_registry,
            key_extractor_registry,
            service_endpoint_registry,
        )
    }
}

impl Worker {
    pub fn new(
        opts: Options,
        method_descriptor_registry: InMemoryMethodDescriptorRegistry,
        key_extractor_registry: KeyExtractorsRegistry,
        service_endpoint_registry: InMemoryServiceEndpointRegistry,
    ) -> Self {
        let Options {
            channel_size,
            external_client_ingress,
            timer_service,
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

        let (ingress_dispatcher_loop, external_client_ingress) = external_client_ingress.build(
            // TODO replace with proper network address once we have a distributed runtime
            external_client_ingress_id,
            method_descriptor_registry,
            invocation_factory,
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

        let in_memory_journal_reader = InMemoryJournalReader::new();

        let invoker = Invoker::new(
            RetryPolicy::None,
            in_memory_journal_reader.clone(),
            service_endpoint_registry,
        );

        let (command_senders, processors): (Vec<_>, Vec<_>) = (0..num_partition_processors)
            .map(|idx| {
                let proposal_sender = consensus.create_proposal_sender();
                let invoker_sender = invoker.create_sender();
                let in_memory_storage = InMemoryPartitionStorage::new();
                in_memory_journal_reader.register(in_memory_storage.clone());
                Self::create_partition_processor(
                    idx,
                    timer_service.clone(),
                    proposal_sender,
                    invoker_sender,
                    network_handle.clone(),
                    network.create_partition_processor_sender(),
                    key_extractor_registry.clone(),
                    in_memory_storage,
                )
            })
            .unzip();

        consensus.register_state_machines(command_senders);

        Self {
            consensus,
            processors,
            network,
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
        timer_service_options: timer::Options,
        proposal_sender: mpsc::Sender<ConsensusMsg>,
        invoker_sender: UnboundedInvokerInputSender,
        network_handle: UnboundedNetworkHandle<shuffle::ShuffleInput, shuffle::ShuffleOutput>,
        ack_sender: PartitionProcessorSender<partition::AckResponse>,
        key_extractor: KeyExtractorsRegistry,
        in_memory_storage: InMemoryPartitionStorage,
    ) -> ((PeerId, mpsc::Sender<ConsensusCommand>), PartitionProcessor) {
        let (command_tx, command_rx) = mpsc::channel(1);
        let processor = PartitionProcessor::new(
            peer_id,
            peer_id,
            timer_service_options,
            command_rx,
            IdentitySender::new(peer_id, proposal_sender),
            invoker_sender,
            network_handle,
            ack_sender,
            key_extractor,
            in_memory_storage,
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
        let mut network_handle = tokio::spawn(self.network.run(shutdown_watch));
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
