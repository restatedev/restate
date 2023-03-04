use crate::ingress_integration::ExternalClientIngressRunner;
use crate::network_integration::FixedPartitionTable;
use crate::service_invocation_factory::DefaultServiceInvocationFactory;
use common::retry_policy::RetryPolicy;
use common::types::{IngressId, PeerId, PeerTarget};
use consensus::Consensus;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use ingress_grpc::{InMemoryMethodDescriptorRegistry, IngressDispatcherLoop};
use invoker::{EndpointMetadata, Invoker, UnboundedInvokerInputSender};
use network::{PartitionProcessorSender, UnboundedNetworkHandle};
use partition::ack::AckableCommand;
use partition::shuffle;
use partition::RocksDBJournalReader;
use service_key_extractor::KeyExtractorsRegistry;
use service_protocol::codec::ProtobufRawEntryCodec;
use std::collections::HashMap;
use storage_rocksdb::RocksDBStorage;
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
    RocksDBStorage,
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
    storage_rocksdb: storage_rocksdb::Options,

    #[command(flatten)]
    external_client_ingress: ingress_grpc::Options,
}

pub struct Worker {
    consensus: Consensus<PartitionProcessorCommand>,
    processors: Vec<PartitionProcessor>,
    network: network_integration::Network,
    invoker:
        Invoker<ProtobufRawEntryCodec, RocksDBJournalReader, HashMap<String, EndpointMetadata>>,
    external_client_ingress_runner: ExternalClientIngressRunner,
}

impl Options {
    pub fn build(self) -> Worker {
        Worker::new(self)
    }
}

impl Worker {
    pub fn new(opts: Options) -> Self {
        let Options {
            channel_size,
            storage_rocksdb,
            external_client_ingress,
            ..
        } = opts;

        let storage = storage_rocksdb.build();
        let num_partition_processors = 10;
        let (raft_in_tx, raft_in_rx) = mpsc::channel(channel_size);

        let ingress_dispatcher_loop = IngressDispatcherLoop::default();

        let network = network_integration::Network::new(
            raft_in_tx,
            ingress_dispatcher_loop.create_response_sender(),
            FixedPartitionTable::new(num_partition_processors),
        );
        let network_ingress_sender = network.create_ingress_sender();

        let method_descriptor_registry = InMemoryMethodDescriptorRegistry::default();
        let key_extractor_registry = KeyExtractorsRegistry::default();
        let invocation_factory = DefaultServiceInvocationFactory::new(key_extractor_registry);

        let external_client_ingress = external_client_ingress.build(
            // TODO replace with proper network address once we have a distributed runtime
            IngressId(
                "127.0.0.1:0"
                    .parse()
                    .expect("Loopback address needs to be valid."),
            ),
            method_descriptor_registry,
            invocation_factory,
            ingress_dispatcher_loop.create_command_sender(),
        );

        let mut consensus = Consensus::new(raft_in_rx, network.create_consensus_sender());

        let network_handle = network.create_network_handle();

        let invoker = Invoker::new(RetryPolicy::None, RocksDBJournalReader, Default::default());

        let (command_senders, processors): (Vec<_>, Vec<_>) = (0..num_partition_processors)
            .map(|idx| {
                let proposal_sender = consensus.create_proposal_sender();
                let invoker_sender = invoker.create_sender();
                Self::create_partition_processor(
                    idx,
                    proposal_sender,
                    invoker_sender,
                    storage.clone(),
                    network_handle.clone(),
                    network.create_partition_processor_sender(),
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

    fn create_partition_processor(
        peer_id: PeerId,
        proposal_sender: mpsc::Sender<ConsensusMsg>,
        invoker_sender: UnboundedInvokerInputSender,
        storage: RocksDBStorage,
        network_handle: UnboundedNetworkHandle<shuffle::ShuffleInput, shuffle::ShuffleOutput>,
        ack_sender: PartitionProcessorSender<partition::AckResponse>,
    ) -> ((PeerId, mpsc::Sender<ConsensusCommand>), PartitionProcessor) {
        let (command_tx, command_rx) = mpsc::channel(1);
        let processor = PartitionProcessor::new(
            peer_id,
            peer_id,
            command_rx,
            IdentitySender::new(peer_id, proposal_sender),
            invoker_sender,
            storage,
            network_handle,
            ack_sender,
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
                let _ = join!(network_handle, consensus_handle, processors_handles.collect::<Vec<_>>(), invoker_handle, external_client_ingress_handle);

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
            }
        }
    }
}
