use common::types::PeerId;
use consensus::{Consensus, ProposalSender, Targeted};
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use invoker::Invoker;
use invoker::InvokerSender;
use network::Network;
use storage_rocksdb::RocksDBStorage;
use tokio::join;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::PollSender;
use tracing::debug;
use util::IdentitySender;

mod partition;
mod util;

type ConsensusCommand = consensus::Command<partition::Command>;
type PartitionProcessor = partition::PartitionProcessor<
    ReceiverStream<ConsensusCommand>,
    IdentitySender<partition::Command>,
    PollSender<invoker::Input>,
    RocksDBStorage,
>;
type TargetedFsmCommand = Targeted<partition::Command>;

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
}

#[derive(Debug)]
pub struct Worker {
    consensus: Consensus<
        partition::Command,
        PollSender<ConsensusCommand>,
        ReceiverStream<TargetedFsmCommand>,
        PollSender<TargetedFsmCommand>,
    >,
    processors: Vec<PartitionProcessor>,
    network: Network<TargetedFsmCommand, PollSender<TargetedFsmCommand>>,
    invoker: Invoker,
    _storage: RocksDBStorage,
}

impl Options {
    pub fn build(self) -> Worker {
        Worker::new(self)
    }
}

impl Worker {
    #[allow(clippy::new_without_default)]
    pub fn new(opts: Options) -> Self {
        let Options {
            channel_size,
            storage_rocksdb,
            ..
        } = opts;

        let storage = storage_rocksdb.build();
        let num_partition_processors = 10;
        let (raft_in_tx, raft_in_rx) = mpsc::channel(channel_size);

        let network = Network::new(PollSender::new(raft_in_tx));

        let mut consensus = Consensus::new(
            ReceiverStream::new(raft_in_rx),
            network.create_consensus_sender(),
        );

        let invoker = Invoker::new();

        let (command_senders, processors): (Vec<_>, Vec<_>) = (0..num_partition_processors)
            .map(|idx| {
                let proposal_sender = consensus.create_proposal_sender();
                let invoker_sender = invoker.create_sender();
                Self::create_partition_processor(
                    idx,
                    proposal_sender,
                    invoker_sender,
                    storage.clone(),
                )
            })
            .unzip();

        consensus.register_state_machines(command_senders);

        Self {
            consensus,
            processors,
            network,
            invoker,
            _storage: storage,
        }
    }

    fn create_partition_processor(
        peer_id: PeerId,
        proposal_sender: ProposalSender<TargetedFsmCommand>,
        invoker_sender: InvokerSender,
        storage: RocksDBStorage,
    ) -> ((PeerId, PollSender<ConsensusCommand>), PartitionProcessor) {
        let (command_tx, command_rx) = mpsc::channel(1);
        let processor = PartitionProcessor::new(
            peer_id,
            peer_id,
            ReceiverStream::new(command_rx),
            IdentitySender::new(peer_id, proposal_sender),
            invoker_sender,
            storage,
        );

        ((peer_id, PollSender::new(command_tx)), processor)
    }

    pub async fn run(self, drain: drain::Watch) {
        let (invoker_shutdown, invoker_drain) = drain::channel();
        let (network_shutdown, network_drain) = drain::channel();

        let mut invoker_handle = tokio::spawn(self.invoker.run(invoker_drain));
        let mut network_handle = tokio::spawn(self.network.run(network_drain));
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
                network_shutdown.drain().await;

                // ignored because we are shutting down
                let _ = join!(network_handle, consensus_handle, processors_handles.collect::<Vec<_>>());

                // at last we shut down the invoker which is safe to shut down once all partition
                // processors have shut down
                invoker_shutdown.drain().await;

                // ignored because we are shutting down
                let _ = invoker_handle.await;

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
            }
        }
    }
}
