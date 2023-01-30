use common::types::PeerId;
use consensus::{Consensus, ProposalSender, Targeted};
use futures::stream::FuturesUnordered;
use futures::TryStreamExt;
use network::Network;
use storage_rocksdb::RocksDBStorage;
use tokio::sync::mpsc;
use tokio::try_join;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::PollSender;
use util::IdentitySender;

mod partition;
mod util;

type ConsensusCommand = consensus::Command<partition::Command>;
type PartitionProcessor = partition::PartitionProcessor<
    ReceiverStream<ConsensusCommand>,
    IdentitySender<partition::Command>,
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

        let (command_senders, processors): (Vec<_>, Vec<_>) = (0..num_partition_processors)
            .map(|idx| {
                let proposal_sender = consensus.create_proposal_sender();
                Self::create_partition_processor(idx, proposal_sender, storage.clone())
            })
            .unzip();

        consensus.register_state_machines(command_senders);

        Self {
            consensus,
            processors,
            network,
            _storage: storage,
        }
    }

    fn create_partition_processor(
        id: PeerId,
        proposal_sender: ProposalSender<TargetedFsmCommand>,
        _storage: RocksDBStorage,
    ) -> ((PeerId, PollSender<ConsensusCommand>), PartitionProcessor) {
        let (command_tx, command_rx) = mpsc::channel(1);
        let processor = PartitionProcessor::new(
            id,
            ReceiverStream::new(command_rx),
            IdentitySender::new(id, proposal_sender),
            _storage,
        );

        ((id, PollSender::new(command_tx)), processor)
    }

    pub async fn run(self, drain: drain::Watch) {
        let network_handle = tokio::spawn(self.network.run(drain.clone()));
        let consensus_handle = tokio::spawn(self.consensus.run());
        let processors_handles: FuturesUnordered<_> = self
            .processors
            .into_iter()
            .map(|partition_processor| tokio::spawn(partition_processor.run()))
            .collect();

        // Only signal shutdown once all handles have been completed
        let _release_shutdown = drain.ignore_signaled();

        try_join!(
            network_handle,
            consensus_handle,
            processors_handles.try_collect::<Vec<_>>()
        )
        .expect("Worker component failed");
    }
}
