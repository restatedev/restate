use crate::{
    ConsensusOrIngressTarget, KeyedMessage, NetworkCommand, PartitionTable, PartitionTableError,
    ShuffleOrIngressTarget, TargetConsensusOrIngress, TargetShuffle, TargetShuffleOrIngress,
    UnboundedNetworkHandle,
};
use common::partitioner::HashPartitioner;
use common::types::{PeerId, PeerTarget};
use std::collections::HashMap;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::SendError;
use tokio_util::sync::PollSender;
use tracing::{debug, trace};

pub type ConsensusSender<T> = PollSender<PeerTarget<T>>;

pub type IngressSender<T> = mpsc::Sender<T>;

pub type PartitionProcessorSender<T> = mpsc::Sender<T>;

/// Component which is responsible for routing messages from different components.
#[derive(Debug)]
pub struct Network<
    ConsensusMsg,
    ShuffleIn,
    ShuffleOut,
    ShuffleToCon,
    ShuffleToIngress,
    IngressOut,
    IngressIn,
    PPOut,
    PPToShuffle,
    PPToIngress,
    PartitionTable,
> {
    /// Receiver for messages from the consensus module
    consensus_in_rx: mpsc::Receiver<PeerTarget<ConsensusMsg>>,

    /// Sender for messages to the consensus module
    consensus_tx: mpsc::Sender<PeerTarget<ConsensusMsg>>,

    network_command_rx: mpsc::UnboundedReceiver<NetworkCommand<ShuffleIn>>,

    shuffle_rx: mpsc::Receiver<ShuffleOut>,

    ingress_in_rx: mpsc::Receiver<IngressOut>,

    ingress_tx: mpsc::Sender<IngressIn>,

    partition_processor_rx: mpsc::Receiver<PPOut>,

    partition_table: PartitionTable,

    // used for creating the ConsensusSender
    consensus_in_tx: mpsc::Sender<PeerTarget<ConsensusMsg>>,

    // used for creating the network handle
    network_command_tx: mpsc::UnboundedSender<NetworkCommand<ShuffleIn>>,
    shuffle_tx: mpsc::Sender<ShuffleOut>,

    // used for creating the ingress sender
    ingress_in_tx: mpsc::Sender<IngressOut>,

    // used for creating the partition processor sender
    partition_processor_tx: mpsc::Sender<PPOut>,

    _shuffle_to_ingress: PhantomData<ShuffleToIngress>,
    _shuffle_to_con: PhantomData<ShuffleToCon>,
    _partition_processor_to_ingress: PhantomData<PPToIngress>,
    _partition_processor_to_shuffle: PhantomData<PPToShuffle>,
}

impl<
        ConsensusMsg,
        ShuffleIn,
        ShuffleOut,
        ShuffleToCon,
        ShuffleToIngress,
        IngressOut,
        IngressIn,
        PPOut,
        PPToShuffle,
        PPToIngress,
        PartitionTable,
    >
    Network<
        ConsensusMsg,
        ShuffleIn,
        ShuffleOut,
        ShuffleToCon,
        ShuffleToIngress,
        IngressOut,
        IngressIn,
        PPOut,
        PPToShuffle,
        PPToIngress,
        PartitionTable,
    >
where
    ConsensusMsg: Debug + Send + Sync + 'static,
    ShuffleIn: Debug + Send + Sync + 'static,
    ShuffleOut: TargetConsensusOrIngress<ShuffleToCon, ShuffleToIngress>,
    ShuffleToCon: KeyedMessage + Into<ConsensusMsg> + Debug,
    ShuffleToIngress: Into<IngressIn> + Debug,
    IngressOut: KeyedMessage + Into<ConsensusMsg> + Debug,
    IngressIn: Debug + Send + Sync + 'static,
    PPOut: TargetShuffleOrIngress<PPToShuffle, PPToIngress>,
    PPToShuffle: TargetShuffle + Into<ShuffleIn> + Debug,
    PPToIngress: Into<IngressIn> + Debug,
    PartitionTable: crate::PartitionTable + Clone,
{
    pub fn new(
        consensus_tx: mpsc::Sender<PeerTarget<ConsensusMsg>>,
        ingress_tx: mpsc::Sender<IngressIn>,
        partition_table: PartitionTable,
    ) -> Self {
        let (consensus_in_tx, consensus_in_rx) = mpsc::channel(64);
        let (shuffle_tx, shuffle_rx) = mpsc::channel(64);
        let (ingress_in_tx, ingress_in_rx) = mpsc::channel(64);
        let (partition_processor_tx, partition_processor_rx) = mpsc::channel(64);
        let (network_command_tx, network_command_rx) = mpsc::unbounded_channel();

        Self {
            consensus_tx,
            consensus_in_rx,
            consensus_in_tx,
            network_command_rx,
            network_command_tx,
            shuffle_rx,
            shuffle_tx,
            ingress_in_rx,
            ingress_tx,
            ingress_in_tx,
            partition_processor_rx,
            partition_processor_tx,
            partition_table,
            _shuffle_to_con: Default::default(),
            _shuffle_to_ingress: Default::default(),
            _partition_processor_to_ingress: Default::default(),
            _partition_processor_to_shuffle: Default::default(),
        }
    }

    pub fn create_consensus_sender(&self) -> ConsensusSender<ConsensusMsg> {
        PollSender::new(self.consensus_in_tx.clone())
    }

    pub fn create_network_handle(&self) -> UnboundedNetworkHandle<ShuffleIn, ShuffleOut> {
        UnboundedNetworkHandle::new(self.network_command_tx.clone(), self.shuffle_tx.clone())
    }

    pub fn create_ingress_sender(&self) -> IngressSender<IngressOut> {
        self.ingress_in_tx.clone()
    }

    pub fn create_partition_processor_sender(&self) -> PartitionProcessorSender<PPOut> {
        self.partition_processor_tx.clone()
    }

    pub async fn run(self, drain: drain::Watch) -> anyhow::Result<()> {
        let Network {
            consensus_in_rx,
            consensus_tx,
            ingress_tx,
            mut network_command_rx,
            shuffle_rx,
            ingress_in_rx,
            partition_processor_rx,
            partition_table,
            ..
        } = self;

        debug!("Run network.");

        let shutdown = drain.signaled();
        let shuffles: Arc<Mutex<HashMap<PeerId, mpsc::Sender<ShuffleIn>>>> =
            Arc::new(Mutex::new(HashMap::new()));

        let mut consensus_forwarder =
            ConsensusForwarder::new(consensus_in_rx, consensus_tx.clone());
        let mut shuffle_router = ShuffleRouter::new(
            shuffle_rx,
            consensus_tx.clone(),
            ingress_tx.clone(),
            partition_table.clone(),
        );
        let mut ingress_forwarder =
            IngressForwarder::new(ingress_in_rx, consensus_tx.clone(), partition_table.clone());
        let mut partition_processor_router = PartitionProcessorRouter::new(
            partition_processor_rx,
            ingress_tx.clone(),
            Arc::clone(&shuffles),
        );

        tokio::pin!(shutdown);

        loop {
            tokio::select! {
                result = consensus_forwarder.run() => {
                    result?
                },
                result = shuffle_router.run() => {
                    result?
                },
                result = ingress_forwarder.run() => {
                    result?
                },
                result = partition_processor_router.run() => {
                    result?
                },
                command = network_command_rx.recv() => {
                    let command = command.expect("Network owns the command sender, that's why the receiver will never be closed.");
                    match command {
                        NetworkCommand::RegisterShuffle { peer_id, shuffle_tx } => {
                            trace!(shuffle_id = peer_id, "Register new shuffle.");
                            shuffles.lock().unwrap().insert(peer_id, shuffle_tx);
                        },
                        NetworkCommand::UnregisterShuffle { peer_id } => {
                            trace!(shuffle_id = peer_id, "Unregister shuffle.");
                            shuffles.lock().unwrap().remove(&peer_id);
                        }
                    };
                },
                _ = &mut shutdown => {
                    debug!("Shutting network down.");
                    break;
                }
            }
        }

        Ok(())
    }
}

async fn lookup_target_peer(
    msg: &impl KeyedMessage,
    partition_table: &impl PartitionTable,
) -> Result<PeerId, PartitionTableError> {
    let partition_key = HashPartitioner::compute_partition_key(&msg.routing_key());
    partition_table
        .partition_key_to_target_peer(partition_key)
        .await
}

struct ConsensusForwarder<T> {
    receiver: mpsc::Receiver<T>,
    sender: mpsc::Sender<T>,
}

impl<T> ConsensusForwarder<T>
where
    T: Debug,
{
    fn new(receiver: mpsc::Receiver<T>, sender: mpsc::Sender<T>) -> Self {
        Self { receiver, sender }
    }

    async fn run(&mut self) -> Result<(), SendError<T>> {
        while let Some(message) = self.receiver.recv().await {
            trace!(?message, "Forwarding consensus message to itself.");
            self.sender.send(message).await?
        }

        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
enum ShuffleRouterError<C, I> {
    #[error("failed resolving target peer: {0}")]
    TargetPeerResolution(#[from] PartitionTableError),
    #[error("failed routing message to consensus: {0}")]
    RoutingToConsensus(SendError<PeerTarget<C>>),
    #[error("failed routing message to ingress: {0}")]
    RoutingToIngress(SendError<I>),
}

struct ShuffleRouter<ShuffleMsg, ShuffleToCon, ShuffleToIngress, ConsensusMsg, IngressMsg, P> {
    receiver: mpsc::Receiver<ShuffleMsg>,
    consensus_tx: mpsc::Sender<PeerTarget<ConsensusMsg>>,
    ingress_tx: mpsc::Sender<IngressMsg>,
    partition_table: P,

    _shuffle_to_consensus: PhantomData<ShuffleToCon>,
    _shuffle_to_ingress: PhantomData<ShuffleToIngress>,
}

impl<ShuffleMsg, ShuffleToCon, ShuffleToIngress, ConsensusMsg, IngressMsg, P>
    ShuffleRouter<ShuffleMsg, ShuffleToCon, ShuffleToIngress, ConsensusMsg, IngressMsg, P>
where
    ShuffleMsg: TargetConsensusOrIngress<ShuffleToCon, ShuffleToIngress>,
    ShuffleToCon: KeyedMessage + Into<ConsensusMsg> + Debug,
    ShuffleToIngress: Into<IngressMsg> + Debug,
    P: PartitionTable,
{
    fn new(
        receiver: mpsc::Receiver<ShuffleMsg>,
        consensus_tx: mpsc::Sender<PeerTarget<ConsensusMsg>>,
        ingress_tx: mpsc::Sender<IngressMsg>,
        partition_table: P,
    ) -> Self {
        Self {
            receiver,
            consensus_tx,
            ingress_tx,
            partition_table,
            _shuffle_to_ingress: Default::default(),
            _shuffle_to_consensus: Default::default(),
        }
    }

    async fn run(&mut self) -> Result<(), ShuffleRouterError<ConsensusMsg, IngressMsg>> {
        while let Some(message) = self.receiver.recv().await {
            match message.target() {
                ConsensusOrIngressTarget::Consensus(msg) => {
                    let target_peer = lookup_target_peer(&msg, &self.partition_table).await?;

                    trace!(target_peer, message = ?msg, "Routing shuffle message to consensus.");

                    self.consensus_tx
                        .send((target_peer, msg.into()))
                        .await
                        .map_err(ShuffleRouterError::RoutingToConsensus)?
                }
                ConsensusOrIngressTarget::Ingress(msg) => {
                    trace!(message = ?msg, "Routing shuffle message to ingress.");
                    self.ingress_tx
                        .send(msg.into())
                        .await
                        .map_err(ShuffleRouterError::RoutingToIngress)?
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
enum IngressForwarderError<C> {
    #[error("failed resolving target peer: {0}")]
    TargetPeerResolution(#[from] PartitionTableError),
    #[error("failed forwarding message: {0}")]
    ForwardingMessage(#[from] SendError<PeerTarget<C>>),
}

struct IngressForwarder<I, C, P> {
    receiver: mpsc::Receiver<I>,
    consensus_tx: mpsc::Sender<PeerTarget<C>>,
    partition_table: P,
}

impl<I, C, P> IngressForwarder<I, C, P>
where
    I: KeyedMessage + Into<C> + Debug,
    P: PartitionTable,
{
    fn new(
        receiver: mpsc::Receiver<I>,
        consensus_tx: mpsc::Sender<PeerTarget<C>>,
        partition_table: P,
    ) -> Self {
        Self {
            receiver,
            consensus_tx,
            partition_table,
        }
    }

    async fn run(&mut self) -> Result<(), IngressForwarderError<C>> {
        while let Some(message) = self.receiver.recv().await {
            let target_peer = lookup_target_peer(&message, &self.partition_table).await?;

            trace!(?message, "Forwarding ingress message to consensus.");

            self.consensus_tx
                .send((target_peer, message.into()))
                .await?
        }

        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
enum PartitionProcessorRouterError<I> {
    #[error("failed routing to ingress: {0}")]
    RoutingToIngress(#[from] SendError<I>),
}

struct PartitionProcessorRouter<PPMsg, PPToShuffle, PPToIngress, ShuffleMsg, IngressMsg> {
    receiver: mpsc::Receiver<PPMsg>,
    ingress_tx: mpsc::Sender<IngressMsg>,
    shuffle_txs: Arc<Mutex<HashMap<PeerId, mpsc::Sender<ShuffleMsg>>>>,

    _pp_to_shuffle: PhantomData<PPToShuffle>,
    _pp_to_ingress: PhantomData<PPToIngress>,
}

impl<PPMsg, PPToShuffle, PPToIngress, ShuffleMsg, IngressMsg>
    PartitionProcessorRouter<PPMsg, PPToShuffle, PPToIngress, ShuffleMsg, IngressMsg>
where
    PPMsg: TargetShuffleOrIngress<PPToShuffle, PPToIngress>,
    PPToShuffle: TargetShuffle + Into<ShuffleMsg> + Debug,
    PPToIngress: Into<IngressMsg> + Debug,
{
    fn new(
        receiver: mpsc::Receiver<PPMsg>,
        ingress_tx: mpsc::Sender<IngressMsg>,
        shuffle_txs: Arc<Mutex<HashMap<PeerId, mpsc::Sender<ShuffleMsg>>>>,
    ) -> Self {
        Self {
            receiver,
            ingress_tx,
            shuffle_txs,
            _pp_to_ingress: Default::default(),
            _pp_to_shuffle: Default::default(),
        }
    }

    async fn run(&mut self) -> Result<(), PartitionProcessorRouterError<IngressMsg>> {
        while let Some(message) = self.receiver.recv().await {
            match message.target() {
                ShuffleOrIngressTarget::Shuffle(msg) => {
                    let shuffle_target = msg.shuffle_target();

                    let shuffle_tx = self
                        .shuffle_txs
                        .lock()
                        .unwrap()
                        .get(&shuffle_target)
                        .cloned();

                    if let Some(shuffle_tx) = shuffle_tx {
                        trace!(message = ?msg, "Routing partition processor message to shuffle.");
                        // can fail if the shuffle was deregistered in the meantime
                        let _ = shuffle_tx.send(msg.into()).await;
                    } else {
                        debug!(
                            "Unknown shuffle target {shuffle_target}. Ignoring message {msg:?}."
                        );
                    }
                }
                ShuffleOrIngressTarget::Ingress(msg) => {
                    trace!(message = ?msg, "Routing partition processor message to ingress.");
                    self.ingress_tx
                        .send(msg.into())
                        .await
                        .map_err(PartitionProcessorRouterError::RoutingToIngress)?;
                }
            }
        }

        Ok(())
    }
}
