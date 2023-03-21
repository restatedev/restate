use crate::{
    ConsensusOrIngressTarget, ConsensusOrShuffleTarget, NetworkCommand, PartitionTable,
    PartitionTableError, ShuffleOrIngressTarget, TargetConsensusOrIngress,
    TargetConsensusOrShuffle, TargetShuffle, TargetShuffleOrIngress, UnboundedNetworkHandle,
};
use common::partitioner::HashPartitioner;
use common::traits::KeyedMessage;
use common::types::{PeerId, PeerTarget};
use std::collections::HashMap;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::SendError;
use tracing::{debug, trace};

pub type ConsensusSender<T> = mpsc::Sender<PeerTarget<T>>;

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
    IngressToCon,
    IngressToShuffle,
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
    _ingress_to_shuffle: PhantomData<IngressToShuffle>,
    _ingress_to_consensus: PhantomData<IngressToCon>,
}

impl<
        ConsensusMsg,
        ShuffleIn,
        ShuffleOut,
        ShuffleToCon,
        ShuffleToIngress,
        IngressOut,
        IngressToCon,
        IngressToShuffle,
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
        IngressToCon,
        IngressToShuffle,
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
    IngressOut: TargetConsensusOrShuffle<IngressToCon, IngressToShuffle>,
    IngressToCon: KeyedMessage + Into<ConsensusMsg> + Debug,
    IngressToShuffle: TargetShuffle + Into<ShuffleIn> + Debug,
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
        channel_size: usize,
    ) -> Self {
        let (consensus_in_tx, consensus_in_rx) = mpsc::channel(channel_size);
        let (shuffle_tx, shuffle_rx) = mpsc::channel(channel_size);
        let (ingress_in_tx, ingress_in_rx) = mpsc::channel(channel_size);
        let (partition_processor_tx, partition_processor_rx) = mpsc::channel(channel_size);
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
            _ingress_to_consensus: Default::default(),
            _ingress_to_shuffle: Default::default(),
        }
    }

    pub fn create_consensus_sender(&self) -> ConsensusSender<ConsensusMsg> {
        self.consensus_in_tx.clone()
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

        let consensus_forwarder = ConsensusForwarder::new(consensus_in_rx, consensus_tx.clone());
        let shuffle_router = ShuffleRouter::new(
            shuffle_rx,
            consensus_tx.clone(),
            ingress_tx.clone(),
            partition_table.clone(),
        );
        let ingress_router = IngressRouter::new(
            ingress_in_rx,
            consensus_tx.clone(),
            Arc::clone(&shuffles),
            partition_table.clone(),
        );
        let partition_processor_router = PartitionProcessorRouter::new(
            partition_processor_rx,
            ingress_tx.clone(),
            Arc::clone(&shuffles),
        );

        let consensus_forwarder = consensus_forwarder.run();
        let shuffle_router = shuffle_router.run();
        let ingress_router = ingress_router.run();
        let partition_processor_router = partition_processor_router.run();

        tokio::pin!(
            shutdown,
            consensus_forwarder,
            shuffle_router,
            ingress_router,
            partition_processor_router
        );

        loop {
            tokio::select! {
                result = &mut consensus_forwarder => {
                    result?
                },
                result = &mut shuffle_router => {
                    result?
                },
                result = &mut ingress_router => {
                    result?
                },
                result = &mut partition_processor_router => {
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

    async fn run(mut self) -> Result<(), SendError<T>> {
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

    async fn run(mut self) -> Result<(), ShuffleRouterError<ConsensusMsg, IngressMsg>> {
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
enum IngressRouterError<C> {
    #[error("failed resolving target peer: {0}")]
    TargetPeerResolution(#[from] PartitionTableError),
    #[error("failed forwarding message: {0}")]
    ForwardingMessage(#[from] SendError<PeerTarget<C>>),
}

struct IngressRouter<I, ItoC, ItoS, C, S, P> {
    receiver: mpsc::Receiver<I>,
    consensus_tx: mpsc::Sender<PeerTarget<C>>,
    shuffle_txs: Arc<Mutex<HashMap<PeerId, mpsc::Sender<S>>>>,
    partition_table: P,

    _ingress_to_consensus: PhantomData<ItoC>,
    _ingress_to_shuffle: PhantomData<ItoS>,
}

impl<I, ItoC, ItoS, C, S, P> IngressRouter<I, ItoC, ItoS, C, S, P>
where
    I: TargetConsensusOrShuffle<ItoC, ItoS>,
    ItoS: TargetShuffle + Into<S> + Debug,
    ItoC: KeyedMessage + Into<C> + Debug,
    P: PartitionTable,
{
    fn new(
        receiver: mpsc::Receiver<I>,
        consensus_tx: mpsc::Sender<PeerTarget<C>>,
        shuffle_txs: Arc<Mutex<HashMap<PeerId, mpsc::Sender<S>>>>,
        partition_table: P,
    ) -> Self {
        Self {
            receiver,
            consensus_tx,
            partition_table,
            shuffle_txs,
            _ingress_to_shuffle: Default::default(),
            _ingress_to_consensus: Default::default(),
        }
    }

    async fn run(mut self) -> Result<(), IngressRouterError<C>> {
        while let Some(message) = self.receiver.recv().await {
            match message.target() {
                ConsensusOrShuffleTarget::Consensus(message) => {
                    let target_peer = lookup_target_peer(&message, &self.partition_table).await?;

                    trace!(?message, "Forwarding ingress message to consensus.");

                    self.consensus_tx
                        .send((target_peer, message.into()))
                        .await?
                }
                ConsensusOrShuffleTarget::Shuffle(message) => {
                    send_to_shuffle(message, &self.shuffle_txs).await;
                }
            }
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

    async fn run(mut self) -> Result<(), PartitionProcessorRouterError<IngressMsg>> {
        while let Some(message) = self.receiver.recv().await {
            match message.target() {
                ShuffleOrIngressTarget::Shuffle(msg) => {
                    send_to_shuffle(msg, &self.shuffle_txs).await
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

async fn send_to_shuffle<M: TargetShuffle + Into<S> + Debug, S>(
    message: M,
    shuffle_txs: &Arc<Mutex<HashMap<PeerId, mpsc::Sender<S>>>>,
) {
    let shuffle_target = message.shuffle_target();
    let shuffle_tx = shuffle_txs.lock().unwrap().get(&shuffle_target).cloned();

    if let Some(shuffle_tx) = shuffle_tx {
        trace!(?message, "Routing partition processor message to shuffle.");
        // can fail if the shuffle was deregistered in the meantime
        let _ = shuffle_tx.send(message.into()).await;
    } else {
        debug!("Unknown shuffle target {shuffle_target}. Ignoring message {message:?}.");
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ConsensusOrIngressTarget, ConsensusOrShuffleTarget, Network, NetworkHandle, PartitionTable,
        PartitionTableError, ShuffleOrIngressTarget, TargetConsensusOrIngress,
        TargetConsensusOrShuffle, TargetShuffle, TargetShuffleOrIngress,
    };
    use common::traits::KeyedMessage;
    use common::types::{PartitionKey, PeerId, PeerTarget};
    use std::fmt::Debug;
    use std::future;
    use test_utils::test;
    use tokio::sync::mpsc;

    #[derive(Debug, Default, Clone)]
    struct MockPartitionTable;

    impl PartitionTable for MockPartitionTable {
        type Future = future::Ready<Result<PeerId, PartitionTableError>>;

        fn partition_key_to_target_peer(&self, _partition_key: PartitionKey) -> Self::Future {
            let peer_id = 0;
            future::ready(Ok(peer_id))
        }
    }

    #[derive(Debug, PartialEq, Copy, Clone)]
    struct ConsensusMsg(u64);

    impl KeyedMessage for ConsensusMsg {
        type RoutingKey<'a> = u64;

        fn routing_key(&self) -> Self::RoutingKey<'_> {
            0
        }
    }

    #[derive(Debug, Copy, Clone, PartialEq)]
    struct IngressMsg(u64);

    #[derive(Debug, Copy, Clone, PartialEq)]
    struct ShuffleMsg(u64);

    impl TargetShuffle for ShuffleMsg {
        fn shuffle_target(&self) -> PeerId {
            0
        }
    }

    #[derive(Debug, Copy, Clone)]
    enum ShuffleOut {
        Consensus(ConsensusMsg),
        Ingress(IngressMsg),
    }

    impl TargetConsensusOrIngress<ConsensusMsg, IngressMsg> for ShuffleOut {
        fn target(self) -> ConsensusOrIngressTarget<ConsensusMsg, IngressMsg> {
            match self {
                ShuffleOut::Consensus(msg) => ConsensusOrIngressTarget::Consensus(msg),
                ShuffleOut::Ingress(msg) => ConsensusOrIngressTarget::Ingress(msg),
            }
        }
    }

    #[derive(Debug, Copy, Clone)]
    enum IngressOut {
        Consensus(ConsensusMsg),
        Shuffle(ShuffleMsg),
    }

    impl TargetConsensusOrShuffle<ConsensusMsg, ShuffleMsg> for IngressOut {
        fn target(self) -> ConsensusOrShuffleTarget<ConsensusMsg, ShuffleMsg> {
            match self {
                IngressOut::Consensus(msg) => ConsensusOrShuffleTarget::Consensus(msg),
                IngressOut::Shuffle(msg) => ConsensusOrShuffleTarget::Shuffle(msg),
            }
        }
    }

    #[derive(Debug, Copy, Clone)]
    enum PPOut {
        Shuffle(ShuffleMsg),
        Ingress(IngressMsg),
    }

    impl TargetShuffleOrIngress<ShuffleMsg, IngressMsg> for PPOut {
        fn target(self) -> ShuffleOrIngressTarget<ShuffleMsg, IngressMsg> {
            match self {
                PPOut::Shuffle(msg) => ShuffleOrIngressTarget::Shuffle(msg),
                PPOut::Ingress(msg) => ShuffleOrIngressTarget::Ingress(msg),
            }
        }
    }

    type MockNetwork = Network<
        ConsensusMsg,
        ShuffleMsg,
        ShuffleOut,
        ConsensusMsg,
        IngressMsg,
        IngressOut,
        ConsensusMsg,
        ShuffleMsg,
        IngressMsg,
        PPOut,
        ShuffleMsg,
        IngressMsg,
        MockPartitionTable,
    >;

    fn mock_network() -> (
        MockNetwork,
        mpsc::Receiver<PeerTarget<ConsensusMsg>>,
        mpsc::Receiver<IngressMsg>,
    ) {
        let (consensus_tx, consensus_rx) = mpsc::channel(1);
        let (ingress_tx, ingress_rx) = mpsc::channel(1);
        let partition_table = MockPartitionTable::default();

        let network = Network::<
            ConsensusMsg,
            ShuffleMsg,
            ShuffleOut,
            ConsensusMsg,
            IngressMsg,
            IngressOut,
            ConsensusMsg,
            ShuffleMsg,
            IngressMsg,
            PPOut,
            ShuffleMsg,
            IngressMsg,
            _,
        >::new(consensus_tx, ingress_tx, partition_table, 1);

        (network, consensus_rx, ingress_rx)
    }

    #[test(tokio::test)]
    async fn no_consensus_message_is_dropped() {
        let (network, mut consensus_rx, _ingress_rx) = mock_network();

        let network_handle = network.create_network_handle();
        let consensus_tx = network.create_consensus_sender();

        let (shutdown_signal, shutdown_watch) = drain::channel();

        let network_join_handle = tokio::spawn(network.run(shutdown_watch));

        let msg_1 = (0, ConsensusMsg(0));
        let msg_2 = (0, ConsensusMsg(1));
        let msg_3 = (0, ConsensusMsg(2));

        consensus_tx.send(msg_1).await.unwrap();
        consensus_tx.send(msg_2).await.unwrap();
        tokio::task::yield_now().await;
        network_handle.unregister_shuffle(0).await.unwrap();
        tokio::task::yield_now().await;
        consensus_tx.send(msg_3).await.unwrap();

        assert_eq!(consensus_rx.recv().await.unwrap(), msg_1);
        assert_eq!(consensus_rx.recv().await.unwrap(), msg_2);
        assert_eq!(consensus_rx.recv().await.unwrap(), msg_3);

        shutdown_signal.drain().await;
        network_join_handle.await.unwrap().unwrap();
    }

    #[test(tokio::test)]
    async fn no_shuffle_to_consensus_message_is_dropped() {
        let msg_1 = ConsensusMsg(0);
        let msg_2 = ConsensusMsg(1);
        let msg_3 = ConsensusMsg(2);

        let input = [
            ShuffleOut::Consensus(msg_1),
            ShuffleOut::Consensus(msg_2),
            ShuffleOut::Consensus(msg_3),
        ];
        let expected_output = [(0, msg_1), (0, msg_2), (0, msg_3)];

        let (network, consensus_rx, _ingress_rx) = mock_network();

        let shuffle_tx = network.create_network_handle().create_shuffle_sender();

        run_router_test(network, shuffle_tx, input, consensus_rx, expected_output).await;
    }

    #[test(tokio::test)]
    async fn no_shuffle_to_ingress_message_is_dropped() {
        let msg_1 = IngressMsg(0);
        let msg_2 = IngressMsg(1);
        let msg_3 = IngressMsg(2);

        let input = [
            ShuffleOut::Ingress(msg_1),
            ShuffleOut::Ingress(msg_2),
            ShuffleOut::Ingress(msg_3),
        ];
        let expected_output = [msg_1, msg_2, msg_3];

        let (network, _consensus_rx, ingress_rx) = mock_network();

        let shuffle_tx = network.create_network_handle().create_shuffle_sender();

        run_router_test(network, shuffle_tx, input, ingress_rx, expected_output).await;
    }

    #[test(tokio::test)]
    async fn no_ingress_to_shuffle_message_is_dropped() {
        let msg_1 = ShuffleMsg(0);
        let msg_2 = ShuffleMsg(1);
        let msg_3 = ShuffleMsg(2);

        let input = [
            IngressOut::Shuffle(msg_1),
            IngressOut::Shuffle(msg_2),
            IngressOut::Shuffle(msg_3),
        ];
        let expected_output = [msg_1, msg_2, msg_3];

        let (network, _consensus_rx, _ingress_rx) = mock_network();

        let network_handle = network.create_network_handle();

        let (shuffle_tx, shuffle_rx) = mpsc::channel(1);

        network_handle
            .register_shuffle(0, shuffle_tx)
            .await
            .unwrap();

        let ingress_tx = network.create_ingress_sender();

        run_router_test(network, ingress_tx, input, shuffle_rx, expected_output).await;
    }

    #[test(tokio::test)]
    async fn no_ingress_to_consensus_message_is_dropped() {
        let msg_1 = ConsensusMsg(0);
        let msg_2 = ConsensusMsg(1);
        let msg_3 = ConsensusMsg(2);

        let input = [
            IngressOut::Consensus(msg_1),
            IngressOut::Consensus(msg_2),
            IngressOut::Consensus(msg_3),
        ];
        let expected_output = [(0, msg_1), (0, msg_2), (0, msg_3)];

        let (network, consensus_rx, _ingress_rx) = mock_network();

        let ingress_tx = network.create_ingress_sender();

        run_router_test(network, ingress_tx, input, consensus_rx, expected_output).await;
    }

    #[test(tokio::test)]
    async fn no_pp_to_shuffle_message_is_dropped() {
        let msg_1 = ShuffleMsg(0);
        let msg_2 = ShuffleMsg(1);
        let msg_3 = ShuffleMsg(2);

        let input = [
            PPOut::Shuffle(msg_1),
            PPOut::Shuffle(msg_2),
            PPOut::Shuffle(msg_3),
        ];
        let expected_output = [msg_1, msg_2, msg_3];

        let (network, _consensus_rx, _ingress_rx) = mock_network();

        let network_handle = network.create_network_handle();

        let (shuffle_tx, shuffle_rx) = mpsc::channel(1);
        network_handle
            .register_shuffle(0, shuffle_tx)
            .await
            .unwrap();

        let pp_tx = network.create_partition_processor_sender();

        run_router_test(network, pp_tx, input, shuffle_rx, expected_output).await;
    }

    #[test(tokio::test)]
    async fn no_pp_to_ingress_message_is_dropped() {
        let msg_1 = IngressMsg(0);
        let msg_2 = IngressMsg(1);
        let msg_3 = IngressMsg(2);

        let input = [
            PPOut::Ingress(msg_1),
            PPOut::Ingress(msg_2),
            PPOut::Ingress(msg_3),
        ];
        let expected_output = [msg_1, msg_2, msg_3];

        let (network, _consensus_rx, ingress_rx) = mock_network();

        let pp_tx = network.create_partition_processor_sender();

        run_router_test(network, pp_tx, input, ingress_rx, expected_output).await;
    }

    async fn run_router_test<Input, Output>(
        network: MockNetwork,
        tx: mpsc::Sender<Input>,
        input: [Input; 3],
        mut rx: mpsc::Receiver<Output>,
        expected_output: [Output; 3],
    ) where
        Input: Debug + Copy,
        Output: PartialEq + Debug,
    {
        let network_handle = network.create_network_handle();

        let (shutdown_signal, shutdown_watch) = drain::channel();
        let network_join_handle = tokio::spawn(network.run(shutdown_watch));

        // we have to yield in order to process register shuffle message
        tokio::task::yield_now().await;

        tx.send(input[0]).await.unwrap();
        tx.send(input[1]).await.unwrap();
        tokio::task::yield_now().await;
        network_handle.unregister_shuffle(99).await.unwrap();
        tokio::task::yield_now().await;
        tx.send(input[2]).await.unwrap();

        for output in expected_output {
            assert_eq!(rx.recv().await.unwrap(), output);
        }

        shutdown_signal.drain().await;
        network_join_handle.await.unwrap().unwrap();
    }
}
