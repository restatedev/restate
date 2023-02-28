use crate::routing::future_impl::{ConsensusSendOperation, LookupMessage, ShuffleSendOperation};
use crate::{
    ConsensusOrIngressTarget, KeyedMessage, NetworkCommand, ShuffleOrIngressTarget,
    TargetConsensusOrIngress, TargetShuffle, TargetShuffleOrIngress, UnboundedNetworkHandle,
};
use common::types::{PeerId, PeerTarget};
use futures::future::{Fuse, FusedFuture};
use futures::FutureExt;
use std::collections::HashMap;
use std::fmt::Debug;
use std::marker::PhantomData;
use tokio::sync::mpsc;
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
{
    pub fn new(
        consensus_tx: mpsc::Sender<PeerTarget<ConsensusMsg>>,
        ingress_tx: mpsc::Sender<IngressIn>,
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
            mut consensus_in_rx,
            consensus_tx,
            ingress_tx,
            mut network_command_rx,
            mut shuffle_rx,
            mut ingress_in_rx,
            mut partition_processor_rx,
            ..
        } = self;

        debug!("Run network.");

        let shutdown = drain.signaled();
        let mut shuffles: HashMap<PeerId, mpsc::Sender<ShuffleIn>> = HashMap::new();

        let consensus_send = ConsensusSendOperation::new(
            |msg| Self::lookup_target_peer(msg),
            |msg| consensus_tx.send(msg),
        );
        let shuffle_send = ShuffleSendOperation::Idle;
        let ingress_send = Fuse::terminated();

        tokio::pin!(consensus_send, shuffle_send, ingress_send, shutdown);

        loop {
            tokio::select! {
                consensus_msg = consensus_in_rx.recv(), if consensus_send.is_terminated() => {
                    let consensus_msg = consensus_msg.expect("Network owns the consensus sender, that's why the receiver will never be closed.");

                    trace!(target_peer = %consensus_msg.0, message = ?consensus_msg.1, "Routing consensus message back to itself.");

                    consensus_send.send(consensus_msg);
                },
                shuffle_msg = shuffle_rx.recv(), if consensus_send.is_terminated() && ingress_send.is_terminated() => {
                    let shuffle_msg = shuffle_msg.expect("Network owns the shuffle sender, that's why the receiver will never be closed.");
                    match shuffle_msg.target() {
                        ConsensusOrIngressTarget::Consensus(msg) => {
                            consensus_send.lookup_target_peer_and_send(LookupMessage::left(msg));
                        },
                        ConsensusOrIngressTarget::Ingress(msg) => {
                            trace!(message = ?msg, "Routing shuffle message to ingress.");

                            ingress_send.set(ingress_tx.send(msg.into()).fuse());
                        }
                    }
                },
                ingress_msg = ingress_in_rx.recv(), if consensus_send.is_terminated() => {
                    let ingress_msg = ingress_msg.expect("Network owns the ingress sender, that's why the receiver will never be closed.");

                    consensus_send.lookup_target_peer_and_send(LookupMessage::right(ingress_msg));
                }
                partition_processor_msg = partition_processor_rx.recv(), if shuffle_send.is_terminated() && ingress_send.is_terminated() => {
                    let partition_processor_msg = partition_processor_msg.expect("Network owns the partition processor sender, that's why the receiver will never be closed.");

                    match partition_processor_msg.target() {
                        ShuffleOrIngressTarget::Shuffle(msg) => {
                            let shuffle_target = msg.shuffle_target();

                            if let Some(shuffle_tx) = shuffles.get(&shuffle_target).cloned() {
                                let owned_permit = shuffle_tx.reserve_owned();

                                trace!(shuffle_target, message = ?msg, "Routing partition processor message to shuffle.");

                                // We need a special send operation future which owns the send future
                                // because while sending, we might modify the shuffles struct which
                                // requires mutable access.
                                shuffle_send.set(ShuffleSendOperation::Sending {
                                    message: Some(msg.into()),
                                    shuffle_target,
                                    owned_permit
                                });
                            } else {
                                debug!("Unknown shuffle target {shuffle_target}. Ignoring message {msg:?}.");
                            }
                        },
                        ShuffleOrIngressTarget::Ingress(msg) => {
                            trace!(message = ?msg, "Routing partition processor message to ingress.");

                            ingress_send.set(ingress_tx.send(msg.into()).fuse());
                        }
                    }
                }
                send_result = &mut consensus_send => {
                    send_result?;
                },
                send_result = &mut shuffle_send => {
                    send_result?;
                },
                send_result = &mut ingress_send => {
                    send_result?;
                },
                command = network_command_rx.recv() => {
                    let command = command.expect("Network owns the command sender, that's why the receiver will never be closed.");
                    match command {
                        NetworkCommand::RegisterShuffle { peer_id, shuffle_tx } => {
                            trace!(shuffle_id = peer_id, "Register new shuffle.");
                            shuffles.insert(peer_id, shuffle_tx);
                        },
                        NetworkCommand::UnregisterShuffle { peer_id } => {
                            trace!(shuffle_id = peer_id, "Unregister shuffle.");
                            shuffles.remove(&peer_id);

                            if shuffle_send.shuffle_target() == Some(peer_id) {
                                // terminate shuffle send operation if the target has been removed
                                shuffle_send.set(ShuffleSendOperation::Idle);
                            }
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

    async fn lookup_target_peer(
        _msg: LookupMessage<ShuffleToCon, IngressOut, ConsensusMsg>,
    ) -> (
        PeerId,
        LookupMessage<ShuffleToCon, IngressOut, ConsensusMsg>,
    ) {
        todo!("https://github.com/restatedev/restate/issues/121");
    }
}

mod future_impl {
    use common::types::{PeerId, PeerTarget};
    use futures::future::FusedFuture;
    use futures::ready;
    use pin_project::pin_project;
    use std::fmt::{Debug, Formatter};
    use std::future::Future;
    use std::marker::PhantomData;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::sync::mpsc::error::SendError;
    use tokio::sync::mpsc::OwnedPermit;
    use tracing::trace;

    pub(super) struct LookupMessage<A, B, Target> {
        msg: Message<A, B>,
        _target: PhantomData<Target>,
    }

    impl<A, B, Target> Debug for LookupMessage<A, B, Target>
    where
        A: Debug,
        B: Debug,
    {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            self.msg.fmt(f)
        }
    }

    enum Message<A, B> {
        A(A),
        B(B),
    }

    impl<A, B> Debug for Message<A, B>
    where
        A: Debug,
        B: Debug,
    {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            match self {
                Message::A(a) => a.fmt(f),
                Message::B(b) => b.fmt(f),
            }
        }
    }

    impl<A, B, Target> LookupMessage<A, B, Target>
    where
        A: Into<Target>,
        B: Into<Target>,
    {
        pub(super) fn left(msg: A) -> Self {
            Self {
                msg: Message::A(msg),
                _target: Default::default(),
            }
        }

        pub(super) fn right(msg: B) -> Self {
            Self {
                msg: Message::B(msg),
                _target: Default::default(),
            }
        }

        fn into_target(self) -> Target {
            match self.msg {
                Message::A(msg) => msg.into(),
                Message::B(msg) => msg.into(),
            }
        }
    }

    #[pin_project(project = StateProj)]
    enum State<LookupFuture, SendingFuture> {
        Idle,
        Lookup {
            #[pin]
            future: LookupFuture,
        },
        Sending {
            #[pin]
            future: SendingFuture,
        },
    }

    #[pin_project]
    pub(super) struct ConsensusSendOperation<
        LookupOp,
        SendOp,
        ConsensusMsg,
        LookupFuture,
        SendingFuture,
        ShuffleMsg,
        IngressMsg,
    > {
        send_op: SendOp,
        lookup_op: LookupOp,
        #[pin]
        state: State<LookupFuture, SendingFuture>,

        _consensus_msg: PhantomData<ConsensusMsg>,
        _shuffle_msg: PhantomData<ShuffleMsg>,
        _ingress_msg: PhantomData<IngressMsg>,
    }

    impl<LookupOp, SendOp, ConsensusMsg, LookupFuture, SendingFuture, ShuffleMsg, IngressMsg>
        ConsensusSendOperation<
            LookupOp,
            SendOp,
            ConsensusMsg,
            LookupFuture,
            SendingFuture,
            ShuffleMsg,
            IngressMsg,
        >
    where
        SendOp: Fn(PeerTarget<ConsensusMsg>) -> SendingFuture,
        LookupOp: Fn(LookupMessage<ShuffleMsg, IngressMsg, ConsensusMsg>) -> LookupFuture,
    {
        pub(super) fn new(lookup_op: LookupOp, send_op: SendOp) -> Self {
            Self {
                lookup_op,
                send_op,
                state: State::Idle,
                _consensus_msg: Default::default(),
                _shuffle_msg: Default::default(),
                _ingress_msg: Default::default(),
            }
        }

        pub(super) fn send(self: &mut Pin<&mut Self>, msg: PeerTarget<ConsensusMsg>) {
            let mut this = self.as_mut().project();

            debug_assert!(
                matches!(this.state.as_mut().project(), StateProj::Idle),
                "Previous consensus send operation has not been completed."
            );

            this.state.set(State::Sending {
                future: (this.send_op)(msg),
            });
        }

        pub(super) fn lookup_target_peer_and_send(
            self: &mut Pin<&mut Self>,
            msg: LookupMessage<ShuffleMsg, IngressMsg, ConsensusMsg>,
        ) {
            let mut this = self.as_mut().project();

            debug_assert!(
                matches!(this.state.as_mut().project(), StateProj::Idle),
                "Previous consensus send operation has not been completed."
            );

            let future = (this.lookup_op)(msg);

            this.state.set(State::Lookup { future })
        }
    }

    #[derive(Debug, thiserror::Error)]
    #[error("failed sending message because {0}")]
    pub(super) struct ConsensusSendError<M: Debug>(#[from] SendError<PeerTarget<M>>);

    impl<LookupOp, SendOp, ConsensusMsg, LookupFuture, SendingFuture, ShuffleMsg, IngressMsg> Future
        for ConsensusSendOperation<
            LookupOp,
            SendOp,
            ConsensusMsg,
            LookupFuture,
            SendingFuture,
            ShuffleMsg,
            IngressMsg,
        >
    where
        SendingFuture: Future<Output = Result<(), SendError<PeerTarget<ConsensusMsg>>>>,
        LookupFuture:
            Future<Output = (PeerId, LookupMessage<ShuffleMsg, IngressMsg, ConsensusMsg>)>,
        ConsensusMsg: Debug,
        SendOp: Fn(PeerTarget<ConsensusMsg>) -> SendingFuture,
        ShuffleMsg: Into<ConsensusMsg> + Debug,
        IngressMsg: Into<ConsensusMsg> + Debug,
    {
        type Output = Result<(), ConsensusSendError<ConsensusMsg>>;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let mut this = self.project();
            loop {
                match this.state.as_mut().project() {
                    StateProj::Idle => return Poll::Pending,
                    StateProj::Lookup { future } => {
                        let (target_peer, msg) = ready!(future.poll(cx));

                        trace!(target_peer, message = ?msg, "Routing message to consensus.");

                        let future = (this.send_op)((target_peer, msg.into_target()));

                        this.state.set(State::Sending { future });
                    }
                    StateProj::Sending { future } => {
                        let result = ready!(future.poll(cx));

                        this.state.set(State::Idle);

                        return Poll::Ready(result.map_err(Into::into));
                    }
                }
            }
        }
    }

    impl<LookupOp, SendOp, ConsensusMsg, LookupFuture, SendingFuture, ShuffleMsg, IngressMsg>
        FusedFuture
        for ConsensusSendOperation<
            LookupOp,
            SendOp,
            ConsensusMsg,
            LookupFuture,
            SendingFuture,
            ShuffleMsg,
            IngressMsg,
        >
    where
        SendingFuture: Future<Output = Result<(), SendError<PeerTarget<ConsensusMsg>>>>,
        LookupFuture:
            Future<Output = (PeerId, LookupMessage<ShuffleMsg, IngressMsg, ConsensusMsg>)>,
        ConsensusMsg: Debug,
        SendOp: Fn(PeerTarget<ConsensusMsg>) -> SendingFuture,
        ShuffleMsg: Into<ConsensusMsg> + Debug,
        IngressMsg: Into<ConsensusMsg> + Debug,
    {
        fn is_terminated(&self) -> bool {
            matches! {self.state, State::Idle}
        }
    }

    #[pin_project(project = ShuffleSendOperationProj)]
    pub(super) enum ShuffleSendOperation<OwnedPermitFuture, ShuffleMsg> {
        Idle,
        Sending {
            shuffle_target: PeerId,
            message: Option<ShuffleMsg>,
            #[pin]
            owned_permit: OwnedPermitFuture,
        },
    }

    impl<OwnedPermitFuture, ShuffleMsg> ShuffleSendOperation<OwnedPermitFuture, ShuffleMsg> {
        pub(super) fn shuffle_target(&self) -> Option<PeerId> {
            match self {
                ShuffleSendOperation::Idle => None,
                ShuffleSendOperation::Sending { shuffle_target, .. } => Some(*shuffle_target),
            }
        }
    }

    impl<OwnedPermitFuture, ShuffleMsg> Future for ShuffleSendOperation<OwnedPermitFuture, ShuffleMsg>
    where
        OwnedPermitFuture: Future<Output = Result<OwnedPermit<ShuffleMsg>, SendError<()>>>,
    {
        type Output = Result<(), SendError<ShuffleMsg>>;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            match self.as_mut().project() {
                ShuffleSendOperationProj::Idle => Poll::Pending,
                ShuffleSendOperationProj::Sending {
                    message,
                    owned_permit,
                    ..
                } => {
                    let owned_permit = ready!(owned_permit.poll(cx));
                    let message = message
                        .take()
                        .expect("Shuffle send operation can only be polled once.");
                    self.set(ShuffleSendOperation::Idle);

                    match owned_permit {
                        Ok(owned_permit) => {
                            owned_permit.send(message);
                            Poll::Ready(Ok(()))
                        }
                        Err(_) => Poll::Ready(Err(SendError(message))),
                    }
                }
            }
        }
    }

    impl<OwnedPermitFuture, ShuffleMsg> FusedFuture
        for ShuffleSendOperation<OwnedPermitFuture, ShuffleMsg>
    where
        OwnedPermitFuture: Future<Output = Result<OwnedPermit<ShuffleMsg>, SendError<()>>>,
    {
        fn is_terminated(&self) -> bool {
            match self {
                ShuffleSendOperation::Idle => true,
                ShuffleSendOperation::Sending { .. } => false,
            }
        }
    }
}
