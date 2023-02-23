use common::types::PeerId;
use futures::{Sink, SinkExt};
use std::collections::HashMap;
use std::fmt::Debug;
use std::future;
use tokio::sync::mpsc;
use tokio_util::sync::PollSender;
use tracing::debug;

pub type ConsensusSender<T> = PollSender<T>;

pub type ShuffleSender<T> = mpsc::Sender<T>;

pub type IngressSender<T> = mpsc::Sender<T>;

#[derive(Debug, thiserror::Error)]
#[error("network is not running")]
pub struct NetworkNotRunning;

pub trait NetworkHandle<ShuffleIn, ShuffleOut> {
    type Future: future::Future<Output = Result<(), NetworkNotRunning>>;

    fn register_shuffle(
        &self,
        peer_id: PeerId,
        shuffle_sender: mpsc::Sender<ShuffleIn>,
    ) -> Self::Future;

    fn create_shuffle_sender(&self) -> ShuffleSender<ShuffleOut>;

    fn unregister_shuffle(&self, peer_id: PeerId) -> Self::Future;
}

enum NetworkCommand<ShuffleIn> {
    RegisterShuffle {
        peer_id: PeerId,
        shuffle_tx: mpsc::Sender<ShuffleIn>,
    },
    UnregisterShuffle {
        peer_id: PeerId,
    },
}

/// Component which is responsible for routing messages from different components.
#[derive(Debug)]
pub struct Network<ConMsg, ConOut, ShuffleIn, ShuffleOut, IngressOut> {
    /// Receiver for messages from the consensus module
    consensus_in_rx: mpsc::Receiver<ConMsg>,

    /// Sender for messages to the consensus module
    consensus_out: ConOut,

    network_command_rx: mpsc::UnboundedReceiver<NetworkCommand<ShuffleIn>>,

    shuffle_rx: mpsc::Receiver<ShuffleOut>,

    ingress_rx: mpsc::Receiver<IngressOut>,

    // used for creating the ConsensusSender
    consensus_in_tx: mpsc::Sender<ConMsg>,

    // used for creating the network handle
    network_command_tx: mpsc::UnboundedSender<NetworkCommand<ShuffleIn>>,
    shuffle_tx: mpsc::Sender<ShuffleOut>,
    ingress_tx: mpsc::Sender<IngressOut>,
}

impl<ConMsg, ConOut, ShuffleIn, ShuffleOut, IngressOut>
    Network<ConMsg, ConOut, ShuffleIn, ShuffleOut, IngressOut>
where
    ConMsg: Send + 'static,
    ConOut: Sink<ConMsg>,
    ConOut::Error: std::error::Error + Send + Sync + Debug + 'static,
{
    pub fn new(consensus_out: ConOut) -> Self {
        let (consensus_in_tx, consensus_in_rx) = mpsc::channel(64);
        let (shuffle_tx, shuffle_rx) = mpsc::channel(64);
        let (ingress_tx, ingress_rx) = mpsc::channel(64);
        let (network_command_tx, network_command_rx) = mpsc::unbounded_channel();

        Self {
            consensus_out,
            consensus_in_rx,
            consensus_in_tx,
            network_command_rx,
            network_command_tx,
            shuffle_rx,
            shuffle_tx,
            ingress_rx,
            ingress_tx,
        }
    }

    pub fn create_consensus_sender(&self) -> ConsensusSender<ConMsg> {
        PollSender::new(self.consensus_in_tx.clone())
    }

    pub fn create_network_handle(&self) -> UnboundedNetworkHandle<ShuffleIn, ShuffleOut> {
        UnboundedNetworkHandle {
            network_command_tx: self.network_command_tx.clone(),
            shuffle_tx: self.shuffle_tx.clone(),
        }
    }

    pub fn create_ingress_sender(&self) -> IngressSender<IngressOut> {
        self.ingress_tx.clone()
    }

    pub async fn run(self, drain: drain::Watch) -> anyhow::Result<()> {
        let Network {
            mut consensus_in_rx,
            consensus_out,
            mut network_command_rx,
            mut shuffle_rx,
            mut ingress_rx,
            ..
        } = self;

        let shutdown = drain.signaled();
        tokio::pin!(shutdown);
        tokio::pin!(consensus_out);

        let mut shuffles = HashMap::new();

        loop {
            tokio::select! {
                message = consensus_in_rx.recv() => {
                    let message = message.expect("Network owns the consensus sender, that's why the receiver will never be closed.");
                    consensus_out.send(message).await?;
                },
                command = network_command_rx.recv() => {
                    let command = command.expect("Network owns the command sender, that's why the receiver will never be closed.");
                    match command {
                        NetworkCommand::RegisterShuffle { peer_id, shuffle_tx } => {
                            shuffles.insert(peer_id, shuffle_tx);
                        },
                        NetworkCommand::UnregisterShuffle { peer_id } => {
                            shuffles.remove(&peer_id);
                        }
                    };
                },
                shuffle_msg = shuffle_rx.recv() => {
                    let _shuffle_msg = shuffle_msg.expect("Network owns the shuffle sender, that's why the receiver will never be closed.");

                    todo!("Need to implement the shuffle logic.");
                },
                ingress_msg = ingress_rx.recv() => {
                    let _ingress_msg = ingress_msg.expect("Network owns the ingress sneder, that's why the receiver will never be closed.");

                    todo!("Need to implement the ingress logic.");
                }
                _ = &mut shutdown => {
                    debug!("Shutting network down.");
                    break;
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct UnboundedNetworkHandle<ShuffleIn, ShuffleOut> {
    network_command_tx: mpsc::UnboundedSender<NetworkCommand<ShuffleIn>>,
    shuffle_tx: mpsc::Sender<ShuffleOut>,
}

impl<ShuffleIn, ShuffleOut> NetworkHandle<ShuffleIn, ShuffleOut>
    for UnboundedNetworkHandle<ShuffleIn, ShuffleOut>
where
    ShuffleIn: Send + 'static,
    ShuffleOut: Send + 'static,
{
    type Future = futures::future::Ready<Result<(), NetworkNotRunning>>;

    fn register_shuffle(
        &self,
        peer_id: PeerId,
        shuffle_tx: mpsc::Sender<ShuffleIn>,
    ) -> Self::Future {
        futures::future::ready(
            self.network_command_tx
                .send(NetworkCommand::RegisterShuffle {
                    peer_id,
                    shuffle_tx,
                })
                .map_err(|_| NetworkNotRunning),
        )
    }

    fn create_shuffle_sender(&self) -> ShuffleSender<ShuffleOut> {
        self.shuffle_tx.clone()
    }

    fn unregister_shuffle(&self, peer_id: PeerId) -> Self::Future {
        futures::future::ready(
            self.network_command_tx
                .send(NetworkCommand::UnregisterShuffle { peer_id })
                .map_err(|_| NetworkNotRunning),
        )
    }
}
