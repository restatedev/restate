use common::types::{PartitionKey, PeerId};
use std::fmt::Debug;
use std::future::Future;
use tokio::sync::mpsc;

mod routing;
mod unbounded_handle;

pub use routing::{Network, PartitionProcessorSender};
pub use unbounded_handle::UnboundedNetworkHandle;

pub type ShuffleSender<T> = mpsc::Sender<T>;

#[derive(Debug, thiserror::Error)]
#[error("network is not running")]
pub struct NetworkNotRunning;

/// Handle to interact with the running network routing component.
pub trait NetworkHandle<ShuffleIn, ShuffleOut> {
    type Future: Future<Output = Result<(), NetworkNotRunning>>;

    fn register_shuffle(
        &self,
        peer_id: PeerId,
        shuffle_sender: mpsc::Sender<ShuffleIn>,
    ) -> Self::Future;

    fn unregister_shuffle(&self, peer_id: PeerId) -> Self::Future;

    fn create_shuffle_sender(&self) -> ShuffleSender<ShuffleOut>;
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

/// Trait for messages that are sent to the shuffle component
pub trait TargetShuffle {
    /// Returns the target shuffle identified by its [`PeerId`].
    fn shuffle_target(&self) -> PeerId;
}

pub enum ConsensusOrIngressTarget<C, I> {
    Consensus(C),
    Ingress(I),
}

/// Trait for messages that are sent to the consensus module or an ingress
pub trait TargetConsensusOrIngress<C, I> {
    /// Returns the target of a message. It can either be an ingress
    /// or the consensus module.
    fn target(self) -> ConsensusOrIngressTarget<C, I>;
}

/// Trait for messages that are sent to a shuffle component or an ingress
pub enum ShuffleOrIngressTarget<S, I> {
    Shuffle(S),
    Ingress(I),
}

pub trait TargetShuffleOrIngress<S, I> {
    /// Returns the target of a message. It can either be a shuffle or an ingress.
    fn target(self) -> ShuffleOrIngressTarget<S, I>;
}

#[derive(Debug, thiserror::Error)]
#[error("Cannot find target peer for partition key {0}")]
pub struct PartitionTableError(PartitionKey);

pub trait PartitionTable {
    type Future: Future<Output = Result<PeerId, PartitionTableError>>;

    fn partition_key_to_target_peer(&self, partition_key: PartitionKey) -> Self::Future;
}
