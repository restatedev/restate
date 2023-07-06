use crate::routing::{lookup_target_peer, send_to_shuffle};
use crate::{
    ConsensusOrShuffleTarget, PartitionTable, PartitionTableError, TargetConsensusOrShuffle,
    TargetShuffle,
};
use restate_types::identifiers::PeerId;
use restate_types::message::PartitionedMessage;
use restate_types::message::PeerTarget;
use std::collections::HashMap;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::SendError;
use tracing::trace;

#[derive(Debug, thiserror::Error)]
pub(super) enum IngressRouterError<C> {
    #[error("failed resolving target peer: {0}")]
    TargetPeerResolution(#[from] PartitionTableError),
    #[error("failed forwarding message: {0}")]
    ForwardingMessage(#[from] SendError<PeerTarget<C>>),
}

pub(super) struct IngressRouter<I, ItoC, ItoS, C, S, P> {
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
    ItoC: PartitionedMessage + Into<C> + Debug,
    P: PartitionTable,
{
    pub(super) fn new(
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

    pub(super) async fn run(mut self) -> Result<(), IngressRouterError<C>> {
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
