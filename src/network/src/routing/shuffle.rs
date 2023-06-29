use crate::routing::lookup_target_peer;
use crate::{
    ConsensusOrIngressTarget, PartitionTable, PartitionTableError, TargetConsensusOrIngress,
};
use restate_types::message::PartitionedMessage;
use restate_types::message::PeerTarget;
use std::fmt::Debug;
use std::marker::PhantomData;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::SendError;
use tracing::trace;

#[derive(Debug, thiserror::Error)]
pub(super) enum ShuffleRouterError<C, I> {
    #[error("failed resolving target peer: {0}")]
    TargetPeerResolution(#[from] PartitionTableError),
    #[error("failed routing message to consensus: {0}")]
    RoutingToConsensus(SendError<PeerTarget<C>>),
    #[error("failed routing message to ingress: {0}")]
    RoutingToIngress(SendError<I>),
}

pub(super) struct ShuffleRouter<
    ShuffleMsg,
    ShuffleToCon,
    ShuffleToIngress,
    ConsensusMsg,
    IngressMsg,
    P,
> {
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
    ShuffleToCon: PartitionedMessage + Into<ConsensusMsg> + Debug,
    ShuffleToIngress: Into<IngressMsg> + Debug,
    P: PartitionTable,
{
    pub(super) fn new(
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

    pub(super) async fn run(mut self) -> Result<(), ShuffleRouterError<ConsensusMsg, IngressMsg>> {
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
