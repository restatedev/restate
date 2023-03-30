use std::collections::HashMap;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

use common::types::PeerId;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::SendError;
use tracing::trace;

use crate::routing::send_to_shuffle;
use crate::{ShuffleOrIngressTarget, TargetShuffle, TargetShuffleOrIngress};

#[derive(Debug, thiserror::Error)]
pub(super) enum PartitionProcessorRouterError<I> {
    #[error("failed routing to ingress: {0}")]
    RoutingToIngress(#[from] SendError<I>),
}

pub(super) struct PartitionProcessorRouter<PPMsg, PPToShuffle, PPToIngress, ShuffleMsg, IngressMsg>
{
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
    pub(super) fn new(
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

    pub(super) async fn run(mut self) -> Result<(), PartitionProcessorRouterError<IngressMsg>> {
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
