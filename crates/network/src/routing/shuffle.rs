// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{
    ConsensusOrIngressTarget, FindPartition, PartitionTableError, TargetConsensusOrIngress,
};
use restate_types::identifiers::WithPartitionKey;
use restate_types::message::PartitionTarget;
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
    RoutingToConsensus(SendError<PartitionTarget<C>>),
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
    consensus_tx: mpsc::Sender<PartitionTarget<ConsensusMsg>>,
    ingress_tx: mpsc::Sender<IngressMsg>,
    partition_table: P,

    _shuffle_to_consensus: PhantomData<ShuffleToCon>,
    _shuffle_to_ingress: PhantomData<ShuffleToIngress>,
}

impl<ShuffleMsg, ShuffleToCon, ShuffleToIngress, ConsensusMsg, IngressMsg, P>
    ShuffleRouter<ShuffleMsg, ShuffleToCon, ShuffleToIngress, ConsensusMsg, IngressMsg, P>
where
    ShuffleMsg: TargetConsensusOrIngress<ShuffleToCon, ShuffleToIngress>,
    ShuffleToCon: WithPartitionKey + Into<ConsensusMsg> + Debug,
    ShuffleToIngress: Into<IngressMsg> + Debug,
    P: FindPartition,
{
    pub(super) fn new(
        receiver: mpsc::Receiver<ShuffleMsg>,
        consensus_tx: mpsc::Sender<PartitionTarget<ConsensusMsg>>,
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
            match message.into_target() {
                ConsensusOrIngressTarget::Consensus(msg) => {
                    let target_partition_id = self
                        .partition_table
                        .find_partition_id(msg.partition_key())?;

                    trace!(target_partition_id, message = ?msg, "Routing shuffle message to consensus.");

                    self.consensus_tx
                        .send((target_partition_id, msg.into()))
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
