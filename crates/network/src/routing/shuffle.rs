// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{FindPartition, PartitionTableError};
use restate_types::identifiers::WithPartitionKey;
use restate_types::message::PartitionTarget;
use restate_wal_protocol::Envelope;
use std::fmt::Debug;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::SendError;
use tracing::trace;

#[derive(Debug, thiserror::Error)]
pub(super) enum ShuffleRouterError<C> {
    #[error("failed resolving target peer: {0}")]
    TargetPeerResolution(#[from] PartitionTableError),
    #[error("failed routing message to consensus: {0}")]
    RoutingToConsensus(SendError<PartitionTarget<C>>),
}

pub(super) struct ShuffleRouter<P> {
    receiver: mpsc::Receiver<Envelope>,
    consensus_tx: mpsc::Sender<PartitionTarget<Envelope>>,
    partition_table: P,
}

impl<P> ShuffleRouter<P>
where
    P: FindPartition,
{
    pub(super) fn new(
        receiver: mpsc::Receiver<Envelope>,
        consensus_tx: mpsc::Sender<PartitionTarget<Envelope>>,
        partition_table: P,
    ) -> Self {
        Self {
            receiver,
            consensus_tx,
            partition_table,
        }
    }

    pub(super) async fn run(mut self) -> Result<(), ShuffleRouterError<Envelope>> {
        while let Some(message) = self.receiver.recv().await {
            let target_partition_id = self
                .partition_table
                .find_partition_id(message.partition_key())?;

            trace!(target_partition_id, message = ?message, "Routing shuffle message to consensus.");

            self.consensus_tx
                .send((target_partition_id, message))
                .await
                .map_err(ShuffleRouterError::RoutingToConsensus)?
        }

        Ok(())
    }
}
