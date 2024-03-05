// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_types::identifiers::WithPartitionKey;
use restate_types::message::PartitionTarget;
use restate_types::partition_table::{FindPartition, PartitionTableError};
use std::fmt::Debug;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::SendError;
use tracing::trace;

#[derive(Debug, thiserror::Error)]
pub(super) enum IngressRouterError<C> {
    #[error("failed resolving target peer: {0}")]
    TargetPeerResolution(#[from] PartitionTableError),
    #[error("failed forwarding message: {0}")]
    ForwardingMessage(#[from] SendError<PartitionTarget<C>>),
}

pub(super) struct IngressRouter<I, C, P> {
    receiver: mpsc::Receiver<I>,
    consensus_tx: mpsc::Sender<PartitionTarget<C>>,
    partition_table: P,
}

impl<I, C, P> IngressRouter<I, C, P>
where
    I: WithPartitionKey + Into<C> + Debug,
    P: FindPartition,
{
    pub(super) fn new(
        receiver: mpsc::Receiver<I>,
        consensus_tx: mpsc::Sender<PartitionTarget<C>>,
        partition_table: P,
    ) -> Self {
        Self {
            receiver,
            consensus_tx,
            partition_table,
        }
    }

    pub(super) async fn run(mut self) -> Result<(), IngressRouterError<C>> {
        while let Some(message) = self.receiver.recv().await {
            let target_partition_id = self
                .partition_table
                .find_partition_id(message.partition_key())?;

            trace!(?message, "Forwarding ingress message to consensus.");

            self.consensus_tx
                .send((target_partition_id, message.into()))
                .await?;
        }

        Ok(())
    }
}
