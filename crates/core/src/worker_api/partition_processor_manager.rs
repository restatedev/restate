// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use tokio::sync::{mpsc, oneshot};

use restate_types::{
    cluster::cluster_state::PartitionProcessorStatus,
    identifiers::{PartitionId, SnapshotId},
};

use crate::ShutdownError;

#[derive(Debug)]
pub enum ProcessorsManagerCommand {
    CreateSnapshot(PartitionId, oneshot::Sender<anyhow::Result<SnapshotId>>),
    GetState(oneshot::Sender<BTreeMap<PartitionId, PartitionProcessorStatus>>),
}

#[derive(Debug, Clone)]
pub struct ProcessorsManagerHandle(mpsc::Sender<ProcessorsManagerCommand>);

impl ProcessorsManagerHandle {
    pub fn new(sender: mpsc::Sender<ProcessorsManagerCommand>) -> Self {
        Self(sender)
    }

    pub async fn create_snapshot(&self, partition_id: PartitionId) -> anyhow::Result<SnapshotId> {
        let (tx, rx) = oneshot::channel();
        self.0
            .send(ProcessorsManagerCommand::CreateSnapshot(partition_id, tx))
            .await?;
        rx.await?
    }

    pub async fn get_state(
        &self,
    ) -> Result<BTreeMap<PartitionId, PartitionProcessorStatus>, ShutdownError> {
        let (tx, rx) = oneshot::channel();
        self.0
            .send(ProcessorsManagerCommand::GetState(tx))
            .await
            .unwrap();
        rx.await.map_err(|_| ShutdownError)
    }
}
