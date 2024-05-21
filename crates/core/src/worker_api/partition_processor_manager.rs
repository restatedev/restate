// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_types::identifiers::PartitionId;
use tokio::sync::{mpsc, oneshot};

use crate::ShutdownError;

#[derive(Debug)]
pub enum ProcessorsManagerCommand {
    GetLivePartitions(oneshot::Sender<Vec<PartitionId>>),
}

#[derive(Debug, Clone)]
pub struct ProcessorsManagerHandle(mpsc::Sender<ProcessorsManagerCommand>);

impl ProcessorsManagerHandle {
    pub fn new(sender: mpsc::Sender<ProcessorsManagerCommand>) -> Self {
        Self(sender)
    }

    pub async fn get_live_partitions(&self) -> Result<Vec<PartitionId>, ShutdownError> {
        let (tx, rx) = oneshot::channel();
        self.0
            .send(ProcessorsManagerCommand::GetLivePartitions(tx))
            .await
            .unwrap();
        rx.await.map_err(|_| ShutdownError)
    }
}
