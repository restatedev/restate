// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use tokio::sync::{mpsc, oneshot};

use restate_node_protocol::cluster_controller::RunMode;
use restate_types::identifiers::{LeaderEpoch, PartitionId};
use restate_types::logs::Lsn;
use restate_types::time::MillisSinceEpoch;
use restate_types::GenerationalNodeId;

use crate::ShutdownError;

#[derive(Debug)]
pub enum ProcessorsManagerCommand {
    GetLivePartitions(oneshot::Sender<Vec<PartitionId>>),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ReplayStatus {
    Starting,
    Active,
    CatchingUp { target_tail_lsn: Lsn },
}

#[derive(Debug, Clone)]
pub struct PartitionProcessorStatus {
    pub updated_at: MillisSinceEpoch,
    pub planned_mode: RunMode,
    pub effective_mode: Option<RunMode>,
    pub last_observed_leader_epoch: Option<LeaderEpoch>,
    pub last_observed_leader_node: Option<GenerationalNodeId>,
    pub last_applied_log_lsn: Option<Lsn>,
    pub last_record_applied_at: Option<MillisSinceEpoch>,
    pub skipped_records: u64,
    pub replay_status: ReplayStatus,
}

impl PartitionProcessorStatus {
    pub fn new(planned_mode: RunMode) -> Self {
        Self {
            updated_at: MillisSinceEpoch::now(),
            planned_mode,
            effective_mode: None,
            last_observed_leader_epoch: None,
            last_observed_leader_node: None,
            last_applied_log_lsn: None,
            last_record_applied_at: None,
            skipped_records: 0,
            replay_status: ReplayStatus::Starting,
        }
    }
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
