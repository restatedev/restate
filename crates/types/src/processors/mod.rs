// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::{Deserialize, Serialize};

use crate::identifiers::LeaderEpoch;
use crate::logs::Lsn;
use crate::time::MillisSinceEpoch;
use crate::GenerationalNodeId;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Eq, PartialEq)]
pub enum RunMode {
    Leader,
    Follower,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum ReplayStatus {
    Starting,
    Active,
    CatchingUp { target_tail_lsn: Lsn },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    pub last_persisted_log_lsn: Option<Lsn>,
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
            last_persisted_log_lsn: None,
        }
    }
}
