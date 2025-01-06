// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use prost_dto::IntoProto;
use serde::{Deserialize, Serialize};

use crate::identifiers::LeaderEpoch;
use crate::logs::Lsn;
use crate::time::MillisSinceEpoch;
use crate::GenerationalNodeId;

#[derive(
    Debug, Clone, Copy, Serialize, Deserialize, Eq, PartialEq, IntoProto, derive_more::Display,
)]
#[proto(target = "crate::protobuf::partition_processor::RunMode")]
pub enum RunMode {
    Leader,
    Follower,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, IntoProto)]
#[proto(target = "crate::protobuf::partition_processor::ReplayStatus")]
pub enum ReplayStatus {
    Starting,
    Active,
    CatchingUp,
}

#[derive(Debug, Clone, Serialize, Deserialize, IntoProto)]
#[proto(target = "crate::protobuf::partition_processor::PartitionProcessorStatus")]
pub struct PartitionProcessorStatus {
    #[proto(required)]
    pub updated_at: MillisSinceEpoch,
    pub planned_mode: RunMode,
    pub effective_mode: RunMode,
    pub last_observed_leader_epoch: Option<LeaderEpoch>,
    pub last_observed_leader_node: Option<GenerationalNodeId>,
    pub last_applied_log_lsn: Option<Lsn>,
    pub last_record_applied_at: Option<MillisSinceEpoch>,
    pub num_skipped_records: u64,
    pub replay_status: ReplayStatus,
    pub last_persisted_log_lsn: Option<Lsn>,
    pub last_archived_log_lsn: Option<Lsn>,
    // Set if replay_status is CatchingUp
    pub target_tail_lsn: Option<Lsn>,
}

impl Default for PartitionProcessorStatus {
    fn default() -> Self {
        Self {
            updated_at: MillisSinceEpoch::now(),
            planned_mode: RunMode::Follower,
            effective_mode: RunMode::Follower,
            last_observed_leader_epoch: None,
            last_observed_leader_node: None,
            last_applied_log_lsn: None,
            last_record_applied_at: None,
            num_skipped_records: 0,
            replay_status: ReplayStatus::Starting,
            last_persisted_log_lsn: None,
            last_archived_log_lsn: None,
            target_tail_lsn: None,
        }
    }
}

impl PartitionProcessorStatus {
    pub fn is_effective_leader(&self) -> bool {
        self.effective_mode == RunMode::Leader
    }

    pub fn new() -> Self {
        Self::default()
    }
}
