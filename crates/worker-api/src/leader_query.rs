// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(dead_code)]

use tokio::sync::mpsc;

use restate_futures_util::command::{Command, UnboundedCommandReceiver, UnboundedCommandSender};
use restate_types::sharding::KeyRange;
use restate_types::vqueues::VQueueId;

use crate::{UserLimitCounterEntry, VQueueSchedulerStatus};

/// Queries that route through the partition processor's main `select!` loop.
///
/// Invoker status queries do NOT travel through this channel: DataFusion calls
/// the invoker's `ChannelStatusReader` directly via the partition-leader handles
/// registry. See `PartitionLeaderHandlesRegistry` in the worker crate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaderQueryKind {
    SchedulerStatus,
    UserLimitCounters,
}

#[derive(Debug, Clone)]
pub enum LeaderQueryRequest {
    SchedulerStatus { keys: KeyRange },
    UserLimitCounters { keys: KeyRange },
}

impl LeaderQueryRequest {
    pub fn kind(&self) -> LeaderQueryKind {
        match self {
            Self::SchedulerStatus { .. } => LeaderQueryKind::SchedulerStatus,
            Self::UserLimitCounters { .. } => LeaderQueryKind::UserLimitCounters,
        }
    }
}

pub type SchedulerStatusEntry = (VQueueId, VQueueSchedulerStatus);

#[derive(Debug, Clone)]
pub enum LeaderQueryResponse {
    SchedulerStatus(Vec<SchedulerStatusEntry>),
    UserLimitCounters(Vec<UserLimitCounterEntry>),
    NotLeader(LeaderQueryKind),
}

pub type LeaderQueryCommand = Command<LeaderQueryRequest, LeaderQueryResponse>;
pub type LeaderQuerySender = UnboundedCommandSender<LeaderQueryRequest, LeaderQueryResponse>;
pub type LeaderQueryReceiver = UnboundedCommandReceiver<LeaderQueryRequest, LeaderQueryResponse>;

pub fn channel() -> (LeaderQuerySender, LeaderQueryReceiver) {
    mpsc::unbounded_channel()
}
