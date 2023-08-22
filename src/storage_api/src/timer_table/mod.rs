// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{GetStream, PutFuture};
use restate_types::identifiers::FullInvocationId;
use restate_types::identifiers::PartitionId;
use restate_types::invocation::ServiceInvocation;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TimerKey {
    pub full_invocation_id: FullInvocationId,
    pub journal_index: u32,
    pub timestamp: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Timer {
    CompleteSleepEntry,
    Invoke(ServiceInvocation),
}

pub trait TimerTable {
    fn add_timer(
        &mut self,
        partition_id: PartitionId,
        timer_key: &TimerKey,
        timer: Timer,
    ) -> PutFuture;

    fn delete_timer(&mut self, partition_id: PartitionId, timer_key: &TimerKey) -> PutFuture;

    fn next_timers_greater_than(
        &mut self,
        partition_id: PartitionId,
        exclusive_start: Option<&TimerKey>,
        limit: usize,
    ) -> GetStream<(TimerKey, Timer)>;
}
