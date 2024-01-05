// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::timer_table::Timer::CompleteSleepEntry;
use crate::Result;
use futures_util::Stream;
use restate_types::identifiers::PartitionId;
use restate_types::identifiers::{InvocationUuid, ServiceId};
use restate_types::invocation::ServiceInvocation;
use std::cmp::Ordering;
use std::future::Future;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct TimerKey {
    pub timestamp: u64,
    pub invocation_uuid: InvocationUuid,
    pub journal_index: u32,
}

impl PartialOrd for TimerKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TimerKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.timestamp
            .cmp(&other.timestamp)
            .then_with(|| self.invocation_uuid.cmp(&other.invocation_uuid))
            .then_with(|| self.journal_index.cmp(&other.journal_index))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Timer {
    CompleteSleepEntry(ServiceId),
    Invoke(ServiceId, ServiceInvocation),
}

impl Timer {
    pub fn service_id(&self) -> &ServiceId {
        match self {
            CompleteSleepEntry(service_id) => service_id,
            Timer::Invoke(service_id, _) => service_id,
        }
    }
}

pub trait TimerTable {
    fn add_timer(
        &mut self,
        partition_id: PartitionId,
        timer_key: &TimerKey,
        timer: Timer,
    ) -> impl Future<Output = ()> + Send;

    fn delete_timer(
        &mut self,
        partition_id: PartitionId,
        timer_key: &TimerKey,
    ) -> impl Future<Output = ()> + Send;

    fn next_timers_greater_than(
        &mut self,
        partition_id: PartitionId,
        exclusive_start: Option<&TimerKey>,
        limit: usize,
    ) -> impl Stream<Item = Result<(TimerKey, Timer)>> + Send;
}
