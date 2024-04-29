// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{protobuf_storage_encode_decode, Result};
use futures_util::Stream;
use restate_types::identifiers::{
    InvocationId, InvocationUuid, PartitionId, PartitionKey, WithPartitionKey,
};
use restate_types::invocation::ServiceInvocation;
use std::cmp::Ordering;
use std::future::Future;

#[derive(Clone, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
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
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Timer {
    CompleteSleepEntry(InvocationId, u32),
    Invoke(ServiceInvocation),
    CleanInvocationStatus(InvocationId),
}

impl WithPartitionKey for Timer {
    fn partition_key(&self) -> PartitionKey {
        match self {
            Timer::CompleteSleepEntry(invocation_id, _) => invocation_id.partition_key(),
            Timer::Invoke(service_invocation) => service_invocation.partition_key(),
            Timer::CleanInvocationStatus(invocation_id) => invocation_id.partition_key(),
        }
    }
}

protobuf_storage_encode_decode!(Timer);

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
