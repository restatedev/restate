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
use restate_types::time::MillisSinceEpoch;
use std::cmp::Ordering;
use std::future::Future;

/// # Important
/// We use the [`TimerKey`] to read the timers in an absolute order. The timer service
/// relies on this order in order to process each timer exactly once. That is the
/// reason why the in-memory and in-rocksdb ordering of the TimerKey needs to be exactly
/// the same.
#[derive(Clone, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
pub struct TimerKey {
    pub timestamp: u64,
    pub kind: TimerKind,
}

impl TimerKey {
    pub fn new_journal_entry(
        timestamp: u64,
        invocation_uuid: InvocationUuid,
        journal_index: u32,
    ) -> Self {
        TimerKey {
            timestamp,
            kind: TimerKind::Journal {
                invocation_uuid,
                journal_index,
            },
        }
    }

    pub fn new_invocation(timestamp: u64, invocation_uuid: InvocationUuid) -> Self {
        TimerKey {
            timestamp,
            kind: TimerKind::Invocation { invocation_uuid },
        }
    }
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
            .then_with(|| self.kind.cmp(&other.kind))
    }
}

#[derive(
    Clone,
    Debug,
    Eq,
    PartialEq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    strum_macros::EnumDiscriminants,
)]
#[strum_discriminants(derive(strum_macros::VariantArray))]
pub enum TimerKind {
    /// Invocation-scoped timers (e.g. a service invocation or clean up of invocation state)
    Invocation { invocation_uuid: InvocationUuid },
    /// Journal-scoped timers (e.g. completing a sleep journal entry)
    Journal {
        invocation_uuid: InvocationUuid,
        journal_index: u32,
    },
}

impl TimerKind {
    pub fn invocation_uuid(&self) -> InvocationUuid {
        *match self {
            TimerKind::Invocation { invocation_uuid } => invocation_uuid,
            TimerKind::Journal {
                invocation_uuid, ..
            } => invocation_uuid,
        }
    }
}

impl PartialOrd for TimerKind {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TimerKind {
    fn cmp(&self, other: &Self) -> Ordering {
        match self {
            TimerKind::Invocation { invocation_uuid } => match other {
                TimerKind::Invocation {
                    invocation_uuid: other_invocation_uuid,
                } => invocation_uuid.cmp(other_invocation_uuid),
                TimerKind::Journal { .. } => Ordering::Less,
            },
            TimerKind::Journal {
                invocation_uuid,
                journal_index,
            } => match other {
                TimerKind::Invocation { .. } => Ordering::Greater,
                TimerKind::Journal {
                    invocation_uuid: other_invocation_uuid,
                    journal_index: other_journal_index,
                } => invocation_uuid
                    .cmp(other_invocation_uuid)
                    .then_with(|| journal_index.cmp(other_journal_index)),
            },
        }
    }
}

impl restate_types::timer::TimerKey for TimerKey {
    fn wake_up_time(&self) -> MillisSinceEpoch {
        self.timestamp.into()
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
