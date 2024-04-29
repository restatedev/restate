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
    pub fn complete_journal_entry(
        timestamp: u64,
        invocation_uuid: InvocationUuid,
        journal_index: u32,
    ) -> Self {
        TimerKey {
            timestamp,
            kind: TimerKind::CompleteJournalEntry {
                invocation_uuid,
                journal_index,
            },
        }
    }

    pub fn invoke(timestamp: u64, invocation_uuid: InvocationUuid) -> Self {
        TimerKey {
            timestamp,
            kind: TimerKind::Invoke { invocation_uuid },
        }
    }

    pub fn clean_invocation_status(timestamp: u64, invocation_uuid: InvocationUuid) -> Self {
        TimerKey {
            timestamp,
            kind: TimerKind::CleanInvocationStatus { invocation_uuid },
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
    /// Delayed invocation
    Invoke { invocation_uuid: InvocationUuid },
    /// Completion of a journal entry
    CompleteJournalEntry {
        invocation_uuid: InvocationUuid,
        journal_index: u32,
    },
    /// Cleaning of invocation status
    CleanInvocationStatus { invocation_uuid: InvocationUuid },
}

impl TimerKind {
    pub fn invocation_uuid(&self) -> InvocationUuid {
        *match self {
            TimerKind::Invoke { invocation_uuid } => invocation_uuid,
            TimerKind::CompleteJournalEntry {
                invocation_uuid, ..
            } => invocation_uuid,
            TimerKind::CleanInvocationStatus { invocation_uuid } => invocation_uuid,
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
            TimerKind::Invoke { invocation_uuid } => match other {
                TimerKind::Invoke {
                    invocation_uuid: other_invocation_uuid,
                } => invocation_uuid.cmp(other_invocation_uuid),
                TimerKind::CompleteJournalEntry { .. }
                | TimerKind::CleanInvocationStatus { .. } => Ordering::Less,
            },
            TimerKind::CompleteJournalEntry {
                invocation_uuid,
                journal_index,
            } => match other {
                TimerKind::Invoke { .. } => Ordering::Greater,
                TimerKind::CompleteJournalEntry {
                    invocation_uuid: other_invocation_uuid,
                    journal_index: other_journal_index,
                } => invocation_uuid
                    .cmp(other_invocation_uuid)
                    .then_with(|| journal_index.cmp(other_journal_index)),
                TimerKind::CleanInvocationStatus { .. } => Ordering::Less,
            },
            TimerKind::CleanInvocationStatus { invocation_uuid } => match other {
                TimerKind::Invoke { .. } | TimerKind::CompleteJournalEntry { .. } => {
                    Ordering::Greater
                }
                TimerKind::CleanInvocationStatus {
                    invocation_uuid: other_invocation_uuid,
                } => invocation_uuid.cmp(other_invocation_uuid),
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
    Invoke(ServiceInvocation),
    CompleteJournalEntry(InvocationId, u32),
    CleanInvocationStatus(InvocationId),
}

impl WithPartitionKey for Timer {
    fn partition_key(&self) -> PartitionKey {
        match self {
            Timer::CompleteJournalEntry(invocation_id, _) => invocation_id.partition_key(),
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
