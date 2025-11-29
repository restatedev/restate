// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::Ordering;

use futures::Stream;

use restate_types::identifiers::{InvocationId, InvocationUuid, PartitionKey, WithPartitionKey};
use restate_types::invocation::ServiceInvocation;
use restate_types::time::MillisSinceEpoch;

use crate::Result;
use crate::protobuf_types::PartitionStoreProtobufValue;

/// # Important
/// We use the [`TimerKey`] to read the timers in an absolute order. The timer service
/// relies on this order in order to process each timer exactly once. That is the
/// reason why the in-memory and in-rocksdb ordering of the TimerKey needs to be exactly
/// the same.
#[derive(Clone, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
pub struct TimerKey {
    pub timestamp: u64,
    pub kind: TimerKeyKind,
}

impl TimerKey {
    fn complete_journal_entry(
        timestamp: u64,
        invocation_uuid: InvocationUuid,
        journal_index: u32,
    ) -> Self {
        TimerKey {
            timestamp,
            kind: TimerKeyKind::CompleteJournalEntry {
                invocation_uuid,
                journal_index,
            },
        }
    }

    fn invoke(timestamp: u64, invocation_uuid: InvocationUuid) -> Self {
        TimerKey {
            timestamp,
            kind: TimerKeyKind::Invoke { invocation_uuid },
        }
    }

    pub fn neo_invoke(timestamp: u64, invocation_uuid: InvocationUuid) -> Self {
        TimerKey {
            timestamp,
            kind: TimerKeyKind::NeoInvoke { invocation_uuid },
        }
    }

    fn clean_invocation_status(timestamp: u64, invocation_uuid: InvocationUuid) -> Self {
        TimerKey {
            timestamp,
            kind: TimerKeyKind::CleanInvocationStatus { invocation_uuid },
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
    strum::EnumDiscriminants,
)]
#[strum_discriminants(derive(strum::VariantArray))]
pub enum TimerKeyKind {
    /// Delayed invocation
    Invoke { invocation_uuid: InvocationUuid },
    /// Delayed invocation using NeoInvocationStatus
    NeoInvoke { invocation_uuid: InvocationUuid },
    /// Completion of a journal entry
    CompleteJournalEntry {
        invocation_uuid: InvocationUuid,
        journal_index: u32,
    },
    /// Cleaning of invocation status
    CleanInvocationStatus { invocation_uuid: InvocationUuid },
}

impl TimerKeyKind {
    pub fn invocation_uuid(&self) -> InvocationUuid {
        *match self {
            TimerKeyKind::Invoke { invocation_uuid } => invocation_uuid,
            TimerKeyKind::CompleteJournalEntry {
                invocation_uuid, ..
            } => invocation_uuid,
            TimerKeyKind::CleanInvocationStatus { invocation_uuid } => invocation_uuid,
            TimerKeyKind::NeoInvoke { invocation_uuid } => invocation_uuid,
        }
    }
}

impl PartialOrd for TimerKeyKind {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TimerKeyKind {
    fn cmp(&self, other: &Self) -> Ordering {
        match self {
            TimerKeyKind::Invoke { invocation_uuid } => match other {
                TimerKeyKind::Invoke {
                    invocation_uuid: other_invocation_uuid,
                } => invocation_uuid.cmp(other_invocation_uuid),
                TimerKeyKind::CompleteJournalEntry { .. }
                | TimerKeyKind::CleanInvocationStatus { .. }
                | TimerKeyKind::NeoInvoke { .. } => Ordering::Less,
            },
            TimerKeyKind::CompleteJournalEntry {
                invocation_uuid,
                journal_index,
            } => match other {
                TimerKeyKind::Invoke { .. } => Ordering::Greater,
                TimerKeyKind::CompleteJournalEntry {
                    invocation_uuid: other_invocation_uuid,
                    journal_index: other_journal_index,
                } => invocation_uuid
                    .cmp(other_invocation_uuid)
                    .then_with(|| journal_index.cmp(other_journal_index)),
                TimerKeyKind::CleanInvocationStatus { .. } | TimerKeyKind::NeoInvoke { .. } => {
                    Ordering::Less
                }
            },
            TimerKeyKind::CleanInvocationStatus { invocation_uuid } => match other {
                TimerKeyKind::Invoke { .. } | TimerKeyKind::CompleteJournalEntry { .. } => {
                    Ordering::Greater
                }
                TimerKeyKind::CleanInvocationStatus {
                    invocation_uuid: other_invocation_uuid,
                } => invocation_uuid.cmp(other_invocation_uuid),
                TimerKeyKind::NeoInvoke { .. } => Ordering::Less,
            },
            TimerKeyKind::NeoInvoke { invocation_uuid } => match other {
                TimerKeyKind::Invoke { .. }
                | TimerKeyKind::CompleteJournalEntry { .. }
                | TimerKeyKind::CleanInvocationStatus { .. } => Ordering::Greater,
                TimerKeyKind::NeoInvoke {
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

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Timer {
    // TODO remove this variant when removing the old invocation status table
    Invoke(Box<ServiceInvocation>),
    CompleteJournalEntry(
        InvocationId,
        u32,
        // removed in v1.6
        // #[serde(default, skip_serializing_if = "num_traits::Zero::is_zero")] InvocationEpoch,
    ),
    // TODO remove this variant when removing the old invocation status table
    CleanInvocationStatus(InvocationId),
    NeoInvoke(InvocationId),
}

impl Timer {
    pub fn complete_journal_entry(
        timestamp: u64,
        invocation_id: InvocationId,
        journal_index: u32,
    ) -> (TimerKey, Self) {
        (
            TimerKey::complete_journal_entry(
                timestamp,
                invocation_id.invocation_uuid(),
                journal_index,
            ),
            Timer::CompleteJournalEntry(invocation_id, journal_index),
        )
    }

    pub fn invoke(timestamp: u64, service_invocation: Box<ServiceInvocation>) -> (TimerKey, Self) {
        (
            TimerKey::invoke(
                timestamp,
                service_invocation.invocation_id.invocation_uuid(),
            ),
            Timer::Invoke(service_invocation),
        )
    }

    pub fn neo_invoke(timestamp: u64, invocation_id: InvocationId) -> (TimerKey, Self) {
        (
            TimerKey::neo_invoke(timestamp, invocation_id.invocation_uuid()),
            Timer::NeoInvoke(invocation_id),
        )
    }

    pub fn clean_invocation_status(
        timestamp: u64,
        invocation_id: InvocationId,
    ) -> (TimerKey, Self) {
        (
            TimerKey::clean_invocation_status(timestamp, invocation_id.invocation_uuid()),
            Timer::CleanInvocationStatus(invocation_id),
        )
    }

    pub fn invocation_id(&self) -> InvocationId {
        match self {
            Timer::Invoke(service_invocation) => service_invocation.invocation_id,
            Timer::CompleteJournalEntry(invocation_id, _) => *invocation_id,
            Timer::CleanInvocationStatus(invocation_id) => *invocation_id,
            Timer::NeoInvoke(invocation_id) => *invocation_id,
        }
    }
}

impl PartitionStoreProtobufValue for Timer {
    type ProtobufType = crate::protobuf_types::v1::Timer;
}

impl WithPartitionKey for Timer {
    fn partition_key(&self) -> PartitionKey {
        match self {
            Timer::CompleteJournalEntry(invocation_id, _) => invocation_id.partition_key(),
            Timer::Invoke(service_invocation) => service_invocation.partition_key(),
            Timer::CleanInvocationStatus(invocation_id) => invocation_id.partition_key(),
            Timer::NeoInvoke(invocation_id) => invocation_id.partition_key(),
        }
    }
}

pub trait ReadTimerTable {
    fn next_timers_greater_than(
        &mut self,
        exclusive_start: Option<&TimerKey>,
        limit: usize,
    ) -> Result<impl Stream<Item = Result<(TimerKey, Timer)>> + Send>;
}

pub trait WriteTimerTable {
    fn put_timer(&mut self, timer_key: &TimerKey, timer: &Timer) -> Result<()>;

    fn delete_timer(&mut self, timer_key: &TimerKey) -> Result<()>;
}
