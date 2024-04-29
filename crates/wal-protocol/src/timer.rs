// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_storage_api::timer_table::{Timer, TimerKey, TimerKind};
use restate_types::identifiers::{EntryIndex, InvocationId, WithPartitionKey};
use restate_types::invocation::ServiceInvocation;
use restate_types::time::MillisSinceEpoch;
use std::borrow::Borrow;
use std::fmt;
use std::hash::{Hash, Hasher};

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TimerValue {
    timer_key: TimerKey,
    value: Timer,
}

impl TimerValue {
    pub fn new(timer_key: TimerKey, value: Timer) -> Self {
        Self { timer_key, value }
    }

    pub fn complete_journal_entry(
        invocation_id: InvocationId,
        wake_up_time: MillisSinceEpoch,
        entry_index: EntryIndex,
    ) -> Self {
        let timer_key = TimerKey::complete_journal_entry(
            wake_up_time.as_u64(),
            invocation_id.invocation_uuid(),
            entry_index,
        );

        Self {
            timer_key,
            value: Timer::CompleteJournalEntry(invocation_id, entry_index),
        }
    }

    pub fn invoke(
        invocation_id: InvocationId,
        wake_up_time: MillisSinceEpoch,
        service_invocation: ServiceInvocation,
    ) -> Self {
        let timer_key = TimerKey::invoke(wake_up_time.as_u64(), invocation_id.invocation_uuid());

        Self {
            timer_key,
            value: Timer::Invoke(service_invocation),
        }
    }

    pub fn clean_invocation_status(
        invocation_id: InvocationId,
        wake_up_time: MillisSinceEpoch,
    ) -> Self {
        TimerValue {
            timer_key: TimerKey::clean_invocation_status(
                wake_up_time.as_u64(),
                invocation_id.invocation_uuid(),
            ),
            value: Timer::CleanInvocationStatus(invocation_id),
        }
    }

    pub fn into_inner(self) -> (TimerKey, Timer) {
        (self.timer_key, self.value)
    }

    pub fn key(&self) -> &TimerKey {
        &self.timer_key
    }

    pub fn value(&self) -> &Timer {
        &self.value
    }

    pub fn invocation_id(&self) -> InvocationId {
        InvocationId::from_parts(
            self.value.partition_key(),
            self.timer_key.kind.invocation_uuid(),
        )
    }

    pub fn wake_up_time(&self) -> MillisSinceEpoch {
        MillisSinceEpoch::from(self.timer_key.timestamp)
    }
}

impl Hash for TimerValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash(&self.timer_key, state);
        // We don't hash the value field.
    }
}

impl PartialEq for TimerValue {
    fn eq(&self, other: &Self) -> bool {
        self.timer_key == other.timer_key
    }
}

impl Eq for TimerValue {}

impl Borrow<TimerKey> for TimerValue {
    fn borrow(&self) -> &TimerKey {
        &self.timer_key
    }
}

impl restate_types::timer::Timer for TimerValue {
    type TimerKey = TimerKey;

    fn timer_key(&self) -> &Self::TimerKey {
        &self.timer_key
    }
}

// Helper to display timer key
#[derive(Debug)]
pub struct TimerKeyDisplay<'a>(pub &'a TimerKey);

impl<'a> fmt::Display for TimerKeyDisplay<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0.kind {
            TimerKind::Invoke { invocation_uuid } => {
                write!(f, "Delayed invocation '{}'", invocation_uuid)
            }
            TimerKind::CompleteJournalEntry {
                invocation_uuid,
                journal_index,
            } => write!(
                f,
                "Complete journal entry [{}] for '{}'",
                journal_index, invocation_uuid
            ),
            TimerKind::CleanInvocationStatus { invocation_uuid } => {
                write!(f, "Clean invocation status '{}'", invocation_uuid)
            }
        }
    }
}
