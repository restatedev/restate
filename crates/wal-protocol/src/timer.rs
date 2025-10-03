// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_storage_api::timer_table::{Timer, TimerKey, TimerKeyKind};
use restate_types::flexbuffers_storage_encode_decode;
use restate_types::identifiers::{EntryIndex, InvocationId};
use restate_types::invocation::{InvocationEpoch, ServiceInvocation};
use restate_types::time::MillisSinceEpoch;
use std::borrow::Borrow;
use std::fmt;
use std::hash::{Hash, Hasher};

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TimerKeyValue {
    timer_key: TimerKey,
    value: Timer,
}

flexbuffers_storage_encode_decode!(TimerKeyValue);

impl TimerKeyValue {
    pub fn new(timer_key: TimerKey, value: Timer) -> Self {
        Self { timer_key, value }
    }

    pub fn complete_journal_entry(
        wake_up_time: MillisSinceEpoch,
        invocation_id: InvocationId,
        entry_index: EntryIndex,
        invocation_epoch: InvocationEpoch,
    ) -> Self {
        let (timer_key, value) = Timer::complete_journal_entry(
            wake_up_time.as_u64(),
            invocation_id,
            entry_index,
            invocation_epoch,
        );

        Self { timer_key, value }
    }

    pub fn invoke(
        wake_up_time: MillisSinceEpoch,
        service_invocation: Box<ServiceInvocation>,
    ) -> Self {
        let (timer_key, value) = Timer::invoke(wake_up_time.as_u64(), service_invocation);

        Self { timer_key, value }
    }

    pub fn neo_invoke(wake_up_time: MillisSinceEpoch, invocation_id: InvocationId) -> Self {
        let (timer_key, value) = Timer::neo_invoke(wake_up_time.as_u64(), invocation_id);

        Self { timer_key, value }
    }

    pub fn clean_invocation_status(
        wake_up_time: MillisSinceEpoch,
        invocation_id: InvocationId,
    ) -> Self {
        let (timer_key, value) =
            Timer::clean_invocation_status(wake_up_time.as_u64(), invocation_id);
        Self { timer_key, value }
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
        self.value.invocation_id()
    }

    pub fn wake_up_time(&self) -> MillisSinceEpoch {
        MillisSinceEpoch::from(self.timer_key.timestamp)
    }
}

impl Hash for TimerKeyValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash(&self.timer_key, state);
        // We don't hash the value field.
    }
}

impl PartialEq for TimerKeyValue {
    fn eq(&self, other: &Self) -> bool {
        self.timer_key == other.timer_key
    }
}

impl Eq for TimerKeyValue {}

impl Borrow<TimerKey> for TimerKeyValue {
    fn borrow(&self) -> &TimerKey {
        &self.timer_key
    }
}

impl restate_types::timer::Timer for TimerKeyValue {
    type TimerKey = TimerKey;

    fn timer_key(&self) -> &Self::TimerKey {
        &self.timer_key
    }
}

// Helper to display timer key
#[derive(Debug)]
pub struct TimerKeyDisplay<'a>(pub &'a TimerKey);

impl fmt::Display for TimerKeyDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0.kind {
            TimerKeyKind::NeoInvoke { invocation_uuid } => {
                write!(f, "Delayed invocation '{invocation_uuid}'")
            }
            TimerKeyKind::Invoke { invocation_uuid } => {
                write!(f, "Delayed invocation '{invocation_uuid}'")
            }
            TimerKeyKind::CompleteJournalEntry {
                invocation_uuid,
                journal_index,
            } => write!(
                f,
                "Complete journal entry [{journal_index}] for '{invocation_uuid}'"
            ),
            TimerKeyKind::CleanInvocationStatus { invocation_uuid } => {
                write!(f, "Clean invocation status '{invocation_uuid}'")
            }
        }
    }
}
