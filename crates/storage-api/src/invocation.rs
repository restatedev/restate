// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_types::PartitionedResourceId;
use restate_types::identifiers::{InvocationId, PartitionKey, StateMutationId, WithPartitionKey};
use restate_types::vqueues::{EntryId, EntryKind};

use crate::vqueue_table::{EntryState, IdentifiesEntry};

/// This is a provisional design for a moving state of an invocation. It should contain
/// only the "dynamic" state of the invocation.
#[derive(Debug, Clone, bilrost::Message)]
pub struct InvocationState {
    #[bilrost(tag(1))]
    status: Status,
}

impl InvocationState {
    pub fn new(status: Status) -> Self {
        Self { status }
    }

    pub fn status(&self) -> Status {
        self.status
    }
}

#[derive(Debug, strum::Display, Clone, Copy, Eq, PartialEq, bilrost::Enumeration)]
#[strum(serialize_all = "snake_case")]
pub enum Status {
    #[bilrost(0)]
    Unknown,
    #[bilrost(1)]
    New,
    #[bilrost(2)]
    Scheduled,
    #[bilrost(3)]
    Paused,
    // -- Statuses for an invocation that has already started (attempted at least once)
    #[bilrost(4)]
    Running,
    #[bilrost(5)]
    Suspended,
    /// Invocation has previously started but has been placed back on the waiting inbox
    /// due to an attempt error.
    #[bilrost(6)]
    BackingOff,
    /// Invocation has previously started but has been placed back on the waiting inbox.
    /// This does not mean that the invocation attempt has failed, it just means that
    /// it has been evicted from the run queue and will be resumed later.
    #[bilrost(7)]
    Yielded,
    /// Inovocation that was suspended or paused and is now waiting for its turn
    /// to run.
    #[bilrost(8)]
    WakingUp,
    ///
    /// -- Terminal states, invocation cannot transition back to any of the previous
    /// statuses
    ///
    #[bilrost(9)]
    Killed,
    #[bilrost(10)]
    Cancelled,
    #[bilrost(11)]
    Failed,
    #[bilrost(12)]
    Succeeded,
}

/// Marks this type as a valid entry state
impl EntryState for InvocationState {}

impl IdentifiesEntry for InvocationId {
    type State = InvocationState;
    const KIND: EntryKind = EntryKind::Invocation;

    fn partition_key(&self) -> PartitionKey {
        WithPartitionKey::partition_key(self)
    }

    fn to_entry_id(&self) -> EntryId {
        EntryId::from(self)
    }
}

/// Empty struct for state mutation (entry state)
/// This allows future evolution as needed.
#[derive(Debug, Clone, Default, bilrost::Message)]
pub struct StateMutationState {}
impl EntryState for StateMutationState {}

impl IdentifiesEntry for StateMutationId {
    type State = StateMutationState;
    const KIND: EntryKind = EntryKind::StateMutation;

    fn partition_key(&self) -> PartitionKey {
        PartitionedResourceId::partition_key(self)
    }

    fn to_entry_id(&self) -> EntryId {
        EntryId::from(self)
    }
}
