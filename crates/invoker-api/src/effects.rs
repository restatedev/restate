// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_types::deployment::PinnedDeployment;
use restate_types::errors::InvocationError;
use restate_types::identifiers::InvocationId;
use restate_types::journal::EntryIndex;
use restate_types::journal::enriched::EnrichedRawEntry;
use restate_types::journal_events::raw::RawEvent;
use restate_types::journal_v2;
use restate_types::journal_v2::CommandIndex;
use restate_types::journal_v2::raw::RawEntry;
use restate_types::storage::{StoredRawEntry, StoredRawEntryHeader};
use restate_types::time::MillisSinceEpoch;
use std::collections::HashSet;

use crate::EffectKind::JournalEntryV2;

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Effect {
    pub invocation_id: InvocationId,
    // removed in v1.6
    // #[cfg_attr(
    //     feature = "serde",
    //     serde(default, skip_serializing_if = "num_traits::Zero::is_zero")
    // )]
    // pub invocation_epoch: InvocationEpoch,
    pub kind: EffectKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
// todo: fix this and box the large variant (EffectKind is 320 bytes)
#[allow(clippy::large_enum_variant)]
pub enum EffectKind {
    /// This is sent before any new entry is created by the invoker.
    /// This won't be sent if the deployment_id is already set.
    PinnedDeployment(PinnedDeployment),
    JournalEntry {
        entry_index: EntryIndex,
        entry: EnrichedRawEntry,
    },
    Suspended {
        waiting_for_completed_entries: HashSet<EntryIndex>,
    },
    // todo remove once we no longer support Bifrost commands written by <= v1.6
    JournalEntryV2 {
        entry: StoredRawEntry,
        /// This is used by the invoker to establish when it's safe to retry
        command_index_to_ack: Option<CommandIndex>,
    },
    JournalEvent {
        event: RawEvent,
    },
    SuspendedV2 {
        waiting_for_notifications: HashSet<journal_v2::NotificationId>,
    },
    Paused {
        paused_event: RawEvent,
    },
    /// This is sent always after [`Self::JournalEntry`] with `OutputStreamEntry`(s).
    End,
    /// This is sent when the invoker exhausted all its attempts to make progress on the specific invocation.
    Failed(InvocationError),
    // New journal entry v2 which only carries the raw entry.
    // Introduced in v1.6.0
    // Start writing in v1.7.0
    JournalEntryV2RawEntry {
        /// This is used by the invoker to establish when it's safe to retry
        command_index_to_ack: Option<CommandIndex>,
        raw_entry: RawEntry,
    },
}

impl EffectKind {
    pub fn journal_entry(
        raw_entry: impl Into<RawEntry>,
        command_index_to_ack: Option<CommandIndex>,
    ) -> Self {
        JournalEntryV2 {
            entry: StoredRawEntry::new(
                StoredRawEntryHeader::new(MillisSinceEpoch::now()),
                raw_entry.into(),
            ),
            command_index_to_ack,
        }
        // todo enable in v1.7.0
        // JournalEntryV2RawEntry {
        //     command_index_to_ack,
        //     raw_entry: raw_entry.into(),
        // }
    }
}
