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
use restate_types::invocation::InvocationEpoch;
use restate_types::journal::EntryIndex;
use restate_types::journal::enriched::EnrichedRawEntry;
use restate_types::journal_v2;
use restate_types::journal_v2::CommandIndex;
use std::collections::HashSet;

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Effect {
    pub invocation_id: InvocationId,
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "num_traits::Zero::is_zero")
    )]
    pub invocation_epoch: InvocationEpoch,
    pub kind: EffectKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
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
    JournalEntryV2 {
        entry: journal_v2::raw::RawEntry,
        /// This is used by the invoker to establish when it's safe to retry
        command_index_to_ack: Option<CommandIndex>,
    },
    SuspendedV2 {
        waiting_for_notifications: HashSet<journal_v2::NotificationId>,
    },
    /// This is sent always after [`Self::JournalEntry`] with `OutputStreamEntry`(s).
    End,
    /// This is sent when the invoker exhausted all its attempts to make progress on the specific invocation.
    Failed(InvocationError),
}
