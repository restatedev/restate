// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;

use restate_clock::RoughTimestamp;
use restate_storage_api::vqueue_table::scheduler::YieldReason;
use restate_types::deployment::PinnedDeployment;
use restate_types::errors::InvocationError;
use restate_types::identifiers::InvocationId;
use restate_types::invocation::FencingToken;
use restate_types::journal::EntryIndex;
use restate_types::journal::enriched::EnrichedRawEntry;
use restate_types::journal_events::raw::RawEvent;
use restate_types::journal_v2::CommandIndex;
use restate_types::journal_v2::raw::RawEntry;
use restate_types::journal_v2::{self, UnresolvedFuture};
use restate_types::storage::StoredRawEntry;

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Effect {
    pub invocation_id: InvocationId,
    pub kind: EffectKind,
}

/// An [`Effect`] tagged with the fencing token of the invoker-task generation that produced it.
///
/// The fencing token is a leader-local, in-memory fence: the partition processor checks it
/// against its in-memory `fencing_tokens` map *before self-proposing* the wrapped effect, then
/// strips it. Only the inner [`Effect`] is ever written to Bifrost, so the token never reaches
/// the log -- it is deliberately kept out of the invocation lifecycle / storage format.
#[derive(Debug)]
pub struct FencedEffect {
    pub fencing_token: FencingToken,
    pub effect: Box<Effect>,
}

#[derive(Debug, Clone)]
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
    // TODO stop writing this from >= v1.8, just use SuspendedV3 anyway even with service protocol < 7
    // todo remove once we no longer support Bifrost commands written by <= v1.7
    SuspendedV2 {
        /// Flattened set of notification ids
        /// can be thought of as `UnresolvedFuture::unknown(waiting_for_notifications)`
        waiting_for_notifications: HashSet<journal_v2::NotificationId>,
    },
    // Introduced in Restate v1.7. With the new service-protocol v7
    SuspendedV3 {
        /// Future tree describing the notifications this invocation is waiting on.
        /// Introduced in Restate v1.7 (protocol version V7). `None` for older invocations.
        awaiting_on: UnresolvedFuture,
    },
    Paused {
        paused_event: RawEvent,
    },
    /// The invoker yielded the invocation back to the scheduler. The partition
    /// processor should re-schedule the invocation (via [`YieldReason`] the
    /// scheduler can apply reason-specific strategies in the future).
    Yield {
        reason: YieldReason,
        #[cfg_attr(feature = "serde", serde(skip_serializing_if = "Option::is_none"))]
        error_event: Option<RawEvent>,
        resume_at: Option<RoughTimestamp>,
    },
    /// This is sent always after [`Self::JournalEntry`] with `OutputStreamEntry`(s).
    End,
    /// This is sent when the invoker exhausted all its attempts to make progress on the specific invocation.
    Failed(InvocationError),
    // New journal entry v2 which only carries the raw entry.
    // Introduced in v1.6.0, started being written in v1.7.0.
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
        Self::JournalEntryV2RawEntry {
            command_index_to_ack,
            raw_entry: raw_entry.into(),
        }
    }

    /// Returns `true` if this effect ends the current invoker-task generation, i.e. after emitting
    /// it the task will not produce any further effects for this attempt (it suspended, paused,
    /// yielded, completed, or failed). The partition processor uses this to release the attempt's
    /// fencing token. Non-terminal effects (deployment pin, journal entries/events) keep the
    /// attempt running.
    pub fn is_terminal(&self) -> bool {
        match self {
            Self::Suspended { .. }
            | Self::SuspendedV2 { .. }
            | Self::SuspendedV3 { .. }
            | Self::Paused { .. }
            | Self::Yield { .. }
            | Self::End
            | Self::Failed(_) => true,
            Self::PinnedDeployment(_)
            | Self::JournalEntry { .. }
            | Self::JournalEntryV2 { .. }
            | Self::JournalEvent { .. }
            | Self::JournalEntryV2RawEntry { .. } => false,
        }
    }
}
