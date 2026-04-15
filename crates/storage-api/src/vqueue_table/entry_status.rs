// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_clock::RoughTimestamp;
use restate_types::vqueues::{EntryId, EntryKind, Seq, VQueueId};

use super::stats::EntryStatistics;
use super::{EntryKey, EntryMetadata, Stage};

#[derive(Debug, strum::Display, Clone, Copy, Eq, PartialEq, bilrost::Enumeration)]
#[strum(serialize_all = "snake_case")]
pub enum Status {
    #[bilrost(0)]
    Unknown,
    #[bilrost(1)]
    New,
    #[bilrost(2)]
    Scheduled,
    // -- Statuses for an invocation that has already started (attempted at least once)
    #[bilrost(3)]
    Running,
    #[bilrost(4)]
    Suspended,
    /// Invocation has previously started but has been placed back on the waiting inbox
    /// due to an attempt error.
    #[bilrost(5)]
    BackingOff,
    /// Invocation has previously started but has been placed back on the waiting inbox.
    /// This does not mean that the invocation attempt has failed, it just means that
    /// it has been evicted from the run queue and will be resumed later.
    #[bilrost(6)]
    Yielded,
    /// Inovocation that was suspended and is now waiting for its turn
    /// to run.
    #[bilrost(7)]
    WakingUp,
    ///
    /// -- Terminal states, invocation cannot transition back to any of the previous
    /// statuses
    ///
    #[bilrost(8)]
    Killed,
    #[bilrost(9)]
    Cancelled,
    #[bilrost(10)]
    Failed,
    #[bilrost(11)]
    Succeeded,
}

impl Status {
    #[inline]
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Unknown | Self::Killed | Self::Cancelled | Self::Failed | Self::Succeeded
        )
    }

    #[inline]
    pub fn can_move_to_run(&self) -> bool {
        matches!(
            self,
            Self::New
                | Self::Scheduled
                | Self::WakingUp
                | Self::Yielded
                | Self::BackingOff
                | Self::Running
        )
    }
}

pub trait EntryStatusHeader {
    fn vqueue_id(&self) -> &VQueueId;
    fn status(&self) -> Status;
    fn entry_id(&self) -> &EntryId;
    fn entry_key(&self) -> &EntryKey;
    fn kind(&self) -> EntryKind;
    fn metadata(&self) -> &EntryMetadata;
    fn stage(&self) -> Stage;
    fn has_lock(&self) -> bool;
    fn next_run_at(&self) -> RoughTimestamp;
    fn seq(&self) -> Seq;
    fn stats(&self) -> &EntryStatistics;
    fn display_entry_id(&self) -> impl std::fmt::Display + '_;
}

/// For future support for extra state storage for entries.
pub trait LazyEntryStatus: EntryStatusHeader {
    fn header(&self) -> &impl EntryStatusHeader;
    fn into_header(self) -> impl EntryStatusHeader + Send + Sync + 'static;

    fn decode_state_owned<E>(&self) -> Option<E>
    where
        E: EntryStatusExtra + bilrost::OwnedMessage + Send + Sized + 'static;

    fn decode_state_borrowed<'b, E>(&'b self) -> Option<E>
    where
        E: EntryStatusExtra + bilrost::BorrowedMessage<'b> + Sized + Send;
}

/// A marker trait for types that can be used as entry extra state values.
pub trait EntryStatusExtra {}
