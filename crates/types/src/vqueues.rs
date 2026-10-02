// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! VQueue identifiers and sequence numbers.
//!
//! See the [identifier module](crate::identifiers) for the relationship between
//! [`EntryId`], [`BaseEntryId`](crate::identifiers::BaseEntryId), and [`CanonicalEntryId`].

mod entry_id;
mod entry_target;
mod seq;
mod vqueue_id;

pub use crate::identifiers::CanonicalEntryId;
pub use entry_id::{EntryId, EntryIdDisplay, EntryKind};
pub use entry_target::{EntryTargetExt, EntryTargetRef, HandlerRef};
pub use seq::Seq;
pub use vqueue_id::{VQueueId, VQueueIdRef};

/// Errors returned when parsing encoded vqueue entry identifiers.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ParseError {
    #[error("bad length")]
    Length,
    #[error("unknown entry kind: {0}")]
    UnknownEntryKind(u8),
    #[error("malformed byte representation of a entry id")]
    MalformedId,
}
