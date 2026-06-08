// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod entry_id;
mod seq;
mod vqueue_id;

pub use entry_id::{EntryId, EntryIdDisplay, EntryKind};
pub use seq::Seq;
pub use vqueue_id::{VQueueId, VQueueIdRef};

/// Errors returned when parsing encoded vqueue entry identifiers.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ParseError {
    #[error("bad length")]
    Length,
    #[error("unknown entry kind: {0}")]
    UnknownEntryKind(u8),
}
