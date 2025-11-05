// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::identifiers::InvocationId;
use crate::identifiers::WithInvocationId;

// Just an alias
pub type EntryIndex = u32;

#[derive(Eq, Hash, PartialEq, Clone, Copy, Debug)]
pub struct JournalEntryId {
    invocation_id: InvocationId,
    journal_index: EntryIndex,
}

impl JournalEntryId {
    pub const fn from_parts(invocation_id: InvocationId, journal_index: EntryIndex) -> Self {
        Self {
            invocation_id,
            journal_index,
        }
    }

    pub fn journal_index(&self) -> EntryIndex {
        self.journal_index
    }
}

impl From<(InvocationId, EntryIndex)> for JournalEntryId {
    fn from(value: (InvocationId, EntryIndex)) -> Self {
        Self::from_parts(value.0, value.1)
    }
}

impl WithInvocationId for JournalEntryId {
    fn invocation_id(&self) -> InvocationId {
        self.invocation_id
    }
}
