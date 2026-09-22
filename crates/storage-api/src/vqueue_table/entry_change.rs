// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_types::identifiers::BaseEntryId;
use restate_types::vqueues::{EntryTargetRef, VQueueId};

use super::stats::EntryStatistics;
use super::{EntryKey, EntryMetadata, EntryStatusHeader, EntryValue, Stage, Status};

/// Queue and target shared by both sides of an entry mutation.
///
/// Neither may change during an update. The target is supplied by the caller;
/// it is not part of the persisted entry status header.
#[derive(Debug)]
pub struct EntryContext<'a> {
    pub qid: &'a VQueueId,
    pub target: &'a EntryTargetRef<'a>,
}

/// A complete logical entry status, borrowing its existing components.
#[derive(Debug, Clone, Copy)]
pub struct EntryStateRef<'a> {
    pub stage: Stage,
    pub status: Status,
    pub entry_key: &'a EntryKey,
    pub metadata: &'a EntryMetadata,
    pub stats: &'a EntryStatistics,
}

impl<'a> EntryStateRef<'a> {
    pub fn from_value(stage: Stage, entry_key: &'a EntryKey, value: &'a EntryValue) -> Self {
        Self {
            stage,
            status: value.status,
            entry_key,
            metadata: &value.metadata,
            stats: &value.stats,
        }
    }

    pub fn from_header(header: &'a impl EntryStatusHeader) -> Self {
        Self {
            stage: header.stage(),
            status: header.status(),
            entry_key: header.entry_key(),
            metadata: header.metadata(),
            stats: header.stats(),
        }
    }

    pub fn base_entry_id(&self, context: &EntryContext<'_>) -> BaseEntryId {
        self.entry_key
            .entry_id()
            .to_base_id(context.qid.partition_key())
    }
}

/// One source mutation. Before-images must include earlier writes in the same
/// transaction; absence is represented only by insertion or deletion.
#[derive(Debug)]
pub enum EntryChange<'a> {
    Insert {
        after: EntryStateRef<'a>,
    },
    Update {
        before: EntryStateRef<'a>,
        after: EntryStateRef<'a>,
    },
    Delete {
        before: EntryStateRef<'a>,
    },
}

impl<'a> EntryChange<'a> {
    pub fn before(&self) -> Option<&EntryStateRef<'a>> {
        match self {
            Self::Insert { .. } => None,
            Self::Update { before, .. } | Self::Delete { before } => Some(before),
        }
    }

    pub fn after(&self) -> Option<&EntryStateRef<'a>> {
        match self {
            Self::Delete { .. } => None,
            Self::Insert { after } | Self::Update { after, .. } => Some(after),
        }
    }
}
