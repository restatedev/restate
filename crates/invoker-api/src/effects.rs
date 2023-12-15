// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_types::errors::InvocationError;
use restate_types::identifiers::FullInvocationId;
use restate_types::identifiers::{DeploymentId, EntryIndex};
use restate_types::journal::enriched::EnrichedRawEntry;
use std::collections::HashSet;

#[derive(Debug)]
pub struct Effect {
    pub full_invocation_id: FullInvocationId,
    pub kind: EffectKind,
}

#[derive(Debug)]
pub enum EffectKind {
    /// This is sent before any new entry is created by the invoker. This won't be sent if the deployment_id is already set.
    SelectedDeployment(DeploymentId),
    JournalEntry {
        entry_index: EntryIndex,
        entry: EnrichedRawEntry,
    },
    Suspended {
        waiting_for_completed_entries: HashSet<EntryIndex>,
    },
    /// This is sent always after [`Self::JournalEntry`] with `OutputStreamEntry`(s).
    End,
    /// This is sent when the invoker exhausted all its attempts to make progress on the specific invocation.
    Failed(InvocationError),
}
