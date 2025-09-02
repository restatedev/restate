// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::journal_events::schema::SysJournalEventsBuilder;
use restate_storage_api::journal_events::EventView;
use restate_types::identifiers::{InvocationId, WithPartitionKey};

#[inline]
pub(crate) fn append_journal_event_row(
    builder: &mut SysJournalEventsBuilder,
    invocation_id: InvocationId,
    journal_event: EventView,
) {
    let mut row = builder.row();

    row.partition_key(invocation_id.partition_key());
    if row.is_id_defined() {
        row.fmt_id(invocation_id);
    }

    row.appended_at(journal_event.append_time.as_u64() as i64);
    row.after_journal_entry_index(journal_event.after_journal_entry_index);
    row.fmt_event_type(journal_event.event.ty());

    if row.is_event_json_defined()
        && let Ok(json) = serde_json::to_string(&journal_event.event.into_event_or_unknown())
    {
        row.event_json(json);
    }
}
