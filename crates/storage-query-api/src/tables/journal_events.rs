// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datafusion::arrow::datatypes::DataType;

use restate_types::identifiers::{InvocationId, PartitionKey};
use restate_types::time::MillisSinceEpoch;
use restate_util_string::ReString;

use crate::{ShardingScheme, define_schema};

define_schema! {
    /// Events that happened during an invocation's execution.
    ///
    /// Events are transient errors, pauses and suspensions. They are not journal entries; use `after_journal_entry_index` to place them between them.
    journal_events {
        name: "sys_journal_events",
        sharding: ShardingScheme::ByPartitionKey { is_leader_required: false },
        sharding_columns { partition_key: PartitionKey, id: InvocationId },
        row: for<'a> JournalEventsView<'a>, append: append_journal_events,
        columns {
            /// Internal column that is used for partitioning the services invocations. Can be ignored.
            partition_key: PartitionKey => DataType::UInt64,
            /// [Invocation ID](/services/invocation/managing-invocations#invocation-id).
            id: InvocationId => DataType::LargeUtf8,
            /// The journal index after which this event happened. This can be used to establish a total order between events and journal entries.
            after_journal_entry_index: u32 => DataType::UInt32,
            /// When the entry was appended to the journal.
            appended_at: MillisSinceEpoch => TimestampMillisecond,
            /// The event type.
            event_type: ReString => DataType::LargeUtf8,
            /// The event serialized as a JSON string.
            event_json: Option<ReString> => DataType::LargeUtf8,
        }
    }
}

/// One stored event, written through the projected row writer. Readers own event
/// decoding; writing through the writer skips unrequested derived columns.
pub struct JournalEventsView<'a>(pub &'a dyn Fn(&mut JournalEventsRowWriter<'_>));

fn append_journal_events(builder: &mut JournalEventsBatchBuilder, view: JournalEventsView<'_>) {
    (view.0)(&mut builder.row());
}
