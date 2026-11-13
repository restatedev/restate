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
    /// Journal entries of invocations.
    ///
    /// The journal records the steps an invocation took (calls, sleeps, state access, promises, ...), one row per entry, ordered by `index`.
    journal {
        name: "sys_journal",
        sharding: ShardingScheme::ByPartitionKey { is_leader_required: false },
        sharding_columns { partition_key: PartitionKey, id: InvocationId },
        row: for<'a> JournalView<'a>, append: append_journal,
        columns {
            /// Internal column that is used for partitioning the services invocations. Can be ignored.
            partition_key: PartitionKey => DataType::UInt64,
            /// [Invocation ID](/services/invocation/managing-invocations#invocation-id).
            id: InvocationId => DataType::LargeUtf8,
            /// The index of this journal entry.
            index: u32 => DataType::UInt32,
            /// The entry type. You can check all the available entry types in [`entries.rs`](https://github.com/restatedev/restate/blob/main/crates/types/src/journal/entries.rs).
            entry_type: ReString => DataType::LargeUtf8,
            /// The name of the entry supplied by the user, if any.
            name: Option<ReString> => DataType::LargeUtf8,
            /// Indicates whether this journal entry has been completed; this is only valid for some entry
            /// types.
            completed: Option<bool> => DataType::Boolean,
            /// If this entry represents an outbound invocation, indicates the ID of that invocation.
            invoked_id: Option<ReString> => DataType::LargeUtf8,
            /// If this entry represents an outbound invocation, indicates the invocation Target. Format
            /// for plain services: `ServiceName/HandlerName`, e.g. `Greeter/greet`. Format for
            /// virtual objects/workflows: `VirtualObjectName/Key/HandlerName`, e.g.
            /// `Greeter/Francesco/greet`.
            invoked_target: Option<ReString> => DataType::LargeUtf8,
            /// If this entry represents a sleep, indicates wakeup time.
            sleep_wakeup_at: Option<MillisSinceEpoch> => TimestampMillisecond,
            /// If this entry is a promise related entry (GetPromise, PeekPromise, CompletePromise), indicates the promise name.
            promise_name: Option<ReString> => DataType::LargeUtf8,
            /// Raw binary representation of the entry. Check the [service protocol](https://github.com/restatedev/service-protocol)
            /// for more details to decode it.
            raw: Option<Vec<u8>> => DataType::LargeBinary,
            /// The byte length of the raw entry. If you are writing a query that only needs to know the length,
            /// reading this field will be much more efficient than reading length(raw).
            raw_length: Option<u64> => DataType::UInt64,
            /// The journal version.
            version: u32 => DataType::UInt32,
            /// The entry serialized as a JSON string. Filled only if journal version is 2.
            entry_json: Option<ReString> => DataType::LargeUtf8,
            /// The EntryLite projection serialized as a JSON string. Filled only if journal version is 2.
            entry_lite_json: Option<ReString> => DataType::LargeUtf8,
            /// When the entry was appended to the journal. Filled only if journal version is 2.
            appended_at: Option<MillisSinceEpoch> => TimestampMillisecond,
        }
    }
}

/// One stored entry, written through the projected row writer. Readers own entry
/// decoding, which depends on the stored service-protocol formats; writing through
/// the writer keeps the raw entry borrowed and skips unrequested derived columns.
pub struct JournalView<'a>(pub &'a dyn Fn(&mut JournalRowWriter<'_>));

fn append_journal(builder: &mut JournalBatchBuilder, view: JournalView<'_>) {
    (view.0)(&mut builder.row());
}
