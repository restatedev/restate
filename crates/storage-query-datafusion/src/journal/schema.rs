// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::table_macro::*;

use datafusion::arrow::datatypes::DataType;

define_sort_order!(sys_journal(partition_key, id));

define_table!(sys_journal (
    /// Internal column that is used for partitioning the services invocations. Can be ignored.
    partition_key: DataType::UInt64,

    /// [Invocation ID](/operate/invocation#invocation-identifier).
    id: DataType::LargeUtf8,

    /// The index of this journal entry.
    index: DataType::UInt32,

    /// The entry type. You can check all the available entry types in [`entries.rs`](https://github.com/restatedev/restate/blob/main/crates/types/src/journal/entries.rs).
    entry_type: DataType::LargeUtf8,

    /// The name of the entry supplied by the user, if any.
    name: DataType::LargeUtf8,

    /// Indicates whether this journal entry has been completed; this is only valid for some entry
    /// types.
    completed: DataType::Boolean,

    /// If this entry represents an outbound invocation, indicates the ID of that invocation.
    invoked_id: DataType::LargeUtf8,

    /// If this entry represents an outbound invocation, indicates the invocation Target. Format
    /// for plain services: `ServiceName/HandlerName`, e.g. `Greeter/greet`. Format for
    /// virtual objects/workflows: `VirtualObjectName/Key/HandlerName`, e.g.
    /// `Greeter/Francesco/greet`.
    invoked_target: DataType::LargeUtf8,

    /// If this entry represents a sleep, indicates wakeup time.
    sleep_wakeup_at: TimestampMillisecond,

    /// If this entry is a promise related entry (GetPromise, PeekPromise, CompletePromise), indicates the promise name.
    promise_name: DataType::LargeUtf8,

    /// Raw binary representation of the entry. Check the [service protocol](https://github.com/restatedev/service-protocol)
    /// for more details to decode it.
    raw: DataType::LargeBinary,

    /// The journal version.
    version: DataType::UInt32,

    /// The entry serialized as a JSON string. Filled only if journal version is 2.
    entry_json: DataType::LargeUtf8,

    /// The EntryLite projection serialized as a JSON string. Filled only if journal version is 2.
    entry_lite_json: DataType::LargeUtf8,

    /// When the entry was appended to the journal. Filled only if journal version is 2.
    appended_at: TimestampMillisecond,
));
