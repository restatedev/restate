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

define_sort_order!(sys_journal_events(partition_key, id));

define_table!(sys_journal_events (
    /// Internal column that is used for partitioning the services invocations. Can be ignored.
    partition_key: DataType::UInt64,

    /// [Invocation ID](/operate/invocation#invocation-identifier).
    id: DataType::LargeUtf8,

    /// The journal index after which this event happened. This can be used to establish a total order between events and journal entries.
    after_journal_entry_index: DataType::UInt32,

    /// When the entry was appended to the journal.
    appended_at: TimestampMillisecond,

    /// The event type.
    event_type: DataType::LargeUtf8,

    /// The event serialized as a JSON string.
    event_json: DataType::LargeUtf8,
));
