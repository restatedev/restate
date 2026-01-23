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

define_sort_order!(sys_inbox(partition_key, id));

define_table!(sys_inbox(
    /// Internal column that is used for partitioning the services invocations. Can be ignored.
    partition_key: DataType::UInt64,

    /// The name of the invoked virtual object/workflow.
    service_name: DataType::LargeUtf8,

    /// The key of the virtual object/workflow.
    service_key: DataType::LargeUtf8,

    /// [Invocation ID](/operate/invocation#invocation-identifier).
    id: DataType::LargeUtf8,

    /// Sequence number in the inbox.
    sequence_number: DataType::UInt64,

    /// Timestamp indicating the start of this invocation.
    /// DEPRECATED: you should not use this field anymore, but join with the sys_invocation table
    created_at: TimestampMillisecond,
));
