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

use crate::table_macro::*;

define_table!(
    /// Partition-local VQueue entry counts grouped by virtual-object dimensions, stage, and status.
    sys_virtual_object_stats(
        /// Name of the virtual-object service whose entries are counted.
        service_name: DataType::LargeUtf8,

        /// Scope of the virtual object. NULL for unscoped virtual objects.
        scope: DataType::LargeUtf8,

        /// Key of the virtual object whose entries are counted.
        key: DataType::LargeUtf8,

        /// Name of the handler. The handler is NULL for state mutations.
        handler: DataType::LargeUtf8,

        /// The kind of operation. One of `state-mutation` or `invocation`.
        kind: DataType::LargeUtf8,

        /// VQueue stage whose entries are counted. One of `inbox`, `running`,
        /// `suspended`, `paused`, or `finished`.
        stage: DataType::LargeUtf8,

        /// The entry processing status. Examples are `new`, `scheduled`, `started`,
        /// `backing-off`, `yielded`, `killed`, `cancelled`, `failed`, and `succeeded`.
        status: DataType::LargeUtf8,

        /// Number of entries in this partition-local stage/status bucket.
        /// Buckets with no entries are omitted.
        num_entries: DataType::UInt64,

        /// Internal column that is used for partitioning virtual objects. Can be ignored.
        partition_key: DataType::UInt64,

        )
);
