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
    /// Partition-local VQueue entry counts grouped by virtual-object dimensions.
    sys_virtual_object_stats(
        /// Physical partition containing these virtual-object statistics.
        partition_id: DataType::UInt32,

        /// Name of the virtual-object service whose entries are counted.
        service_name: DataType::LargeUtf8,

        /// Key of the virtual object whose entries are counted.
        key: DataType::LargeUtf8,

        /// Scope of the virtual object. NULL for unscoped virtual objects.
        scope: DataType::LargeUtf8,

        /// Name of the handler. The handler is NULL for state mutations.
        handler: DataType::LargeUtf8,

        /// The kind of operation. One of `state-mutation` or `invocation`.
        kind: DataType::LargeUtf8,

        /// Internal column that is used for partitioning virtual objects. Can be ignored.
        partition_key: DataType::UInt64,

        /// The number of entries that are in the inbox. The inbox is the priority
        /// queue that the scheduler uses to choose which entries to run next.
        num_inbox: DataType::UInt64,

        /// The number of entries that are currently running.
        num_running: DataType::UInt64,

        /// The number of entries that are suspended.
        num_suspended: DataType::UInt64,

        /// The number of entries that are paused.
        num_paused: DataType::UInt64,

        /// The number of entries that have finished processing and are pending
        /// deletion or archival.
        num_finished: DataType::UInt64,
        )
);
