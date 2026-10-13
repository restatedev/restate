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
    /// Inspection of the covering busy-queue index, including zero-count queues until metadata deletion.
    _idx_busy_vqueue(
        /// Total inbox, running, suspended, and paused entries.
        total_non_completed: DataType::UInt64,
        /// Latest creation, enqueue, start, attempt, or finish timestamp.
        last_modified: TimestampMillisecond,
        /// Scope, or NULL for an unscoped queue.
        scope: DataType::LargeUtf8,
        /// VQueue identity.
        vqueue_id: DataType::LargeUtf8,
        /// Entries waiting in the inbox.
        num_inbox: DataType::UInt64,
        /// Running entries.
        num_running: DataType::UInt64,
        /// Suspended entries.
        num_suspended: DataType::UInt64,
        /// Paused entries.
        num_paused: DataType::UInt64,
        /// Finished entries retained until deletion.
        num_finished: DataType::UInt64,
        /// Partition key encoded in the VQueue ID.
        partition_key: DataType::UInt64,
    )
);
