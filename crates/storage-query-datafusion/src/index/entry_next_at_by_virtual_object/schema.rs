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
    /// Inspection of virtual-object invocations and state mutations ordered by identity, stage, next time, and sequence.
    _idx_entry_next_at_by_virtual_object(
        /// Virtual-object service name.
        service_name: DataType::LargeUtf8,
        /// Scope, or NULL for an unscoped object.
        scope: DataType::LargeUtf8,
        /// Virtual-object key.
        key: DataType::LargeUtf8,
        /// VQueue stage.
        stage: DataType::LargeUtf8,
        /// Next transition time, stored in seconds and exposed in milliseconds.
        next_at: TimestampMillisecond,
        /// Sequence breaking same-second ties.
        seq: DataType::UInt64,
        /// Canonical entry incarnation ID.
        canonical_id: DataType::LargeUtf8,
        /// Resource ID without its sequence suffix.
        entry_id: DataType::LargeUtf8,
        /// Partition key encoded in the canonical ID.
        partition_key: DataType::UInt64,
    )
);
