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
    /// Experimental inspection of the persisted stage/transition-time entry index.
    /// Includes invocations and state mutations, including finished entries until deletion.
    /// Entries predating index activation are not backfilled; this is not an authoritative inventory.
    _idx_entry_by_stage(
        /// Physical partition containing the index entry.
        partition_id: DataType::UInt32,
        /// VQueue stage, not the invocation status reported by sys_invocation.
        stage: DataType::LargeUtf8,
        /// Time of the entry's last stage transition, at millisecond precision.
        transitioned_at: TimestampMillisecond,
        /// Entry processing status, such as new, scheduled, started, or succeeded.
        status: DataType::LargeUtf8,
        /// Canonical entry ID stored as the index's primary-key suffix.
        canonical_id: DataType::LargeUtf8,
        /// Resource ID derived from the canonical ID, without its sequence suffix.
        entry_id: DataType::LargeUtf8,
        /// Partition key encoded in the canonical ID.
        partition_key: DataType::UInt64,
    )
);
