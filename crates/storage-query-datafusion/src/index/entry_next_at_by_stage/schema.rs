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
    /// Experimental inspection of the persisted stage/next-transition-time entry index.
    /// Includes invocations and state mutations, including finished entries until deletion.
    /// Entries predating index activation are not backfilled; this is not an authoritative inventory.
    _idx_entry_next_at_by_stage(
        /// VQueue stage, not the invocation status reported by sys_invocation.
        stage: DataType::LargeUtf8,
        /// Scheduled next transition time from the entry ordering key.
        /// Stored with second precision and exposed as a millisecond timestamp.
        next_at: TimestampMillisecond,
        /// Sequence breaking ties between transitions in the same second.
        seq: DataType::UInt64,
        /// Canonical entry ID stored as the index's primary-key suffix.
        canonical_id: DataType::LargeUtf8,
        /// Resource ID derived from the canonical ID, without its sequence suffix.
        entry_id: DataType::LargeUtf8,
        /// Partition key encoded in the canonical ID.
        partition_key: DataType::UInt64,
    )
);
