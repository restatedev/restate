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
    /// Experimental inspection of persisted invocation secondary-index entries.
    /// Contains retained VQueue invocations, including Finished entries until deletion.
    /// State mutations are excluded. Entries predating index activation are not backfilled,
    /// so this table is not an authoritative inventory of all invocations.
    _idx_invocation_by_service(
        /// Physical partition containing the index entry.
        partition_id: DataType::UInt32,
        /// Name of the invocation's service.
        service_name: DataType::LargeUtf8,
        /// VQueue stage, not the invocation status reported by sys_invocation.
        stage: DataType::LargeUtf8,
        /// Time of the entry's last stage transition, at millisecond precision.
        transitioned_at: TimestampMillisecond,
        /// Full internal hybrid-logical timestamp, including its logical counter.
        /// Use this column when ordering entries with transitions in the same millisecond.
        transitioned_at_hlc: DataType::UInt64,
        /// Invocation ID stored as the index's primary-key suffix.
        invocation_id: DataType::LargeUtf8,
        /// Partition key encoded in the invocation ID.
        partition_key: DataType::UInt64,
    )
);
