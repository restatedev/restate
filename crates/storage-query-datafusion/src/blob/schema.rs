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

define_sort_order!(sys_blob(created_at));

define_table!(
    /// Blobs stored by each partition, such as invocation outputs. Use the kind-specific
    /// tables (e.g. `sys_blob_output`) to read the decoded payload.
    sys_blob(
    /// Internal column that is used for partitioning the blobs. Can be ignored.
    partition_key: DataType::UInt64,

    /// Blob ID, formatted as `blob_1...`. Matches `sys_invocation_status.completion_blob_id`.
    id: DataType::LargeUtf8,

    /// Timestamp indicating when the blob was created. Derived from the blob ID.
    created_at: TimestampMillisecond,

    /// The kind of the blob. Either `output`, `state` or `generic`.
    kind: DataType::LargeUtf8,

    /// Number of references to this blob. A blob with `ref_count = 0` is marked for deletion
    /// and has no payload.
    ref_count: DataType::UInt32,

    /// Length of the blob payload in bytes.
    payload_length: DataType::UInt64,
));
