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
    /// VQueue entry counts grouped by service dimensions across the cluster.
    sys_service_stats(
        /// Name of the service whose entries are counted.
        service_name: DataType::LargeUtf8,

        /// Name of the handler. The handler is NULL for operations that are not linked
        /// to a handler (e.g. state mutations).
        handler: DataType::LargeUtf8,

        /// The kind of operation. Examples are `state-mutation` and `invocation`.
        kind: DataType::LargeUtf8,

        /// VQueue stage whose entries are counted. One of `inbox`, `running`,
        /// `suspended`, `paused`, or `finished`.
        stage: DataType::LargeUtf8,

        /// The entry processing status. Examples are `new`, `scheduled`, `started`,
        /// `backing-off`, `yielded`, `killed`, `cancelled`, `failed`, and `succeeded`.
        status: DataType::LargeUtf8,

        /// Number of entries aggregated across all physical partitions.
        num_entries: DataType::UInt64,
    )
);
