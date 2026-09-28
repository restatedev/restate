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
    /// VQueue entry counts grouped by deployment, service, and stage across the cluster.
    sys_deployment_stats(
        /// Identifier of the deployment whose entries are counted.
        deployment_id: DataType::LargeUtf8,

        /// Name of the service whose entries are counted.
        service_name: DataType::LargeUtf8,

        /// VQueue stage whose entries are counted. One of `inbox`, `running`,
        /// `suspended`, `paused`, or `finished`.
        stage: DataType::LargeUtf8,

        /// Number of entries aggregated across all physical partitions.
        num_entries: DataType::UInt64,
    )
);
