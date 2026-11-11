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
use strum::VariantArray;

use restate_storage_api::vqueue_table::{Stage, Status};
use restate_util_string::ReString;

use crate::{ShardingScheme, define_schema};

define_schema! {
    /// Current VQueue entry counts by pinned deployment and service.
    /// Combines handlers, scopes, and entry kinds; excludes entries without a deployment.
    deployment_stats {
        name: "sys_deployment_stats",
        sharding: ShardingScheme::ByPartitionId { is_leader_required: false },
        // Rows aggregate a partition's entries; no column identifies the partition.
        sharding_columns {},
        columns {
            /// The deployment identifier as exposed by the statistics table.
            #[starts_with]
            deployment_id: ReString => DataType::LargeUtf8,
            /// The service name.
            #[starts_with]
            service_name: ReString => DataType::LargeUtf8,
            /// VQueue stage, including unknown and finished entries.
            #[values(Stage::VARIANTS.iter().copied())]
            stage: Stage => DataType::LargeUtf8,
            /// Entry processing status, including unknown and terminal statuses.
            #[values(Status::VARIANTS.iter().copied())]
            status: Status => DataType::LargeUtf8,
            /// Partition-local contribution; the public view sums contributions.
            num_entries: u64 => DataType::UInt64,
        }
    }
}
