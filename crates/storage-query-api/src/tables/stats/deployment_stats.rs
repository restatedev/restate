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

use restate_storage_api::vqueue_table::Stage;
use restate_types::vqueues::EffectiveStatus;
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
            /// VQueue stage, including finished entries; NULL for a stage this version doesn't know.
            #[values(Stage::VALUES)]
            stage: Stage => DataType::LargeUtf8,
            /// The entries' effective status, as in `sys_vqueue_entry_status`:
            /// what they are doing now, which also tells their stage. NULL for a
            /// stored stage and status this version doesn't know.
            #[values(EffectiveStatus::VALUES)]
            status: EffectiveStatus => DataType::LargeUtf8,
            /// Partition-local contribution; the public view sums contributions.
            num_entries: u64 => DataType::UInt64,
        }
    }
}
