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

use restate_util_string::ReString;

use crate::{ShardingScheme, define_schema};

define_schema! {
    /// The cluster's partitions, from this coordinator's partition table, with the
    /// leader and memberships this coordinator has observed.
    partitions {
        name: "partitions",
        sharding: ShardingScheme::Coordinator,
        columns {
            /// Partition ID.
            partition_id: u32 => DataType::UInt32,
            /// First partition key of the partition.
            start_key: u64 => DataType::UInt64,
            /// Last partition key of the partition, inclusive.
            end_key: u64 => DataType::UInt64,
            /// The log serving the partition.
            log_id: u32 => DataType::UInt32,
            /// Name of the partition's column family.
            cf_name: ReString => DataType::Utf8,
            /// The observed leader's generational node ID; NULL when no leader is known.
            leader_gen_node_id: Option<ReString> => DataType::Utf8,
            /// The observed leader's node ID; NULL when no leader is known.
            leader_plain_node_id: Option<ReString> => DataType::Utf8,
            /// The observed leader epoch.
            leader_epoch: ReString => DataType::Utf8,
            /// Version of the observed current membership.
            v_current: u32 => DataType::UInt32,
            /// The nodes of the observed current membership, comma-separated.
            current_replica_set: ReString => DataType::Utf8,
            /// Version of the observed next membership; NULL when no change is under way.
            v_next: Option<u32> => DataType::UInt32,
            /// The nodes of the observed next membership, comma-separated; NULL when no
            /// change is under way.
            next_replica_set: Option<ReString> => DataType::Utf8,
            /// Version of the partition table the row comes from.
            partition_table_version: u32 => DataType::UInt32,
        }
    }
}
