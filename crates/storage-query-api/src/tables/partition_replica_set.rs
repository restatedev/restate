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
    /// One row per member of each partition's current and next membership, as this
    /// coordinator has observed them.
    partition_replica_set {
        name: "partition_replica_set",
        sharding: ShardingScheme::Coordinator,
        columns {
            /// Partition ID.
            partition_id: u32 => DataType::UInt32,
            /// The member's node ID.
            plain_node_id: ReString => DataType::Utf8,
            /// The member's generational node ID; NULL unless this coordinator's gossip
            /// sees the member alive.
            alive_gen_node_id: Option<ReString> => DataType::Utf8,
            /// TRUE when the member is the observed leader; NULL otherwise.
            is_leader: Option<bool> => DataType::Boolean,
            /// Version of the membership the row belongs to.
            membership_version: u32 => DataType::UInt32,
            /// Which membership the row belongs to: `current` or `next`. A member of both
            /// has a row for each.
            membership: ReString => DataType::Utf8,
            /// The leader epoch, for the leader's row; NULL otherwise.
            leader_epoch: Option<ReString> => DataType::Utf8,
            /// The member's durable log LSN, as observed; NULL when unknown.
            durable_lsn: Option<u64> => DataType::UInt64,
        }
    }
}
