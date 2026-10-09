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
    /// Partition ReplicaSet table
    partition_replica_set(
        // Partition ID
        partition_id: DataType::UInt32,

        // The partition leader plain node id acquired from gossip
        plain_node_id: DataType::Utf8,

        // If the node is alive, this will have the generational node id
        alive_gen_node_id: DataType::Utf8,

        // Is this believed to be the current leader of the partition?
        is_leader: DataType::Boolean,

        // The observed membership version from gossip
        // this can be either the current or next depending on the value of `membership`
        membership_version: DataType::UInt32,

        /// This can be either `current` or `next`.
        membership: DataType::Utf8,

        /// If this is the leader, what is the current leader epoch?
        leader_epoch: DataType::Utf8,

        /// Durable log LSN if reported via gossip
        durable_lsn: DataType::UInt64,
    )
);
