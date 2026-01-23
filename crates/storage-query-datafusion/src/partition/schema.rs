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
    /// Partition table
    partition(
        /// Partition ID
        partition_id: DataType::UInt32,

        /// Partition start key
        start_key: DataType::UInt64,

        /// Partition end key
        end_key: DataType::UInt64,

        /// The log id serving this partition
        log_id: DataType::UInt32,

        /// The column family name
        cf_name: DataType::Utf8,

        /// The database name
        db_name: DataType::Utf8,

        /// The partition leader generational node id acquired from gossip
        leader_gen_node_id: DataType::Utf8,

        /// The partition leader plain node id acquired from gossip
        leader_plain_node_id: DataType::Utf8,

        /// The observed leader epoch from gossip
        leader_epoch: DataType::Utf8,

        /// The observed current membership version from gossip
        v_current: DataType::UInt32,

        /// A comma separated list of nodes representing the current members of the replica-set
        current_replica_set: DataType::Utf8,

        /// The observed next membership version from gossip
        v_next: DataType::UInt32,

        /// A comma separated list of nodes representing the next members of the replica-set
        next_replica_set: DataType::Utf8,

        /// Current known metadata version
        partition_table_version: DataType::UInt32,
    )
);
