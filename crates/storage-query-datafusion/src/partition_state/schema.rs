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
    /// Observed partition state
    partition_state(
        /// Partition ID
        partition_id: DataType::UInt32,

        /// Node ID where the partition is running
        plain_node_id: DataType::Utf8,

        /// Node generation
        gen_node_id: DataType::Utf8,

        /// Observed target run mode of partition (leader, follower)
        target_mode: DataType::Utf8,

        /// Effective partition run mode of partition (candidate, becoming_leader, leader, follower)
        effective_mode: DataType::Utf8,

        /// Last updated
        updated_at: TimestampMillisecond,

        /// Last observed leader epoch
        leader_epoch: DataType::UInt64,

        /// Last observed leader node id
        leader: DataType::Utf8,

        /// Last applied log LSN
        applied_log_lsn: DataType::UInt64,

        /// Last record applied at
        last_record_applied_at: TimestampMillisecond,

        /// Replay status. NULL if the processor is broken, since it is not replaying
        replay_status: DataType::Utf8,

        /// Durable log LSN
        durable_log_lsn: DataType::UInt64,

        /// Last archived log LSN
        archived_log_lsn: DataType::UInt64,

        /// Target tail LSN
        target_tail_lsn: DataType::UInt64,

        /// Version of the rule book currently applied by the partition processor
        applied_rule_book_version: DataType::UInt32,

        /// Version of the schema currently applied by the partition processor
        applied_schema_version: DataType::UInt32,

        /// State-machine features currently enabled on the partition processor.
        /// Query membership with `array_has(enabled_features, 'vqueues')`.
        enabled_features: Utf8List,

        /// Why the node gave up on running this partition processor, if it did.
        /// NULL while the processor is healthy.
        broken_reason: DataType::Utf8,

        /// Partition-store on-disk local storage features.
        enabled_storage_features: Utf8List,

        /// LSN of the record that turned byte offsets on for the partition, which they count
        /// from. NULL while the partition doesn't track byte offsets.
        byte_offsets_base_lsn: DataType::UInt64,

        /// How far the partition has applied its log in bytes, since `byte_offsets_base_lsn`.
        /// Replicas at the same applied LSN normally agree on it, but it can differ slightly
        /// between them. NULL while the partition doesn't track byte offsets.
        log_byte_offset: DataType::UInt64,

        /// How much data this replica has written to its partition store in bytes, before
        /// compression, since `byte_offsets_base_lsn`, as of its last commit. Local to each
        /// replica: replicas batch their writes differently, a replica that crashed writes what it
        /// hadn't flushed again in different batches, and a replica restored from a snapshot
        /// continues from the snapshot's. NULL while the partition doesn't track byte offsets.
        partition_store_byte_offset: DataType::UInt64,
    )
);
