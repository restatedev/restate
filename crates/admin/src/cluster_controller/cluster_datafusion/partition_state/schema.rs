// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(dead_code)]

use restate_datafusion::prelude::*;

define_table!(
    /// Observed partition state
    partition_state(
        /// Partition ID
        partition_id: DataType::UInt32,

        /// Node ID where the partition is running
        plain_node_id: DataType::Utf8,

        /// Node generation
        gen_node_id: DataType::Utf8,

        /// Observed target run mode of partition (LEADER, FOLLOWER)
        target_mode: DataType::Utf8,

        /// Effective partition run mode of partition (LEADER, FOLLOWER)
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

        /// Number of skipped records
        skipped_records: DataType::UInt64,

        /// Replay status
        replay_status: DataType::Utf8,

        /// Last persisted log LSN
        persisted_log_lsn: DataType::UInt64,

        /// Last archived log LSN
        archived_log_lsn: DataType::UInt64,

        /// Target tail LSN
        target_tail_lsn: DataType::UInt64,
    )
);
