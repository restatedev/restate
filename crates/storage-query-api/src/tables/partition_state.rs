// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datafusion::arrow::datatypes::DataType;

use restate_clock::time::MillisSinceEpoch;
use restate_util_string::ReString;

use crate::{ShardingScheme, define_schema};

define_schema! {
    /// Partition state observed by this coordinator's local controller.
    partition_state {
        name: "partition_state",
        sharding: ShardingScheme::Coordinator,
        columns {
            /// Partition ID.
            partition_id: u32 => DataType::UInt32,
            /// Node ID where the partition is running.
            plain_node_id: ReString => DataType::Utf8,
            /// Node generation.
            gen_node_id: ReString => DataType::Utf8,
            /// Observed target run mode (leader, follower).
            target_mode: ReString => DataType::Utf8,
            /// Effective partition run mode.
            effective_mode: ReString => DataType::Utf8,
            /// Last updated.
            updated_at: MillisSinceEpoch => TimestampMillisecond,
            /// Last observed leader epoch.
            leader_epoch: Option<u64> => DataType::UInt64,
            /// Last observed leader node ID.
            leader: Option<ReString> => DataType::Utf8,
            /// Last applied log LSN.
            applied_log_lsn: Option<u64> => DataType::UInt64,
            /// Last record applied at.
            last_record_applied_at: Option<MillisSinceEpoch> => TimestampMillisecond,
            /// Replay status, absent when the processor is broken.
            replay_status: Option<ReString> => DataType::Utf8,
            /// Durable log LSN.
            durable_log_lsn: Option<u64> => DataType::UInt64,
            /// Last archived log LSN.
            archived_log_lsn: Option<u64> => DataType::UInt64,
            /// Target tail LSN.
            target_tail_lsn: Option<u64> => DataType::UInt64,
            /// Applied rule-book version.
            applied_rule_book_version: Option<u32> => DataType::UInt32,
            /// Applied schema version.
            applied_schema_version: Option<u32> => DataType::UInt32,
            /// Enabled state-machine features.
            enabled_features: Vec<Option<ReString>> => Utf8List,
            /// Why the processor stopped running, absent while healthy.
            broken_reason: Option<ReString> => DataType::Utf8,
            /// Enabled partition-store features.
            enabled_storage_features: Vec<Option<ReString>> => Utf8List,
        }
    }
}
