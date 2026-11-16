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
    /// Each partition processor's state as its worker reports it, with one row for
    /// every replica this coordinator expects even when no report came back: the
    /// `report` column says which rows are reports, and the other rows have no
    /// processor values.
    partition_state {
        name: "partition_state",
        sharding: ShardingScheme::Coordinator,
        columns {
            /// Partition ID.
            partition_id: u32 => DataType::UInt32,
            /// Node ID of the replica; NULL for a partition with no known replicas.
            plain_node_id: Option<ReString> => DataType::Utf8,
            /// Generation of the node that reported; NULL unless reported.
            gen_node_id: Option<ReString> => DataType::Utf8,
            /// Observed target run mode (leader, follower); NULL unless reported.
            target_mode: Option<ReString> => DataType::Utf8,
            /// Effective partition run mode; NULL unless reported.
            effective_mode: Option<ReString> => DataType::Utf8,
            /// When the processor last updated its state; NULL unless reported.
            updated_at: Option<MillisSinceEpoch> => TimestampMillisecond,
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
            /// Enabled state-machine features; NULL unless reported.
            enabled_features: Option<Vec<Option<ReString>>> => Utf8List,
            /// Why the processor stopped running, absent while healthy.
            broken_reason: Option<ReString> => DataType::Utf8,
            /// Enabled partition-store features; NULL unless reported.
            enabled_storage_features: Option<Vec<Option<ReString>>> => Utf8List,
            /// LSN of the record that turned byte offsets on for the partition, which
            /// they count from. NULL while the partition doesn't track byte offsets.
            byte_offsets_base_lsn: Option<u64> => DataType::UInt64,
            /// How far the partition has applied its log in bytes, since
            /// `byte_offsets_base_lsn`. Replicas at the same applied LSN normally agree
            /// on it, but it can differ slightly between them. NULL while the partition
            /// doesn't track byte offsets.
            log_byte_offset: Option<u64> => DataType::UInt64,
            /// How much data this replica has written to its partition store in bytes,
            /// before compression, since `byte_offsets_base_lsn`, as of its last commit.
            /// Local to each replica: replicas batch their writes differently, a replica
            /// that crashed writes what it hadn't flushed again in different batches,
            /// and a replica restored from a snapshot continues from the snapshot's.
            /// NULL while the partition doesn't track byte offsets.
            partition_store_byte_offset: Option<u64> => DataType::UInt64,
            /// Whether the row is a report: `reported` (the worker's own report),
            /// `node_unavailable` (no complete report from the node: it wasn't asked, or
            /// its request failed, timed out or broke off), `not_reported` (the node's
            /// complete report has no entry for this partition) or `no_known_replicas`
            /// (the partition has no known replicas and no report).
            report: ReString => DataType::Utf8,
        }
    }
}
