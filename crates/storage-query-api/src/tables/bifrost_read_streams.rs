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
    /// Active bifrost read streams on each node in the cluster.
    bifrost_read_streams {
        name: "bifrost_read_streams",
        sharding: ShardingScheme::ByNode(None),
        sharding_columns { plain_node_id: PlainNodeId, gen_node_id: GenerationalNodeId },
        columns {
            /// The PlainNodeId of the node hosting this read stream.
            plain_node_id: ReString => DataType::Utf8,
            /// Current known generation ID of the node.
            gen_node_id: ReString => DataType::Utf8,
            /// The log being read.
            log_id: u32 => DataType::UInt32,
            /// Who reads, as named by the stream's owner, e.g. `pp-3` for partition 3's processor.
            name: Option<ReString> => DataType::Utf8,
            /// What the owner currently does with the stream, e.g. `leader` or `follower` for a
            /// partition processor.
            role: Option<ReString> => DataType::Utf8,
            /// The loglet ID of the current segment.
            loglet_id: Option<ReString> => DataType::Utf8,
            /// Next LSN to be read.
            read_pointer: u64 => DataType::UInt64,
            /// Inclusive max LSN to read to (u64::MAX for tailing streams).
            end_lsn: u64 => DataType::UInt64,
            /// Current state machine state (e.g. "Reading", "FindingLoglet").
            state: ReString => DataType::Utf8,
            /// The safe known tail LSN (only available while reading).
            safe_known_tail: Option<u64> => DataType::UInt64,
            /// Index of the segment currently being read from.
            current_segment: Option<u32> => DataType::UInt32,
            /// Whether this is a tailing read stream (end_lsn == MAX).
            is_tailing: bool => DataType::Boolean,
            /// Data records the stream has delivered to its reader since it was created, across
            /// all segments.
            records_read: u64 => DataType::UInt64,
            /// The size in bytes of those records' bodies, as the log stores them.
            bytes_read: u64 => DataType::UInt64,
        }
    }
}
