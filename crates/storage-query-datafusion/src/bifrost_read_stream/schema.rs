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
    /// Active bifrost read streams on each node in the cluster.
    bifrost_read_streams(
        /// The PlainNodeId of the node hosting this read stream.
        plain_node_id: DataType::Utf8,
        /// Current known generation ID of the node.
        gen_node_id: DataType::Utf8,
        /// Unique stream identifier (node-local).
        stream_id: DataType::Utf8,
        /// The log being read.
        log_id: DataType::UInt32,
        /// Next LSN to be read.
        read_pointer: DataType::UInt64,
        /// Inclusive max LSN to read to (u64::MAX for tailing streams).
        end_lsn: DataType::UInt64,
        /// Current state machine state (e.g. "Reading", "FindingLoglet").
        state: DataType::Utf8,
        /// The safe known tail LSN (only available while reading).
        safe_known_tail: DataType::UInt64,
        /// Index of the segment currently being read from.
        current_segment: DataType::UInt32,
        /// The loglet ID of the current segment (log_id combined with segment_index).
        loglet_id: DataType::Utf8,
        /// Whether this is a tailing read stream (end_lsn == MAX).
        is_tailing: DataType::Boolean,
        /// Whether the read stream has terminated.
        is_terminated: DataType::Boolean,
        /// How far behind the known tail the read pointer is (tail - read_pointer).
        /// Null if the tail is unknown.
        behind_count: DataType::UInt64,
    )
);
