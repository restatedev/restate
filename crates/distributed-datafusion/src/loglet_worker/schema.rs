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
    /// Information about loglet state on each log-server node in the cluster.
    loglet_workers(
        /// The PlainNodeId of the log-server hosting this loglet.
        plain_node_id: DataType::Utf8,
        /// Current known generation ID
        gen_node_id: DataType::Utf8,
        /// The unique loglet identifier.
        loglet_id: DataType::Utf8,
        /// The log ID this loglet belongs to (derived from loglet_id).
        log_id: DataType::UInt32,
        /// The segment index within the log chain (derived from loglet_id).
        segment_index: DataType::UInt32,
        /// The durable local tail offset.
        local_tail: DataType::UInt32,
        /// Whether the loglet is sealed.
        is_sealed: DataType::Boolean,
        /// The trim point offset.
        trim_point: DataType::UInt32,
        /// The known global tail offset.
        known_global_tail: DataType::UInt32,
        /// The sequencer node ID (if known).
        sequencer: DataType::Utf8,
        /// Whether there is an active loglet worker task for this loglet.
        is_active: DataType::Boolean,
        /// The pending (staging) local tail, including enqueued but uncommitted writes.
        /// Only set when the worker is active.
        staging_local_tail: DataType::UInt32,
        /// Whether a seal has been enqueued but not yet committed. Only set when active.
        seal_enqueued: DataType::Boolean,
        /// Whether the worker is accepting new writes. Only set when active.
        accepting_writes: DataType::Boolean,
        /// Number of pending seal RPCs. Only set when active.
        pending_seals: DataType::UInt32,
        /// Number of pending tail-wait RPCs. Only set when active.
        pending_tail_waiters: DataType::UInt32,
        /// Timestamp of the last request to the worker. Only set when active.
        last_request_at: TimestampMillisecond,
    )
);
