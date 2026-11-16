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

use restate_clock::time::MillisSinceEpoch;
use restate_types::nodes_config::Role;
use restate_util_string::ReString;

use crate::{ShardingScheme, define_schema};

define_schema! {
    /// The loglets on each log server of the cluster, with their local state and,
    /// for loglets with a running worker, the worker's state.
    loglet_workers {
        name: "loglet_workers",
        sharding: ShardingScheme::ByNode(Some(Role::LogServer)),
        sharding_columns { plain_node_id: PlainNodeId, gen_node_id: GenerationalNodeId },
        columns {
            /// Node ID of the log server holding the loglet.
            plain_node_id: ReString => DataType::Utf8,
            /// Current known generation of the log server.
            gen_node_id: ReString => DataType::Utf8,
            /// Loglet ID.
            loglet_id: ReString => DataType::Utf8,
            /// The log the loglet belongs to, derived from the loglet ID.
            log_id: u32 => DataType::UInt32,
            /// The loglet's segment index in the log's chain, derived from the loglet ID.
            segment_index: u32 => DataType::UInt32,
            /// The durable local tail offset.
            local_tail: u32 => DataType::UInt32,
            /// Whether the loglet is sealed.
            is_sealed: bool => DataType::Boolean,
            /// The trim point offset.
            trim_point: u32 => DataType::UInt32,
            /// The known global tail offset.
            known_global_tail: u32 => DataType::UInt32,
            /// The loglet's sequencer node; NULL when not known.
            sequencer: Option<ReString> => DataType::Utf8,
            /// Whether a loglet worker task is running for the loglet.
            is_active: bool => DataType::Boolean,
            /// The pending local tail, including enqueued but uncommitted writes; NULL
            /// unless the worker is active.
            staging_local_tail: Option<u32> => DataType::UInt32,
            /// Whether a seal is enqueued but not committed yet; NULL unless the worker is
            /// active.
            seal_enqueued: Option<bool> => DataType::Boolean,
            /// Whether the worker accepts new writes; NULL unless the worker is active.
            accepting_writes: Option<bool> => DataType::Boolean,
            /// Number of pending seal requests; NULL unless the worker is active.
            pending_seals: Option<u32> => DataType::UInt32,
            /// Number of pending tail-wait requests; NULL unless the worker is active.
            pending_tail_waiters: Option<u32> => DataType::UInt32,
            /// When the worker last received a request; NULL unless the worker is active.
            last_request_at: Option<MillisSinceEpoch> => TimestampMillisecond,
        }
    }
}
