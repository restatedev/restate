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
    /// The segments of each log, from this coordinator's logs metadata.
    logs {
        name: "logs",
        sharding: ShardingScheme::Coordinator,
        columns {
            /// Log ID.
            log_id: u32 => DataType::UInt32,
            /// Index of the segment in the log's chain.
            segment_index: u32 => DataType::UInt32,
            /// First LSN of the segment.
            base_lsn: u64 => DataType::UInt64,
            /// The segment's loglet provider, for example `replicated`.
            kind: ReString => DataType::Utf8,
            /// Loglet ID; NULL unless the segment is replicated.
            loglet_id: Option<ReString> => DataType::Utf8,
            /// The loglet's sequencer node; NULL unless the segment is replicated.
            sequencer: Option<ReString> => DataType::Utf8,
            /// The loglet's replication property; NULL unless the segment is replicated.
            replication: Option<ReString> => DataType::Utf8,
            /// The log servers holding the loglet; NULL unless the segment is replicated.
            nodeset: Option<ReString> => DataType::Utf8,
            /// Version of the logs metadata the row comes from.
            logs_metadata_version: u32 => DataType::UInt32,
        }
    }
}
