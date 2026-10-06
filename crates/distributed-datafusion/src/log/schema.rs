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
    log(
        /// Log ID
        log_id: DataType::UInt32,

        /// Segment index
        segment_index: DataType::UInt32,

        /// Segment start lsn
        base_lsn: DataType::UInt64,

        /// Segment provider kind
        kind: DataType::Utf8,

        // replicated loglet specific params

        /// Loglet ID
        loglet_id: DataType::Utf8,

        /// Sequencer node id
        sequencer: DataType::Utf8,

        /// Loglet replication factor
        replication: DataType::Utf8,

        /// Log server id used by the loglet.
        nodeset: DataType::Utf8,

        /// Current known metadata version
        logs_metadata_version: DataType::UInt32,
    )
);
