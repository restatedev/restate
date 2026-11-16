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
    /// The running configuration of every node, one row per configuration key.
    /// Values of keys that may hold secrets are redacted.
    config {
        name: "config",
        sharding: ShardingScheme::ByNode(None),
        sharding_columns { plain_node_id: PlainNodeId, gen_node_id: GenerationalNodeId },
        columns {
            /// The PlainNodeId of the node hosting this config.
            plain_node_id: ReString => DataType::Utf8,
            /// Current known generation ID of the node.
            gen_node_id: ReString => DataType::Utf8,
            /// Config key where each level is dot separated. For example `worker.storage.rocksdb-memory-budget`
            key: ReString => DataType::Utf8,
            /// Config value.
            value: ReString => DataType::Utf8,
        }
    }
}
