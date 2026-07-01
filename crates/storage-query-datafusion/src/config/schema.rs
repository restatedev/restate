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
    /// Introspect cluster config.
    config(
        /// The PlainNodeId of the node hosting this config.
        plain_node_id: DataType::Utf8,
        /// Current known generation ID of the node.
        gen_node_id: DataType::Utf8,
        /// Config key where each level is dot separated. For example `worker.storage.rocksdb-memory-budget`
        key: DataType::Utf8,
        /// Config value.
        value: DataType::Utf8,
    )
);
