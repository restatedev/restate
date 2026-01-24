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
    /// Introspect cluster nodes.
    node(
        /// Node ID
        plain_node_id: DataType::Utf8,

        /// Current known generation ID
        gen_node_id: DataType::Utf8,

        state: DataType::Utf8,

        /// Node Name
        name: DataType::Utf8,

        /// Node advertised address
        address: DataType::Utf8,

        /// Node location
        location: DataType::Utf8,

        /// Node has is admin
        has_admin_role: DataType::Boolean,

        /// Node has is worker
        has_worker_role: DataType::Boolean,

        /// Node is metadata server
        has_metadata_server_role: DataType::Boolean,

        /// Node has is log server
        has_log_server_role: DataType::Boolean,

        /// Node has is http-ingress
        has_ingress_role: DataType::Boolean,

        /// Node storage state. Only set of node is also a log-server
        storage_state: DataType::Utf8,

        /// Worker state. Only set of node is also a worker
        worker_state: DataType::Utf8,

        /// Node metadata server state.
        metadata_server_state: DataType::Utf8,

        /// Current known metadata version
        nodes_configuration_version: DataType::UInt32,
    )
);
