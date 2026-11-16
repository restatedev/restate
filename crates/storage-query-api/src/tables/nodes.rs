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
    /// The nodes of the cluster, from this coordinator's nodes configuration, with
    /// their state as this coordinator's gossip sees it.
    nodes {
        name: "nodes",
        sharding: ShardingScheme::Coordinator,
        columns {
            /// Node ID.
            plain_node_id: ReString => DataType::Utf8,
            /// Current known generation of the node.
            gen_node_id: ReString => DataType::Utf8,
            /// The node's state as gossip sees it: `alive`, `dead` or `failing-over`.
            state: ReString => DataType::Utf8,
            /// Node name.
            name: ReString => DataType::Utf8,
            /// Advertised address.
            address: ReString => DataType::Utf8,
            /// Control address.
            ctrl_address: ReString => DataType::Utf8,
            /// Location.
            location: ReString => DataType::Utf8,
            /// Whether the node has the admin role.
            has_admin_role: bool => DataType::Boolean,
            /// Whether the node has the worker role.
            has_worker_role: bool => DataType::Boolean,
            /// Whether the node has the metadata server role.
            has_metadata_server_role: bool => DataType::Boolean,
            /// Whether the node has the log server role.
            has_log_server_role: bool => DataType::Boolean,
            /// Whether the node has the HTTP ingress role.
            has_ingress_role: bool => DataType::Boolean,
            /// Log server storage state; NULL unless the node is a log server.
            storage_state: Option<ReString> => DataType::Utf8,
            /// Worker state; NULL unless the node is a worker.
            worker_state: Option<ReString> => DataType::Utf8,
            /// Metadata server state; NULL unless the node is a metadata server.
            metadata_server_state: Option<ReString> => DataType::Utf8,
            /// Version of the nodes configuration the row comes from.
            nodes_configuration_version: u32 => DataType::UInt32,
            /// The node's last seen binary version; nodes older than v1.6.0 don't report it.
            binary_version: ReString => DataType::Utf8,
        }
    }
}
