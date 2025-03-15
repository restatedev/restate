// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(dead_code)]

use datafusion::arrow::datatypes::DataType;

use crate::table_macro::*;

define_table!(
    /// Introspect cluster nodes. Available only on `Admin` nodes
    node(
    /// Node ID
    id: DataType::UInt32,

    /// Current known generation ID
    generation: DataType::UInt32,

    /// Node Name
    name: DataType::Utf8,

    /// Node advertised address
    address: DataType::Utf8,

    /// Node location
    location: DataType::Utf8,

    /// Node is admin
    admin_role: DataType::Boolean,

    /// Node is worker
    worker_role: DataType::Boolean,

    /// Node is metadata server
    metadata_server_role: DataType::Boolean,

    /// Node is log server
    log_server_role: DataType::Boolean,

    /// Node is http-ingress
    ingress_role: DataType::Boolean,

    /// Node storage state. Only set of node is also a log-server
    storage_state: DataType::Utf8,

    /// Node metadata server state.
    metadata_server_state: DataType::Utf8,

));

fn test() {}
