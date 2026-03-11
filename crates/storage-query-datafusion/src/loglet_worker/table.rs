// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use restate_core::Metadata;
use restate_types::nodes_config::Role;

use crate::context::QueryContext;
use crate::node_fan_out::{NodeFanOutTableProvider, RoleBasedNodeLocator};
use crate::remote_query_scanner_client::RemoteScannerService;
use crate::table_providers::Scan;

use super::schema::LogletWorkersBuilder;

pub(crate) const TABLE_NAME: &str = "loglet_workers";

/// Registers the `loglet_workers` fan-out table in the query context.
pub(crate) fn register_self(
    ctx: &QueryContext,
    metadata: Metadata,
    remote_scanner: Arc<dyn RemoteScannerService>,
    local_scanner: Option<Arc<dyn Scan>>,
) -> datafusion::common::Result<()> {
    let schema = LogletWorkersBuilder::schema();

    let table = NodeFanOutTableProvider::new(
        schema,
        Arc::new(RoleBasedNodeLocator::new(Role::LogServer, metadata)),
        remote_scanner,
        local_scanner,
        TABLE_NAME,
    );

    ctx.register_non_partitioned_table(TABLE_NAME, Arc::new(table))
}
