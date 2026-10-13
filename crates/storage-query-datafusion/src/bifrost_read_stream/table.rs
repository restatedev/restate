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

use datafusion::common::TableReference;
use datafusion::execution::context::SessionContext;

use restate_core::Metadata;
use restate_types::nodes_config::Role;

use crate::node_fan_out::{NodeFanOutTableProvider, RoleBasedNodeLocator};
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::table_providers::Scan;

use super::schema::BifrostReadStreamsBuilder;

// Also used as the wire scanner identifier; only SQL registration is schema-qualified.
pub(crate) const TABLE_NAME: &str = "bifrost_read_streams";

/// Registers the `bifrost_read_streams` fan-out table in the query context.
///
/// This table fans out to all nodes that have the Worker role, since bifrost
/// read streams are typically created by partition processors (workers).
/// However, any node that has bifrost initialized can have active read streams.
pub(crate) fn register_self(
    ctx: &SessionContext,
    metadata: Metadata,
    remote_scanner_manager: RemoteScannerManager,
    local_scanner: Option<Arc<dyn Scan>>,
) -> datafusion::common::Result<()> {
    let schema = BifrostReadStreamsBuilder::schema();

    // Fan out to all nodes — any node role can have active bifrost read streams
    // (workers read partition logs, admin/controller may read for diagnostics).
    let table = NodeFanOutTableProvider::new(
        schema,
        Arc::new(RoleBasedNodeLocator::new(Role::Worker, metadata)),
        remote_scanner_manager,
        local_scanner,
        TABLE_NAME,
    );

    ctx.register_table(
        TableReference::full("restate", "cluster", TABLE_NAME),
        Arc::new(table),
    )
    .map(|_| ())
}
