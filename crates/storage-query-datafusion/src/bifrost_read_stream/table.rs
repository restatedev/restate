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

use datafusion::catalog::TableProvider;

use restate_core::Metadata;
use restate_storage_query_api::QueryEngineTable;
use restate_types::nodes_config::Role;

use crate::node_fan_out::{NodeFanOutTableProvider, RoleBasedNodeLocator};
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::table_providers::Scan;

use super::schema::{BifrostReadStreamsBuilder, BifrostReadStreamsTable};

/// Builds the `bifrost_read_streams` fan-out table provider.
///
/// This table fans out to all nodes that have the Worker role, since bifrost
/// read streams are typically created by partition processors (workers).
/// However, any node that has bifrost initialized can have active read streams.
impl BifrostReadStreamsTable {
    pub(crate) fn create_provider(
        metadata: Metadata,
        remote_scanner_manager: RemoteScannerManager,
        local_scanner: Option<Arc<dyn Scan>>,
    ) -> Arc<dyn TableProvider> {
        let schema = BifrostReadStreamsBuilder::schema();

        // Fan out to all nodes — any node role can have active bifrost read streams
        // (workers read partition logs, admin/controller may read for diagnostics).
        let table = NodeFanOutTableProvider::new(
            schema,
            Arc::new(RoleBasedNodeLocator::new(Role::Worker, metadata)),
            remote_scanner_manager,
            local_scanner,
            Self::identity(),
        );

        Arc::new(table)
    }
}
