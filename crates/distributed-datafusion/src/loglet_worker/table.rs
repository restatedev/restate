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

use super::schema::{LogletWorkersBuilder, LogletWorkersTable};

impl LogletWorkersTable {
    pub(crate) fn create_provider(
        metadata: Metadata,
        remote_scanner_manager: RemoteScannerManager,
        local_scanner: Option<Arc<dyn Scan>>,
    ) -> Arc<dyn TableProvider> {
        let schema = LogletWorkersBuilder::schema();

        let table = NodeFanOutTableProvider::new(
            schema,
            Arc::new(RoleBasedNodeLocator::new(Role::LogServer, metadata)),
            remote_scanner_manager,
            local_scanner,
            Self::identity(),
        );

        Arc::new(table)
    }
}
