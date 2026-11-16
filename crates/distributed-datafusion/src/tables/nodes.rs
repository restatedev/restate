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
use restate_storage_query_api::tables::nodes::Nodes;
use restate_types::cluster_state::ClusterState;

use crate::table_providers::GenericTableProvider;

crate::tables::table_adapter!(pub(crate) NodesTable, Nodes);

impl NodesTable {
    /// The coordinator's nodes, read by the default engine's scanner, so both
    /// engines show the same rows.
    pub(crate) fn create_provider(
        metadata: Metadata,
        cluster_state: ClusterState,
    ) -> Arc<dyn TableProvider> {
        Arc::new(GenericTableProvider::declared::<Self>(
            Some(restate_storage_query_datafusion::node::create_scanner(
                metadata,
                cluster_state,
            )),
            None,
        ))
    }
}
