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
use restate_storage_query_api::tables::config::Config;

use crate::node_fan_out::NodeFanOutTableProvider;
use crate::source_registry::SourceRegistry;
use crate::table_providers::Scan;

crate::tables::table_adapter!(pub ConfigTable, Config);

impl ConfigTable {
    /// Asks every node for its running configuration. Each node answers with the
    /// local scanner it registers, the same one the default engine uses, which
    /// redacts values that may be secret.
    pub(crate) fn create_provider(
        metadata: Metadata,
        sources: SourceRegistry,
        local_scanner: Option<Arc<dyn Scan>>,
    ) -> Arc<dyn TableProvider> {
        Arc::new(NodeFanOutTableProvider::declared::<Self>(
            metadata,
            sources,
            local_scanner,
        ))
    }
}
