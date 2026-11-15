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

use crate::node_fan_out::NodeFanOutTableProvider;
use crate::source_registry::SourceRegistry;
use crate::table_providers::Scan;

use super::schema::BifrostReadStreamsTable;

/// Builds the `bifrost_read_streams` fan-out table provider.
///
/// This table fans out to all nodes: bifrost read streams can be active on any role.
impl BifrostReadStreamsTable {
    pub(crate) fn create_provider(
        metadata: Metadata,
        sources: SourceRegistry,
        local_scanner: Option<Arc<dyn Scan>>,
    ) -> Arc<dyn TableProvider> {
        let table = NodeFanOutTableProvider::declared::<Self>(metadata, sources, local_scanner);

        Arc::new(table)
    }
}
