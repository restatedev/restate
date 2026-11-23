// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroUsize;
use std::sync::Arc;

use datafusion::catalog::TableProvider;
use datafusion::physical_plan::PhysicalExpr;

use restate_partition_store::state_table::{ScopedStateBinding, StateBinding};
use restate_storage_query_api::SelectionSchema;
use restate_storage_query_api::tables::state::State;
use restate_util_string::ReString;

use crate::QuerySource;
use crate::context::SelectPartitions;
use crate::schema_scan::LookupScanner;
use crate::sharding::PartitionSource;
use crate::source_registry::SourceRegistry;
use crate::table_providers::PartitionedTableProvider;

#[cfg(test)]
mod tests;

crate::tables::table_adapter!(pub(crate) StateTable, State);

#[derive(Debug, Clone)]
pub(crate) struct LegacyStateSource;
#[derive(Debug, Clone)]
pub(crate) struct ScopedStateSource;

impl QuerySource for LegacyStateSource {
    type Schema = State;

    fn identity() -> ReString {
        "state.legacy".into()
    }
}
impl QuerySource for ScopedStateSource {
    type Schema = State;

    fn identity() -> ReString {
        "state.scoped".into()
    }
}
impl PartitionSource for LegacyStateSource {
    const ORDERS: &'static [restate_storage_query_api::ordering::NativeOrder] = <<StateBinding as restate_storage_query_api::SchemaBinding>::Table as restate_storage_query_api::Table>::NATIVE_ORDERS;
    /// A finite set of complete state keys is looked up instead of read.
    const PRIMARY_ACCESS: Option<crate::access::SelectPrimary> =
        Some(crate::access::select_lookup::<StateBinding>);

    fn can_read(filters: &[Arc<dyn PhysicalExpr>]) -> anyhow::Result<bool> {
        Ok(State::analyze(filters, NonZeroUsize::new(16).unwrap())?
            .iter()
            .any(|branch| branch.scope.contains(&None)))
    }
}
impl PartitionSource for ScopedStateSource {
    const ORDERS: &'static [restate_storage_query_api::ordering::NativeOrder] = <<ScopedStateBinding as restate_storage_query_api::SchemaBinding>::Table as restate_storage_query_api::Table>::NATIVE_ORDERS;
    /// A finite set of complete state keys is looked up instead of read.
    const PRIMARY_ACCESS: Option<crate::access::SelectPrimary> =
        Some(crate::access::select_lookup::<ScopedStateBinding>);
}

impl StateTable {
    pub(crate) fn create_provider(
        partitions: impl SelectPartitions,
        sources: &SourceRegistry,
    ) -> Arc<dyn TableProvider> {
        Arc::new(
            PartitionedTableProvider::declared::<LegacyStateSource>(partitions, sources)
                .with_source::<ScopedStateSource>(sources),
        )
    }

    pub(crate) fn register_scanners(sources: &SourceRegistry) {
        sources.register_declared_scanner::<LegacyStateSource>(Arc::new(LookupScanner::<
            StateBinding,
        >::new()));
        sources.register_declared_scanner::<ScopedStateSource>(Arc::new(LookupScanner::<
            ScopedStateBinding,
        >::new()));
    }
}
