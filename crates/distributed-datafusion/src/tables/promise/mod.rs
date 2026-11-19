// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! `sys_promise` over both stored promise layouts, as two sources of one schema.

use std::num::NonZeroUsize;
use std::sync::Arc;

use datafusion::catalog::TableProvider;
use datafusion::physical_plan::PhysicalExpr;

use restate_partition_store::promise_table::{PromiseBinding, ScopedPromiseBinding};
use restate_storage_query_api::SelectionSchema;
use restate_storage_query_api::tables::promise::{Promise, PromiseColumn};
use restate_util_string::ReString;

use crate::QuerySource;
use crate::context::SelectPartitions;
use crate::schema_scan::LookupScanner;
use crate::sharding::PartitionSource;
use crate::source_registry::SourceRegistry;
use crate::table_providers::PartitionedTableProvider;

#[cfg(test)]
mod tests;

crate::tables::table_adapter!(pub(crate) SysPromiseTable, Promise);

/// Promises of unscoped workflows in the pre-scope layout.
#[derive(Debug, Clone)]
pub(crate) struct LegacyPromiseSource;
/// Promises in the scoped layout.
#[derive(Debug, Clone)]
pub(crate) struct ScopedPromiseSource;

impl QuerySource for LegacyPromiseSource {
    type Schema = Promise;

    fn identity() -> ReString {
        "sys_promise.legacy".into()
    }
}
impl QuerySource for ScopedPromiseSource {
    type Schema = Promise;

    fn identity() -> ReString {
        "sys_promise.scoped".into()
    }
}
impl PartitionSource for LegacyPromiseSource {
    const ORDERING: &'static [PromiseColumn] = &[PromiseColumn::PartitionKey];
    /// A finite set of complete promise keys is looked up instead of read.
    const PRIMARY_ACCESS: Option<crate::access::SelectPrimary> =
        Some(crate::access::select_lookup::<PromiseBinding>);

    /// The legacy layout holds only unscoped promises.
    fn can_read(filters: &[Arc<dyn PhysicalExpr>]) -> anyhow::Result<bool> {
        Ok(Promise::analyze(filters, NonZeroUsize::new(16).unwrap())?
            .iter()
            .any(|branch| branch.scope.contains(&None)))
    }
}
impl PartitionSource for ScopedPromiseSource {
    const ORDERING: &'static [PromiseColumn] = &[PromiseColumn::PartitionKey];
    /// A finite set of complete promise keys is looked up instead of read.
    const PRIMARY_ACCESS: Option<crate::access::SelectPrimary> =
        Some(crate::access::select_lookup::<ScopedPromiseBinding>);
}

impl SysPromiseTable {
    pub(crate) fn create_provider(
        partitions: impl SelectPartitions,
        sources: &SourceRegistry,
    ) -> Arc<dyn TableProvider> {
        Arc::new(
            PartitionedTableProvider::declared::<LegacyPromiseSource>(partitions, sources)
                .with_source::<ScopedPromiseSource>(sources),
        )
    }

    pub(crate) fn register_scanners(sources: &SourceRegistry) {
        sources.register_declared_scanner::<LegacyPromiseSource>(Arc::new(LookupScanner::<
            PromiseBinding,
        >::new()));
        sources.register_declared_scanner::<ScopedPromiseSource>(Arc::new(LookupScanner::<
            ScopedPromiseBinding,
        >::new()));
    }
}
