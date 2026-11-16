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
use restate_storage_query_api::tables::state::{State, StateColumn};
use restate_util_string::ReString;

use crate::QuerySource;
use crate::context::SelectPartitions;
use crate::schema_scan::SchemaScanner;
use crate::sharding::PartitionSource;
use crate::source_registry::SourceRegistry;
use crate::table_providers::PartitionedTableProvider;

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
    const ORDERING: &'static [StateColumn] = &[StateColumn::PartitionKey];

    fn can_read(filters: &[Arc<dyn PhysicalExpr>]) -> anyhow::Result<bool> {
        Ok(State::analyze(filters, NonZeroUsize::new(16).unwrap())?
            .iter()
            .any(|branch| branch.scope.contains(&None)))
    }
}
impl PartitionSource for ScopedStateSource {
    const ORDERING: &'static [StateColumn] = &[StateColumn::PartitionKey];
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
        sources.register_declared_scanner::<LegacyStateSource>(Arc::new(SchemaScanner::<
            StateBinding,
        >::new()));
        sources.register_declared_scanner::<ScopedStateSource>(Arc::new(SchemaScanner::<
            ScopedStateBinding,
        >::new()));
    }
}
