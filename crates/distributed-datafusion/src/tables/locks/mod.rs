// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! `sys_locks`, read through the partition-store lock binding.

use std::sync::Arc;

use datafusion::catalog::TableProvider;

use restate_partition_store::locks_table::LocksBinding;
use restate_storage_query_api::tables::locks::{Locks, LocksColumn};

use crate::context::SelectPartitions;
use crate::schema_scan::SchemaScanner;
use crate::sharding::PartitionSource;
use crate::source_registry::SourceRegistry;
use crate::table_providers::{PartitionedTableProvider, ScanPartition};

#[cfg(test)]
mod tests;

crate::tables::table_adapter!(pub(crate) SysLocksTable, Locks);

impl PartitionSource for SysLocksTable {
    const ORDERING: &'static [LocksColumn] = &[LocksColumn::PartitionKey];
}

impl SysLocksTable {
    pub(crate) fn create_provider(
        partitions: impl SelectPartitions,
        sources: &SourceRegistry,
    ) -> Arc<dyn TableProvider> {
        Arc::new(PartitionedTableProvider::declared::<Self>(
            partitions, sources,
        ))
    }

    pub(crate) fn create_local_scanner() -> impl ScanPartition {
        SchemaScanner::<LocksBinding>::new()
    }
}
