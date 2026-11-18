// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The cluster tables `registry` and `migration_jobs`: what each partition replica
//! on a worker node records about its indexes and background migration jobs.
//!
//! Each worker answers with the scanners of the default engine
//! ([`restate_storage_query_datafusion::migration_records`]), registered with
//! [`crate::local_scanners::register_cluster_scanners`], so both engines show the
//! same rows. As there, a node whose report finds an unreadable record returns
//! its other replicas' rows, then ends with an error naming each unreadable
//! replica; the fan-out turns it into that node's warning.

use std::sync::Arc;

use datafusion::catalog::TableProvider;

use restate_core::Metadata;
use restate_storage_query_api::tables::migration_jobs::MigrationJobs;
use restate_storage_query_api::tables::registry::Registry;

use crate::node_fan_out::NodeFanOutTableProvider;
use crate::source_registry::SourceRegistry;

crate::tables::table_adapter!(pub RegistryTable, Registry);
crate::tables::table_adapter!(pub MigrationJobsTable, MigrationJobs);

impl RegistryTable {
    /// The `registry` fan-out table over the worker nodes. A worker's scanner must
    /// be registered in `sources` before this provider is created.
    pub(crate) fn create_provider(
        metadata: Metadata,
        sources: SourceRegistry,
    ) -> Arc<dyn TableProvider> {
        Arc::new(NodeFanOutTableProvider::declared::<Self>(
            metadata, sources, None,
        ))
    }
}

impl MigrationJobsTable {
    /// The `migration_jobs` fan-out table over the worker nodes. A worker's
    /// scanner must be registered in `sources` before this provider is created.
    pub(crate) fn create_provider(
        metadata: Metadata,
        sources: SourceRegistry,
    ) -> Arc<dyn TableProvider> {
        Arc::new(NodeFanOutTableProvider::declared::<Self>(
            metadata, sources, None,
        ))
    }
}
