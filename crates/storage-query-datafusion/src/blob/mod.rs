// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub(crate) mod output;
mod row;
pub(crate) mod schema;
mod table;
#[cfg(test)]
mod tests;

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;

use restate_partition_store::PartitionStoreManager;
use restate_storage_api::blob_table::BlobEntry;
use restate_types::blob::BlobKind;

use crate::context::SelectPartitions;
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::table_providers::ScanPartition;
use output::schema::SysBlobOutputTable;
use schema::{SysBlobBuilder, SysBlobTable};
use table::BlobTable;

/// `sys_blob`: every blob with its kind-independent metadata.
impl BlobTable for SysBlobTable {
    const KIND: Option<BlobKind> = None;

    type Builder = SysBlobBuilder;

    fn schema() -> SchemaRef {
        SysBlobBuilder::schema()
    }

    fn append_row(builder: &mut Self::Builder, entry: BlobEntry<'_>) {
        row::append_blob_row(builder, entry)
    }
}

impl SysBlobTable {
    pub(crate) fn create_provider(
        partition_selector: impl SelectPartitions,
        remote_scanner_manager: &RemoteScannerManager,
    ) -> Arc<dyn TableProvider> {
        table::create_provider::<Self>(partition_selector, remote_scanner_manager)
    }

    pub(crate) fn create_local_scanner(
        partition_store_manager: Arc<PartitionStoreManager>,
    ) -> impl ScanPartition {
        table::create_local_scanner::<Self>(partition_store_manager)
    }
}

impl SysBlobOutputTable {
    pub(crate) fn create_provider(
        partition_selector: impl SelectPartitions,
        remote_scanner_manager: &RemoteScannerManager,
    ) -> Arc<dyn TableProvider> {
        table::create_provider::<Self>(partition_selector, remote_scanner_manager)
    }

    pub(crate) fn create_local_scanner(
        partition_store_manager: Arc<PartitionStoreManager>,
    ) -> impl ScanPartition {
        table::create_local_scanner::<Self>(partition_store_manager)
    }
}
