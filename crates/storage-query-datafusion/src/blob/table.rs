// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::marker::PhantomData;
use std::ops::ControlFlow;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;

use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_storage_api::StorageError;
use restate_storage_api::blob_table::{BlobEntry, ScanBlobTable, ScanBlobTableRange};
use restate_storage_query_api::QueryEngineTable;
use restate_types::blob::BlobKind;

use super::schema::sys_blob_sort_order;
use crate::context::SelectPartitions;
use crate::filter::{BlobIdFilter, FirstMatchingPartitionKeyExtractor};
use crate::partition_store_scanner::{LocalPartitionsScanner, ScanLocalPartition};
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::table_providers::{PartitionedTableProvider, ScanPartition};

/// A SQL table over the blob table. `sys_blob` lists every blob with its metadata, while
/// kind-specific tables (e.g. `sys_blob_output`) list the blobs of one kind and decode their
/// payload into extra columns. All of them share the same scan, filter pushdown and partition
/// routing.
pub(crate) trait BlobTable: QueryEngineTable {
    /// Only blobs of this kind are listed. `None` lists all blobs.
    const KIND: Option<BlobKind>;

    type Builder: crate::table_util::Builder + Send + Sync + 'static;

    fn schema() -> SchemaRef;

    fn append_row(builder: &mut Self::Builder, entry: BlobEntry<'_>);
}

pub(crate) fn create_provider<T: BlobTable>(
    partition_selector: impl SelectPartitions,
    remote_scanner_manager: &RemoteScannerManager,
) -> Arc<dyn TableProvider> {
    Arc::new(PartitionedTableProvider::new(
        partition_selector,
        T::schema(),
        // Blobs are keyed by creation time, so rows are not ordered by partition key.
        sys_blob_sort_order(),
        remote_scanner_manager.create_distributed_scanner::<T>(),
        FirstMatchingPartitionKeyExtractor::grouped_partition_key().with_grouped_blob_id("id"),
    ))
}

pub(crate) fn create_local_scanner<T: BlobTable>(
    partition_store_manager: Arc<PartitionStoreManager>,
) -> impl ScanPartition {
    LocalPartitionsScanner::<BlobScanner<T>>::new(partition_store_manager)
}

#[derive(Debug)]
struct BlobScanner<T>(PhantomData<T>);

impl<T: BlobTable> ScanLocalPartition for BlobScanner<T> {
    type Builder = T::Builder;
    type Item<'a> = BlobEntry<'a>;
    type ConversionError = std::convert::Infallible;
    type Filter = BlobIdFilter;

    fn for_each_row<
        F: for<'a> FnMut(Self::Item<'a>) -> ControlFlow<Result<(), Self::ConversionError>>
            + Send
            + Sync
            + 'static,
    >(
        partition_store: &PartitionStore,
        range: BlobIdFilter,
        mut f: F,
    ) -> Result<impl Future<Output = restate_storage_api::Result<()>> + Send, StorageError> {
        partition_store.for_each_blob(range.into(), move |item| {
            if T::KIND.is_some_and(|kind| kind != item.kind) {
                return ControlFlow::Continue(());
            }
            f(item).map_break(Result::unwrap)
        })
    }

    fn append_row<'a>(
        row_builder: &mut Self::Builder,
        value: Self::Item<'a>,
    ) -> Result<(), Self::ConversionError> {
        T::append_row(row_builder, value);
        Ok(())
    }
}

impl From<BlobIdFilter> for ScanBlobTableRange {
    fn from(value: BlobIdFilter) -> Self {
        match value.blob_ids {
            Some(selection) => ScanBlobTableRange::BlobIdSet(selection.ids),
            None => ScanBlobTableRange::CreatedAt(value.created_at),
        }
    }
}
