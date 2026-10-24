// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Debug;
use std::sync::Arc;

use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_storage_api::StorageError;
use restate_storage_api::blob_table::{BlobEntry, ScanBlobTable, ScanBlobTableRange};

use super::row::append_blob_row;
use super::schema::{SysBlobBuilder, sys_blob_sort_order};
use crate::context::{QueryContext, SelectPartitions};
use crate::filter::{BlobIdFilter, FirstMatchingPartitionKeyExtractor};
use crate::partition_store_scanner::{LocalPartitionsScanner, ScanLocalPartition};
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::table_providers::{PartitionedTableProvider, ScanPartition};

const NAME: &str = "sys_blob";

pub(crate) fn register_self(
    ctx: &QueryContext,
    partition_selector: impl SelectPartitions,
    partition_store_manager: Arc<PartitionStoreManager>,
    remote_scanner_manager: &RemoteScannerManager,
) -> datafusion::common::Result<()> {
    let local_scanner = Arc::new(LocalPartitionsScanner::new(
        partition_store_manager,
        BlobScanner,
    )) as Arc<dyn ScanPartition>;

    let table = PartitionedTableProvider::new(
        partition_selector,
        SysBlobBuilder::schema(),
        sys_blob_sort_order(),
        remote_scanner_manager.create_distributed_scanner(NAME, local_scanner),
        FirstMatchingPartitionKeyExtractor::default().with_grouped_blob_id("id"),
    );
    ctx.register_partitioned_table(NAME, Arc::new(table))
}

#[derive(Debug, Clone)]
struct BlobScanner;

impl ScanLocalPartition for BlobScanner {
    type Builder = SysBlobBuilder;
    type Item<'a> = BlobEntry<'a>;
    type ConversionError = std::convert::Infallible;
    type Filter = BlobIdFilter;

    fn for_each_row<
        F: for<'a> FnMut(
                Self::Item<'a>,
            ) -> std::ops::ControlFlow<Result<(), Self::ConversionError>>
            + Send
            + Sync
            + 'static,
    >(
        partition_store: &PartitionStore,
        range: BlobIdFilter,
        mut f: F,
    ) -> Result<impl Future<Output = restate_storage_api::Result<()>> + Send, StorageError> {
        partition_store.for_each_blob(range.into(), move |item| f(item).map_break(Result::unwrap))
    }

    fn append_row<'a>(
        row_builder: &mut Self::Builder,
        value: Self::Item<'a>,
    ) -> Result<(), Self::ConversionError> {
        append_blob_row(row_builder, value);
        Ok(())
    }
}

impl From<BlobIdFilter> for ScanBlobTableRange {
    fn from(value: BlobIdFilter) -> Self {
        match value.blob_ids {
            Some(selection) => ScanBlobTableRange::BlobIdSet(selection.ids),
            None => ScanBlobTableRange::PartitionKey(value.partition_keys),
        }
    }
}
