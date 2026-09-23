// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::ControlFlow;
use std::sync::Arc;

use restate_partition_store::index::EntryNextAtByStageServiceKeyView;
use restate_partition_store::{IteratorMetrics, PartitionStore, PartitionStoreManager};
use restate_storage_api::StorageError;
use restate_storage_api::index::EntryNextAtByService;

use crate::context::{QueryContext, SelectPartitions};
use crate::filter::{FirstMatchingPartitionKeyExtractor, PointReadFanout};
use crate::index::table::{IndexFilter, register};
use crate::partition_store_scanner::ScanLocalPartition;
use crate::remote_query_scanner_manager::RemoteScannerManager;

use super::schema::IdxEntryNextAtByServiceBuilder;

pub(crate) fn register_self(
    ctx: &QueryContext,
    selector: impl SelectPartitions,
    manager: Arc<PartitionStoreManager>,
    remote: &RemoteScannerManager,
) -> datafusion::common::Result<()> {
    register::<EntryNextAtByServiceScanner>(
        ctx,
        selector,
        manager,
        remote,
        "_idx_entry_next_at_by_service",
        IdxEntryNextAtByServiceBuilder::schema(),
        FirstMatchingPartitionKeyExtractor::partition_key(PointReadFanout::PerPartition)
            .with_grouped_vqueue_entry_id("canonical_id")
            .with_grouped_vqueue_entry_id("entry_id"),
    )
}

#[derive(Debug, Clone, Default)]
struct EntryNextAtByServiceScanner;

impl ScanLocalPartition for EntryNextAtByServiceScanner {
    type Builder = IdxEntryNextAtByServiceBuilder;
    type Item<'a> = EntryNextAtByStageServiceKeyView<'a>;
    type ConversionError = StorageError;
    type Filter = IndexFilter<EntryNextAtByService>;

    fn for_each_row<F>(
        store: &PartitionStore,
        filter: Self::Filter,
        metrics: Option<IteratorMetrics>,
        f: F,
    ) -> Result<impl Future<Output = restate_storage_api::Result<()>> + Send, StorageError>
    where
        F: for<'a> FnMut(Self::Item<'a>) -> ControlFlow<Result<(), StorageError>>
            + Send
            + Sync
            + 'static,
    {
        store.scan_entry_next_at_by_service(filter.range, &filter.predicate, metrics, f)
    }

    fn append_row<'a>(
        builder: &mut Self::Builder,
        key: Self::Item<'a>,
    ) -> Result<(), StorageError> {
        let mut row = builder.row();
        if row.is_stage_defined() {
            row.stage(key.stage.decode()?.as_str());
        }
        if row.is_service_name_defined() {
            row.service_name(key.service_name.decode()?);
        }
        if row.is_next_at_defined() {
            row.next_at(key.next_at.decode()?.as_unix_millis().as_u64() as i64);
        }
        if row.is_seq_defined() {
            row.seq(key.seq.decode()?.as_u64());
        }
        if row.is_canonical_id_defined()
            || row.is_entry_id_defined()
            || row.is_partition_key_defined()
        {
            let id = key.canonical_id.decode()?;
            if row.is_canonical_id_defined() {
                row.fmt_canonical_id(id);
            }
            if row.is_entry_id_defined() {
                row.fmt_entry_id(id.to_base_entry_id());
            }
            if row.is_partition_key_defined() {
                row.partition_key(id.partition_key());
            }
        }
        Ok(())
    }
}
