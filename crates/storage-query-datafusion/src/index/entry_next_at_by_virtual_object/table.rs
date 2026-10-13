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

use datafusion::catalog::TableProvider;

use restate_partition_store::index::EntryNextAtByVirtualObjectStageKeyView;
use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_storage_api::StorageError;
use restate_storage_api::index::EntryNextAtByVirtualObject;

use crate::context::SelectPartitions;
use crate::filter::{FirstMatchingPartitionKeyExtractor, PointReadFanout};
use crate::index::table::{IndexFilter, create_provider};
use crate::partition_store_scanner::{LocalPartitionsScanner, ScanLocalPartition};
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::table_providers::ScanPartition;

use super::schema::{IdxEntryNextAtByVirtualObjectBuilder, IdxEntryNextAtByVirtualObjectTable};

impl IdxEntryNextAtByVirtualObjectTable {
    pub(crate) fn create_provider(
        selector: impl SelectPartitions,
        remote: &RemoteScannerManager,
    ) -> Arc<dyn TableProvider> {
        create_provider::<Self>(
            selector,
            remote,
            IdxEntryNextAtByVirtualObjectBuilder::schema(),
            FirstMatchingPartitionKeyExtractor::partition_key(PointReadFanout::PerPartition)
                .with_grouped_vqueue_entry_id("canonical_id")
                .with_grouped_vqueue_entry_id("entry_id"),
        )
    }

    pub(crate) fn create_local_scanner(manager: Arc<PartitionStoreManager>) -> impl ScanPartition {
        LocalPartitionsScanner::<EntryNextAtByVirtualObjectScanner>::new(manager)
    }
}

#[derive(Debug, Clone, Default)]
struct EntryNextAtByVirtualObjectScanner;

impl ScanLocalPartition for EntryNextAtByVirtualObjectScanner {
    type Builder = IdxEntryNextAtByVirtualObjectBuilder;
    type Item<'a> = EntryNextAtByVirtualObjectStageKeyView<'a>;
    type ConversionError = StorageError;
    type Filter = IndexFilter<EntryNextAtByVirtualObject>;

    fn for_each_row<F>(
        store: &PartitionStore,
        filter: Self::Filter,
        f: F,
    ) -> Result<impl Future<Output = restate_storage_api::Result<()>> + Send, StorageError>
    where
        F: for<'a> FnMut(Self::Item<'a>) -> ControlFlow<Result<(), StorageError>>
            + Send
            + Sync
            + 'static,
    {
        store.scan_entry_next_at_by_virtual_object(filter.range, &filter.predicate, filter.live, f)
    }

    fn append_row<'a>(
        builder: &mut Self::Builder,
        key: Self::Item<'a>,
    ) -> Result<(), StorageError> {
        let mut row = builder.row();
        if row.is_service_name_defined() {
            row.service_name(key.service_name.decode()?);
        }
        if row.is_scope_defined()
            && let Some(scope) = key.scope.decode()?
        {
            row.scope(scope);
        }
        if row.is_key_defined() {
            row.key(key.key.decode()?);
        }
        if row.is_stage_defined() {
            row.stage(key.stage.decode()?.as_str());
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
