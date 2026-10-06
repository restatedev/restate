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

use bytes::Bytes;
use datafusion::catalog::TableProvider;

use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_storage_api::StorageError;
use restate_storage_api::state_table::ScanStateTable;
use restate_types::identifiers::ServiceId;
use restate_types::sharding::KeyRange;

use crate::context::SelectPartitions;
use crate::filter::PartitionKeySelector;
use crate::partition_store_scanner::{LocalPartitionsScanner, ScanLocalPartition};
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::state::row::append_state_row;
use crate::state::schema::{StateBuilder, StateTable, state_sort_order};
use crate::table_providers::{PartitionedTableProvider, ScanPartition};

impl StateTable {
    pub(crate) fn create_provider(
        partition_selector: impl SelectPartitions,
        remote_scanner_manager: &RemoteScannerManager,
    ) -> Arc<dyn TableProvider> {
        let table = PartitionedTableProvider::new(
            partition_selector,
            StateBuilder::schema(),
            state_sort_order(),
            remote_scanner_manager.create_distributed_scanner::<Self>(),
            PartitionKeySelector::default().with_scope_or_service_key("scope", "service_key"),
        );
        Arc::new(table)
    }

    pub(crate) fn create_local_scanner(
        partition_store_manager: Arc<PartitionStoreManager>,
    ) -> impl ScanPartition {
        LocalPartitionsScanner::<StateScanner>::new(partition_store_manager)
    }
}

#[derive(Debug, Clone)]
struct StateScanner;

impl ScanLocalPartition for StateScanner {
    type Builder = StateBuilder;
    type Item<'a> = (ServiceId, Bytes, &'a [u8]);
    type ConversionError = std::convert::Infallible;
    type Filter = KeyRange;

    fn for_each_row<
        F: for<'a> FnMut(
                Self::Item<'a>,
            ) -> std::ops::ControlFlow<Result<(), Self::ConversionError>>
            + Send
            + Sync
            + 'static,
    >(
        partition_store: &PartitionStore,
        range: KeyRange,
        mut f: F,
    ) -> Result<impl Future<Output = restate_storage_api::Result<()>> + Send, StorageError> {
        partition_store.for_each_user_state(range, move |item| f(item).map_break(Result::unwrap))
    }

    fn append_row<'a>(
        row_builder: &mut Self::Builder,
        value: Self::Item<'a>,
    ) -> Result<(), Self::ConversionError> {
        append_state_row(row_builder, value.0, value.1, value.2);
        Ok(())
    }
}
