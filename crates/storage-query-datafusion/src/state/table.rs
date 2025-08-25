// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Debug;
use std::ops::RangeInclusive;
use std::sync::Arc;

use bytes::Bytes;
use futures::Stream;

use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_storage_api::StorageError;
use restate_storage_api::state_table::ScanStateTable;
use restate_types::identifiers::{PartitionKey, ServiceId};

use crate::context::{QueryContext, SelectPartitions};
use crate::partition_filter::FirstMatchingPartitionKeyExtractor;
use crate::partition_store_scanner::{LocalPartitionsScanner, ScanLocalPartition};
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::state::row::append_state_row;
use crate::state::schema::{StateBuilder, state_sort_order};
use crate::table_providers::{PartitionedTableProvider, ScanPartition};

const NAME: &str = "state";

pub(crate) fn register_self(
    ctx: &QueryContext,
    partition_selector: impl SelectPartitions,
    local_partition_store_manager: Option<PartitionStoreManager>,
    remote_scanner_manager: &RemoteScannerManager,
) -> datafusion::common::Result<()> {
    let local_scanner = local_partition_store_manager.map(|partition_store_manager| {
        Arc::new(LocalPartitionsScanner::new(
            partition_store_manager,
            StateScanner,
        )) as Arc<dyn ScanPartition>
    });
    let table = PartitionedTableProvider::new(
        partition_selector,
        StateBuilder::schema(),
        state_sort_order(),
        remote_scanner_manager.create_distributed_scanner(NAME, local_scanner),
        FirstMatchingPartitionKeyExtractor::default().with_service_key("service_key"),
    );
    ctx.register_partitioned_table(NAME, Arc::new(table))
}

#[derive(Debug, Clone)]
struct StateScanner;

impl ScanLocalPartition for StateScanner {
    type Builder = StateBuilder;
    type Item = (ServiceId, Bytes, Bytes);

    fn scan_partition_store(
        partition_store: &PartitionStore,
        range: RangeInclusive<PartitionKey>,
    ) -> Result<impl Stream<Item = restate_storage_api::Result<Self::Item>> + Send, StorageError>
    {
        partition_store.scan_all_user_states(range)
    }

    fn append_row(row_builder: &mut Self::Builder, value: Self::Item) {
        append_state_row(row_builder, value.0, value.1, value.2);
    }
}
