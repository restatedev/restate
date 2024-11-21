// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::Bytes;
use std::fmt::Debug;
use std::ops::RangeInclusive;
use std::sync::Arc;

use futures::Stream;

use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_storage_api::state_table::ReadOnlyStateTable;
use restate_types::identifiers::{PartitionKey, ServiceId};

use crate::context::{QueryContext, SelectPartitions};
use crate::partition_store_scanner::{LocalPartitionsScanner, ScanLocalPartition};
use crate::state::row::append_state_row;
use crate::state::schema::StateBuilder;
use crate::table_providers::{PartitionedTableProvider, ScanPartition};

const NAME: &str = "state";

pub(crate) fn register_self(
    ctx: &QueryContext,
    partition_selector: impl SelectPartitions,
    local_partition_store_manager: Option<PartitionStoreManager>,
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
        ctx.create_distributed_scanner(NAME, local_scanner),
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
        _range: RangeInclusive<PartitionKey>,
    ) -> impl Stream<Item = restate_storage_api::Result<Self::Item>> + Send {
        partition_store.get_all_user_states()
    }

    fn append_row(row_builder: &mut Self::Builder, _: &mut String, value: Self::Item) {
        append_state_row(row_builder, value.0, value.1, value.2);
    }
}
