// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
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
use crate::table_providers::PartitionedTableProvider;

pub(crate) fn register_self(
    ctx: &QueryContext,
    partition_selector: impl SelectPartitions,
    partition_store_manager: PartitionStoreManager,
) -> datafusion::common::Result<()> {
    let table = PartitionedTableProvider::new(
        partition_selector,
        StateBuilder::schema(),
        LocalPartitionsScanner::new(partition_store_manager, StateScanner),
    );

    ctx.as_ref()
        .register_table("state", Arc::new(table))
        .map(|_| ())
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
