// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
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

use futures::Stream;

use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_storage_api::invocation_status_table::{
    InvocationStatus, ReadOnlyInvocationStatusTable,
};
use restate_types::identifiers::{InvocationId, PartitionKey};

use crate::context::{QueryContext, SelectPartitions};
use crate::invocation_status::row::append_invocation_status_row;
use crate::invocation_status::schema::InvocationStatusBuilder;
use crate::partition_store_scanner::{LocalPartitionsScanner, ScanLocalPartition};
use crate::table_providers::PartitionedTableProvider;

pub(crate) fn register_self(
    ctx: &QueryContext,
    partition_selector: impl SelectPartitions,
    partition_store_manager: PartitionStoreManager,
) -> datafusion::common::Result<()> {
    let status_table = PartitionedTableProvider::new(
        partition_selector,
        InvocationStatusBuilder::schema(),
        LocalPartitionsScanner::new(partition_store_manager, StatusScanner),
    );

    ctx.as_ref()
        .register_table("sys_invocation_status", Arc::new(status_table))
        .map(|_| ())
}

#[derive(Debug, Clone)]
struct StatusScanner;

impl ScanLocalPartition for StatusScanner {
    type Builder = InvocationStatusBuilder;
    type Item = (InvocationId, InvocationStatus);

    fn scan_partition_store(
        partition_store: &PartitionStore,
        range: RangeInclusive<PartitionKey>,
    ) -> impl Stream<Item = restate_storage_api::Result<Self::Item>> + Send {
        partition_store.all_invocation_statuses(range)
    }

    fn append_row(
        row_builder: &mut Self::Builder,
        string_buffer: &mut String,
        (invocation_id, invocation_status): Self::Item,
    ) {
        append_invocation_status_row(row_builder, string_buffer, invocation_id, invocation_status)
    }
}
