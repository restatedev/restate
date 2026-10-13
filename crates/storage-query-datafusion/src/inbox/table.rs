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

use datafusion::execution::context::SessionContext;

use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_storage_api::StorageError;
use restate_storage_api::inbox_table::{ScanInboxTable, SequenceNumberInboxEntry};
use restate_types::sharding::KeyRange;

use crate::context::SelectPartitions;
use crate::filter::FirstMatchingPartitionKeyExtractor;
use crate::inbox::row::append_inbox_row;
use crate::inbox::schema::{SysInboxBuilder, sys_inbox_sort_order};
use crate::partition_store_scanner::{LocalPartitionsScanner, ScanLocalPartition};
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::table_providers::{PartitionedTableProvider, ScanPartition};

const NAME: &str = "sys_inbox";

pub(crate) fn register_self(
    ctx: &SessionContext,
    partition_selector: impl SelectPartitions,
    remote_scanner_manager: &RemoteScannerManager,
) -> datafusion::common::Result<()> {
    let table = PartitionedTableProvider::new(
        partition_selector,
        SysInboxBuilder::schema(),
        sys_inbox_sort_order(),
        remote_scanner_manager.create_distributed_scanner(NAME),
        FirstMatchingPartitionKeyExtractor::default()
            .with_service_key("service_key")
            .with_invocation_id("id"),
    );
    ctx.register_table(NAME, Arc::new(table)).map(|_| ())
}

pub(crate) fn register_local_scanner(
    partition_store_manager: Arc<PartitionStoreManager>,
    remote_scanner_manager: &RemoteScannerManager,
) {
    let scanner = Arc::new(LocalPartitionsScanner::new(
        partition_store_manager,
        InboxScanner,
    )) as Arc<dyn ScanPartition>;

    remote_scanner_manager.register_partition_scanner(NAME, scanner);
}

#[derive(Debug, Clone)]
struct InboxScanner;

impl ScanLocalPartition for InboxScanner {
    type Builder = SysInboxBuilder;
    type Item<'a> = SequenceNumberInboxEntry;
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
        partition_store.for_each_inbox(range, move |item| f(item).map_break(Result::unwrap))
    }

    fn append_row<'a>(
        row_builder: &mut Self::Builder,
        value: Self::Item<'a>,
    ) -> Result<(), Self::ConversionError> {
        append_inbox_row(row_builder, value);
        Ok(())
    }
}
