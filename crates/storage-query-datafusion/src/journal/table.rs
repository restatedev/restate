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

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use tokio::sync::mpsc::Sender;

use restate_partition_store::journal_table::OwnedJournalRow;
use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_types::identifiers::PartitionKey;

use crate::context::{QueryContext, SelectPartitions};
use crate::journal::row::append_journal_row;
use crate::journal::schema::JournalBuilder;
use crate::partition_store_scanner::{LocalPartitionsScanner, ScanLocalPartition};
use crate::table_providers::PartitionedTableProvider;

pub(crate) fn register_self(
    ctx: &QueryContext,
    partition_selector: impl SelectPartitions,
    partition_store_manager: PartitionStoreManager,
) -> datafusion::common::Result<()> {
    let journal_table = PartitionedTableProvider::new(
        partition_selector,
        JournalBuilder::schema(),
        LocalPartitionsScanner::new(partition_store_manager, JournalScanner),
    );

    ctx.as_ref()
        .register_table("sys_journal", Arc::new(journal_table))
        .map(|_| ())
}

#[derive(Debug, Clone)]
struct JournalScanner;

impl ScanLocalPartition for JournalScanner {
    async fn scan_partition_store(
        partition_store: PartitionStore,
        tx: Sender<Result<RecordBatch, datafusion::error::DataFusionError>>,
        range: RangeInclusive<PartitionKey>,
        projection: SchemaRef,
    ) {
        let rows = partition_store.all_journal(range);
        for_each_journal(projection, tx, rows).await;
    }
}

async fn for_each_journal<'a, I>(
    schema: SchemaRef,
    tx: Sender<datafusion::common::Result<RecordBatch>>,
    rows: I,
) where
    I: Iterator<Item = OwnedJournalRow> + 'a,
{
    let mut builder = JournalBuilder::new(schema.clone());
    let mut temp = String::new();
    for row in rows {
        append_journal_row(&mut builder, &mut temp, row);
        if builder.full() {
            let batch = builder.finish();
            if tx.send(Ok(batch)).await.is_err() {
                // not sure what to do here?
                // the other side has hung up on us.
                // we probably don't want to panic, is it will cause the entire process to exit
                return;
            }
            builder = JournalBuilder::new(schema.clone());
        }
    }
    if !builder.empty() {
        let result = builder.finish();
        let _ = tx.send(Ok(result)).await;
    }
}
