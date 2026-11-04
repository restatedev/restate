// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;
use std::fmt::Debug;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::FutureExt;

use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_storage_api::StorageError;
use restate_storage_api::scan_sink::{ScanFlow, ScanSink};
use restate_storage_api::vqueue_table::filters::ScanEntryIdFilter;
use restate_storage_api::vqueue_table::{
    EntryKey, EntryValue, RawStatusHeaderRef, ScanVQueueEntries, ScanVQueueEntryStatusTable, Stage,
};
use restate_types::identifiers::BaseEntryId;
use restate_types::vqueues::{CanonicalEntryId, VQueueId};

use crate::context::{QueryContext, SelectPartitions};
use crate::filter::{FirstMatchingPartitionKeyExtractor, VQueueFilter};
use crate::partition_store_scanner::{LocalPartitionsScanner, ScanLocalPartition};
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::row_sink::RowSink;
use crate::statistics::{DEPLOYMENT_ROW_ESTIMATE, RowEstimate, TableStatisticsBuilder};
use crate::table_providers::{PartitionedTableProvider, ScanPartition};
use crate::vqueues::row::{append_vqueues_row, append_vqueues_status_row};
use crate::vqueues::schema::SysVqueuesBuilder;

const NAME: &str = "sys_vqueues";

pub(crate) fn register_self(
    ctx: &QueryContext,
    partition_selector: impl SelectPartitions,
    partition_store_manager: Arc<PartitionStoreManager>,
    remote_scanner_manager: &RemoteScannerManager,
) -> datafusion::common::Result<()> {
    let local_scanner = Arc::new(LocalPartitionsScanner::new(
        partition_store_manager,
        VQueuesScanner,
    )) as Arc<dyn ScanPartition>;

    let schema = SysVqueuesBuilder::schema();
    let statistics = TableStatisticsBuilder::new(schema.clone())
        .with_num_rows_estimate(RowEstimate::Large)
        .with_partition_key()
        .with_primary_key("entry_id")
        .with_primary_key("canonical_id")
        .with_foreign_key("deployment", DEPLOYMENT_ROW_ESTIMATE)
        .with_foreign_key("id", RowEstimate::Small);

    let table = PartitionedTableProvider::new(
        partition_selector,
        schema,
        Vec::new(),
        remote_scanner_manager.create_distributed_scanner(NAME, local_scanner),
        FirstMatchingPartitionKeyExtractor::default()
            .with_grouped_vqueue_entry_id("entry_id")
            .with_grouped_vqueue_entry_id("canonical_id")
            .with_partitioned_resource_id::<VQueueId>("id"),
    )
    .with_statistics(statistics.build());

    ctx.register_partitioned_table(NAME, Arc::new(table))
}

#[derive(Debug, Clone)]
struct VQueuesScanner;

enum VQueueRow<'a> {
    Stage(&'a VQueueId, Stage, &'a EntryKey, &'a EntryValue),
    Status(&'a CanonicalEntryId, &'a RawStatusHeaderRef<'a>),
}

impl ScanLocalPartition for VQueuesScanner {
    type Builder = SysVqueuesBuilder;
    type Item<'a> = VQueueRow<'a>;
    type ConversionError = std::convert::Infallible;
    type Filter = VQueueFilter;

    fn for_each_row(
        partition_store: &PartitionStore,
        filter: VQueueFilter,
        sink: RowSink<Self>,
    ) -> Result<impl Future<Output = restate_storage_api::Result<RowSink<Self>>> + Send, StorageError>
    {
        let scan = if let Some(entry_ids) = filter.entry_ids {
            let sink = VQueuesSink {
                rows: sink,
                stages: filter.stages,
            };
            partition_store
                .for_each_vqueue_entry_status(ScanEntryIdFilter::EntryIdSet(entry_ids.ids), sink)?
                .boxed()
        } else {
            let sink = VQueuesSink {
                rows: sink,
                // The scan reads the requested stages only.
                stages: None,
            };
            partition_store
                .for_each_vqueue_entry(
                    filter.partition_keys,
                    filter.stages.unwrap_or_default(),
                    sink,
                )?
                .boxed()
        };
        Ok(async move { Ok(scan.await?.rows) })
    }

    fn append_row<'a>(
        row_builder: &mut Self::Builder,
        value: Self::Item<'a>,
    ) -> Result<(), Self::ConversionError> {
        match value {
            VQueueRow::Stage(qid, stage, entry_key, entry) => {
                append_vqueues_row(row_builder, qid, stage, entry_key, entry);
            }
            VQueueRow::Status(id, header) => {
                append_vqueues_status_row(row_builder, id, header);
            }
        }
        Ok(())
    }
}

/// Turns entry statuses and stage entries into [`VQueueRow`]s. Skips the
/// statuses whose stage isn't in `stages`, if given.
struct VQueuesSink {
    rows: RowSink<VQueuesScanner>,
    stages: Option<BTreeSet<Stage>>,
}

impl ScanFlow for VQueuesSink {
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<ControlFlow<()>> {
        self.rows.poll_ready(cx)
    }
}

impl<'a> ScanSink<(&'a BaseEntryId, &'a RawStatusHeaderRef<'a>)> for VQueuesSink {
    fn on_item(
        &mut self,
        (id, header): (&'a BaseEntryId, &'a RawStatusHeaderRef<'a>),
    ) -> ControlFlow<()> {
        if self
            .stages
            .as_ref()
            .is_some_and(|stages| !stages.contains(&header.stage))
        {
            return ControlFlow::Continue(());
        }
        let canonical_id = id.canonicalize(header.seq);
        self.rows.on_item(VQueueRow::Status(&canonical_id, header))
    }
}

impl<'a> ScanSink<(&'a VQueueId, Stage, &'a EntryKey, &'a EntryValue)> for VQueuesSink {
    fn on_item(
        &mut self,
        (qid, stage, entry_key, entry): (&'a VQueueId, Stage, &'a EntryKey, &'a EntryValue),
    ) -> ControlFlow<()> {
        self.rows
            .on_item(VQueueRow::Stage(qid, stage, entry_key, entry))
    }
}
