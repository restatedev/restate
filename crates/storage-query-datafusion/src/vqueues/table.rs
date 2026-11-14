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

use bilrost::OwnedMessage;
use datafusion::catalog::TableProvider;

use restate_partition_store::keys::selection::KeySelection;
use restate_partition_store::keys::{KeyDecode, KeyDecoder, KeyKind};
use restate_partition_store::vqueue_table::{
    EntryStatusRecords, EntryStatusScan, FinishedIndexTable, InboxIndexTable, PausedIndexTable,
    RunningIndexTable, SuspendedIndexTable,
};
use restate_partition_store::{PartitionStoreManager, QueryIoExecutor, ReadSnapshot};
use restate_storage_api::StorageError;
use restate_storage_api::scan::ScanOutcome;
use restate_storage_api::vqueue_table::filters::ScanEntryIdFilter;
use restate_storage_api::vqueue_table::{EntryKey, EntryValue, Stage};
use restate_types::vqueues::VQueueId;

use crate::context::SelectPartitions;
use crate::filter::{FirstMatchingPartitionKeyExtractor, VQueueFilter};
use crate::partition_store_scanner::{LocalPartitionsScanner, Rows, ScanLocalPartition};
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::statistics::{DEPLOYMENT_ROW_ESTIMATE, RowEstimate, TableStatisticsBuilder};
use crate::table_providers::{PartitionedTableProvider, ScanPartition};
use crate::vqueues::row::{append_vqueues_row, append_vqueues_status_row};
use crate::vqueues::schema::{SysVqueuesBuilder, SysVqueuesTable};

impl SysVqueuesTable {
    pub(crate) fn create_provider(
        partition_selector: impl SelectPartitions,
        remote_scanner_manager: &RemoteScannerManager,
    ) -> Arc<dyn TableProvider> {
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
            remote_scanner_manager.create_distributed_scanner::<Self>(),
            FirstMatchingPartitionKeyExtractor::default()
                .with_grouped_vqueue_entry_id("entry_id")
                .with_grouped_vqueue_entry_id("canonical_id")
                .with_partitioned_resource_id::<VQueueId>("id"),
        )
        .with_statistics(statistics.build());

        Arc::new(table)
    }

    pub(crate) fn create_local_scanner(
        partition_store_manager: Arc<PartitionStoreManager>,
        io_executor: QueryIoExecutor,
    ) -> impl ScanPartition {
        LocalPartitionsScanner::<VQueuesScanner>::new(partition_store_manager, io_executor)
    }
}

#[derive(Debug, Clone)]
struct VQueuesScanner;

impl ScanLocalPartition for VQueuesScanner {
    type Builder = SysVqueuesBuilder;
    type Filter = VQueueFilter;

    async fn scan(
        snapshot: &ReadSnapshot,
        executor: &QueryIoExecutor,
        filter: VQueueFilter,
        mut rows: Rows<Self::Builder>,
    ) -> restate_storage_api::Result<ScanOutcome<Rows<Self::Builder>>> {
        if let Some(entry_ids) = filter.entry_ids {
            let stages = filter.stages;
            return rows
                .read::<EntryStatusRecords>(
                    snapshot,
                    executor,
                    "df-vqueues-status",
                    Arc::new(EntryStatusScan::from(ScanEntryIdFilter::EntryIdSet(
                        entry_ids.ids,
                    ))),
                    move |builder, (id, header)| {
                        if stages
                            .as_ref()
                            .is_some_and(|stages| !stages.contains(&header.stage))
                        {
                            return Ok(());
                        }

                        let canonical_id = id.canonicalize(header.seq);
                        append_vqueues_status_row(builder, &canonical_id, &header);
                        Ok(())
                    },
                )
                .await;
        }

        // Carry the same output and snapshot through the stages. A stop or error
        // must not open the next stage, even when the output has a partial batch.
        for stage in [
            Stage::Inbox,
            Stage::Running,
            Stage::Suspended,
            Stage::Paused,
            Stage::Finished,
        ] {
            if filter
                .stages
                .as_ref()
                .is_some_and(|stages| !stages.is_empty() && !stages.contains(&stage))
            {
                continue;
            }
            macro_rules! read_stage {
                ($table:ty) => {
                    rows.read::<$table>(
                        snapshot,
                        executor,
                        "df-vqueues",
                        Arc::new(KeySelection::all().restrict_scope(filter.partition_keys)),
                        move |builder, record| append_stage(builder, stage, record),
                    )
                    .await?
                };
            }
            let outcome = match stage {
                Stage::Inbox => read_stage!(InboxIndexTable),
                Stage::Running => read_stage!(RunningIndexTable),
                Stage::Suspended => read_stage!(SuspendedIndexTable),
                Stage::Paused => read_stage!(PausedIndexTable),
                Stage::Finished => read_stage!(FinishedIndexTable),
                Stage::Unknown => unreachable!("only stored stages are selected"),
            };
            rows = match outcome {
                ScanOutcome::Exhausted(rows) => rows,
                stopped => return Ok(stopped),
            };
        }
        Ok(ScanOutcome::Exhausted(rows))
    }
}

fn append_stage<K>(
    builder: &mut SysVqueuesBuilder,
    stage: Stage,
    (key, value): (KeyDecoder<'_, K>, &[u8]),
) -> restate_storage_api::Result<()> {
    // The stage physical tables retain the full legacy key. Decode the same
    // queue/entry tuple and transition-time value the V1 table has always shown.
    let mut key = key.remaining;
    KeyKind::deserialize(&mut key)?;
    let qid = <VQueueId as KeyDecode>::decode(&mut key)?;
    let entry_key = <EntryKey as KeyDecode>::decode(&mut key)?;
    let entry = EntryValue::decode(value).map_err(StorageError::BilrostDecode)?;
    append_vqueues_row(builder, &qid, stage, &entry_key, &entry);
    Ok(())
}
