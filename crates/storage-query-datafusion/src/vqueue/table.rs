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
use std::ops::ControlFlow;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::{EntryKey, EntryValue, ScanVQueueInboxStages, Stage};
use restate_types::identifiers::StateMutationId;
use restate_types::vqueues::VQueueId;

use crate::context::{QueryContext, SelectPartitions};
use crate::filter::{FirstMatchingPartitionKeyExtractor, VQueueFilter};
use crate::partition_store_scanner::{LocalPartitionsScanner, ScanLocalPartition};
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::table_providers::{PartitionedTableProvider, ScanPartition};
use crate::vqueue::row::append_vqueues_row;
use crate::vqueue::schema::{SysVqueuesBuilder, sys_vqueues_sort_order};

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

    let table = PartitionedTableProvider::new(
        partition_selector,
        SysVqueuesBuilder::schema(),
        sys_vqueues_sort_order(),
        remote_scanner_manager.create_distributed_scanner(NAME, local_scanner),
        FirstMatchingPartitionKeyExtractor::default()
            .with_partitioned_resource_id::<VQueueId>("id")
            // entry_id can be an invocation id or state mutation id
            .with_invocation_id("entry_id")
            .with_partitioned_resource_id::<StateMutationId>("entry_id"),
    );

    ctx.register_partitioned_table(NAME, Arc::new(table))
}

#[derive(Debug, Clone)]
struct VQueuesScanner;

const SCAN_STAGE_ORDER: [Stage; 5] = [
    Stage::Finished,
    Stage::Inbox,
    Stage::Paused,
    Stage::Running,
    Stage::Suspended,
];

impl ScanLocalPartition for VQueuesScanner {
    type Builder = SysVqueuesBuilder;
    type Item<'a> = (&'a VQueueId, Stage, &'a EntryKey, &'a EntryValue);
    type ConversionError = std::convert::Infallible;
    type Filter = VQueueFilter;

    fn for_each_row<
        F: for<'a> FnMut(Self::Item<'a>) -> ControlFlow<Result<(), Self::ConversionError>>
            + Send
            + Sync
            + 'static,
    >(
        partition_store: &PartitionStore,
        filter: VQueueFilter,
        f: F,
    ) -> Result<impl Future<Output = restate_storage_api::Result<()>> + Send, StorageError> {
        let range = filter.partition_keys;
        let stages: Vec<_> = if let Some(requested_stages) = filter.stages {
            SCAN_STAGE_ORDER
                .into_iter()
                .filter(|stage| requested_stages.contains(stage))
                .collect()
        } else {
            SCAN_STAGE_ORDER.to_vec()
        };

        let callback = Arc::new(Mutex::new(f));
        let should_stop = Arc::new(AtomicBool::new(false));

        Ok(async move {
            let scan_result: restate_storage_api::Result<()> = async {
                for stage in stages {
                    if should_stop.load(Ordering::Relaxed) {
                        break;
                    }

                    let callback = callback.clone();
                    let should_stop = should_stop.clone();

                    partition_store
                        .for_each_vqueue_inbox_entry(range.clone(), stage, move |item| {
                            let mut callback = callback
                                .lock()
                                .expect("vqueue scan callback lock should not be poisoned");
                            let control_flow = (*callback)(item);
                            if control_flow.is_break() {
                                should_stop.store(true, Ordering::Relaxed);
                            }
                            control_flow.map_break(Result::unwrap)
                        })?
                        .await?;
                }

                Ok(())
            }
            .await;

            // The callback can own types with blocking cleanup logic (e.g. BatchSender).
            // Drop it off the async runtime worker thread.
            let _ = std::thread::spawn(move || drop(callback)).join();

            scan_result
        })
    }

    fn append_row<'a>(
        row_builder: &mut Self::Builder,
        value: Self::Item<'a>,
    ) -> Result<(), Self::ConversionError> {
        let (qid, stage, entry_key, entry) = value;
        append_vqueues_row(row_builder, qid, stage, entry_key, entry);
        Ok(())
    }
}
