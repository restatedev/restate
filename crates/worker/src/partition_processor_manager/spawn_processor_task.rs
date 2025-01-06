// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::RangeInclusive;
use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tracing::{debug, info, instrument, warn};

use restate_bifrost::Bifrost;
use restate_core::{Metadata, RuntimeTaskHandle, TaskCenter, TaskKind};
use restate_invoker_impl::Service as InvokerService;
use restate_partition_store::snapshots::LocalPartitionSnapshot;
use restate_partition_store::{OpenMode, PartitionStore, PartitionStoreManager};
use restate_service_protocol::codec::ProtobufRawEntryCodec;
use restate_types::config::{Configuration, WorkerOptions};
use restate_types::identifiers::{PartitionId, PartitionKey};
use restate_types::live::Live;
use restate_types::logs::Lsn;
use restate_types::partition_processor::PartitionProcessorStatus;
use restate_types::schema::Schema;

use crate::invoker_integration::EntryEnricher;
use crate::partition::invoker_storage_reader::InvokerStorageReader;
use crate::partition::snapshots::SnapshotRepository;
use crate::partition::ProcessorError;
use crate::partition_processor_manager::processor_state::StartedProcessor;
use crate::PartitionProcessorBuilder;

pub struct SpawnPartitionProcessorTask {
    task_name: &'static str,
    partition_id: PartitionId,
    key_range: RangeInclusive<PartitionKey>,
    configuration: Live<Configuration>,
    bifrost: Bifrost,
    partition_store_manager: PartitionStoreManager,
    snapshot_repository: Option<SnapshotRepository>,
    fast_forward_lsn: Option<Lsn>,
}

impl SpawnPartitionProcessorTask {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        task_name: &'static str,
        partition_id: PartitionId,
        key_range: RangeInclusive<PartitionKey>,
        configuration: Live<Configuration>,
        bifrost: Bifrost,
        partition_store_manager: PartitionStoreManager,
        snapshot_repository: Option<SnapshotRepository>,
        fast_forward_lsn: Option<Lsn>,
    ) -> Self {
        Self {
            task_name,
            partition_id,
            key_range,
            configuration,
            bifrost,
            partition_store_manager,
            snapshot_repository,
            fast_forward_lsn,
        }
    }

    #[instrument(
        skip_all,
        fields(
            partition_id=%self.partition_id,
        )
    )]
    pub fn run(
        self,
    ) -> anyhow::Result<(
        StartedProcessor,
        RuntimeTaskHandle<Result<(), ProcessorError>>,
    )> {
        let Self {
            task_name,
            partition_id,
            key_range,
            configuration,
            bifrost,
            partition_store_manager,
            snapshot_repository,
            fast_forward_lsn,
        } = self;

        let config = configuration.pinned();
        let schema = Metadata::with_current(|m| m.updateable_schema());
        let invoker: InvokerService<
            InvokerStorageReader<PartitionStore>,
            EntryEnricher<Schema, ProtobufRawEntryCodec>,
            Schema,
        > = InvokerService::from_options(
            &config.common.service_client,
            &config.worker.invoker,
            EntryEnricher::new(schema.clone()),
            schema,
        )?;

        let status_reader = invoker.status_reader();

        let (control_tx, control_rx) = mpsc::channel(2);
        let (rpc_tx, rpc_rx) = mpsc::channel(128);
        let status = PartitionProcessorStatus::new();
        let (watch_tx, watch_rx) = watch::channel(status.clone());

        let options = &configuration.pinned().worker;

        let pp_builder = PartitionProcessorBuilder::new(
            partition_id,
            key_range.clone(),
            status,
            options,
            control_rx,
            rpc_rx,
            watch_tx,
            invoker.handle(),
        );

        let invoker_name = Box::leak(Box::new(format!("invoker-{partition_id}")));
        let invoker_config = configuration.clone().map(|c| &c.worker.invoker);

        let root_task_handle = TaskCenter::current().start_runtime(
            TaskKind::PartitionProcessor,
            task_name,
            Some(pp_builder.partition_id),
            {
                let options = options.clone();
                let key_range = key_range.clone();

                move || async move {
                    let partition_store = open_partition_store(
                        partition_id,
                        partition_store_manager,
                        snapshot_repository,
                        fast_forward_lsn,
                        &options,
                        key_range,
                    )
                    .await?;

                    TaskCenter::spawn_child(
                        TaskKind::SystemService,
                        invoker_name,
                        invoker.run(invoker_config),
                    )
                    .map_err(|e| ProcessorError::from(anyhow::anyhow!(e)))?;

                    pp_builder
                        .build::<ProtobufRawEntryCodec>(bifrost, partition_store)
                        .await
                        .map_err(ProcessorError::from)?
                        .run()
                        .await
                }
            },
        )?;

        let state = StartedProcessor::new(
            root_task_handle.cancellation_token().clone(),
            key_range,
            control_tx,
            status_reader,
            rpc_tx,
            watch_rx,
        );

        Ok((state, root_task_handle))
    }
}

async fn open_partition_store(
    partition_id: PartitionId,
    partition_store_manager: PartitionStoreManager,
    snapshot_repository: Option<SnapshotRepository>,
    fast_forward_lsn: Option<Lsn>,
    options: &WorkerOptions,
    key_range: RangeInclusive<PartitionKey>,
) -> anyhow::Result<PartitionStore> {
    let partition_store_exists = partition_store_manager
        .has_partition_store(partition_id)
        .await;

    if partition_store_exists && fast_forward_lsn.is_none() {
        // We have an initialized partition store, and no fast-forward target - go on and open it.
        Ok(partition_store_manager
            .open_partition_store(
                partition_id,
                key_range,
                OpenMode::OpenExisting,
                &options.storage.rocksdb,
            )
            .await?)
    } else {
        // We either don't have an existing local partition store initialized - or we have a
        // fast-forward LSN target for the local state (probably due to seeing a log trim-gap).
        Ok(create_or_recreate_store(
            partition_id,
            partition_store_manager,
            snapshot_repository,
            fast_forward_lsn,
            &options,
            key_range,
        )
        .await?)
    }
}

/// (Re-)creates a fresh partition store based on the available snapshot and an optional
/// fast-forward LSN. Assumes that the partition store does not yet exist, unless a fast-forward
/// LSN is set. If a fast-forward LSN is set, but there is no snapshot repository, this will always
/// fail. An existing store will be dropped only if a snapshot is found in the repository with an
/// LSN greater than the fast-forward target.
async fn create_or_recreate_store(
    partition_id: PartitionId,
    partition_store_manager: PartitionStoreManager,
    snapshot_repository: Option<SnapshotRepository>,
    fast_forward_lsn: Option<Lsn>,
    options: &&WorkerOptions,
    key_range: RangeInclusive<PartitionKey>,
) -> anyhow::Result<PartitionStore> {
    // Attempt to get the latest available snapshot from the snapshot repository:
    let snapshot = match snapshot_repository {
        Some(repository) => {
            debug!("Looking for partition snapshot from which to bootstrap partition store");
            // todo(pavel): pass target LSN to repository
            repository.get_latest(partition_id).await?
        }
        None => {
            debug!("No snapshot repository configured");
            None
        }
    };

    Ok(match (snapshot, fast_forward_lsn) {
        (None, None) => {
            info!("No snapshot found to bootstrap partition, creating new store");
            partition_store_manager
                .open_partition_store(
                    partition_id,
                    key_range,
                    OpenMode::CreateIfMissing,
                    &options.storage.rocksdb,
                )
                .await?
        }
        (Some(snapshot), None) => {
            // Based on the assumptions for calling this method, we should only reach this point if
            // there is no existing store - we can import without first dropping the column family.
            info!(partition_id = %partition_id, "Found partition snapshot, restoring it");
            import_snapshot(
                partition_id,
                key_range,
                snapshot,
                partition_store_manager,
                options,
            )
            .await?
        }
        (Some(snapshot), Some(fast_forward_lsn))
            if snapshot.min_applied_lsn >= fast_forward_lsn =>
        {
            // We trust that the fast_forward_lsn is greater than the locally applied LSN.
            info!(
                latest_snapshot_lsn = ?snapshot.min_applied_lsn,
                ?fast_forward_lsn,
                "Found snapshot with LSN >= target LSN, dropping local partition store state",
            );
            partition_store_manager.drop_partition(partition_id).await;
            import_snapshot(
                partition_id,
                key_range,
                snapshot,
                partition_store_manager,
                options,
            )
            .await?
        }
        (maybe_snapshot, Some(fast_forward_lsn)) => {
            // Play it safe and keep the partition store intact; we can't do much else at this
            // point. We'll likely halt again as soon as the processor starts up.
            if let Some(snapshot) = maybe_snapshot {
                warn!(
                    ?snapshot.min_applied_lsn,
                    ?fast_forward_lsn,
                    "Latest available snapshot is older than the the fast-forward target LSN!",
                );
            } else {
                warn!(
                    ?fast_forward_lsn,
                    "A fast-forward target LSN is set, but no snapshot available for partition!",
                );
            }

            // We expect the processor startup attempt will fail, avoid spinning too fast.
            // todo(pavel): replace this with RetryPolicy
            tokio::time::sleep(Duration::from_millis(
                10_000 + rand::random::<u64>() % 10_000,
            ))
            .await;

            partition_store_manager
                .open_partition_store(
                    partition_id,
                    key_range,
                    OpenMode::OpenExisting,
                    &options.storage.rocksdb,
                )
                .await?
        }
    })
}

async fn import_snapshot(
    partition_id: PartitionId,
    key_range: RangeInclusive<PartitionKey>,
    snapshot: LocalPartitionSnapshot,
    partition_store_manager: PartitionStoreManager,
    options: &WorkerOptions,
) -> anyhow::Result<PartitionStore> {
    let snapshot_path = snapshot.base_dir.clone();
    match partition_store_manager
        .open_partition_store_from_snapshot(
            partition_id,
            key_range.clone(),
            snapshot,
            &options.storage.rocksdb,
        )
        .await
    {
        Ok(partition_store) => {
            let res = tokio::fs::remove_dir_all(&snapshot_path).await;
            if let Err(e) = res {
                // This is not critical; since we move the SST files into RocksDB on import,
                // at worst only the snapshot metadata file will remain in the staging dir
                warn!(
                    snapshot_path = %snapshot_path.display(),
                    "Failed to remove local snapshot directory, continuing with startup: {}",
                    e,
                );
            }
            Ok(partition_store)
        }
        Err(e) => {
            warn!(
                snapshot_path = %snapshot_path.display(),
                "Failed to import snapshot, local snapshot data retained"
            );
            Err(anyhow::anyhow!(e))
        }
    }
}
