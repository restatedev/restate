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

use tokio::sync::{mpsc, watch};
use tracing::{debug, info, instrument, warn};

use restate_bifrost::Bifrost;
use restate_core::{Metadata, RuntimeTaskHandle, TaskCenter, TaskKind};
use restate_invoker_impl::Service as InvokerService;
use restate_partition_store::{OpenMode, PartitionStore, PartitionStoreManager};
use restate_service_protocol::codec::ProtobufRawEntryCodec;
use restate_types::config::Configuration;
use restate_types::identifiers::{PartitionId, PartitionKey};
use restate_types::live::Live;
use restate_types::partition_processor::PartitionProcessorStatus;
use restate_types::schema::Schema;

use crate::invoker_integration::EntryEnricher;
use crate::partition::invoker_storage_reader::InvokerStorageReader;
use crate::partition::snapshots::SnapshotRepository;
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
    ) -> Self {
        Self {
            task_name,
            partition_id,
            key_range,
            configuration,
            bifrost,
            partition_store_manager,
            snapshot_repository,
        }
    }

    #[instrument(
        skip_all,
        fields(
            partition_id=%self.partition_id,
        )
    )]
    pub fn run(self) -> anyhow::Result<(StartedProcessor, RuntimeTaskHandle<anyhow::Result<()>>)> {
        let Self {
            task_name,
            partition_id,
            key_range,
            configuration,
            bifrost,
            partition_store_manager,
            snapshot_repository,
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
                    let partition_store = if !partition_store_manager
                        .has_partition_store(pp_builder.partition_id)
                        .await
                    {
                        let snapshot = if snapshot_repository.is_none() {
                            debug!(
                                partition_id = %partition_id,
                                "No snapshot repository configured",
                            );
                            None
                        } else {
                            debug!(
                                partition_id = %partition_id,
                                "Looking for partition snapshot from which to bootstrap partition store",
                            );
                            snapshot_repository.expect("is some").get_latest(partition_id).await?
                        };


                        if let Some(snapshot) = snapshot {
                            info!(
                                partition_id = %partition_id,
                                "Found snapshot to bootstrap partition, restoring it",
                            );

                            let snapshot_path = snapshot.base_dir.clone();
                            match partition_store_manager
                                .open_partition_store_from_snapshot(
                                    partition_id,
                                    key_range.clone(),
                                    snapshot,
                                    &options.storage.rocksdb,
                                )
                                .await {
                                Ok(partition_store) => {
                                    let res = tokio::fs::remove_dir_all(&snapshot_path).await;
                                    if let Err(e) = res {
                                        // This is not critical; since we move the SST files into RocksDB on import, at
                                        // worst the snapshot metadata file will be retained.
                                        warn!(
                                            partition_id = %partition_id,
                                            ?snapshot_path,
                                            "Failed to remove local snapshot directory, continuing with startup: {:?}",
                                            e
                                        );
                                    }
                                    partition_store
                                }
                                Err(e) => {
                                    warn!(
                                        partition_id = %partition_id,
                                        ?snapshot_path,
                                        "Failed to import snapshot, local copy retained"
                                    );
                                    return Err(anyhow::anyhow!(e));
                                }
                            }
                        } else {
                            info!(
                                    partition_id = %partition_id,
                                    "No snapshot found to bootstrap partition, creating new store",
                                );
                            partition_store_manager
                                .open_partition_store(
                                    partition_id,
                                    key_range,
                                    OpenMode::CreateIfMissing,
                                    &options.storage.rocksdb,
                                )
                                .await?
                        }
                    } else {
                        partition_store_manager
                            .open_partition_store(
                                partition_id,
                                key_range,
                                OpenMode::OpenExisting,
                                &options.storage.rocksdb,
                            )
                            .await?
                    };

                    TaskCenter::spawn_child(
                        TaskKind::SystemService,
                        invoker_name,
                        invoker.run(invoker_config),
                    )?;

                    pp_builder
                        .build::<ProtobufRawEntryCodec>(bifrost, partition_store)
                        .await?
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
