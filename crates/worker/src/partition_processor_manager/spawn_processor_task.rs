// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tracing::{info, instrument, warn};

use restate_core::network::{ShardSender, TransportConnect};
use restate_core::{RuntimeTaskHandle, TaskCenter, TaskKind, cancellation_token};
use restate_ingestion_client::IngestionClient;
use restate_partition_store::PartitionStoreManager;
use restate_platform::prelude::ReString;
use restate_types::cluster::cluster_state::PartitionProcessorStatus;
use restate_types::logs::Lsn;
use restate_types::partitions::Partition;
use restate_wal_protocol::Envelope;

use crate::PartitionProcessorBuilder;
use crate::partition::NodeContext;
use crate::partition::{ProcessorError, TargetLeaderState};
use crate::partition_processor_manager::processor_state::StartedProcessor;

pub struct SpawnPartitionProcessorTask<T> {
    node_ctx: NodeContext,
    task_name: ReString,
    partition: Partition,
    partition_store_manager: Arc<PartitionStoreManager>,
    fast_forward_lsn: Option<Lsn>,
    ingestion_client: IngestionClient<T, Envelope>,
}

impl<T> SpawnPartitionProcessorTask<T>
where
    T: TransportConnect,
{
    pub fn new(
        node_ctx: NodeContext,
        task_name: ReString,
        partition: Partition,
        partition_store_manager: Arc<PartitionStoreManager>,
        fast_forward_lsn: Option<Lsn>,
        ingestion_client: IngestionClient<T, Envelope>,
    ) -> Self {
        Self {
            node_ctx,
            task_name,
            partition,
            partition_store_manager,
            fast_forward_lsn,
            ingestion_client,
        }
    }

    /// Start the spawn processor task. The task is delayed by the given `delay`.
    #[instrument(
        level = "error",
        skip_all,
        fields(
            partition_id=%self.partition.partition_id,
        )
    )]
    pub fn run(
        self,
        delay: Option<Duration>,
    ) -> anyhow::Result<(
        StartedProcessor,
        RuntimeTaskHandle<Result<(), ProcessorError>>,
    )> {
        let Self {
            node_ctx,
            task_name,
            partition,
            partition_store_manager,
            fast_forward_lsn,
            ingestion_client,
        } = self;

        let (control_tx, control_rx) = watch::channel(TargetLeaderState::Follower);
        let (net_tx, net_rx) = ShardSender::new();
        let (watch_tx, watch_rx) = watch::channel(PartitionProcessorStatus::default());

        let pp_builder = PartitionProcessorBuilder::new(control_rx, net_rx, watch_tx, node_ctx);

        let key_range = partition.key_range;

        let root_task_handle = TaskCenter::current().start_runtime(
            TaskKind::PartitionProcessor,
            task_name,
            Some(partition.partition_id),
            {
                move || async move {
                    let cancellation = cancellation_token();
                    let wait_for_delay = async {
                        if let Some(delay) = delay {
                            tokio::time::sleep(delay).await;
                        }
                    };

                    if cancellation
                        .run_until_cancelled(wait_for_delay)
                        .await
                        .is_none()
                    {
                        info!(
                            partition_id = %partition.partition_id,
                            "Partition processor stopped due to cancellation signal"
                        );
                        return Ok(());
                    }

                    // RocksDB operations continue on the storage pool if their awaiting future is
                    // dropped. Once opening starts, drain it before honoring cancellation so a
                    // replacement processor cannot race the abandoned open.
                    let partition_store = partition_store_manager
                        .open(&partition, fast_forward_lsn)
                        .await;

                    if cancellation.is_cancelled() {
                        info!(
                            partition_id = %partition.partition_id,
                            "Partition processor stopped due to cancellation signal"
                        );
                        return Ok(());
                    }

                    let mut partition_store = partition_store.map_err(ProcessorError::from)?;

                    // verify that this partition store is not sealed
                    if let Some(seal) = partition_store.get_seal_marker().await? {
                        warn!("Local partition store for partition {} is sealed due to {}. The \
                        partition store is not safe to use by this node and will need to be replaced \
                        by a safe snapshot before continuing!",
                        partition.partition_id,
                        seal,
                        );
                        return Err(ProcessorError::from(seal));
                    }

                    let db = partition_store.into_inner();

                    let run_result = async move {
                        let pp = pp_builder
                            .build(ingestion_client, db).await?;
                        pp.run().await
                    }
                    .await;

                    info!(
                        partition_id = %partition.partition_id,
                        "Partition processor stopped"
                    );
                    run_result
                }
            },
        )?;

        let state = StartedProcessor::new(
            root_task_handle.cancellation_token().clone(),
            key_range,
            control_tx,
            net_tx,
            watch_rx,
        );

        Ok((state, root_task_handle))
    }
}
