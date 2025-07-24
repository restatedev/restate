// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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

use tokio::sync::{mpsc, watch};
use tracing::{instrument, warn};

use restate_bifrost::Bifrost;
use restate_core::{Metadata, RuntimeTaskHandle, TaskCenter, TaskKind};
use restate_invoker_impl::Service as InvokerService;
use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_service_protocol::codec::ProtobufRawEntryCodec;
use restate_types::SharedString;
use restate_types::cluster::cluster_state::PartitionProcessorStatus;
use restate_types::config::Configuration;
use restate_types::live::Live;
use restate_types::live::LiveLoadExt;
use restate_types::logs::Lsn;
use restate_types::partitions::Partition;
use restate_types::partitions::state::PartitionReplicaSetStates;
use restate_types::schema::Schema;

use crate::PartitionProcessorBuilder;
use crate::invoker_integration::EntryEnricher;
use crate::partition::invoker_storage_reader::InvokerStorageReader;
use crate::partition::{ProcessorError, TargetLeaderState};
use crate::partition_processor_manager::processor_state::StartedProcessor;

pub struct SpawnPartitionProcessorTask {
    task_name: SharedString,
    partition: Partition,
    configuration: Live<Configuration>,
    bifrost: Bifrost,
    replica_set_states: PartitionReplicaSetStates,
    partition_store_manager: PartitionStoreManager,
    fast_forward_lsn: Option<Lsn>,
}

impl SpawnPartitionProcessorTask {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        task_name: SharedString,
        partition: Partition,
        configuration: Live<Configuration>,
        bifrost: Bifrost,
        replica_set_states: PartitionReplicaSetStates,
        partition_store_manager: PartitionStoreManager,
        fast_forward_lsn: Option<Lsn>,
    ) -> Self {
        Self {
            task_name,
            partition,
            configuration,
            bifrost,
            replica_set_states,
            partition_store_manager,
            fast_forward_lsn,
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
            task_name,
            partition,
            configuration,
            bifrost,
            replica_set_states,
            partition_store_manager,
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

        let (control_tx, control_rx) = watch::channel(TargetLeaderState::Follower);
        let (net_tx, net_rx) = mpsc::channel(128);
        let status = PartitionProcessorStatus::new();
        let (watch_tx, watch_rx) = watch::channel(status.clone());

        let pp_builder = PartitionProcessorBuilder::new(
            partition.clone(),
            status,
            control_rx,
            net_rx,
            watch_tx,
            invoker.handle(),
        );

        let invoker_name = Arc::from(format!("invoker-{}", partition.partition_id));
        let invoker_config = configuration.clone().map(|c| &c.worker.invoker);

        let root_task_handle = TaskCenter::current().start_runtime(
            TaskKind::PartitionProcessor,
            task_name,
            Some(pp_builder.partition.partition_id),
            {
                move || async move {
                    if let Some(delay) = delay {
                        tokio::time::sleep(delay).await;
                    }

                    let partition_store = partition_store_manager
                        .open(&pp_builder.partition, fast_forward_lsn)
                        .await?;

                    // invoker needs to outlive the partition processor when shutdown signal is
                    // received. This is why it's not spawned as a "child".
                    TaskCenter::spawn(
                        TaskKind::SystemService,
                        invoker_name,
                        invoker.run(invoker_config),
                    )
                    .map_err(|e| ProcessorError::from(anyhow::anyhow!(e)))?;

                    pp_builder
                        .build(bifrost, partition_store, replica_set_states)
                        .await
                        .map_err(ProcessorError::from)?
                        .run()
                        .await
                }
            },
        )?;

        let state = StartedProcessor::new(
            root_task_handle.cancellation_token().clone(),
            partition.key_range,
            control_tx,
            status_reader,
            net_tx,
            watch_rx,
        );

        Ok((state, root_task_handle))
    }
}
