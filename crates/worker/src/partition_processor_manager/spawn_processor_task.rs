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
use tracing::instrument;

use crate::invoker_integration::EntryEnricher;
use crate::partition::invoker_storage_reader::InvokerStorageReader;
use crate::partition_processor_manager::processor_state::StartedProcessor;
use crate::PartitionProcessorBuilder;
use restate_bifrost::Bifrost;
use restate_core::{task_center, Metadata, RuntimeRootTaskHandle, TaskKind};
use restate_invoker_impl::Service as InvokerService;
use restate_partition_store::{OpenMode, PartitionStore, PartitionStoreManager};
use restate_service_protocol::codec::ProtobufRawEntryCodec;
use restate_types::cluster::cluster_state::PartitionProcessorStatus;
use restate_types::config::Configuration;
use restate_types::identifiers::{PartitionId, PartitionKey};
use restate_types::live::Live;
use restate_types::schema::Schema;
use restate_types::GenerationalNodeId;

pub struct SpawnPartitionProcessorTask {
    task_name: &'static str,
    node_id: GenerationalNodeId,
    partition_id: PartitionId,
    key_range: RangeInclusive<PartitionKey>,
    configuration: Live<Configuration>,
    metadata: Metadata,
    bifrost: Bifrost,
    partition_store_manager: PartitionStoreManager,
}

impl SpawnPartitionProcessorTask {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        task_name: &'static str,
        node_id: GenerationalNodeId,
        partition_id: PartitionId,
        key_range: RangeInclusive<PartitionKey>,
        configuration: Live<Configuration>,
        metadata: Metadata,
        bifrost: Bifrost,
        partition_store_manager: PartitionStoreManager,
    ) -> Self {
        Self {
            task_name,
            node_id,
            partition_id,
            key_range,
            configuration,
            metadata,
            bifrost,
            partition_store_manager,
        }
    }

    #[instrument(
        skip_all,
        fields(
            partition_id=%self.partition_id,
        )
    )]
    pub async fn run(
        self,
    ) -> anyhow::Result<(StartedProcessor, RuntimeRootTaskHandle<anyhow::Result<()>>)> {
        let Self {
            task_name,
            node_id,
            partition_id,
            key_range,
            configuration,
            metadata,
            bifrost,
            partition_store_manager,
        } = self;

        let config = configuration.pinned();
        let schema = metadata.updateable_schema();
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
            node_id,
            partition_id,
            key_range.clone(),
            status,
            options,
            control_rx,
            rpc_rx,
            watch_tx,
            invoker.handle(),
        );

        let invoker_name = Box::leak(Box::new(format!("invoker-{}", partition_id)));
        let invoker_config = configuration.clone().map(|c| &c.worker.invoker);

        let tc = task_center();
        let root_task_handle = tc.clone().start_runtime(
            TaskKind::PartitionProcessor,
            task_name,
            Some(pp_builder.partition_id),
            {
                let options = options.clone();
                let key_range = key_range.clone();
                let partition_store = partition_store_manager
                    .open_partition_store(
                        partition_id,
                        key_range,
                        OpenMode::CreateIfMissing,
                        &options.storage.rocksdb,
                    )
                    .await?;

                move || async move {
                    tc.spawn_child(
                        TaskKind::SystemService,
                        invoker_name,
                        Some(pp_builder.partition_id),
                        invoker.run(invoker_config),
                    )?;

                    pp_builder
                        .build::<ProtobufRawEntryCodec>(tc, bifrost, partition_store)
                        .await?
                        .run()
                        .await
                }
            },
        )?;

        let state = StartedProcessor::new(
            root_task_handle.cancellation_token(),
            key_range,
            control_tx,
            status_reader,
            rpc_tx,
            watch_rx,
        );

        Ok((state, root_task_handle))
    }
}
