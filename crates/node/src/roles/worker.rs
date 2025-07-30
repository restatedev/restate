// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use codederror::CodedError;

use restate_bifrost::Bifrost;
use restate_core::network::MessageRouterBuilder;
use restate_core::network::Networking;
use restate_core::network::TransportConnect;
use restate_core::worker_api::ProcessorsManagerHandle;
use restate_core::{MetadataWriter, TaskCenter};
use restate_core::{ShutdownError, TaskKind};
use restate_storage_query_datafusion::context::QueryContext;
use restate_types::health::HealthStatus;
use restate_types::partitions::state::PartitionReplicaSetStates;
use restate_types::protobuf::common::WorkerStatus;
use restate_worker::Worker;

#[derive(Debug, thiserror::Error, CodedError)]
pub enum WorkerRoleError {
    #[error("worker failed: {0}")]
    Worker(
        #[from]
        #[code]
        restate_worker::Error,
    ),
    #[error(transparent)]
    #[code(unknown)]
    Shutdown(#[from] ShutdownError),
}

#[derive(Debug, thiserror::Error, CodedError)]
pub enum SchemaError {
    #[error("failed to fetch schema updates: {0}")]
    #[code(unknown)]
    Fetch(#[from] tonic::Status),
    #[error("failed updating subscriptions: {0}")]
    #[code(unknown)]
    Subscription(#[from] restate_worker::WorkerHandleError),
}

#[derive(Debug, thiserror::Error, CodedError)]
pub enum WorkerRoleBuildError {
    #[error("failed creating worker: {0}")]
    Worker(
        #[from]
        #[code]
        restate_worker::BuildError,
    ),
}

pub struct WorkerRole {
    worker: Worker,
}

impl WorkerRole {
    pub async fn create<T: TransportConnect>(
        health_status: HealthStatus<WorkerStatus>,
        replica_set_states: PartitionReplicaSetStates,
        router_builder: &mut MessageRouterBuilder,
        networking: Networking<T>,
        bifrost: Bifrost,
        metadata_writer: MetadataWriter,
    ) -> Result<Self, WorkerRoleBuildError> {
        let worker = Worker::create(
            health_status,
            replica_set_states,
            networking,
            bifrost,
            router_builder,
            metadata_writer,
        )
        .await?;

        Ok(WorkerRole { worker })
    }

    pub fn partition_processor_manager_handle(&self) -> ProcessorsManagerHandle {
        self.worker.partition_processor_manager_handle()
    }

    pub fn storage_query_context(&self) -> &QueryContext {
        self.worker.storage_query_context()
    }

    pub fn start(self) -> anyhow::Result<()> {
        TaskCenter::spawn(TaskKind::WorkerRole, "worker-service", async {
            self.worker.run().await
        })?;

        Ok(())
    }
}
