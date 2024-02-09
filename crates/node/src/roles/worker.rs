// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::Options;
use codederror::CodedError;
use restate_admin::service::AdminService;
use restate_bifrost::{Bifrost, BifrostService};
use restate_meta::{FileMetaStorage, MetaService};
use restate_schema_api::subscription::SubscriptionResolver;
use restate_schema_impl::Schemas;
use restate_storage_rocksdb::RocksDBStorage;
use restate_worker::{KafkaIngressOptions, Worker};
use restate_worker_api::Handle;
use restate_worker_api::SubscriptionController;
use tracing::info;

#[derive(Debug, thiserror::Error, CodedError)]
pub enum WorkerRoleError {
    #[error("admin service failed: {0}")]
    AdminService(
        #[from]
        #[code]
        restate_admin::Error,
    ),
    #[error("meta failed: {0}")]
    MetaService(
        #[from]
        #[code]
        restate_meta::Error,
    ),
    #[error("worker failed: {0}")]
    Worker(
        #[from]
        #[code]
        restate_worker::Error,
    ),
    #[error("bifrost failed: {0}")]
    #[code(unknown)]
    Bifrost(#[from] restate_bifrost::Error),
    #[error("admin panicked: {0}")]
    #[code(unknown)]
    AdminPanic(tokio::task::JoinError),
    #[error("meta panicked: {0}")]
    #[code(unknown)]
    MetaPanic(tokio::task::JoinError),
    #[error("worker panicked: {0}")]
    #[code(unknown)]
    WorkerPanic(tokio::task::JoinError),
    #[error("bifrost panicked: {0}")]
    #[code(unknown)]
    BifrostPanic(tokio::task::JoinError),
}

#[derive(Debug, thiserror::Error, CodedError)]
pub enum WorkerRoleBuildError {
    #[error("failed creating worker: {0}")]
    Worker(
        #[from]
        #[code]
        restate_worker::BuildError,
    ),
    #[error("failed creating meta: {0}")]
    Meta(
        #[from]
        #[code]
        restate_meta::BuildError,
    ),
}

pub struct WorkerRole {
    admin: AdminService,
    meta: MetaService<FileMetaStorage, KafkaIngressOptions>,
    worker: Worker,
    bifrost: BifrostService,
}

impl WorkerRole {
    pub fn rocksdb_storage(&self) -> &RocksDBStorage {
        self.worker.rocksdb_storage()
    }

    pub fn bifrost_handle(&self) -> Bifrost {
        self.bifrost.handle()
    }

    pub async fn run(mut self, shutdown_watch: drain::Watch) -> Result<(), WorkerRoleError> {
        let shutdown_signal = shutdown_watch.signaled();

        let (inner_shutdown_signal, inner_shutdown_watch) = drain::channel();

        // Init the meta. This will reload the schemas in memory.
        self.meta.init().await?;
        let schemas = self.meta.schemas();

        let worker_command_tx = self.worker.worker_command_tx();
        let storage_query_context = self.worker.storage_query_context().clone();

        let mut meta_handle = tokio::spawn(self.meta.run(inner_shutdown_watch.clone()));
        let mut admin_handle = tokio::spawn(self.admin.run(
            inner_shutdown_watch.clone(),
            worker_command_tx.clone(),
            Some(storage_query_context),
        ));

        // Ensures bifrost has initial metadata synced up before starting the worker.
        let mut bifrost_handle = self.bifrost.start(inner_shutdown_watch.clone()).await?;

        let mut worker_handle = tokio::spawn(self.worker.run(inner_shutdown_watch));

        // The reason we reload subscriptions here is because
        // reload_subscriptions writes to a bounded channel read by the worker.
        // If the worker is not running, this could deadlock when reaching the channel capacity.
        // While here, we're safe to assume the worker is running and will read from that channel.
        Self::reload_subscriptions(schemas, worker_command_tx.subscription_controller_handle())
            .await;

        tokio::select! {
            _ = shutdown_signal => {
                info!("Stopping worker role");
                let _ = tokio::join!(inner_shutdown_signal.drain(), admin_handle, meta_handle, worker_handle, bifrost_handle);
            },
            result = &mut meta_handle => {
                result.map_err(WorkerRoleError::MetaPanic)??;
                panic!("Unexpected termination of meta.");
            },
            result = &mut admin_handle => {
                result.map_err(WorkerRoleError::AdminPanic)??;
                panic!("Unexpected termination of admin.");
            },
            result = &mut worker_handle => {
                result.map_err(WorkerRoleError::WorkerPanic)??;
                panic!("Unexpected termination of worker.");
            }
            result = &mut bifrost_handle => {
                result.map_err(WorkerRoleError::BifrostPanic)??;
                panic!("Unexpected termination of bifrost service.");
            },
        }

        Ok(())
    }

    async fn reload_subscriptions(
        schemas: Schemas,
        worker_command_tx: impl SubscriptionController + Send + Sync + Sized,
    ) {
        for subscription in schemas.list_subscriptions(&[]) {
            let _ = worker_command_tx.start_subscription(subscription).await;
        }
    }
}

impl TryFrom<Options> for WorkerRole {
    type Error = WorkerRoleBuildError;

    fn try_from(options: Options) -> Result<Self, Self::Error> {
        let bifrost = options.bifrost.build(options.worker.partitions);
        let meta = options.meta.build(options.worker.kafka.clone())?;
        let admin = options.admin.build(meta.schemas(), meta.meta_handle());
        let worker = options.worker.build(meta.schemas(), bifrost.handle())?;

        Ok(WorkerRole {
            admin,
            meta,
            worker,
            bifrost,
        })
    }
}
