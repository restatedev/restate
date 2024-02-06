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
use restate_bifrost::BifrostService;
use restate_meta::{FileMetaStorage, MetaService};
use restate_node_ctrl::service::NodeCtrlService;
use restate_worker::Worker;
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
    #[error("node ctrl service failed: {0}")]
    NodeCtrlService(
        #[from]
        #[code]
        restate_node_ctrl::Error,
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
    #[error("node ctrl service panicked: {0}")]
    #[code(unknown)]
    NodeCtrlPanic(tokio::task::JoinError),
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
    meta: MetaService<FileMetaStorage>,
    node_ctrl: NodeCtrlService,
    worker: Worker,
    bifrost: BifrostService,
}

impl WorkerRole {
    pub async fn run(mut self, shutdown_watch: drain::Watch) -> Result<(), WorkerRoleError> {
        let shutdown_signal = shutdown_watch.signaled();

        let (inner_shutdown_signal, inner_shutdown_watch) = drain::channel();

        // Init the meta. This will reload the schemas in memory.
        self.meta.init().await?;

        let worker_command_tx = self.worker.worker_command_tx();
        let storage_query_context = self.worker.storage_query_context().clone();

        let mut node_ctrl_handle = tokio::spawn(self.node_ctrl.run(inner_shutdown_watch.clone()));
        let mut meta_handle = tokio::spawn(
            self.meta
                .run(inner_shutdown_watch.clone(), worker_command_tx.clone()),
        );
        let mut admin_handle = tokio::spawn(self.admin.run(
            inner_shutdown_watch.clone(),
            worker_command_tx,
            Some(storage_query_context),
        ));

        // Ensures bifrost has initial metadata synced up before starting the worker.
        let mut bifrost_handle = self.bifrost.start(inner_shutdown_watch.clone()).await?;

        let mut worker_handle = tokio::spawn(self.worker.run(inner_shutdown_watch));

        tokio::select! {
            _ = shutdown_signal => {
                info!("Stopping worker role");
                let _ = tokio::join!(inner_shutdown_signal.drain(), admin_handle, meta_handle, worker_handle, node_ctrl_handle, bifrost_handle);
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
            result = &mut node_ctrl_handle => {
                result.map_err(WorkerRoleError::NodeCtrlPanic)??;
                panic!("Unexpected termination of node ctrl service.");
            },
            result = &mut bifrost_handle => {
                result.map_err(WorkerRoleError::BifrostPanic)??;
                panic!("Unexpected termination of bifrost service.");
            },
        }

        Ok(())
    }
}

impl TryFrom<Options> for WorkerRole {
    type Error = WorkerRoleBuildError;

    fn try_from(options: Options) -> Result<Self, Self::Error> {
        let bifrost = options.bifrost.build(options.worker.partitions);
        let meta = options.meta.build()?;
        let admin = options
            .admin
            .build(meta.schemas(), meta.meta_handle(), bifrost.handle());
        let worker = options.worker.build(meta.schemas(), bifrost.handle())?;
        let node_ctrl = options
            .node_ctrl
            .build(Some(worker.rocksdb_storage().clone()), bifrost.handle());

        Ok(WorkerRole {
            admin,
            meta,
            node_ctrl,
            worker,
            bifrost,
        })
    }
}
