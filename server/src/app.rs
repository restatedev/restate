// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use codederror::CodedError;
use restate_admin::service::AdminService;
use restate_bifrost::BifrostService;
use restate_meta::{FileMetaStorage, MetaService};
use restate_node_ctrl::service::NodeCtrlService;
use restate_worker::Worker;

#[derive(Debug, thiserror::Error, CodedError)]
pub enum ApplicationError {
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
    BifrostService(#[from] restate_bifrost::Error),
    #[error("bifrost panicked: {0}")]
    #[code(unknown)]
    BifrostPanic(tokio::task::JoinError),
    #[error("meta panicked: {0}")]
    #[code(unknown)]
    MetaPanic(tokio::task::JoinError),
    #[error("worker panicked: {0}")]
    #[code(unknown)]
    WorkerPanic(tokio::task::JoinError),
    #[error("node ctrl service failed: {0}")]
    NodeCtrlService(
        #[from]
        #[code]
        restate_node_ctrl::Error,
    ),
    #[error("node ctrl service panicked: {0}")]
    #[code(unknown)]
    NodeCtrlPanic(tokio::task::JoinError),
    #[error("admin panicked: {0}")]
    #[code(unknown)]
    AdminPanic(tokio::task::JoinError),
}

#[derive(Debug, thiserror::Error, CodedError)]
pub enum BuildError {
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

pub struct Application {
    node_ctrl: NodeCtrlService,
    meta: MetaService<FileMetaStorage>,
    admin: AdminService,
    worker: Worker,
    bifrost: BifrostService,
}

impl Application {
    pub fn new(
        node_ctrl: restate_node_ctrl::Options,
        meta: restate_meta::Options,
        worker: restate_worker::Options,
        admin: restate_admin::Options,
        bifrost: restate_bifrost::Options,
    ) -> Result<Self, BuildError> {
        // create bifrost service
        let bifrost = bifrost.build(worker.partitions);
        let meta = meta.build()?;
        // create cluster admin server
        let admin = admin.build(meta.schemas(), meta.meta_handle(), bifrost.handle());
        // create worker service
        let worker = worker.build(meta.schemas(), bifrost.handle())?;

        let node_ctrl = node_ctrl.build(Some(worker.rocksdb_storage().clone()), bifrost.handle());

        Ok(Self {
            node_ctrl,
            admin,
            meta,
            worker,
            bifrost,
        })
    }

    pub async fn run(mut self, drain: drain::Watch) -> Result<(), ApplicationError> {
        let (shutdown_signal, shutdown_watch) = drain::channel();
        // start node ctrl service base
        let mut node_ctrl_handle = tokio::spawn(self.node_ctrl.run(shutdown_watch.clone()));
        // Init the meta. This will reload the schemas in memory.
        self.meta.init().await?;
        let worker_command_tx = self.worker.worker_command_tx();
        let storage_query_context = self.worker.storage_query_context().clone();
        let mut meta_handle = tokio::spawn(
            self.meta
                .run(shutdown_watch.clone(), worker_command_tx.clone()),
        );
        let mut admin_handle = tokio::spawn(self.admin.run(
            shutdown_watch.clone(),
            worker_command_tx,
            Some(storage_query_context),
        ));
        // Ensures bifrost has initial metadata synced up before starting the worker.
        let mut bifrost_handle = self.bifrost.start(shutdown_watch.clone()).await?;
        let mut worker_handle = tokio::spawn(self.worker.run(shutdown_watch));

        let shutdown = drain.signaled();
        tokio::pin!(shutdown);

        tokio::select! {
            _ = shutdown => {
                let _ = tokio::join!(
                            shutdown_signal.drain(),
                            admin_handle,
                            meta_handle,
                            worker_handle,
                            node_ctrl_handle,
                            bifrost_handle
                        );
            },
            result = &mut meta_handle => {
                result.map_err(ApplicationError::MetaPanic)??;
                panic!("Unexpected termination of meta.");
            },
            result = &mut admin_handle => {
                result.map_err(ApplicationError::AdminPanic)??;
                panic!("Unexpected termination of admin.");
            },
            result = &mut worker_handle => {
                result.map_err(ApplicationError::WorkerPanic)??;
                panic!("Unexpected termination of worker.");
            }
            result = &mut node_ctrl_handle => {
                result.map_err(ApplicationError::NodeCtrlPanic)??;
                panic!("Unexpected termination of node ctrl service.");
            },
            result = &mut bifrost_handle => {
                result.map_err(ApplicationError::BifrostPanic)??;
                panic!("Unexpected termination of bifrost service.");
            },
        }

        Ok(())
    }
}
