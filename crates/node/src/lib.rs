// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod options;

use codederror::CodedError;
use restate_admin::service::AdminService;
use restate_bifrost::BifrostService;
use restate_meta::{FileMetaStorage, MetaService};
use restate_node_ctrl::service::NodeCtrlService;
use restate_types::identifiers::NodeId;
use restate_worker::Worker;
use tracing::info;

pub use options::{Options, OptionsBuilder as NodeOptionsBuilder};
pub use restate_admin::OptionsBuilder as AdminOptionsBuilder;
pub use restate_meta::OptionsBuilder as MetaOptionsBuilder;
pub use restate_worker::{OptionsBuilder as WorkerOptionsBuilder, RocksdbOptionsBuilder};

#[derive(Debug, thiserror::Error, CodedError)]
pub enum Error {
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
    #[error("node-ctrl failed: {0}")]
    NodeCtrl(
        #[from]
        #[code]
        restate_node_ctrl::Error,
    ),
    #[error("bifrost failed: {0}")]
    #[code(unknown)]
    BifrostService(#[from] restate_bifrost::Error),
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

pub struct Node {
    node_id: NodeId,

    admin: AdminService,
    meta: MetaService<FileMetaStorage>,
    node_ctrl: NodeCtrlService,
    worker: Worker,
    bifrost: BifrostService,
}

impl Node {
    pub fn new(node_id: impl Into<NodeId>, options: Options) -> Result<Self, BuildError> {
        let bifrost = options.bifrost.build(options.worker.partitions);
        let meta = options.meta.build()?;
        let admin = options
            .admin
            .build(meta.schemas(), meta.meta_handle(), bifrost.handle());
        let worker = options.worker.build(meta.schemas(), bifrost.handle())?;
        let node_ctrl = options
            .node_ctrl
            .build(Some(worker.rocksdb_storage().clone()), bifrost.handle());

        Ok(Node {
            node_id: node_id.into(),
            admin,
            meta,
            node_ctrl,
            worker,
            bifrost,
        })
    }

    pub async fn run(mut self, shutdown_watch: drain::Watch) -> Result<(), Error> {
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
                info!(node_id = %self.node_id, "Shutting down node");
                let _ = tokio::join!(inner_shutdown_signal.drain(), admin_handle, meta_handle, worker_handle, node_ctrl_handle, bifrost_handle);
            },
            result = &mut meta_handle => {
                result.map_err(Error::MetaPanic)??;
                panic!("Unexpected termination of meta.");
            },
            result = &mut admin_handle => {
                result.map_err(Error::AdminPanic)??;
                panic!("Unexpected termination of admin.");
            },
            result = &mut worker_handle => {
                result.map_err(Error::WorkerPanic)??;
                panic!("Unexpected termination of worker.");
            }
            result = &mut node_ctrl_handle => {
                result.map_err(Error::NodeCtrlPanic)??;
                panic!("Unexpected termination of node ctrl service.");
            },
            result = &mut bifrost_handle => {
                result.map_err(Error::BifrostPanic)??;
                panic!("Unexpected termination of bifrost service.");
            },
        }

        Ok(())
    }
}
