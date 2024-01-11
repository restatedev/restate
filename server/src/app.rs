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
use restate_meta::Meta;
use restate_node_ctrl::service::NodeCtrlService;
use restate_worker::Worker;

#[derive(Debug, thiserror::Error, CodedError)]
pub enum ApplicationError {
    #[error("meta failed: {0}")]
    Meta(
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
}

#[derive(Debug, thiserror::Error, CodedError)]
#[error("failed creating restate application: {cause}")]
pub struct BuildError {
    #[from]
    #[code]
    cause: restate_worker::BuildError,
}

pub struct Application {
    node_ctrl: NodeCtrlService,
    meta: Meta,
    worker: Worker,
}

impl Application {
    pub fn new(
        node_ctrl: restate_node_ctrl::Options,
        meta: restate_meta::Options,
        worker: restate_worker::Options,
    ) -> Result<Self, BuildError> {
        let meta = meta.build();
        let worker = worker.build(meta.schemas())?;

        let node_ctrl = node_ctrl.build();

        Ok(Self {
            node_ctrl,
            meta,
            worker,
        })
    }

    pub async fn run(mut self, drain: drain::Watch) -> Result<(), ApplicationError> {
        let (shutdown_signal, shutdown_watch) = drain::channel();
        // start node ctrl service base
        let mut node_ctrl_handle = tokio::spawn(self.node_ctrl.run(shutdown_watch.clone()));

        // Init the meta. This will reload the schemas in memory.
        self.meta.init().await?;

        let worker_command_tx = self.worker.worker_command_tx();
        let mut meta_handle =
            tokio::spawn(self.meta.run(shutdown_watch.clone(), worker_command_tx));
        let mut worker_handle = tokio::spawn(self.worker.run(shutdown_watch));

        let shutdown = drain.signaled();
        tokio::pin!(shutdown);

        tokio::select! {
            _ = shutdown => {
                let _ = tokio::join!(shutdown_signal.drain(), meta_handle, worker_handle, node_ctrl_handle);
            },
            result = &mut meta_handle => {
                result.map_err(ApplicationError::MetaPanic)??;
                panic!("Unexpected termination of meta.");
            },
            result = &mut worker_handle => {
                result.map_err(ApplicationError::WorkerPanic)??;
                panic!("Unexpected termination of worker.");
            }
            result = &mut node_ctrl_handle => {
                result.map_err(ApplicationError::NodeCtrlPanic)??;
                panic!("Unexpected termination of node ctrl service.");
            },
        }

        Ok(())
    }
}
