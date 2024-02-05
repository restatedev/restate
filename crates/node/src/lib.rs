// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod admin;
mod options;
mod worker;

use codederror::CodedError;
use futures::future::OptionFuture;
use restate_types::identifiers::NodeId;
use tracing::{info, instrument};

use crate::admin::AdminRole;
use crate::worker::WorkerRole;
pub use options::{Options, OptionsBuilder as NodeOptionsBuilder};
pub use restate_admin::OptionsBuilder as AdminOptionsBuilder;
pub use restate_meta::OptionsBuilder as MetaOptionsBuilder;
pub use restate_worker::{OptionsBuilder as WorkerOptionsBuilder, RocksdbOptionsBuilder};

#[derive(Debug, thiserror::Error, CodedError)]
pub enum Error {
    #[error("admin failed: {0}")]
    Admin(
        #[from]
        #[code]
        admin::Error,
    ),
    #[error("worker failed: {0}")]
    Worker(
        #[from]
        #[code]
        worker::Error,
    ),
    #[error("admin panicked: {0}")]
    #[code(unknown)]
    AdminPanic(tokio::task::JoinError),
    #[error("worker panicked: {0}")]
    #[code(unknown)]
    WorkerPanic(tokio::task::JoinError),
}

#[derive(Debug, thiserror::Error, CodedError)]
pub enum BuildError {
    #[error("building worker failed: {0}")]
    Worker(
        #[from]
        #[code]
        worker::BuildError,
    ),
    #[error("building admin failed: {0}")]
    Admin(
        #[from]
        #[code]
        admin::BuildError,
    ),
}

pub struct Node {
    node_id: NodeId,

    admin_role: Option<AdminRole>,
    worker_role: Option<WorkerRole>,
}

impl Node {
    pub fn new(
        node_id: impl Into<NodeId>,
        admin_role: Option<Options>,
        worker_role: Option<Options>,
    ) -> Result<Self, BuildError> {
        let admin_role = admin_role.map(AdminRole::try_from).transpose()?;
        let worker_role = worker_role.map(WorkerRole::try_from).transpose()?;

        Ok(Node {
            node_id: node_id.into(),
            admin_role,
            worker_role,
        })
    }

    #[instrument(level = "debug", skip_all, fields(node.id = %self.node_id))]
    pub async fn run(self, shutdown_watch: drain::Watch) -> Result<(), Error> {
        let shutdown_signal = shutdown_watch.signaled();

        let (inner_shutdown_signal, inner_shutdown_watch) = drain::channel();

        let admin_handle: OptionFuture<_> = self
            .admin_role
            .map(|admin| admin.run(inner_shutdown_watch.clone()))
            .into();
        let worker_handle: OptionFuture<_> = self
            .worker_role
            .map(|worker| worker.run(inner_shutdown_watch))
            .into();
        tokio::pin!(admin_handle, worker_handle);

        tokio::select! {
            _ = shutdown_signal => {
                info!("Shutting down node");
                tokio::join!(inner_shutdown_signal.drain(), admin_handle, worker_handle);
            },
            Some(admin_result) = &mut admin_handle => {
                admin_result?;
                panic!("Unexpected termination of admin.");
            },
            Some(worker_result) = &mut worker_handle => {
                worker_result?;
                panic!("Unexpected termination of worker.");
            },
        }

        Ok(())
    }
}
