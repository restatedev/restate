// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// todo(asoli): remove once this is fleshed out
#![allow(dead_code)]

use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::BoxStream;

use restate_core::network::{Networking, TransportConnect};
use restate_core::{Metadata, ShutdownError, TaskCenter};
use restate_types::logs::{KeyFilter, LogletOffset, Record, TailState};
use restate_types::replicated_loglet::ReplicatedLogletParams;

use crate::loglet::{Loglet, LogletCommit, OperationError, SendableLogletReadStream};

use super::rpc_routers::LogServersRpc;

#[derive(derive_more::Debug)]
pub(super) struct ReplicatedLoglet<T> {
    my_params: ReplicatedLogletParams,
    #[debug(skip)]
    task_center: TaskCenter,
    #[debug(skip)]
    metadata: Metadata,
    #[debug(skip)]
    logservers_rpc: LogServersRpc,
    #[debug(skip)]
    networking: Networking<T>,
}

impl<T: TransportConnect> ReplicatedLoglet<T> {
    pub fn new(
        my_params: ReplicatedLogletParams,
        task_center: TaskCenter,
        metadata: Metadata,
        networking: Networking<T>,
        logservers_rpc: LogServersRpc,
    ) -> Self {
        Self {
            my_params,
            task_center,
            metadata,
            networking,
            logservers_rpc,
        }
    }
}

#[async_trait]
impl<T: TransportConnect> Loglet for ReplicatedLoglet<T> {
    async fn create_read_stream(
        self: Arc<Self>,
        _filter: KeyFilter,
        _from: LogletOffset,
        _to: Option<LogletOffset>,
    ) -> Result<SendableLogletReadStream, OperationError> {
        todo!()
    }

    fn watch_tail(&self) -> BoxStream<'static, TailState<LogletOffset>> {
        todo!()
    }

    async fn enqueue_batch(&self, _payloads: Arc<[Record]>) -> Result<LogletCommit, ShutdownError> {
        todo!()
    }

    async fn find_tail(&self) -> Result<TailState<LogletOffset>, OperationError> {
        todo!()
    }

    async fn get_trim_point(&self) -> Result<Option<LogletOffset>, OperationError> {
        todo!()
    }

    /// Trim the log to the minimum of new_trim_point and last_committed_offset
    /// new_trim_point is inclusive (will be trimmed)
    async fn trim(&self, _new_trim_point: LogletOffset) -> Result<(), OperationError> {
        todo!()
    }

    async fn seal(&self) -> Result<(), OperationError> {
        todo!()
    }
}
