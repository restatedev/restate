// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::Future;

use futures::FutureExt;
use tokio::sync::oneshot;

use restate_bifrost::loglet::OperationError;
use restate_core::ShutdownError;
use restate_memory::MemoryLease;
use restate_types::logs::LogletId;
use restate_types::net::log_server::{Digest, GetDigest, GetRecords, Records, Seal, Store, Trim};

use crate::metadata::{LogStoreMarker, LogletState};

pub type Result<T, E = OperationError> = std::result::Result<T, E>;

pub trait LogStore: Clone + Send + 'static {
    /// Loads the [`LogStoreMarker`] for this node
    fn load_marker(&self) -> impl Future<Output = Result<Option<LogStoreMarker>>> + Send + '_;
    /// Unconditionally stores this marker value on this node
    fn store_marker(&self, marker: LogStoreMarker) -> impl Future<Output = Result<()>> + Send;
    /// Reads the loglet state from storage and returns a new [`LogletState`] value.
    /// Note that this value will only be connected to its own clones, any previously loaded
    /// [`LogletState`] will not observe the values in this one.
    fn load_loglet_state(
        &self,
        loglet_id: LogletId,
    ) -> impl Future<Output = Result<LogletState, OperationError>> + Send;

    fn enqueue_store(
        &self,
        store_message: Store,
        set_sequencer_in_metadata: bool,
        reservation: MemoryLease,
    ) -> impl Future<Output = Result<AsyncToken, OperationError>> + Send;

    fn enqueue_seal(
        &self,
        seal_message: Seal,
    ) -> impl Future<Output = Result<AsyncToken, OperationError>> + Send;

    fn enqueue_trim(
        &self,
        trim_message: Trim,
    ) -> impl Future<Output = Result<AsyncToken, OperationError>> + Send;

    fn read_records(
        &self,
        get_records_message: GetRecords,
        loglet_state: &LogletState,
    ) -> impl Future<Output = Result<Records, OperationError>> + Send;

    fn get_records_digest(
        &self,
        get_records_message: GetDigest,
        loglet_state: &LogletState,
    ) -> impl Future<Output = Result<Digest, OperationError>> + Send;
}

/// A future that resolves when a log-store operation is completed
pub struct AsyncToken {
    rx: oneshot::Receiver<Result<()>>,
}

impl AsyncToken {
    pub(crate) fn new(rx: oneshot::Receiver<Result<()>>) -> Self {
        Self { rx }
    }
}

impl std::future::Future for AsyncToken {
    type Output = Result<()>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.rx
            .poll_unpin(cx)
            .map_err(|_| OperationError::Shutdown(ShutdownError))?
    }
}
