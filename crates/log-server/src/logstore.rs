// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;
use std::future::Future;
use std::sync::Arc;

use tokio::sync::SetOnce;

use restate_bifrost::loglet::OperationError;
use restate_memory::MemoryLease;
use restate_types::health::{HealthStatus, LogServerStatus};
use restate_types::logs::LogletId;
use restate_types::net::log_server::{Digest, GetDigest, GetRecords, Records, Seal, Store, Trim};

use restate_futures_util::monotonic_token::{Token, TokenListener};

use crate::metadata::{LogStoreMarker, LogletState};

pub type Result<T, E = OperationError> = std::result::Result<T, E>;

/// A marker type for commit/durability notifications.
///
/// Used within Token<Commit> to signal the last durable operation.
pub struct Commit;

// --- Store-level health state ---

/// Reason why the log store has disabled writes.
#[derive(Debug)]
#[allow(unused)]
#[non_exhaustive]
pub enum WriteDisableReason {
    /// Store writes disabled due to shutdown
    Shutdown,
    /// Store manually disabled (e.g., admin/maintenance operation).
    Manual,
    /// An error occurred while writing to storage.
    Error(Box<dyn std::error::Error + Send + Sync + 'static>),
}

impl fmt::Display for WriteDisableReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Shutdown => write!(f, "shutdown"),
            Self::Manual => write!(f, "manual"),
            Self::Error(source) => write!(f, "log-store error: {}", source),
        }
    }
}

/// Shared store-level state that tracks whether writes are disabled.
///
/// This is a store-wide singleton (one per `LogStore` instance) shared between
/// the writer task and all loglet workers. Once writes are disabled, the reason
/// is permanently recorded via [`SetOnce`].
///
/// The writer task calls [`disable_writes`] on failsafe. Loglet workers observe
/// the state via [`wait_disabled`] (async) or [`write_disable_reason`] (sync).
///
/// [`disable_writes`]: LogStoreState::disable_writes
/// [`wait_disabled`]: LogStoreState::wait_disabled
/// [`write_disable_reason`]: LogStoreState::write_disable_reason
#[derive(Clone)]
pub struct LogStoreState {
    disabled_writes: Arc<SetOnce<WriteDisableReason>>,
    health_status: HealthStatus<LogServerStatus>,
}

impl LogStoreState {
    pub fn new(health_status: HealthStatus<LogServerStatus>) -> Self {
        Self {
            disabled_writes: Arc::new(SetOnce::new()),
            health_status,
        }
    }

    pub fn update_health_status(&self, status: LogServerStatus) {
        self.health_status.update(status);
    }

    /// Disables writes with the given reason. If writes are already disabled,
    /// the call is a no-op (first reason wins).
    pub fn disable_writes(&self, reason: WriteDisableReason) {
        // Ignore the error — it means someone else already set the reason.
        let _ = self.disabled_writes.set(reason);
        self.health_status.update(LogServerStatus::Failsafe);
    }

    /// Waits until writes are disabled and returns the reason.
    pub async fn wait_disabled(&self) -> &WriteDisableReason {
        self.disabled_writes.wait().await
    }

    pub fn accepting_writes(&self) -> bool {
        !self.disabled_writes.initialized()
    }
}

/// Storage backend for the log-server.
///
/// `LogStore` provides setup and read operations. All write operations go
/// through a [`LogletWriter`] handle obtained via [`create_loglet_writer`].
/// This ensures that every writer is registered with the underlying writer
/// task and automatically unregistered when the handle is dropped.
///
/// [`create_loglet_writer`]: LogStore::create_loglet_writer
pub trait LogStore: Clone + Send + 'static {
    /// A per-loglet write handle. Created via [`LogStore::create_loglet_writer`].
    type Writer: LogletWriter;

    fn commit_listener(&self) -> TokenListener<Commit>;

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

    /// Returns the shared store-level state.
    fn state(&self) -> &LogStoreState;

    /// Creates a per-loglet writer handle. This registers the loglet's state
    /// with the underlying writer task so it can advance the tail watch after
    /// durable commits. When the returned handle (and all its clones) are
    /// dropped, the loglet is automatically unregistered.
    fn new_loglet_writer(&self, loglet_id: LogletId, loglet_state: &LogletState) -> Self::Writer;

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

/// Per-loglet write handle.
///
/// Created via [`LogStore::create_loglet_writer`]. Provides all write
/// operations (store, seal, trim) and exposes the store-level state.
///
/// All write methods are fire-and-forget: they enqueue the operation and
/// return immediately. Callers wait for durability by watching the
/// corresponding field on [`LogletState`] (local-tail watch for stores,
/// seal flag for seals, trim-point watch for trims) or the store state.
pub trait LogletWriter: Send + 'static {
    /// Enqueues a store batch for writing. The writer derives the committed
    /// offset from the store message and advances the registered loglet's tail
    /// watch after the batch is durably committed.
    fn enqueue_store(
        &mut self,
        store_message: Store,
        set_sequencer_in_metadata: bool,
        reservation: MemoryLease,
    ) -> Option<Token<Commit>>;

    /// Enqueues a seal operation. After durable commit, the writer notifies the seal
    /// through the registered TailOffsetWatch.
    fn enqueue_seal(&mut self, seal_message: Seal) -> Option<Token<Commit>>;

    /// Enqueues a trim operation. After durable commit, the writer will advance
    /// the trim-point in via the LogState's trim-point watch.
    fn enqueue_trim(&mut self, trim_message: Trim) -> Option<Token<Commit>>;

    /// Closes this log-store writer and unregisters from the writer task.
    fn close(&mut self);
}
