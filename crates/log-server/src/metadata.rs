// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use tokio::sync::watch;
use tokio::sync::{Mutex as AsyncMutex, mpsc, oneshot};

use crate::logstore::LogStore;
use restate_bifrost::loglet::OperationError;
use restate_clock::time::MillisSinceEpoch;
use restate_types::logs::{LogletId, LogletOffset, SequenceNumber, TailOffsetWatch, TailState};
use restate_types::storage::StorageMarker;
use restate_types::{GenerationalNodeId, PlainNodeId};

/// A marker stored in log-server-level loglet storage
pub type LogStoreMarker = StorageMarker<PlainNodeId>;

/// Caches loglet state in memory
#[derive(Default, Clone)]
pub struct LogletStateMap {
    inner: Arc<AsyncMutex<HashMap<LogletId, LogletState>>>,
}

impl LogletStateMap {
    // todo (optimization to preload from log_store)
    pub async fn load_all<S: LogStore>(_log_store: &S) -> Result<Self, OperationError> {
        Ok(Self::default())
    }

    /// Returns a snapshot of all currently cached loglet states.
    pub async fn snapshot(&self) -> HashMap<LogletId, LogletState> {
        let guard = self.inner.lock().await;
        guard.clone()
    }

    pub async fn get_or_load<S: LogStore>(
        &self,
        loglet_id: LogletId,
        log_store: &S,
    ) -> Result<LogletState, OperationError> {
        let mut guard = self.inner.lock().await;
        if let Some(state) = guard.get(&loglet_id) {
            return Ok(state.clone());
        }

        let state = log_store.load_loglet_state(loglet_id).await?;
        guard.insert(loglet_id, state);
        // just inserted it, safe to unwrap.
        Ok(guard.get(&loglet_id).cloned().unwrap())
    }
}

/// Metadata stored for every loglet-id known to this node.
/// Cheap to clone. Clones share the state.
#[derive(Clone)]
pub struct LogletState {
    /// The sequence remains unknown until the first incoming operation that
    /// informs us what the sequencer is.
    sequencer: Arc<OnceLock<GenerationalNodeId>>,
    local_tail: TailOffsetWatch,
    trim_point: watch::Sender<LogletOffset>,
    known_global_tail: GlobalTailTracker,
}

impl LogletState {
    pub fn new(
        sequencer_node: Option<GenerationalNodeId>,
        local_tail: LogletOffset,
        sealed: bool,
        trim_point: LogletOffset,
        known_global_tail: LogletOffset,
    ) -> Self {
        let local_tail = TailOffsetWatch::new(TailState::new(sealed, local_tail));
        let trim_point = watch::Sender::new(trim_point);
        let sequencer = Arc::new(OnceLock::new());
        let known_global_tail = GlobalTailTracker::new(known_global_tail);
        if let Some(sequencer_node) = sequencer_node {
            let _ = sequencer.set(sequencer_node);
        }
        Self {
            sequencer,
            local_tail,
            trim_point,
            known_global_tail,
        }
    }

    pub fn sequencer(&self) -> Option<&GenerationalNodeId> {
        self.sequencer.get()
    }

    /// Returns true if successful
    pub fn set_sequencer(&mut self, sequencer: GenerationalNodeId) -> bool {
        self.sequencer.set(sequencer).is_ok()
    }

    pub fn notify_seal(&self) {
        self.local_tail.notify_seal();
    }

    pub fn local_tail_watch(&self) -> &TailOffsetWatch {
        &self.local_tail
    }

    pub fn get_local_tail_watch(&self) -> TailOffsetWatch {
        self.local_tail.clone()
    }

    pub fn subscribe_local_tail(&self) -> watch::Receiver<TailState<LogletOffset>> {
        self.local_tail.subscribe()
    }

    pub fn subscribe_global_tail(&self) -> watch::Receiver<LogletOffset> {
        let mut rx = self.known_global_tail.subscribe();
        rx.mark_changed();
        rx
    }

    pub fn is_sealed(&self) -> bool {
        self.local_tail.is_sealed()
    }

    pub fn local_tail(&self) -> TailState<LogletOffset> {
        *self.local_tail.get()
    }

    pub fn known_global_tail(&self) -> LogletOffset {
        self.known_global_tail.get()
    }

    pub fn notify_known_global_tail(&self, known_global_tail: LogletOffset) {
        self.known_global_tail.notify(known_global_tail)
    }

    pub fn trim_point(&self) -> LogletOffset {
        *self.trim_point.borrow()
    }

    pub fn update_trim_point(&mut self, new_trim_point: LogletOffset) -> bool {
        self.trim_point.send_if_modified(|t| {
            if new_trim_point > *t {
                *t = new_trim_point;
                true
            } else {
                false
            }
        })
    }
}

#[derive(Clone)]
pub struct GlobalTailTracker {
    watch_tx: watch::Sender<LogletOffset>,
}

impl Default for GlobalTailTracker {
    fn default() -> Self {
        Self {
            watch_tx: watch::Sender::new(LogletOffset::OLDEST),
        }
    }
}

impl GlobalTailTracker {
    pub fn new(initial_offset: LogletOffset) -> Self {
        Self {
            watch_tx: watch::Sender::new(initial_offset),
        }
    }

    pub fn get(&self) -> LogletOffset {
        *self.watch_tx.borrow()
    }

    /// Returns a watch receiver for the global tail. The receiver is
    /// pre-marked as changed so the first `changed().await` resolves
    /// immediately with the current value.
    #[allow(dead_code)]
    pub fn subscribe(&self) -> watch::Receiver<LogletOffset> {
        let mut rx = self.watch_tx.subscribe();
        rx.mark_changed();
        rx
    }

    pub fn notify(&self, potential_global_tail: LogletOffset) {
        self.watch_tx.send_if_modified(|known_global_tail| {
            if potential_global_tail > *known_global_tail {
                *known_global_tail = potential_global_tail;
                return true;
            }
            false
        });
    }
}

pub enum IntrospectLogletWorker {
    GetState(oneshot::Sender<LogletWorkerState>),
}

/// Snapshot of a loglet worker's live operational state, published for introspection.
#[derive(Debug, Clone)]
pub struct LogletWorkerState {
    pub staging_local_tail: LogletOffset,
    pub accepting_writes: bool,
    pub seal_enqueued: bool,
    pub pending_stores: u32,
    pub pending_repair_stores: u32,
    pub pending_seals: u32,
    pub pending_trims: u32,
    pub pending_tail_waiters: u32,
    pub last_request_at: MillisSinceEpoch,
}

/// Thread-safe registry of active loglet workers' introspection channels.
///
/// The `RequestPump` registers workers when they start and unregisters them
/// when they shut down. External observers (e.g., the `loglet_workers`
/// DataFusion table scanner) use this to send on-demand introspection
/// commands to each active worker.
#[derive(Clone, Default)]
pub struct ActiveWorkerMap {
    inner: Arc<Mutex<HashMap<LogletId, mpsc::Sender<IntrospectLogletWorker>>>>,
}

impl ActiveWorkerMap {
    /// Registers a worker's introspection channel.
    pub fn register(&self, loglet_id: LogletId, sender: mpsc::Sender<IntrospectLogletWorker>) {
        self.inner
            .lock()
            .expect("ActiveWorkerMap lock poisoned")
            .insert(loglet_id, sender);
    }

    /// Removes a worker's introspection channel (called when the worker shuts down).
    pub fn remove(&self, loglet_id: &LogletId) {
        self.inner
            .lock()
            .expect("ActiveWorkerMap lock poisoned")
            .remove(loglet_id);
    }

    /// Queries all active workers for their current state.
    ///
    /// Sends `GetState` to each registered worker and collects responses.
    /// Workers that fail to respond (channel full or closed) are skipped.
    pub async fn get_all_worker_states(&self) -> HashMap<LogletId, LogletWorkerState> {
        let senders: Vec<_> = {
            let guard = self.inner.lock().expect("ActiveWorkerMap lock poisoned");
            guard.iter().map(|(id, tx)| (*id, tx.clone())).collect()
        };

        let mut result = HashMap::with_capacity(senders.len());
        for (loglet_id, tx) in senders {
            let (reply_tx, reply_rx) = oneshot::channel();
            if tx
                .try_send(IntrospectLogletWorker::GetState(reply_tx))
                .is_ok()
                && let Ok(state) = reply_rx.await
            {
                result.insert(loglet_id, state);
            }
        }
        result
    }
}
