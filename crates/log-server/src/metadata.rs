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
use std::sync::{Arc, OnceLock};

use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::watch;

use crate::logstore::LogStore;
use restate_bifrost::loglet::OperationError;
use restate_core::ShutdownError;
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

    pub fn get_local_tail_watch(&self) -> TailOffsetWatch {
        self.local_tail.clone()
    }

    pub fn get_global_tail_tracker(&self) -> GlobalTailTracker {
        self.known_global_tail.clone()
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

    pub fn notify(&self, potential_global_tail: LogletOffset) {
        self.watch_tx.send_if_modified(|known_global_tail| {
            if potential_global_tail > *known_global_tail {
                *known_global_tail = potential_global_tail;
                return true;
            }
            false
        });
    }

    pub async fn wait_for_offset(
        &self,
        offset: LogletOffset,
    ) -> Result<LogletOffset, ShutdownError> {
        let mut receiver = self.watch_tx.subscribe();
        receiver.mark_changed();
        receiver
            .wait_for(|current| *current >= offset)
            .await
            .map(|m| *m)
            .map_err(|_| ShutdownError)
    }
}
