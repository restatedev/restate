// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
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

use chrono::Utc;
use dashmap::DashMap;
use tokio::sync::watch;
use xxhash_rust::xxh3::Xxh3Builder;

use restate_bifrost::loglet::util::TailOffsetWatch;
use restate_bifrost::loglet::OperationError;
use restate_types::logs::{LogletOffset, SequenceNumber, TailState};
use restate_types::replicated_loglet::ReplicatedLogletId;
use restate_types::{GenerationalNodeId, PlainNodeId};

use crate::logstore::LogStore;

/// A marker stored in log-server-level loglet storage
///
/// The marker is used to sanity-check if the loglet storage is correctly initialized and whether
/// we lost the database or not.
///
/// The marker is stored as Json to help with debugging.
#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct LogStoreMarker {
    my_node_id: PlainNodeId,
    created_at: chrono::DateTime<Utc>,
}

impl LogStoreMarker {
    pub fn new(my_node_id: PlainNodeId) -> Self {
        Self {
            my_node_id,
            created_at: Utc::now(),
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("infallible serde")
    }

    pub fn from_slice(data: impl AsRef<[u8]>) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(data.as_ref())
    }

    pub fn node_id(&self) -> PlainNodeId {
        self.my_node_id
    }

    pub fn created_at(&self) -> chrono::DateTime<Utc> {
        self.created_at
    }
}

/// Caches loglet state in memory
#[derive(Default)]
pub struct LogletStateMap {
    inner: HashMap<ReplicatedLogletId, LogletState, Xxh3Builder>,
}

impl LogletStateMap {
    // todo (optimization to preload from log_store)
    pub async fn load_all<S: LogStore>(_log_store: &S) -> Result<Self, OperationError> {
        Ok(Self::default())
    }

    pub async fn get_or_load<S: LogStore>(
        &mut self,
        loglet_id: ReplicatedLogletId,
        log_store: &S,
    ) -> Result<LogletState, OperationError> {
        if let Some(state) = self.inner.get(&loglet_id) {
            return Ok(state.clone());
        }

        let state = log_store.load_loglet_state(loglet_id).await?;
        self.inner.insert(loglet_id, state);
        // just inserted it, safe to unwrap.
        Ok(self.inner.get(&loglet_id).cloned().unwrap())
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
}

impl LogletState {
    pub fn new(
        sequencer_node: Option<GenerationalNodeId>,
        local_tail: LogletOffset,
        sealed: bool,
        trim_point: LogletOffset,
    ) -> Self {
        let local_tail = TailOffsetWatch::new(TailState::new(sealed, local_tail));
        let trim_point = watch::Sender::new(trim_point);
        let sequencer = Arc::new(OnceLock::new());
        if let Some(sequencer_node) = sequencer_node {
            let _ = sequencer.set(sequencer_node);
        }
        Self {
            sequencer,
            local_tail,
            trim_point,
        }
    }

    pub fn sequencer(&self) -> Option<&GenerationalNodeId> {
        self.sequencer.get()
    }

    /// Returns true if successful
    pub fn set_sequencer(&mut self, sequencer: GenerationalNodeId) -> bool {
        self.sequencer.set(sequencer).is_ok()
    }

    pub fn get_tail_watch(&self) -> TailOffsetWatch {
        self.local_tail.clone()
    }

    pub fn is_sealed(&self) -> bool {
        self.local_tail.is_sealed()
    }

    pub fn local_tail(&self) -> watch::Ref<'_, TailState<LogletOffset>> {
        self.local_tail.get()
    }

    #[allow(unused)]
    pub fn trim_point(&self) -> LogletOffset {
        *self.trim_point.borrow()
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
    pub fn subscribe(&self) -> watch::Receiver<LogletOffset> {
        let mut receiver = self.watch_tx.subscribe();
        receiver.mark_changed();
        receiver
    }

    #[allow(unused)]
    pub fn known_global_tail(&self) -> LogletOffset {
        *self.watch_tx.borrow()
    }

    pub fn maybe_update(&self, potential_global_tail: LogletOffset) {
        self.watch_tx.send_if_modified(|known_global_tail| {
            if potential_global_tail > *known_global_tail {
                *known_global_tail = potential_global_tail;
                return true;
            }
            false
        });
    }
}

/// Tracks known global tail for all loglets
#[derive(Default, Clone)]
pub struct GlobalTailTrackerMap {
    inner: Arc<DashMap<ReplicatedLogletId, GlobalTailTracker, Xxh3Builder>>,
}

impl GlobalTailTrackerMap {
    #[allow(unused)]
    pub fn known_global_tail(&self, loglet_id: ReplicatedLogletId) -> LogletOffset {
        self.inner
            .entry(loglet_id)
            .or_default()
            .value()
            .known_global_tail()
    }

    pub fn get_tracker(&self, loglet_id: ReplicatedLogletId) -> GlobalTailTracker {
        self.inner.entry(loglet_id).or_default().value().clone()
    }
}
