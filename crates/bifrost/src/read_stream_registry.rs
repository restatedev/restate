// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Registry for active bifrost read streams, providing introspection support.
//!
//! Each [`LogReadStream`] registers itself on creation and deregisters on drop.
//! The stream periodically updates its shared [`ReadStreamState`] which can be
//! read by introspection scanners without interrupting the stream.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use ahash::HashMap;
use parking_lot::Mutex;

use restate_types::logs::LogletId;
use restate_types::logs::metadata::SegmentIndex;
use restate_types::logs::{LogId, Lsn};

/// Compact representation of a read stream's current state, updated by the
/// stream itself on each state transition.
#[derive(Debug, Clone)]
pub struct ReadStreamState {
    /// The log being read.
    pub log_id: LogId,
    /// Next LSN to be read.
    pub read_pointer: Lsn,
    /// Inclusive max LSN the stream will read to (`Lsn::MAX` for tailing).
    pub end_lsn: Lsn,
    /// Human-readable name of the current state machine state.
    pub state: &'static str,
    /// The safe known tail (only available when in the `Reading` state).
    pub safe_known_tail: Option<Lsn>,
    /// Index of the segment currently being read from.
    pub current_segment: Option<SegmentIndex>,
    /// The loglet ID of the current segment
    pub loglet_id: Option<LogletId>,
}

impl ReadStreamState {
    pub(crate) fn new(log_id: LogId, read_pointer: Lsn, end_lsn: Lsn) -> Self {
        Self {
            log_id,
            read_pointer,
            end_lsn,
            state: "New",
            safe_known_tail: None,
            current_segment: None,
            loglet_id: None,
        }
    }
}

/// Shared mutable reference to a stream's state.
pub type SharedReadStreamState = Arc<Mutex<ReadStreamState>>;

/// A unique identifier for a read stream within this node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, derive_more::Display)]
#[display("{}", _0)]
pub struct ReadStreamId(u64);

/// Registry of all active read streams on this node.
///
/// The registry is stored in [`BifrostInner`] and is shared (via `Clone`)
/// across all `Bifrost` handles. It is cheap to clone.
#[derive(Clone, Debug, Default)]
pub struct ActiveReadStreamRegistry {
    inner: Arc<RegistryInner>,
}

#[derive(Debug, Default)]
struct RegistryInner {
    next_id: AtomicU64,
    streams: Mutex<HashMap<ReadStreamId, SharedReadStreamState>>,
}

impl ActiveReadStreamRegistry {
    /// Registers a new read stream and returns its unique ID and the shared
    /// state handle.
    pub(crate) fn register(&self, state: ReadStreamState) -> (ReadStreamId, SharedReadStreamState) {
        let id = ReadStreamId(self.inner.next_id.fetch_add(1, Ordering::Relaxed));
        let shared = Arc::new(Mutex::new(state));
        self.inner.streams.lock().insert(id, shared.clone());
        (id, shared)
    }

    /// Removes a read stream from the registry.
    pub(crate) fn unregister(&self, id: ReadStreamId) {
        self.inner.streams.lock().remove(&id);
    }

    /// Returns a snapshot of all active read stream states.
    pub fn snapshot(&self) -> Vec<ReadStreamState> {
        let guard = self.inner.streams.lock();
        guard.values().map(|shared| shared.lock().clone()).collect()
    }
}
