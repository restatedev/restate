// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use tokio::sync::mpsc::{self, error::TrySendError};

use crate::network::{BidiStreamError, StreamEnvelope};

type StreamTag = u64;
type DashMap<K, V> = dashmap::DashMap<K, V, ahash::RandomState>;
type StreamSender = mpsc::Sender<StreamEnvelope>;

/// A tracker for responses but can be used to track responses for requests that were dispatched
/// via other mechanisms (e.g. ingress flow)
#[derive(Default)]
pub struct StreamTracker {
    streams: DashMap<StreamTag, StreamSender>,
}

impl StreamTracker {
    pub fn register_stream(&self, id: StreamTag, sender: StreamSender) {
        self.streams.insert(id, sender);
    }

    pub fn has_stream(&self, id: StreamTag) -> bool {
        self.streams.contains_key(&id)
    }

    pub fn pop_stream_sender(&self, id: &StreamTag) -> Option<StreamSender> {
        self.streams.remove(id).map(|(_, v)| v)
    }

    /// Routes the envelope to the stream’s local receiver.
    ///
    /// Returns `StreamNotFound` if the stream ID is unknown. If the receiver is
    /// closed, it yields `StreamDropped` and removes the tracker. When the stream is
    /// at capacity, the message is dropped and `LoadShedding` is returned.
    pub fn forward_envelope(
        &self,
        id: &StreamTag,
        envelope: StreamEnvelope,
    ) -> Result<(), BidiStreamError> {
        let Some(sender) = self.streams.get(id) else {
            return Err(BidiStreamError::StreamNotFound);
        };

        let result = sender.try_send(envelope).map_err(|err| match err {
            TrySendError::Closed(_) => BidiStreamError::StreamDropped,
            TrySendError::Full(_) => BidiStreamError::LoadShedding,
        });

        // avoid holding a reference into the dashmap
        // before dropping the tracker
        drop(sender);

        if let Err(BidiStreamError::StreamDropped) = &result {
            self.streams.remove(id);
        }

        result
    }
}
