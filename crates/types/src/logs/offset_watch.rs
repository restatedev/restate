// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use tokio::sync::watch;
use tokio_stream::wrappers::WatchStream;

use super::LogletOffset;
use super::TailState;

#[derive(Debug, thiserror::Error)]
#[error("tail watch terminated")]
pub struct WatchTerminated;

#[derive(Clone)]
pub struct TailOffsetWatch {
    sender: watch::Sender<TailState<LogletOffset>>,
}

// Passthrough Debug to TailState
impl std::fmt::Debug for TailOffsetWatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&*self.sender.borrow(), f)
    }
}

// Passthrough Display to TailState
impl std::fmt::Display for TailOffsetWatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&*self.sender.borrow(), f)
    }
}

impl TailOffsetWatch {
    pub fn new(tail: TailState<LogletOffset>) -> Self {
        let sender = watch::Sender::new(tail);
        Self { sender }
    }

    /// Inform the watch that the tail might have changed.
    pub fn notify(&self, sealed: bool, offset: LogletOffset) {
        self.sender.send_if_modified(|v| v.combine(sealed, offset));
    }

    /// Update that the offset might have changed without updating the seal
    pub fn notify_offset_update(&self, offset: LogletOffset) {
        self.sender.send_if_modified(|v| v.combine(false, offset));
    }

    pub fn notify_seal(&self) {
        self.sender.send_if_modified(|v| v.seal());
    }

    pub fn latest_offset(&self) -> LogletOffset {
        self.sender.borrow().offset()
    }

    pub fn get(&self) -> watch::Ref<'_, TailState<LogletOffset>> {
        self.sender.borrow()
    }

    pub fn is_sealed(&self) -> bool {
        self.sender.borrow().is_sealed()
    }

    pub async fn wait_for_seal(&self) -> Result<(), WatchTerminated> {
        let mut receiver = self.sender.subscribe();
        receiver.mark_changed();
        receiver
            .wait_for(|tail| tail.is_sealed())
            .await
            .map_err(|_| WatchTerminated)?;
        Ok(())
    }

    pub async fn wait_for_offset(
        &self,
        offset: LogletOffset,
    ) -> Result<TailState<LogletOffset>, WatchTerminated> {
        let mut receiver = self.sender.subscribe();
        receiver.mark_changed();
        receiver
            .wait_for(|tail_state| match tail_state {
                TailState::Sealed(tail) | TailState::Open(tail) => *tail >= offset,
            })
            .await
            .map(|m| *m)
            .map_err(|_| WatchTerminated)
    }

    pub async fn wait_for_offset_or_seal(
        &self,
        offset: LogletOffset,
    ) -> Result<TailState<LogletOffset>, WatchTerminated> {
        let mut receiver = self.sender.subscribe();
        receiver.mark_changed();
        receiver
            .wait_for(|tail_state| match tail_state {
                TailState::Sealed(_) => true,
                TailState::Open(tail) if *tail >= offset => true,
                _ => false,
            })
            .await
            .map(|m| *m)
            .map_err(|_| WatchTerminated)
    }

    pub fn subscribe(&self) -> watch::Receiver<TailState<LogletOffset>> {
        let mut receiver = self.sender.subscribe();
        receiver.mark_changed();
        receiver
    }

    /// The first yielded value is the latest known tail
    pub fn to_stream(&self) -> WatchStream<TailState<LogletOffset>> {
        let mut receiver = self.sender.subscribe();
        receiver.mark_changed();
        WatchStream::new(receiver)
    }
}
