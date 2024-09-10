// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_core::ShutdownError;
use tokio::sync::watch;
use tokio_stream::wrappers::WatchStream;

use restate_types::logs::TailState;

use super::LogletOffset;

#[derive(Debug, Clone)]
pub struct TailOffsetWatch {
    sender: watch::Sender<TailState<LogletOffset>>,
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

    pub async fn wait_for_seal(&self) -> Result<(), ShutdownError> {
        let mut receiver = self.sender.subscribe();
        receiver.mark_changed();
        receiver
            .wait_for(|tail| tail.is_sealed())
            .await
            .map_err(|_| ShutdownError)?;
        Ok(())
    }

    /// The first yielded value is the latest known tail
    pub fn to_stream(&self) -> WatchStream<TailState<LogletOffset>> {
        let mut receiver = self.sender.subscribe();
        receiver.mark_changed();
        WatchStream::new(receiver)
    }
}
