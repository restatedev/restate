// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
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

use restate_core::ShutdownError;

use crate::TailState;

use super::LogletOffset;

#[derive(Debug)]
pub struct TailOffsetWatch {
    sender: watch::Sender<TailState<LogletOffset>>,
    receiver: watch::Receiver<TailState<LogletOffset>>,
}

impl TailOffsetWatch {
    pub fn new(tail: TailState<LogletOffset>) -> Self {
        let (sender, receiver) = watch::channel(tail);
        Self { sender, receiver }
    }

    /// Inform the watch that the tail might have changed.
    pub fn notify(&self, sealed: bool, offset: LogletOffset) {
        self.sender.send_if_modified(|v| v.combine(sealed, offset));
    }

    pub fn notify_seal(&self) {
        self.sender.send_if_modified(|v| v.seal());
    }

    /// Blocks until the tail is beyond the given offset.
    pub async fn wait_for(&self, offset: LogletOffset) -> Result<(), ShutdownError> {
        self.receiver
            .clone()
            .wait_for(|v| v.offset() > offset)
            .await
            .map_err(|_| ShutdownError)?;
        Ok(())
    }

    pub fn to_stream(&self) -> WatchStream<TailState<LogletOffset>> {
        WatchStream::new(self.receiver.clone())
    }
}
