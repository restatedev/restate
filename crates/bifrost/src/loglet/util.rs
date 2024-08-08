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

use super::LogletOffset;
use crate::TailState;

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

    pub fn to_stream(&self) -> WatchStream<TailState<LogletOffset>> {
        WatchStream::new(self.receiver.clone())
    }
}
