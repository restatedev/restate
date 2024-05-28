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

use restate_core::ShutdownError;

use crate::loglet::LogletOffset;

#[derive(Debug)]
pub struct OffsetWatch {
    sender: watch::Sender<LogletOffset>,
    receive: watch::Receiver<LogletOffset>,
}

impl OffsetWatch {
    pub fn new(offset: LogletOffset) -> Self {
        let (send, receive) = watch::channel(offset);
        Self {
            sender: send,
            receive,
        }
    }

    /// Inform the watch that the offset has changed.
    pub fn notify(&self, offset: LogletOffset) {
        self.sender.send_if_modified(|v| {
            if offset > *v {
                *v = offset;
                true
            } else {
                false
            }
        });
    }

    /// Blocks until the offset is greater or equal to the given offset.
    pub async fn wait_for(&self, offset: LogletOffset) -> Result<(), ShutdownError> {
        self.receive
            .clone()
            .wait_for(|v| *v >= offset)
            .await
            .map_err(|_| ShutdownError)?;
        Ok(())
    }

    pub fn receiver(&self) -> watch::Receiver<LogletOffset> {
        self.receive.clone()
    }
}
