// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_core::network::{Oneshot, Reciprocal};
use restate_types::logs::{LogletId, LogletOffset, SequenceNumber, TailState};
use restate_types::net::log_server::{Status, Trimmed};

use super::{OnComplete, WriteStorageTask};

pub struct TrimStorageTask {
    loglet_id: LogletId,
    trim_point: LogletOffset,
    reply_to: Option<Reciprocal<Oneshot<Trimmed>>>,
}

impl TrimStorageTask {
    pub fn new(
        loglet_id: LogletId,
        trim_point: LogletOffset,
        reply_to: Reciprocal<Oneshot<Trimmed>>,
    ) -> Self {
        Self {
            loglet_id,
            trim_point,
            reply_to: Some(reply_to),
        }
    }

    pub fn trim_point(&self) -> LogletOffset {
        self.trim_point
    }
}

impl Drop for TrimStorageTask {
    fn drop(&mut self) {
        if let Some(reply_to) = self.reply_to.take() {
            reply_to.send(
                Trimmed::new(
                    TailState::new(false, LogletOffset::INVALID),
                    LogletOffset::INVALID,
                )
                .with_status(Status::Disabled),
            );
        }
    }
}

impl OnComplete for TrimStorageTask {
    fn on_complete(
        &mut self,
        local_tail: TailState<LogletOffset>,
        global_tail: LogletOffset,
        status: Status,
    ) {
        if let Some(reply_to) = self.reply_to.take() {
            reply_to.send(Trimmed::new(local_tail, global_tail).with_status(status));
        }
    }
}

impl WriteStorageTask for TrimStorageTask {
    fn loglet_id(&self) -> LogletId {
        self.loglet_id
    }
}
