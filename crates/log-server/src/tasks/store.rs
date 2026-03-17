// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use tokio::time::Instant;
use tracing::trace;

use restate_core::network::{Oneshot, Reciprocal};
use restate_time_util::DurationExt;
use restate_types::logs::{LogletId, LogletOffset, SequenceNumber as _, TailState};
use restate_types::net::log_server::{Status, Stored};

use super::{OnComplete, WriteStorageTask};

pub struct StoreStorageTask {
    loglet_id: LogletId,
    created_at: Instant,
    processing_started_at: Option<Instant>,
    reply_to: Option<Reciprocal<Oneshot<Stored>>>,
}

impl StoreStorageTask {
    pub fn new(loglet_id: LogletId, reply_to: Reciprocal<Oneshot<Stored>>) -> Self {
        Self {
            loglet_id,
            created_at: Instant::now(),
            processing_started_at: None,
            reply_to: Some(reply_to),
        }
    }
}

impl Drop for StoreStorageTask {
    fn drop(&mut self) {
        if let Some(reply_to) = self.reply_to.take() {
            reply_to.send(Stored::new(
                TailState::new(false, LogletOffset::INVALID),
                LogletOffset::INVALID,
            ));
        }
    }
}

impl OnComplete for StoreStorageTask {
    fn on_complete(
        &mut self,
        local_tail: TailState<LogletOffset>,
        global_tail: LogletOffset,
        status: Status,
    ) {
        if let Some(reply_to) = self.reply_to.take() {
            trace!(
                loglet_id = %self.loglet_id,
                "Store request completed [{status}]. local_tail: {local_tail}, global_tail: {global_tail}, total={}, enqueue_duration: {}, commit_duration: {}.",
                self.created_at.elapsed().friendly(),
                self.processing_started_at.unwrap_or_else(Instant::now).duration_since(self.created_at).friendly(),
                Instant::now().duration_since(self.processing_started_at.unwrap_or_else(Instant::now)).friendly(),

            );
            reply_to.send(Stored::new(local_tail, global_tail).with_status(status));
        }
    }
}

impl WriteStorageTask for StoreStorageTask {
    fn on_start(&mut self) {
        self.processing_started_at = Some(Instant::now());
    }
}
