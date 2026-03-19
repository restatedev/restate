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
use restate_types::logs::{LogletId, LogletOffset, TailOffsetWatch, TailState};
use restate_types::net::log_server::{Sealed, Status};

use super::{OnComplete, WriteStorageTask};

pub struct SealStorageTask {
    loglet_id: LogletId,
    local_tail_watch: TailOffsetWatch,
    created_at: Instant,
    processing_started_at: Option<Instant>,
    reply_to: Option<Reciprocal<Oneshot<Sealed>>>,
}

impl SealStorageTask {
    pub fn new(
        loglet_id: LogletId,
        local_tail_watch: TailOffsetWatch,
        reply_to: Reciprocal<Oneshot<Sealed>>,
    ) -> Self {
        Self {
            loglet_id,
            created_at: Instant::now(),
            processing_started_at: None,
            local_tail_watch,
            reply_to: Some(reply_to),
        }
    }
}

impl OnComplete for SealStorageTask {
    fn on_complete(
        &mut self,
        local_tail: TailState<LogletOffset>,
        global_tail: LogletOffset,
        status: Status,
    ) {
        if let Some(reply_to) = self.reply_to.take() {
            if matches!(status, Status::Ok) {
                self.local_tail_watch.notify_seal();
                trace!(
                    loglet_id = %self.loglet_id,
                    "Seal request completed [{status}]. local_tail: {local_tail}, global_tail: {global_tail}, total={}, enqueue_duration: {}, commit_duration: {}.",
                    self.created_at.elapsed().friendly(),
                    self.processing_started_at.unwrap_or_else(Instant::now).duration_since(self.created_at).friendly(),
                    Instant::now().duration_since(self.processing_started_at.unwrap_or_else(Instant::now)).friendly(),

                );
            }
            reply_to.send(Sealed::new(local_tail, global_tail).with_status(status));
        }
    }
}

impl WriteStorageTask for SealStorageTask {
    fn loglet_id(&self) -> LogletId {
        self.loglet_id
    }

    fn on_start(&mut self) {
        self.processing_started_at = Some(Instant::now());
    }
}
