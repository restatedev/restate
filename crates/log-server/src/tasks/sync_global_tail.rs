// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_types::logs::{LogletId, LogletOffset, TailState};
use restate_types::net::log_server::Status;

use super::{OnComplete, WriteStorageTask};

pub struct SyncGlobalTailStorageTask {
    loglet_id: LogletId,
    global_tail: LogletOffset,
}

impl SyncGlobalTailStorageTask {
    pub fn new(loglet_id: LogletId, global_tail: LogletOffset) -> Self {
        Self {
            loglet_id,
            global_tail,
        }
    }

    pub fn global_tail(&self) -> LogletOffset {
        self.global_tail
    }
}

impl OnComplete for SyncGlobalTailStorageTask {
    fn on_complete(
        &mut self,
        _local_tail: TailState<LogletOffset>,
        _global_tail: LogletOffset,
        _status: Status,
    ) {
    }
}

impl WriteStorageTask for SyncGlobalTailStorageTask {
    fn loglet_id(&self) -> LogletId {
        self.loglet_id
    }
}
