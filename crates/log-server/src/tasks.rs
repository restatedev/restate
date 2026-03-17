// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod seal;
mod store;
mod sync_global_tail;
mod trim;

pub use seal::SealStorageTask;
pub use store::StoreStorageTask;
pub use sync_global_tail::SyncGlobalTailStorageTask;
pub use trim::TrimStorageTask;

use restate_types::logs::{LogletId, LogletOffset, TailState};
use restate_types::net::log_server::Status;

pub trait OnComplete: Send {
    /// Called by the writer when the task is completed and can be disposed.
    fn on_complete(
        &mut self,
        local_tail: TailState<LogletOffset>,
        global_tail: LogletOffset,
        status: Status,
    );
}

/// Storage tasks should only run their notification logic on (drop)
pub trait WriteStorageTask: OnComplete {
    fn loglet_id(&self) -> LogletId;
    /// Called by the writer when it starts processing the task.
    fn on_start(&mut self) {}
}
