// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use strum::EnumProperty;

#[derive(
    Clone,
    Debug,
    Copy,
    Hash,
    PartialEq,
    Eq,
    Ord,
    PartialOrd,
    derive_more::Display,
    derive_more::From,
    derive_more::Into,
)]
pub struct TaskId(u64);

/// Describes the types of tasks TaskCenter manages.
///
/// Properties can be assigned to task kinds:
///   * `OnCancel` - What to do when the task is cancelled:
///     - `ignore                 - Ignores the tokio task. The task will be dropped on tokio
///                                 runtime drop.
///     - `abort`                 - Aborts the tokio task (default)
///     - `wait`  (default)       - Wait for graceful shutdown. The task must respond
///                                  to cancellation_watcher() or check periodically for
///                                  is_cancellation_requested()
///
///   * `OnError`  - What to do if the task returned Err(_)
///     - `log`                   - Log an error
///     - `shutdown` (default)    - Shutdown the node (task center global shutdown)
#[derive(
    Clone,
    Copy,
    Debug,
    Eq,
    PartialEq,
    strum_macros::EnumProperty,
    strum_macros::IntoStaticStr,
    strum_macros::Display,
)]
pub enum TaskKind {
    #[cfg(any(test, feature = "test-util"))]
    TestRunner,
    /// Do not use. This is a special task kind that indicate that work is running within
    /// task_center but its lifecycle is not managed by it.
    InPlace,
    /// Tasks used during system initialization. Short lived but will shutdown the node if they
    /// failed.
    #[strum(props(OnCancel = "abort"))]
    SystemBoot,
    #[strum(props(OnCancel = "abort"))]
    MetadataBackgroundSync,
    RpcServer,
    RoleRunner,
    SystemService,
    Ingress,
    PartitionProcessor,
    // -- Bifrost Tasks
    /// A background task that the system needs for its operation. The task requires a system
    /// shutdown on errors and the system will wait for its graceful cancellation on shutdown.
    BifrostBackgroundHighPriority,
    #[strum(props(OnCancel = "abort", OnError = "log"))]
    Disposable,
}

impl TaskKind {
    pub fn should_shutdown_on_error(&self) -> bool {
        self.on_error() == "shutdown"
    }

    pub fn should_wait_on_cancel(&self) -> bool {
        self.on_cancel() == "wait"
    }

    pub fn should_abort_on_cancel(&self) -> bool {
        self.on_cancel() == "abort"
    }

    fn on_cancel(&self) -> &'static str {
        self.get_str("OnCancel").unwrap_or("wait")
    }

    fn on_error(&self) -> &'static str {
        self.get_str("OnError").unwrap_or("shutdown")
    }
}

pub enum FailureBehaviour {
    Shutdown,
}
