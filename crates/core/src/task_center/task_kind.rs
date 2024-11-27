// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::atomic::{AtomicU64, Ordering};

use strum::EnumProperty;

static NEXT_TASK_ID: AtomicU64 = const { AtomicU64::new(1) };

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

impl Default for TaskId {
    fn default() -> Self {
        Self(NEXT_TASK_ID.fetch_add(1, Ordering::Relaxed))
    }
}

impl TaskId {
    pub const ROOT: TaskId = TaskId(0);

    pub fn new() -> Self {
        Default::default()
    }
}

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
    strum::EnumProperty,
    strum::IntoStaticStr,
    derive_more::Display,
)]
pub enum TaskKind {
    #[cfg(any(test, feature = "test-util"))]
    TestRunner,
    /// Do not use. This is a special task kind that indicate that work is running within
    /// task_center but its lifecycle is not managed by it.
    InPlace,
    /// Tasks used during system initialization. Short-lived but will shut down the node if they
    /// failed.
    #[strum(props(OnCancel = "abort"))]
    SystemBoot,
    #[strum(props(OnCancel = "abort"))]
    MetadataBackgroundSync,
    RpcServer,
    #[strum(props(OnCancel = "abort", OnError = "log"))]
    RpcConnection,
    /// A task that handles a single RPC request. The task is executed on the default runtime to
    /// decouple it from the lifetime of the originating runtime. Use this task kind if you want to
    /// make sure that the rpc response is sent even if the originating runtime is dropped.
    #[strum(props(OnCancel = "abort", OnError = "log", runtime = "default"))]
    RpcResponse,
    /// A type for ingress until we start enforcing timeouts for inflight requests. This enables us
    /// to shut down cleanly without waiting indefinitely.
    #[strum(props(OnCancel = "abort", runtime = "ingress"))]
    IngressServer,
    RoleRunner,
    SystemService,
    #[strum(props(OnCancel = "abort", runtime = "ingress"))]
    Ingress,
    /// Kafka ingestion related task
    Kafka,
    PartitionProcessor,
    /// Longer-running, low-priority tasks that is responsible for the export, and potentially
    /// upload to remote storage, of partition store snapshots.
    #[strum(props(OnCancel = "abort", OnError = "log"))]
    PartitionSnapshotProducer,
    #[strum(props(OnError = "log"))]
    ConnectionReactor,
    Shuffle,
    Cleaner,
    MetadataStore,
    Background,
    // -- Bifrost Tasks
    /// A background task that the system needs for its operation. The task requires a system
    /// shutdown on errors and the system will wait for its graceful cancellation on shutdown.
    #[strum(props(runtime = "default"))]
    BifrostBackgroundHighPriority,
    #[strum(props(OnCancel = "abort", runtime = "default"))]
    BifrostBackgroundLowPriority,
    /// A background appender. The task will log on errors but the system will wait for its
    /// graceful cancellation on shutdown.
    #[strum(props(OnCancel = "wait", OnError = "log"))]
    BifrostAppender,
    #[strum(props(OnCancel = "abort", OnError = "log"))]
    Disposable,
    LogletProvider,
    #[strum(props(OnCancel = "abort"))]
    Watchdog,
    #[strum(props(OnCancel = "wait", runtime = "default"))]
    SequencerAppender,
    // -- Replicated loglet tasks
    /// Receives messages from remote sequencers on nodes with local sequencer. This is also used
    /// in remote sequencer to handle responses of rpc messages.
    #[strum(props(OnCancel = "abort", runtime = "default"))]
    NetworkMessageHandler,
    ReplicatedLogletReadStream,
    #[strum(props(OnCancel = "abort"))]
    // -- Log-server tasks
    LogletWriter,
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

    pub fn runtime(&self) -> AsyncRuntime {
        match self.get_str("runtime").unwrap_or("inherit") {
            "inherit" => AsyncRuntime::Inherit,
            "default" => AsyncRuntime::Default,
            "ingress" => AsyncRuntime::Ingress,
            _ => panic!("Invalid runtime for task kind: {self}"),
        }
    }
}

pub enum FailureBehaviour {
    Shutdown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, strum::IntoStaticStr, strum::Display)]
pub enum AsyncRuntime {
    /// Run on the same runtime at which the spawn took place.
    Inherit,
    /// Run on the default runtime
    Default,
    /// Run on ingress runtime
    Ingress,
}
