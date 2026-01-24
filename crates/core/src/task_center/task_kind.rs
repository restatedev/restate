// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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
///     - `ignore                 - Ignores the tokio task. The task will be dropped on tokio runtime drop.
///     - `abort`                 - Aborts the tokio task
///     - `wait`  (default)       - Wait for graceful shutdown. The task must respond to cancellation_watcher() or check periodically for is_cancellation_requested()
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
    NodeRpcServer,
    AdminApiServer,
    LogServerRole,
    #[strum(props(OnError = "log", runtime = "default"))]
    SocketHandler,
    /// An http2 stream handler created by the server-side of the connection.
    #[strum(props(OnError = "log", runtime = "default"))]
    H2ServerStream,
    /// An http2 stream handler created by the client-side of the connection.
    #[strum(props(OnError = "log", runtime = "default"))]
    H2ClientStream,
    /// A type for ingress until we start enforcing timeouts for inflight requests. This enables us
    /// to shut down cleanly without waiting indefinitely.
    #[strum(props(OnCancel = "abort"))]
    HttpIngressRole,
    WorkerRole,
    RoleRunner,
    /// Cluster controller is the first thing that gets stopped when the server is shut down
    ClusterController,
    #[strum(props(runtime = "default"))]
    FailureDetector,
    SystemService,
    #[strum(props(OnCancel = "abort"))]
    Ingress,
    /// Kafka ingestion related task
    Kafka,
    PartitionProcessor,
    #[strum(props(runtime = "default"))]
    PartitionProcessorManager,
    /// Low-priority tasks responsible for partition snapshot-related I/O.
    #[strum(props(OnCancel = "abort", OnError = "log"))]
    PartitionSnapshotProducer,
    #[strum(props(OnError = "log", runtime = "default"))]
    ConnectionReactor,
    /// Used only in tests
    #[strum(props(OnError = "log", runtime = "default"))]
    RemoteConnectionReactor,
    /// connection reactor for loopback connections
    #[strum(props(OnError = "log", runtime = "default"))]
    LocalReactor,
    Shuffle,
    Cleaner,
    LogTrimmer,
    MetadataServer,
    Background,
    // -- Bifrost Tasks
    #[strum(props(runtime = "default"))]
    BifrostWatchdog,
    /// A background task that the system needs for its operation. The task requires a system
    /// shutdown on errors and the system will wait for its graceful cancellation on shutdown.
    #[strum(props(runtime = "default"))]
    BifrostBackgroundHighPriority,
    #[strum(props(OnCancel = "abort", runtime = "default", OnError = "log"))]
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
    /// Append records to the replicated loglet by storing them on a write-quorum of log servers. It
    /// is important that these tasks complete if the sequencer does not fail because they update
    /// the sequencer state (local and global tails of the loglet). If this does not happen (e.g.
    /// because the task is running on the partition processor runtime which gets stopped), then
    /// subsequent appends might be blocked until the sequencer learns about the current local tails
    /// via the periodic tail check.
    #[strum(props(runtime = "default"))]
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
    // - Datafusion
    DfScanner,
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
}
