// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::pin::Pin;
use std::task::{ready, Poll};

use futures::FutureExt;
use strum::EnumProperty;
use tokio::runtime::RuntimeMetrics;
use tokio_util::sync::CancellationToken;

use crate::ShutdownError;

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
    /// Tasks used during system initialization. Short lived but will shutdown the node if they
    /// failed.
    #[strum(props(OnCancel = "abort"))]
    SystemBoot,
    #[strum(props(OnCancel = "abort"))]
    MetadataBackgroundSync,
    RpcServer,
    #[strum(props(OnCancel = "abort", OnError = "log"))]
    RpcConnection,
    /// A type for ingress until we start enforcing timeouts for inflight requests. This enables us
    /// to shutdown cleanly without waiting indefinitely.
    #[strum(props(OnCancel = "abort", runtime = "ingress"))]
    IngressServer,
    RoleRunner,
    SystemService,
    #[strum(props(OnCancel = "abort", runtime = "ingress"))]
    Ingress,
    PartitionProcessor,
    #[strum(props(OnError = "log"))]
    ConnectionReactor,
    Shuffle,
    Cleaner,
    MetadataStore,
    // -- Bifrost Tasks
    /// A background task that the system needs for its operation. The task requires a system
    /// shutdown on errors and the system will wait for its graceful cancellation on shutdown.
    BifrostBackgroundHighPriority,
    /// A background appender. The task will log on errors but the system will wait for its
    /// graceful cancellation on shutdown.
    #[strum(props(OnCancel = "wait", OnError = "log"))]
    BifrostAppender,
    #[strum(props(OnCancel = "abort", OnError = "log"))]
    Disposable,
    LogletProvider,
    #[strum(props(OnCancel = "abort"))]
    Watchdog,
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
            _ => panic!("Invalid runtime for task kind: {}", self),
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

/// A future that represents a task spawned on the TaskCenter.
///
/// Awaiting this future waits for the task to complete.
pub struct TaskHandle<T> {
    pub(crate) cancellation_token: CancellationToken,
    pub(crate) inner_handle: tokio::task::JoinHandle<T>,
}

impl<T> TaskHandle<T> {
    /// Abort the task immediately. This will abort the task at the next yielding point. If the
    /// task is running a blocking call, it'll not be aborted until it can yield to the runtime.
    pub fn abort(&self) {
        self.inner_handle.abort();
    }

    /// Trigger graceful cancellation of the task
    pub fn cancel(&self) {
        self.cancellation_token.cancel()
    }

    /// Returns true if cancellation was requested. Note that this doesn't mean that
    /// the task has finished. To check if the task has finished or not, use `is_finished()`
    pub fn is_cancellation_requested(&self) -> bool {
        self.cancellation_token.is_cancelled()
    }

    /// Returns true if the task has finished executing. Note that this might return
    /// `false` after calling `abort()` since termination process takes time.
    pub fn is_finished(&self) -> bool {
        self.inner_handle.is_finished()
    }
}

impl<T> std::future::Future for TaskHandle<T> {
    type Output = Result<T, ShutdownError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        match ready!(self.inner_handle.poll_unpin(cx)) {
            Ok(v) => Poll::Ready(Ok(v)),
            Err(_) => Poll::Ready(Err(ShutdownError)),
        }
    }
}

/// A handle for a dedicated runtime managed by task-center
pub struct RuntimeHandle {
    pub(crate) cancellation_token: CancellationToken,
    pub(crate) inner: tokio::runtime::Runtime,
}

impl RuntimeHandle {
    /// Trigger graceful shutdown of the runtime. This will trigger the cancellation token of the
    /// root localset running on this runtime. Shutdown is not guaranteed, it depends on whether
    /// the root localset awaits the cancellation token or not.
    pub fn cancel(&self) {
        self.cancellation_token.cancel()
    }

    pub fn metrics(&self) -> RuntimeMetrics {
        self.inner.metrics()
    }

    /// Returns true if cancellation was requested. Note that this doesn't mean that
    /// the runtime has been terminated.
    pub fn is_cancellation_requested(&self) -> bool {
        self.cancellation_token.is_cancelled()
    }
}
