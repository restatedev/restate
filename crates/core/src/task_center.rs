// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use futures::{Future, FutureExt};
use metrics::counter;
use restate_types::config::CommonOptions;
use tokio::task::JoinHandle;
use tokio::task_local;
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};
use tracing::{debug, error, info, instrument, trace, warn};

use restate_types::identifiers::PartitionId;

use crate::metric_definitions::{TC_FINISHED, TC_SPAWN, TC_STATUS_COMPLETED, TC_STATUS_FAILED};
use crate::{metric_definitions, Metadata, TaskId, TaskKind};

static NEXT_TASK_ID: AtomicU64 = AtomicU64::new(0);
const EXIT_CODE_FAILURE: i32 = 1;

#[derive(Debug, Clone, Copy, thiserror::Error)]
#[error("system is shutting down")]
pub struct ShutdownError;

#[derive(Debug, thiserror::Error)]
pub enum TaskCenterBuildError {
    #[error(transparent)]
    Tokio(#[from] tokio::io::Error),
}

/// Used to create a new task center. In practice, there should be a single task center for the
/// entire process but we might need to create more than one in integration test scenarios.
#[derive(Default)]
pub struct TaskCenterBuilder {
    default_runtime_handle: Option<tokio::runtime::Handle>,
    default_runtime: Option<tokio::runtime::Runtime>,
    options: Option<CommonOptions>,
    #[cfg(any(test, feature = "test-util"))]
    pause_time: bool,
}

impl TaskCenterBuilder {
    pub fn default_runtime_handle(mut self, handle: tokio::runtime::Handle) -> Self {
        self.default_runtime_handle = Some(handle);
        self.default_runtime = None;
        self
    }

    pub fn options(mut self, options: CommonOptions) -> Self {
        self.options = Some(options);
        self
    }

    pub fn default_runtime(mut self, runtime: tokio::runtime::Runtime) -> Self {
        self.default_runtime_handle = Some(runtime.handle().clone());
        self.default_runtime = Some(runtime);
        self
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn pause_time(mut self, pause_time: bool) -> Self {
        self.pause_time = pause_time;
        self
    }

    pub fn build(mut self) -> Result<TaskCenter, TaskCenterBuildError> {
        let options = self.options.unwrap_or_default();
        if self.default_runtime_handle.is_none() {
            let mut default_runtime_builder = tokio_builder(&options);
            #[cfg(any(test, feature = "test-util"))]
            if self.pause_time {
                default_runtime_builder.start_paused(self.pause_time);
            }
            let default_runtime = default_runtime_builder.build()?;
            self.default_runtime_handle = Some(default_runtime.handle().clone());
            self.default_runtime = Some(default_runtime);
        }

        metric_definitions::describe_metrics();
        Ok(TaskCenter {
            inner: Arc::new(TaskCenterInner {
                default_runtime_handle: self.default_runtime_handle.unwrap(),
                default_runtime: self.default_runtime,
                global_cancel_token: CancellationToken::new(),
                shutdown_requested: AtomicBool::new(false),
                current_exit_code: AtomicI32::new(0),
                tasks: Mutex::new(HashMap::new()),
                global_metadata: OnceLock::new(),
            }),
        })
    }
}

fn tokio_builder(common_opts: &CommonOptions) -> tokio::runtime::Builder {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all().thread_name_fn(|| {
        static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
        let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
        format!("rs:worker-{}", id)
    });

    if let Some(worker_threads) = common_opts.default_thread_pool_size {
        builder.worker_threads(worker_threads);
    }

    builder
}

/// Task center is used to manage long-running and background tasks and their lifecycle.
#[derive(Clone)]
pub struct TaskCenter {
    inner: Arc<TaskCenterInner>,
}

static_assertions::assert_impl_all!(TaskCenter: Send, Sync, Clone);

impl TaskCenter {
    /// Use to monitor an on-going shutdown when requested
    pub fn watch_shutdown(&self) -> WaitForCancellationFutureOwned {
        self.inner.global_cancel_token.clone().cancelled_owned()
    }

    /// Use to monitor an on-going shutdown when requested
    pub fn shutdown_token(&self) -> CancellationToken {
        self.inner.global_cancel_token.clone()
    }

    /// The exit code that the process should exit with.
    pub fn exit_code(&self) -> i32 {
        self.inner.current_exit_code.load(Ordering::Acquire)
    }

    /// Triggers a shutdown of the system. All running tasks will be asked gracefully
    /// to cancel but we will only wait for tasks with a TaskKind that has the property
    /// "OnCancel" set to "wait".
    #[instrument(level = "error", skip(self, exit_code))]
    pub async fn shutdown_node(&self, reason: &str, exit_code: i32) {
        let inner = self.inner.clone();
        if inner
            .shutdown_requested
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .unwrap_or_else(|e| e)
        {
            // already shutting down....
            return;
        }
        let start = Instant::now();
        inner.current_exit_code.store(exit_code, Ordering::Release);

        if exit_code != 0 {
            warn!("** Shutdown requested");
        } else {
            info!("** Shutdown requested");
        }
        self.cancel_tasks(None, None).await;
        // notify outer components that we have completed the shutdown.
        self.inner.global_cancel_token.cancel();
        info!("** Shutdown completed in {:?}", start.elapsed());
    }

    /// Attempt to set the global metadata handle. This should be called once
    /// at the startup of the node.
    pub fn try_set_global_metadata(&self, metadata: Metadata) -> bool {
        self.inner.global_metadata.set(metadata).is_ok()
    }

    #[track_caller]
    fn spawn_inner<F>(
        &self,
        kind: TaskKind,
        name: &'static str,
        partition_id: Option<PartitionId>,
        cancel: CancellationToken,
        future: F,
    ) -> TaskId
    where
        F: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        let inner = self.inner.clone();
        let id = TaskId::from(NEXT_TASK_ID.fetch_add(1, Ordering::SeqCst));
        let task = Arc::new(Task {
            id,
            name,
            kind,
            partition_id,
            cancel: cancel.clone(),
            join_handle: Mutex::new(None),
        });

        inner.tasks.lock().unwrap().insert(id, Arc::clone(&task));
        // Clone the currently set METADATA (and is Some()), otherwise fallback to global metadata.
        let metadata = METADATA
            .try_with(|m| m.clone())
            .ok()
            .flatten()
            .or_else(|| inner.global_metadata.get().cloned());

        let mut handle_mut = task.join_handle.lock().unwrap();

        let task_cloned = Arc::clone(&task);
        let join_handle = inner.default_runtime_handle.spawn(wrapper(
            self.clone(),
            id,
            kind,
            task_cloned,
            cancel,
            metadata,
            future,
        ));
        *handle_mut = Some(join_handle);
        drop(handle_mut);
        let kind_str: &'static str = kind.into();
        counter!(TC_SPAWN, "kind" => kind_str).increment(1);
        // Task is ready
        id
    }

    /// Launch a new task
    #[track_caller]
    pub fn spawn<F>(
        &self,
        kind: TaskKind,
        name: &'static str,
        partition_id: Option<PartitionId>,
        future: F,
    ) -> Result<TaskId, ShutdownError>
    where
        F: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        let inner = self.inner.clone();
        if inner.shutdown_requested.load(Ordering::Acquire) {
            return Err(ShutdownError);
        }
        Ok(self.spawn_unchecked(kind, name, partition_id, future))
    }

    // Allows for spawning a new task without checking if the system is shutting down. This means
    // that this task might not be able to finish if the system is shutting down.
    #[track_caller]
    fn spawn_unchecked<F>(
        &self,
        kind: TaskKind,
        name: &'static str,
        partition_id: Option<PartitionId>,
        future: F,
    ) -> TaskId
    where
        F: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        let cancel = CancellationToken::new();
        self.spawn_inner(kind, name, partition_id, cancel, future)
    }

    /// Spawn a new task that is a child of the current task. The child task will be cancelled if the parent
    /// task is cancelled. At the moment, the parent task will not automatically wait for children tasks to
    /// finish before completion, but this might change in the future if the need for that arises.
    #[track_caller]
    pub fn spawn_child<F>(
        &self,
        kind: TaskKind,
        name: &'static str,
        partition_id: Option<PartitionId>,
        future: F,
    ) -> Result<TaskId, ShutdownError>
    where
        F: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        let inner = self.inner.clone();
        if inner.shutdown_requested.load(Ordering::Acquire) {
            return Err(ShutdownError);
        }

        let parent_id =
            current_task_id().expect("spawn_child called outside of a task-center task");
        let parent_kind =
            current_task_kind().expect("spawn_child called outside of a task-center task");
        let parent_name = CURRENT_TASK
            .try_with(|ct| ct.name)
            .expect("spawn_child called outside of a task-center task");

        let cancel = cancellation_token().child_token();
        let result = self.spawn_inner(kind, name, partition_id, cancel, future);

        debug!(
            kind = ?parent_kind,
            name = ?parent_name,
            child_kind = ?kind,
            "Task \"{}\" {} spawned \"{}\" {}",
            parent_name, parent_id, name, result
        );
        Ok(result)
    }

    /// Cancelling the child will not cancel the parent. Note that parent task will not
    /// wait for children tasks. The parent task is allowed to finish before children.
    #[track_caller]
    pub fn spawn_child_unchecked<F>(
        &self,
        kind: TaskKind,
        name: &'static str,
        partition_id: Option<PartitionId>,
        future: F,
    ) -> TaskId
    where
        F: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        let cancel = cancellation_token().child_token();
        self.spawn_inner(kind, name, partition_id, cancel, future)
    }

    /// Signal and wait for tasks to stop.
    ///
    ///
    /// You can select which tasks to cancel. Any None arguments are ignored.
    /// For example, to shut down all MetadataBackgroundSync tasks:
    ///
    ///   cancel_tasks(Some(TaskKind::MetadataBackgroundSync), None)
    ///
    /// Or to shut down all tasks for a particular partition ID:
    ///
    ///   cancel_tasks(None, Some(partition_id))
    ///
    pub async fn cancel_tasks(&self, kind: Option<TaskKind>, partition_id: Option<PartitionId>) {
        let inner = self.inner.clone();
        let mut victims = Vec::new();

        {
            let tasks = inner.tasks.lock().unwrap();
            for task in tasks.values() {
                if (kind.is_none() || Some(task.kind) == kind)
                    && (partition_id.is_none() || task.partition_id == partition_id)
                {
                    task.cancel.cancel();
                    victims.push((Arc::clone(task), task.kind, task.partition_id));
                }
            }
        }

        for (task, task_kind, partition_id) in victims {
            let join_handle = {
                let mut task_mut = task.join_handle.lock().unwrap();
                // Task is not running anymore or another cancel is waiting for it.
                task_mut.take()
            };
            if let Some(mut join_handle) = join_handle {
                if task_kind.should_abort_on_cancel() {
                    // We should not wait, instead, just abort the tokio task.
                    debug!(kind = ?task_kind, name = ?task.name, partition_id = ?partition_id, "task {} aborted!", task.id);
                    join_handle.abort();
                } else if task_kind.should_wait_on_cancel() {
                    // Give the task a chance to finish before logging.
                    if tokio::time::timeout(Duration::from_secs(1), &mut join_handle)
                        .await
                        .is_err()
                    {
                        info!(kind = ?task_kind, name = ?task.name, partition_id = ?partition_id, "waiting for task {} to shutdown", task.id);
                        // Ignore join errors on cancel. on_finish already takes care
                        let _ = join_handle.await;
                        info!(kind = ?task_kind, name = ?task.name, partition_id = ?partition_id, "task {} completed", task.id);
                    }
                } else {
                    // Ignore the task. the task will be dropped on tokio runtime drop.
                }
            } else {
                // Possibly one of:
                //  * The task had not even fully started yet.
                //  * It was shut down concurrently and already exited (or failed)
            }
        }
    }

    /// Sets the current task_center but doesn't create a task. Use this when you need to run a
    /// future within task_center scope.
    pub fn block_on<F, O>(
        &self,
        name: &'static str,
        partition_id: Option<PartitionId>,
        future: F,
    ) -> O
    where
        F: Future<Output = O>,
    {
        self.inner
            .default_runtime_handle
            .block_on(self.run_in_scope(name, partition_id, future))
    }

    /// Sets the current task_center but doesn't create a task. Use this when you need to run a
    /// future within task_center scope.
    pub async fn run_in_scope<F, O>(
        &self,
        name: &'static str,
        partition_id: Option<PartitionId>,
        future: F,
    ) -> O
    where
        F: Future<Output = O>,
    {
        let cancel_token = CancellationToken::new();
        let id = TaskId::from(NEXT_TASK_ID.fetch_add(1, Ordering::SeqCst));
        let task = Arc::new(Task {
            id,
            name,
            kind: TaskKind::InPlace,
            partition_id,
            cancel: cancel_token.clone(),
            join_handle: Mutex::new(None),
        });
        // Clone the currently set METADATA (and is Some()), otherwise fallback to global metadata.
        let metadata = METADATA
            .try_with(|m| m.clone())
            .ok()
            .flatten()
            .or_else(|| self.inner.global_metadata.get().cloned());
        METADATA
            .scope(
                metadata,
                CURRENT_TASK_CENTER.scope(
                    self.clone(),
                    CANCEL_TOKEN.scope(cancel_token, CURRENT_TASK.scope(task, future)),
                ),
            )
            .await
    }

    /// Sets the current task_center but doesn't create a task. Use this when you need to run a
    /// closure within task_center scope.
    pub fn run_in_scope_sync<F, O>(
        &self,
        name: &'static str,
        partition_id: Option<PartitionId>,
        f: F,
    ) -> O
    where
        F: FnOnce() -> O,
    {
        let cancel_token = CancellationToken::new();
        let id = TaskId::from(NEXT_TASK_ID.fetch_add(1, Ordering::SeqCst));
        let task = Arc::new(Task {
            id,
            name,
            kind: TaskKind::InPlace,
            partition_id,
            cancel: cancel_token.clone(),
            join_handle: Mutex::new(None),
        });
        // Clone the currently set METADATA (and is Some()), otherwise fallback to global metadata.
        let metadata = METADATA
            .try_with(|m| m.clone())
            .ok()
            .flatten()
            .or_else(|| self.inner.global_metadata.get().cloned());
        METADATA.sync_scope(metadata, || {
            CURRENT_TASK_CENTER.sync_scope(self.clone(), || {
                CANCEL_TOKEN.sync_scope(cancel_token, || CURRENT_TASK.sync_scope(task, f))
            })
        })
    }

    /// Take control over the running task from task-center. This returns None if the task was not
    /// found, completed, or has been cancelled.
    pub fn take_task(&self, task_id: TaskId) -> Option<JoinHandle<()>> {
        let inner = self.inner.clone();
        let task = {
            // find the task
            let mut tasks = inner.tasks.lock().unwrap();
            tasks.remove(&task_id)?
        };

        let mut task_mut = task.join_handle.lock().unwrap();
        // Task is not running anymore or a cancellation is already in progress.
        task_mut.take()
    }

    /// Request cancellation of a task. This returns the join handle if the task was found and was
    /// not already cancelled or completed. The returned task will not be awaited by task-center on
    /// shutdown, and it's the responsibility of the caller to join or abort.
    pub fn cancel_task(&self, task_id: TaskId) -> Option<JoinHandle<()>> {
        let inner = self.inner.clone();
        let task = {
            // find the task
            let tasks = inner.tasks.lock().unwrap();
            let task = tasks.get(&task_id)?;
            // request cancellation
            task.cancel.cancel();
            Arc::clone(task)
        };

        let mut task_mut = task.join_handle.lock().unwrap();
        // Task is not running anymore or a cancellation is already in progress.
        task_mut.take()
    }

    async fn on_finish(
        &self,
        result: std::result::Result<
            anyhow::Result<()>,
            std::boxed::Box<dyn std::any::Any + std::marker::Send>,
        >,
        kind: TaskKind,
        task_id: TaskId,
    ) {
        let kind_str: &'static str = kind.into();
        let inner = self.inner.clone();
        // Remove our entry from the tasks map.
        let Some(task) = inner.tasks.lock().unwrap().remove(&task_id) else {
            // This can happen if the task ownership was taken by calling take_task(id);
            return;
        };

        let should_shutdown_on_error = kind.should_shutdown_on_error();
        let mut request_node_shutdown = false;
        {
            match result {
                Ok(Ok(())) => {
                    trace!(kind = ?kind, name = ?task.name, "Task {} exited normally", task_id);
                    counter!(TC_FINISHED, "kind" => kind_str, "status" => TC_STATUS_COMPLETED)
                        .increment(1);
                }
                Ok(Err(err)) => {
                    if err.root_cause().downcast_ref::<ShutdownError>().is_some() {
                        // The task failed to spawn other tasks because the system
                        // is already shutting down, we ignore those errors.
                        return;
                    }
                    if should_shutdown_on_error {
                        error!(kind = ?kind, name = ?task.name,
                            "Shutting down: task {} failed with: {:?}",
                            task_id, err
                        );
                        request_node_shutdown = true;
                    } else {
                        error!(kind = ?kind, name = ?task.name, "Task {} failed with: {:?}", task_id, err);
                    }
                    counter!(TC_FINISHED, "kind" => kind_str, "status" => TC_STATUS_FAILED)
                        .increment(1);
                }
                Err(err) => {
                    counter!(TC_FINISHED, "kind" => kind_str, "status" => TC_STATUS_FAILED)
                        .increment(1);
                    if should_shutdown_on_error {
                        error!(kind = ?kind, name = ?task.name, "Shutting down: task {} panicked: {:?}", task_id, err);
                        request_node_shutdown = true;
                    } else {
                        error!(kind = ?kind, name = ?task.name, "Task {} panicked: {:?}", task_id, err);
                    }
                }
            }
        }

        if request_node_shutdown {
            // Note that the task itself has been already removed from the task map, so shutdown
            // will not wait for its completion.
            self.shutdown_node(
                &format!("task {} failed and requested a shutdown", task.name),
                EXIT_CODE_FAILURE,
            )
            .await;
        }
    }
}

struct TaskCenterInner {
    default_runtime_handle: tokio::runtime::Handle,
    /// We hold on to the owned Runtime to ensure it's dropped when task center is dropped. If this
    /// is None, it means that it's the responsibility of the Handle owner to correctly drop
    /// tokio's runtime after dropping the task center.
    #[allow(dead_code)]
    default_runtime: Option<tokio::runtime::Runtime>,
    global_cancel_token: CancellationToken,
    shutdown_requested: AtomicBool,
    current_exit_code: AtomicI32,
    tasks: Mutex<HashMap<TaskId, Arc<Task>>>,
    global_metadata: OnceLock<Metadata>,
}

pub struct Task {
    /// It's nice to have a unique ID for each task.
    id: TaskId,
    name: &'static str,
    kind: TaskKind,
    /// cancel this token to request cancelling this task.
    cancel: CancellationToken,

    /// Tasks associated with a specific partition ID will have this set. This allows
    /// for cancellation of tasks associated with that partition.
    partition_id: Option<PartitionId>,
    join_handle: Mutex<Option<JoinHandle<()>>>,
}

task_local! {
    // This is a cancellation token which will be cancelled when a task needs to shut down.
    static CANCEL_TOKEN: CancellationToken;

    // Tasks have self-reference.
    static CURRENT_TASK: Arc<Task>;

    // Current task center
    static CURRENT_TASK_CENTER: TaskCenter;

    // Metadata handle
    static METADATA: Option<Metadata>;
}

/// This wrapper function runs in a newly-spawned task. It initializes the
/// task-local variables and calls the payload function.
async fn wrapper<F>(
    task_center: TaskCenter,
    task_id: TaskId,
    kind: TaskKind,
    task: Arc<Task>,
    cancel_token: CancellationToken,
    metadata: Option<Metadata>,
    future: F,
) where
    F: Future<Output = anyhow::Result<()>> + Send + 'static,
{
    debug!(kind = ?kind, name = ?task.name, "Starting task {}", task_id);

    let result = CURRENT_TASK_CENTER
        .scope(
            task_center.clone(),
            CANCEL_TOKEN.scope(
                cancel_token,
                CURRENT_TASK.scope(
                    task,
                    METADATA.scope(metadata, {
                        // We use AssertUnwindSafe here so that the wrapped function
                        // doesn't need to be UnwindSafe. We should not do anything after
                        // unwinding that'd risk us being in unwind-unsafe behavior.
                        AssertUnwindSafe(future).catch_unwind()
                    }),
                ),
            ),
        )
        .await;
    task_center.on_finish(result, kind, task_id).await;
}

/// The current task-center task kind. This returns None if we are not in the scope
/// of a task-center task.
pub fn current_task_kind() -> Option<TaskKind> {
    CURRENT_TASK.try_with(|ct| ct.kind).ok()
}

/// The current task-center task Id. This returns None if we are not in the scope
/// of a task-center task.
pub fn current_task_id() -> Option<TaskId> {
    CURRENT_TASK.try_with(|ct| ct.id).ok()
}

/// Accepss to global metadata handle. This available in task-center tasks only!
#[track_caller]
pub fn metadata() -> Metadata {
    METADATA
        .try_with(|m| m.clone())
        .expect("metadata() called outside task-center scope")
        .expect("metadata() called before global metadata was set")
}

/// The current partition Id associated to the running task-center task.
pub fn current_task_partition_id() -> Option<PartitionId> {
    CURRENT_TASK.try_with(|ct| ct.partition_id).ok().flatten()
}

/// Get the current task center. Use this to spawn tasks on the current task center.
/// This must be called from within a task-center task.
pub fn task_center() -> TaskCenter {
    CURRENT_TASK_CENTER
        .try_with(|t| t.clone())
        .expect("task_center() called in a task-center task")
}

/// A Future that can be used to check if the current task has been requested to
/// shut down.
pub async fn cancellation_watcher() {
    let token = cancellation_token();
    token.cancelled().await;
}

/// Clones the current task's cancellation token, which can be moved across tasks.
///
/// When the system is shutting down, or the current task is being cancelled by a
/// cancel_task() call, or if it's a child and the parent is being cancelled by a
/// cancel_task() call, this cancellation token will be set to cancelled.
pub fn cancellation_token() -> CancellationToken {
    let res = CANCEL_TOKEN.try_with(|t| t.clone());

    if cfg!(any(test, feature = "test-util")) {
        // allow in tests to call from non-task-center tasks.
        res.unwrap_or_default()
    } else {
        res.expect("cancellation_token() called in in a task-center task")
    }
}

/// Has the current task been requested to cancel?
pub fn is_cancellation_requested() -> bool {
    if let Ok(cancel) = CANCEL_TOKEN.try_with(|t| t.clone()) {
        cancel.is_cancelled()
    } else {
        if cfg!(any(test, feature = "test-util")) {
            warn!("is_cancellation_requested() called in task-center task");
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use googletest::prelude::*;
    use restate_test_util::assert_eq;
    use restate_types::config::CommonOptionsBuilder;
    use tracing_test::traced_test;

    #[tokio::test(start_paused = true)]
    #[traced_test]
    async fn test_basic_lifecycle() -> Result<()> {
        let common_opts = CommonOptionsBuilder::default()
            .default_thread_pool_size(3)
            .build()
            .unwrap();
        let tc = TaskCenterBuilder::default()
            .options(common_opts)
            .default_runtime_handle(tokio::runtime::Handle::current())
            .build()?;
        let start = tokio::time::Instant::now();
        tc.spawn(TaskKind::RoleRunner, "worker-role", None, async {
            info!("Hello async");
            tokio::time::sleep(Duration::from_secs(10)).await;
            assert_eq!(TaskKind::RoleRunner, current_task_kind().unwrap());
            info!("Bye async");
            Ok(())
        })
        .unwrap();

        tc.cancel_tasks(None, None).await;
        assert!(logs_contain("Hello async"));
        assert!(logs_contain("Bye async"));
        assert!(start.elapsed() >= Duration::from_secs(10));
        Ok(())
    }
}
