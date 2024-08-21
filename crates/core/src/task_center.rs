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
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use futures::{Future, FutureExt};
use metrics::{counter, gauge};
use parking_lot::Mutex;
use tokio::runtime::RuntimeMetrics;
use tokio::task::LocalSet;
use tokio::task_local;
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};
use tracing::{debug, error, info, instrument, trace, warn};

use restate_types::config::CommonOptions;
use restate_types::identifiers::PartitionId;
use restate_types::GenerationalNodeId;

use crate::metric_definitions::{TC_FINISHED, TC_SPAWN, TC_STATUS_COMPLETED, TC_STATUS_FAILED};
use crate::{
    metric_definitions, Metadata, RuntimeHandle, ShutdownError, ShutdownSourceErr, TaskHandle,
    TaskId, TaskKind,
};

static WORKER_ID: AtomicUsize = const { AtomicUsize::new(0) };
static NEXT_TASK_ID: AtomicU64 = const { AtomicU64::new(0) };
const EXIT_CODE_FAILURE: i32 = 1;

#[derive(Clone)]
pub struct TaskContext {
    /// It's nice to have a unique ID for each task.
    id: TaskId,
    name: &'static str,
    kind: TaskKind,
    /// cancel this token to request cancelling this task.
    cancellation_token: CancellationToken,
    /// Tasks associated with a specific partition ID will have this set. This allows
    /// for cancellation of tasks associated with that partition.
    partition_id: Option<PartitionId>,
    metadata: Option<Metadata>,
}

#[derive(Debug, thiserror::Error)]
pub enum TaskCenterBuildError {
    #[error(transparent)]
    Tokio(#[from] tokio::io::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("Runtime with name {0} already exists")]
    AlreadyExists(String),
    #[error(transparent)]
    Shutdown(#[from] ShutdownError),
}

/// Used to create a new task center. In practice, there should be a single task center for the
/// entire process but we might need to create more than one in integration test scenarios.
#[derive(Default)]
pub struct TaskCenterBuilder {
    default_runtime_handle: Option<tokio::runtime::Handle>,
    default_runtime: Option<tokio::runtime::Runtime>,
    ingress_runtime_handle: Option<tokio::runtime::Handle>,
    ingress_runtime: Option<tokio::runtime::Runtime>,
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

    pub fn ingress_runtime_handle(mut self, handle: tokio::runtime::Handle) -> Self {
        self.ingress_runtime_handle = Some(handle);
        self.ingress_runtime = None;
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

    pub fn ingress_runtime(mut self, runtime: tokio::runtime::Runtime) -> Self {
        self.ingress_runtime_handle = Some(runtime.handle().clone());
        self.ingress_runtime = Some(runtime);
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
            let mut default_runtime_builder = tokio_builder("worker", &options);
            #[cfg(any(test, feature = "test-util"))]
            if self.pause_time {
                default_runtime_builder.start_paused(self.pause_time);
            }
            let default_runtime = default_runtime_builder.build()?;
            self.default_runtime_handle = Some(default_runtime.handle().clone());
            self.default_runtime = Some(default_runtime);
        }

        if self.ingress_runtime_handle.is_none() {
            let mut ingress_runtime_builder = tokio_builder("ingress", &options);
            #[cfg(any(test, feature = "test-util"))]
            if self.pause_time {
                ingress_runtime_builder.start_paused(self.pause_time);
            }
            let ingress_runtime = ingress_runtime_builder.build()?;
            self.ingress_runtime_handle = Some(ingress_runtime.handle().clone());
            self.ingress_runtime = Some(ingress_runtime);
        }

        if cfg!(any(test, feature = "test-util")) {
            eprintln!("!!!! Runnning with test-util enabled !!!!");
        }
        metric_definitions::describe_metrics();
        Ok(TaskCenter {
            inner: Arc::new(TaskCenterInner {
                default_runtime_handle: self.default_runtime_handle.unwrap(),
                default_runtime: self.default_runtime,
                ingress_runtime_handle: self.ingress_runtime_handle.unwrap(),
                ingress_runtime: self.ingress_runtime,
                global_cancel_token: CancellationToken::new(),
                shutdown_requested: AtomicBool::new(false),
                current_exit_code: AtomicI32::new(0),
                managed_tasks: Mutex::new(HashMap::new()),
                global_metadata: OnceLock::new(),
                managed_runtimes: Mutex::new(HashMap::with_capacity(64)),
            }),
        })
    }
}

fn tokio_builder(prefix: &'static str, common_opts: &CommonOptions) -> tokio::runtime::Builder {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all().thread_name_fn(move || {
        let id = WORKER_ID.fetch_add(1, Ordering::Relaxed);
        format!("rs:{}-{}", prefix, id)
    });

    builder.worker_threads(common_opts.default_thread_pool_size());

    builder
}

/// Task center is used to manage long-running and background tasks and their lifecycle.
#[derive(Clone)]
pub struct TaskCenter {
    inner: Arc<TaskCenterInner>,
}

static_assertions::assert_impl_all!(TaskCenter: Send, Sync, Clone);

impl TaskCenter {
    pub fn default_runtime_metrics(&self) -> RuntimeMetrics {
        self.inner.default_runtime_handle.metrics()
    }

    pub fn ingress_runtime_metrics(&self) -> RuntimeMetrics {
        self.inner.ingress_runtime_handle.metrics()
    }

    pub fn managed_runtime_metrics(&self) -> Vec<(&'static str, RuntimeMetrics)> {
        let guard = self.inner.managed_runtimes.lock();
        guard.iter().map(|(k, v)| (*k, v.metrics())).collect()
    }

    /// Submit telemetry for all runtimes to metrics recorder
    pub fn submit_metrics(&self) {
        Self::submit_runtime_metrics("default", self.default_runtime_metrics());
        Self::submit_runtime_metrics("ingress", self.ingress_runtime_metrics());

        // Partition processor runtimes
        let processor_runtimes = self.managed_runtime_metrics();
        for (task_name, metrics) in processor_runtimes {
            Self::submit_runtime_metrics(task_name, metrics);
        }
    }

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
        self.inner.current_exit_code.load(Ordering::Relaxed)
    }

    pub fn metadata(&self) -> Option<&Metadata> {
        self.inner.global_metadata.get()
    }

    fn submit_runtime_metrics(runtime: &'static str, stats: RuntimeMetrics) {
        gauge!("restate.tokio.num_workers", "runtime" => runtime).set(stats.num_workers() as f64);
        gauge!("restate.tokio.blocking_threads", "runtime" => runtime)
            .set(stats.num_blocking_threads() as f64);
        gauge!("restate.tokio.blocking_queue_depth", "runtime" => runtime)
            .set(stats.blocking_queue_depth() as f64);
        gauge!("restate.tokio.num_alive_tasks", "runtime" => runtime)
            .set(stats.num_alive_tasks() as f64);
        gauge!("restate.tokio.io_driver_ready_count", "runtime" => runtime)
            .set(stats.io_driver_ready_count() as f64);
        gauge!("restate.tokio.remote_schedule_count", "runtime" => runtime)
            .set(stats.remote_schedule_count() as f64);
        // per worker stats
        for idx in 0..stats.num_workers() {
            gauge!("restate.tokio.worker_overflow_count", "runtime" => runtime, "worker" =>
            idx.to_string())
            .set(stats.worker_overflow_count(idx) as f64);
            gauge!("restate.tokio.worker_poll_count", "runtime" => runtime, "worker" => idx.to_string())
            .set(stats.worker_poll_count(idx) as f64);
            gauge!("restate.tokio.worker_park_count", "runtime" => runtime, "worker" => idx.to_string())
            .set(stats.worker_park_count(idx) as f64);
            gauge!("restate.tokio.worker_noop_count", "runtime" => runtime, "worker" => idx.to_string())
            .set(stats.worker_noop_count(idx) as f64);
            gauge!("restate.tokio.worker_steal_count", "runtime" => runtime, "worker" => idx.to_string())
            .set(stats.worker_steal_count(idx) as f64);
            gauge!("restate.tokio.worker_total_busy_duration_seconds", "runtime" => runtime, "worker" => idx.to_string())
            .set(stats.worker_total_busy_duration(idx).as_secs_f64());
            gauge!("restate.tokio.worker_mean_poll_time", "runtime" => runtime, "worker" => idx.to_string())
            .set(stats.worker_mean_poll_time(idx).as_secs_f64());
        }
    }

    /// Clone the currently set METADATA (and if Some()). Otherwise falls back to global metadata.
    #[track_caller]
    fn clone_metadata(&self) -> Option<Metadata> {
        CONTEXT
            .try_with(|m| m.metadata.clone())
            .ok()
            .flatten()
            .or_else(|| self.inner.global_metadata.get().cloned())
    }
    /// Triggers a shutdown of the system. All running tasks will be asked gracefully
    /// to cancel but we will only wait for tasks with a TaskKind that has the property
    /// "OnCancel" set to "wait".
    #[instrument(level = "error", skip(self, exit_code))]
    pub async fn shutdown_node(&self, reason: &str, exit_code: i32) {
        let inner = self.inner.clone();
        if inner
            .shutdown_requested
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .unwrap_or_else(|e| e)
        {
            // already shutting down....
            return;
        }
        let start = Instant::now();
        inner.current_exit_code.store(exit_code, Ordering::Relaxed);

        if exit_code != 0 {
            warn!("** Shutdown requested");
        } else {
            info!("** Shutdown requested");
        }
        self.cancel_tasks(None, None).await;
        self.shutdown_managed_runtimes();
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
        let id = TaskId::from(NEXT_TASK_ID.fetch_add(1, Ordering::Relaxed));
        let metadata = self.clone_metadata();
        let context = TaskContext {
            id,
            name,
            kind,
            partition_id,
            cancellation_token: cancel.clone(),
            metadata,
        };
        let task = Arc::new(Task {
            context: context.clone(),
            handle: Mutex::new(None),
        });

        inner.managed_tasks.lock().insert(id, Arc::clone(&task));

        let mut handle_mut = task.handle.lock();

        let fut = wrapper(self.clone(), context, future);
        *handle_mut = Some(self.spawn_on_runtime(kind, name, cancel, fut));
        // drop the lock
        drop(handle_mut);
        // Task is ready
        id
    }

    fn spawn_on_runtime<F, T>(
        &self,
        kind: TaskKind,
        name: &'static str,
        cancellation_token: CancellationToken,
        fut: F,
    ) -> TaskHandle<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let kind_str: &'static str = kind.into();
        let runtime_name: &'static str = kind.runtime().into();
        let tokio_task = tokio::task::Builder::new().name(name);
        counter!(TC_SPAWN, "kind" => kind_str, "runtime" => runtime_name).increment(1);
        let runtime = match kind.runtime() {
            crate::AsyncRuntime::Inherit => &tokio::runtime::Handle::current(),
            crate::AsyncRuntime::Default => &self.inner.default_runtime_handle,
            crate::AsyncRuntime::Ingress => &self.inner.ingress_runtime_handle,
        };
        let inner_handle = tokio_task
            .spawn_on(fut, runtime)
            .expect("runtime can spawn tasks");

        TaskHandle {
            cancellation_token,
            inner_handle,
        }
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
        if self.inner.shutdown_requested.load(Ordering::Relaxed) {
            return Err(ShutdownError);
        }
        Ok(self.spawn_unchecked(kind, name, partition_id, future))
    }

    #[track_caller]
    pub fn spawn_unmanaged<F, T>(
        &self,
        kind: TaskKind,
        name: &'static str,
        partition_id: Option<PartitionId>,
        future: F,
    ) -> Result<TaskHandle<T>, ShutdownError>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        if self.inner.shutdown_requested.load(Ordering::Relaxed) {
            return Err(ShutdownError);
        }

        let cancel = CancellationToken::new();
        let id = TaskId::from(NEXT_TASK_ID.fetch_add(1, Ordering::Relaxed));
        let metadata = self.clone_metadata();
        let context = TaskContext {
            id,
            name,
            kind,
            partition_id,
            cancellation_token: cancel.clone(),
            metadata,
        };

        let fut = unmanaged_wrapper(self.clone(), context, future);

        Ok(self.spawn_on_runtime(kind, name, cancel, fut))
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

    /// Must be called within a Localset-scoped task, not from a normal spawned task.
    /// If ran from a non-localset task, this will panic.
    pub fn spawn_local<F>(
        &self,
        kind: TaskKind,
        name: &'static str,
        future: F,
    ) -> Result<TaskId, ShutdownError>
    where
        F: Future<Output = anyhow::Result<()>> + 'static,
    {
        let cancellation_token = CancellationToken::new();
        let id = TaskId::from(NEXT_TASK_ID.fetch_add(1, Ordering::Relaxed));
        let context = TaskContext {
            id,
            name,
            kind,
            cancellation_token,
            // We must be within task-context already. let's get inherit partition_id
            partition_id: CONTEXT.with(|c| c.partition_id),
            metadata: Some(metadata()),
        };

        Self::spawn_local_inner(self, context, future);
        Ok(id)
    }

    fn spawn_local_inner<F>(tc: &TaskCenter, context: TaskContext, future: F)
    where
        F: Future<Output = anyhow::Result<()>> + 'static,
    {
        let cancel = context.cancellation_token.clone();
        let name = context.name;
        let task = Arc::new(Task {
            context: context.clone(),
            handle: Mutex::new(None),
        });

        let inner = tc.inner.clone();
        inner
            .managed_tasks
            .lock()
            .insert(context.id, Arc::clone(&task));
        let mut handle_mut = task.handle.lock();

        let fut = wrapper(tc.clone(), context, future);

        let tokio_task = tokio::task::Builder::new().name(name);
        let inner_handle = tokio_task
            .spawn_local(fut)
            .expect("must run from a LocalSet");
        *handle_mut = Some(TaskHandle {
            cancellation_token: cancel,
            inner_handle,
        });

        // drop the lock
        drop(handle_mut);
    }

    pub fn start_runtime<F>(
        &self,
        root_task_kind: TaskKind,
        runtime_name: &'static str,
        partition_id: Option<PartitionId>,
        run: impl FnOnce() -> F + Send + 'static,
    ) -> Result<TaskId, RuntimeError>
    where
        F: Future<Output = anyhow::Result<()>> + 'static,
    {
        if self.inner.shutdown_requested.load(Ordering::Relaxed) {
            return Err(ShutdownError.into());
        }

        let cancel = CancellationToken::new();
        let metadata = self.clone_metadata();

        // hold a lock while creating the runtime to avoid concurrent runtimes with the same name
        let mut runtimes_guard = self.inner.managed_runtimes.lock();
        if runtimes_guard.contains_key(runtime_name) {
            warn!(
                "Failed to start new runtime, a runtime with name {} already exists",
                runtime_name
            );
            return Err(RuntimeError::AlreadyExists(runtime_name.to_owned()));
        }

        // todo: configure the runtime according to a new runtime kind perhaps?
        let thread_builder = std::thread::Builder::new().name(format!("rt:{}", runtime_name));
        let mut builder = tokio::runtime::Builder::new_current_thread();
        let rt = builder
            .enable_all()
            .build()
            .expect("runtime builder succeeds");
        let tc = self.clone();

        let rt_handle = Arc::new(RuntimeHandle {
            cancellation_token: cancel.clone(),
            inner: rt,
        });

        runtimes_guard.insert(runtime_name, rt_handle.clone());

        // release the lock.
        drop(runtimes_guard);

        let id = TaskId::from(NEXT_TASK_ID.fetch_add(1, Ordering::Relaxed));
        let context = TaskContext {
            id,
            name: runtime_name,
            kind: root_task_kind,
            cancellation_token: cancel,
            partition_id,
            metadata,
        };

        // start the work on the runtime
        let _ = thread_builder
            .spawn(move || {
                let localset = LocalSet::new();
                {
                    let _local_guard = localset.enter();
                    Self::spawn_local_inner(&tc, context, run()); // , &localset);
                    rt_handle.inner.block_on(localset);
                }
                debug!("Runtime {} completed", runtime_name);
                tc.drop_runtime(runtime_name);
            })
            .unwrap();

        Ok(id)
    }

    fn drop_runtime(&self, name: &'static str) {
        let mut runtimes_guard = self.inner.managed_runtimes.lock();
        if runtimes_guard.remove(name).is_some() {
            trace!("Runtime {} was dropped", name);
        }
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
        if self.inner.shutdown_requested.load(Ordering::Relaxed) {
            return Err(ShutdownError);
        }

        let parent_id =
            current_task_id().expect("spawn_child called outside of a task-center task");
        // From this point onwards, we unwrap() directly with the assumption that we are in task-center
        // context and that the previous (expect) guards against reaching this point if we are
        // outside task-center.
        let parent_kind = current_task_kind().unwrap();
        let parent_name = CONTEXT.try_with(|ctx| ctx.name).unwrap();

        let cancel = CONTEXT
            .try_with(|ctx| ctx.cancellation_token.child_token())
            .unwrap();
        let result = self.spawn_inner(kind, name, partition_id, cancel, future);

        trace!(
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
        let cancel = CONTEXT
            .try_with(|ctx| ctx.cancellation_token.child_token())
            .expect("spawning inside task-center context");
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
            let tasks = inner.managed_tasks.lock();
            for task in tasks.values() {
                if (kind.is_none() || Some(task.context.kind) == kind)
                    && (partition_id.is_none() || task.context.partition_id == partition_id)
                {
                    task.context.cancellation_token.cancel();
                    victims.push((Arc::clone(task), task.kind(), task.partition_id()));
                }
            }
        }

        for (task, task_kind, partition_id) in victims {
            let handle = {
                let mut task_mut = task.handle.lock();
                // Task is not running anymore or another cancel is waiting for it.
                task_mut.take()
            };
            if let Some(mut handle) = handle {
                if task_kind.should_abort_on_cancel() {
                    // We should not wait, instead, just abort the tokio task.
                    debug!(kind = ?task_kind, name = ?task.name(), partition_id = ?partition_id, "task {} aborted!", task.id());
                    handle.abort();
                } else if task_kind.should_wait_on_cancel() {
                    // Give the task a chance to finish before logging.
                    if tokio::time::timeout(Duration::from_secs(2), &mut handle)
                        .await
                        .is_err()
                    {
                        info!(kind = ?task_kind, name = ?task.name(), partition_id = ?partition_id, "waiting for task {} to shutdown", task.id());
                        // Ignore join errors on cancel. on_finish already takes care
                        let _ = handle.await;
                        info!(kind = ?task_kind, name = ?task.name(), partition_id = ?partition_id, "task {} completed", task.id());
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

    pub fn shutdown_managed_runtimes(&self) {
        let mut runtimes = self.inner.managed_runtimes.lock();
        for (_, runtime) in runtimes.drain() {
            if let Some(runtime) = Arc::into_inner(runtime) {
                runtime.inner.shutdown_background();
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
        let cancel = CancellationToken::new();
        let id = TaskId::from(NEXT_TASK_ID.fetch_add(1, Ordering::Relaxed));
        let metadata = self.clone_metadata();
        let context = TaskContext {
            id,
            name,
            kind: TaskKind::InPlace,
            cancellation_token: cancel.clone(),
            partition_id,
            metadata,
        };

        CURRENT_TASK_CENTER
            .scope(self.clone(), CONTEXT.scope(context, future))
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
        let cancel = CancellationToken::new();
        let id = TaskId::from(NEXT_TASK_ID.fetch_add(1, Ordering::Relaxed));
        let metadata = self.clone_metadata();
        let context = TaskContext {
            id,
            name,
            kind: TaskKind::InPlace,
            partition_id,
            cancellation_token: cancel.clone(),
            metadata,
        };
        CURRENT_TASK_CENTER.sync_scope(self.clone(), || CONTEXT.sync_scope(context, f))
    }

    /// Take control over the running task from task-center. This returns None if the task was not
    /// found, completed, or has been cancelled.
    pub fn take_task(&self, task_id: TaskId) -> Option<TaskHandle<()>> {
        let inner = self.inner.clone();
        let task = {
            // find the task
            let mut tasks = inner.managed_tasks.lock();
            tasks.remove(&task_id)?
        };

        let mut task_mut = task.handle.lock();
        // Task is not running anymore or a cancellation is already in progress.
        task_mut.take()
    }

    /// Request cancellation of a task. This returns the join handle if the task was found and was
    /// not already cancelled or completed. The returned task will not be awaited by task-center on
    /// shutdown, and it's the responsibility of the caller to join or abort.
    pub fn cancel_task(&self, task_id: TaskId) -> Option<TaskHandle<()>> {
        let inner = self.inner.clone();
        let task = {
            // find the task
            let tasks = inner.managed_tasks.lock();
            let task = tasks.get(&task_id)?;
            // request cancellation
            task.cancel();
            Arc::clone(task)
        };

        let mut task_mut = task.handle.lock();
        // Task is not running anymore or a cancellation is already in progress.
        task_mut.take()
    }

    async fn on_finish(
        &self,
        task_id: TaskId,
        result: std::result::Result<
            anyhow::Result<()>,
            std::boxed::Box<dyn std::any::Any + std::marker::Send>,
        >,
    ) {
        let inner = self.inner.clone();
        // Remove our entry from the tasks map.
        let Some(task) = inner.managed_tasks.lock().remove(&task_id) else {
            // This can happen if the task ownership was taken by calling take_task(id);
            return;
        };
        let kind_str: &'static str = task.kind().into();

        let should_shutdown_on_error = task.kind().should_shutdown_on_error();
        let mut request_node_shutdown = false;
        {
            match result {
                Ok(Ok(())) => {
                    trace!(kind = ?task.kind(), name = ?task.name(), "Task {} exited normally", task_id);
                    counter!(TC_FINISHED, "kind" => kind_str, "status" => TC_STATUS_COMPLETED)
                        .increment(1);
                }
                Ok(Err(err)) => {
                    if err
                        .root_cause()
                        .downcast_ref::<ShutdownSourceErr>()
                        .is_some()
                    {
                        // The task failed to spawn other tasks because the system
                        // is already shutting down, we ignore those errors.
                        tracing::debug!(kind = ?task.kind(), name = ?task.name(), "[Shutdown] Task {} stopped due to shutdown", task_id);
                        return;
                    }
                    if should_shutdown_on_error {
                        error!(kind = ?task.kind(), name = ?task.name(),
                            "Shutting down: task {} failed with: {:?}",
                            task_id, err
                        );
                        request_node_shutdown = true;
                    } else {
                        error!(kind = ?task.kind(), name = ?task.name(), "Task {} failed with: {:?}", task_id, err);
                    }
                    counter!(TC_FINISHED, "kind" => kind_str, "status" => TC_STATUS_FAILED)
                        .increment(1);
                }
                Err(err) => {
                    counter!(TC_FINISHED, "kind" => kind_str, "status" => TC_STATUS_FAILED)
                        .increment(1);
                    if should_shutdown_on_error {
                        error!(kind = ?task.kind(), name = ?task.name(), "Shutting down: task {} panicked: {:?}", task_id, err);
                        request_node_shutdown = true;
                    } else {
                        error!(kind = ?task.kind(), name = ?task.name(), "Task {} panicked: {:?}", task_id, err);
                    }
                }
            }
        }

        if request_node_shutdown {
            // Note that the task itself has been already removed from the task map, so shutdown
            // will not wait for its completion.
            self.shutdown_node(
                &format!("task {} failed and requested a shutdown", task.name()),
                EXIT_CODE_FAILURE,
            )
            .await;
        }
    }
}

struct TaskCenterInner {
    default_runtime_handle: tokio::runtime::Handle,
    ingress_runtime_handle: tokio::runtime::Handle,
    managed_runtimes: Mutex<HashMap<&'static str, Arc<RuntimeHandle>>>,
    /// We hold on to the owned Runtime to ensure it's dropped when task center is dropped. If this
    /// is None, it means that it's the responsibility of the Handle owner to correctly drop
    /// tokio's runtime after dropping the task center.
    #[allow(dead_code)]
    default_runtime: Option<tokio::runtime::Runtime>,
    #[allow(dead_code)]
    ingress_runtime: Option<tokio::runtime::Runtime>,
    global_cancel_token: CancellationToken,
    shutdown_requested: AtomicBool,
    current_exit_code: AtomicI32,
    managed_tasks: Mutex<HashMap<TaskId, Arc<Task>>>,
    global_metadata: OnceLock<Metadata>,
}

pub struct Task<R = ()> {
    context: TaskContext,
    handle: Mutex<Option<TaskHandle<R>>>,
}

impl<R> Task<R> {
    fn id(&self) -> TaskId {
        self.context.id
    }

    fn name(&self) -> &'static str {
        self.context.name
    }

    fn kind(&self) -> TaskKind {
        self.context.kind
    }

    fn partition_id(&self) -> Option<PartitionId> {
        self.context.partition_id
    }

    fn cancel(&self) {
        self.context.cancellation_token.cancel()
    }
}

task_local! {
    // Tasks provide access to their context
    static CONTEXT: TaskContext;

    // Current task center
    static CURRENT_TASK_CENTER: TaskCenter;
}

/// This wrapper function runs in a newly-spawned task. It initializes the
/// task-local variables and calls the payload function.
async fn wrapper<F>(task_center: TaskCenter, context: TaskContext, future: F)
where
    F: Future<Output = anyhow::Result<()>> + 'static,
{
    let id = context.id;
    trace!(kind = ?context.kind, name = ?context.name, "Starting task {}", context.id);

    let result = CURRENT_TASK_CENTER
        .scope(
            task_center.clone(),
            CONTEXT.scope(context, {
                // We use AssertUnwindSafe here so that the wrapped function
                // doesn't need to be UnwindSafe. We should not do anything after
                // unwinding that'd risk us being in unwind-unsafe behavior.
                AssertUnwindSafe(future).catch_unwind()
            }),
        )
        .await;
    task_center.on_finish(id, result).await;
}

/// Like wrapper but doesn't call on_finish nor it catches panics
async fn unmanaged_wrapper<F, T>(task_center: TaskCenter, context: TaskContext, future: F) -> T
where
    F: Future<Output = T> + 'static,
{
    trace!(kind = ?context.kind, name = ?context.name, "Starting task {}", context.id);

    CURRENT_TASK_CENTER
        .scope(task_center.clone(), CONTEXT.scope(context, future))
        .await
}

/// The current task-center task kind. This returns None if we are not in the scope
/// of a task-center task.
pub fn current_task_kind() -> Option<TaskKind> {
    CONTEXT.try_with(|ctx| ctx.kind).ok()
}

/// The current task-center task Id. This returns None if we are not in the scope
/// of a task-center task.
pub fn current_task_id() -> Option<TaskId> {
    CONTEXT.try_with(|ctx| ctx.id).ok()
}

/// Access to global metadata handle. This is available in task-center tasks only!
#[track_caller]
pub fn metadata() -> Metadata {
    CONTEXT
        .try_with(|ctx| ctx.metadata.clone())
        .expect("metadata() called outside task-center scope")
        .expect("metadata() called before global metadata was set")
}

/// Access to this node id. This is available in task-center tasks only!
#[track_caller]
pub fn my_node_id() -> GenerationalNodeId {
    CONTEXT
        .try_with(|ctx| ctx.metadata.as_ref().map(|m| m.my_node_id()))
        .expect("my_node_id() called outside task-center scope")
        .expect("my_node_id() called before global metadata was set")
}

/// The current partition Id associated to the running task-center task.
pub fn current_task_partition_id() -> Option<PartitionId> {
    CONTEXT.try_with(|ctx| ctx.partition_id).ok().flatten()
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
    let res = CONTEXT.try_with(|ctx| ctx.cancellation_token.clone());

    if cfg!(any(test, feature = "test-util")) {
        // allow in tests to call from non-task-center tasks.
        res.unwrap_or_default()
    } else {
        res.expect("cancellation_token() called in in a task-center task")
    }
}

/// Has the current task been requested to cancel?
pub fn is_cancellation_requested() -> bool {
    CONTEXT
        .try_with(|ctx| ctx.cancellation_token.is_cancelled())
        .unwrap_or_else(|_| {
            if cfg!(any(test, feature = "test-util")) {
                warn!("is_cancellation_requested() called outside task-center context");
            }
            false
        })
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
            .ingress_runtime_handle(tokio::runtime::Handle::current())
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
