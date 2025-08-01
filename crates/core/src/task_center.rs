// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod builder;
mod extensions;
mod handle;
mod monitoring;
mod runtime;
mod task;
mod task_kind;

pub use builder::*;
pub use extensions::*;
pub use handle::*;
pub use monitoring::*;
pub use runtime::*;
pub use task::*;
pub use task_kind::*;

use std::collections::HashMap;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use futures::FutureExt;
use futures::future::BoxFuture;
use metrics::counter;
use parking_lot::Mutex;
use tokio::sync::oneshot;
use tokio::task::LocalSet;
use tokio::task_local;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, trace, warn};

use crate::metric_definitions::{self, STATUS_COMPLETED, STATUS_FAILED, TC_FINISHED, TC_SPAWN};
use crate::{Metadata, ShutdownError, ShutdownSourceErr};
use restate_types::SharedString;
use restate_types::cluster_state::ClusterState;
use restate_types::config::Configuration;
use restate_types::health::{Health, NodeStatus};
use restate_types::identifiers::PartitionId;
use restate_types::{GenerationalNodeId, NodeId};

const EXIT_CODE_FAILURE: i32 = 1;

task_local! {
    // Current task center
    pub(self) static CURRENT_TASK_CENTER: handle::Handle;
    // Tasks provide access to their context
    static TASK_CONTEXT: TaskContext;

    /// Access to a task-level global overrides.
    static OVERRIDES: GlobalOverrides;
}

#[derive(Default, Clone)]
struct GlobalOverrides {
    metadata: Option<Metadata>,
    //config: Arc<Configuration>,
}

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("Runtime with name {0} already exists")]
    AlreadyExists(String),
    #[error(transparent)]
    Shutdown(#[from] ShutdownError),
}

/// Task center is used to manage long-running and background tasks and their lifecycle.
pub struct TaskCenter {
    _private: (),
}

impl TaskCenter {
    pub fn try_current() -> Option<Handle> {
        Self::try_with_current(Clone::clone)
    }

    pub fn try_with_current<F, R>(f: F) -> Option<R>
    where
        F: FnOnce(&Handle) -> R,
    {
        CURRENT_TASK_CENTER.try_with(|tc| f(tc)).ok()
    }

    /// Get the current task center. Use this to spawn tasks on the current task center.
    /// This must be called from within a task-center task.
    #[track_caller]
    pub fn current() -> Handle {
        Self::with_current(Clone::clone)
    }

    #[track_caller]
    pub fn with_current<F, R>(f: F) -> R
    where
        F: FnOnce(&Handle) -> R,
    {
        CURRENT_TASK_CENTER
            .try_with(|tc| f(tc))
            .expect("called outside task-center task")
    }

    #[track_caller]
    /// Attempt to access task-level overridden metadata first, if we don't have an override,
    /// fallback to task-center's level metadata.
    pub(crate) fn with_metadata<F, R>(f: F) -> Option<R>
    where
        F: FnOnce(&Metadata) -> R,
    {
        Self::with_current(|tc| tc.with_metadata(f))
    }

    /// Attempt to set the global metadata handle. This should be called once
    /// at the startup of the node.
    pub fn try_set_global_metadata(metadata: Metadata) -> bool {
        Self::with_current(|tc| tc.try_set_global_metadata(metadata))
    }

    /// Set a future callback to be executed after task center finishes shutdown.
    pub fn set_on_shutdown(on_shutdown: BoxFuture<'static, ()>) {
        Self::with_current(|tc| tc.set_on_shutdown(on_shutdown))
    }

    /// Returns true if the task center was requested to shutdown
    pub fn is_shutdown_requested() -> bool {
        Self::with_current(|tc| tc.is_shutdown_requested())
    }

    /// Returns an error if a shutdown has been requested.
    pub fn check_shutdown() -> Result<(), ShutdownError> {
        Self::with_current(|tc| tc.check_shutdown())
    }

    /// Returns true if our node ID is set and our failure detector says that we are alive.
    pub fn is_my_node_alive() -> bool {
        let Some(node_id) = Metadata::with_current(|m| m.my_node_id_opt()) else {
            return false;
        };

        Self::with_current(|tc| tc.cluster_state().is_alive(NodeId::from(node_id)))
    }

    /// Launch a new task
    #[track_caller]
    pub fn spawn<F>(
        kind: TaskKind,
        name: impl Into<SharedString>,
        future: F,
    ) -> Result<TaskId, ShutdownError>
    where
        F: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        Self::with_current(|tc| tc.spawn(kind, name, future))
    }

    /// Spawn a new task that is a child of the current task. The child task will be cancelled if the parent
    /// task is cancelled. At the moment, the parent task will not automatically wait for children tasks to
    /// finish before completion, but this might change in the future if the need for that arises.
    ///
    /// If the parent task is being cancelled at the time of spawning, this call will fail.
    /// Existing children tasks will receive cancellation signal but it's up to the task to react
    /// to the `cancellation_token()` in a cooperative manner.
    #[track_caller]
    pub fn spawn_child<F>(
        kind: TaskKind,
        name: impl Into<SharedString>,
        future: F,
    ) -> Result<TaskId, ShutdownError>
    where
        F: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        Self::with_current(|tc| tc.spawn_child(kind, name, future))
    }

    /// An unmanaged task is one that is not automatically cancelled by the task center on
    /// shutdown. Moreover, the task ID will not be registered with task center and therefore
    /// cannot be "taken" by calling [`TaskCenter::take_task`].
    ///
    /// This task is spawned as a child of the current task. If the current task is being cancelled,
    /// this will fail to spawn, but already spawned tasks will continue to run unless they are
    /// manually cancelled (hence the name "unmanaged").
    #[track_caller]
    pub fn spawn_unmanaged_child<F, T>(
        kind: TaskKind,
        name: impl Into<SharedString>,
        future: F,
    ) -> Result<TaskHandle<T>, ShutdownError>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        Self::with_current(|tc| tc.spawn_unmanaged_child(kind, name, future))
    }

    /// An unmanaged task is one that is not automatically cancelled by the task center on
    /// shutdown. Moreover, the task ID will not be registered with task center and therefore
    /// cannot be "taken" by calling [`TaskCenter::take_task`].
    #[track_caller]
    pub fn spawn_unmanaged<F, T>(
        kind: TaskKind,
        name: impl Into<SharedString>,
        future: F,
    ) -> Result<TaskHandle<T>, ShutdownError>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        Self::with_current(|tc| tc.spawn_unmanaged(kind, name, future))
    }

    /// Must be called within a Localset-scoped task, not from a normal spawned task.
    /// If ran from a non-localset task, this will panic.
    #[track_caller]
    pub fn spawn_local<F>(
        kind: TaskKind,
        name: &'static str,
        future: F,
    ) -> Result<TaskId, ShutdownError>
    where
        F: Future<Output = anyhow::Result<()>> + 'static,
    {
        Self::with_current(|tc| tc.spawn_local(kind, name, future))
    }

    /// Starts the `root_future` on a new runtime. The runtime is stopped once the root future
    /// completes.
    #[track_caller]
    pub fn start_runtime<F>(
        root_task_kind: TaskKind,
        runtime_name: &'static str,
        partition_id: Option<PartitionId>,
        root_future: impl FnOnce() -> F + Send + 'static,
    ) -> Result<RuntimeTaskHandle<anyhow::Result<()>>, RuntimeError>
    where
        F: Future<Output = anyhow::Result<()>> + 'static,
    {
        Self::with_current(|tc| {
            tc.start_runtime(root_task_kind, runtime_name, partition_id, root_future)
        })
    }

    /// Take control over the running task from task-center. This returns None if the task was not
    /// found, completed, or has been cancelled.
    #[track_caller]
    pub fn take_task(task_id: TaskId) -> Option<TaskHandle<()>> {
        Self::with_current(|tc| tc.take_task(task_id))
    }

    /// Request cancellation of a task. This returns the join handle if the task was found and was
    /// not already cancelled or completed. The returned task will not be awaited by task-center on
    /// shutdown, and it's the responsibility of the caller to join or abort.
    #[track_caller]
    pub fn cancel_task(task_id: TaskId) -> Option<TaskHandle<()>> {
        Self::with_current(|tc| tc.cancel_task(task_id))
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
    pub async fn cancel_tasks(kind: Option<TaskKind>, partition_id: Option<PartitionId>) {
        Self::current().cancel_tasks(kind, partition_id).await
    }

    /// Triggers a shutdown of the system. All running tasks will be asked gracefully
    /// to cancel but we will only wait for tasks with a TaskKind that has the property
    /// "OnCancel" set to "wait".
    pub async fn shutdown_node(reason: &str, exit_code: i32) {
        Self::current().shutdown_node(reason, exit_code).await
    }

    #[track_caller]
    pub fn shutdown_managed_runtimes() {
        Self::with_current(|tc| tc.shutdown_managed_runtimes())
    }

    /// Sets the current task_center but doesn't create a task. Use this when you need to run a
    /// future within task_center scope.
    #[track_caller]
    pub fn block_on<F, O>(&self, future: F) -> O
    where
        F: Future<Output = O>,
    {
        Self::with_current(|tc| tc.block_on(future))
    }

    /// Sets the current task_center but doesn't create a task. Use this when you need to run a
    /// closure within task_center scope.
    #[track_caller]
    pub fn run_sync<F, O>(f: F) -> O
    where
        F: FnOnce() -> O,
    {
        Self::with_current(|tc| tc.run_sync(f))
    }
}

struct TaskCenterInner {
    #[allow(dead_code)]
    /// used in Debug impl to distinguish between multiple task-centers
    id: u16,
    /// Should we start new runtimes with paused clock?
    #[allow(dead_code)]
    #[cfg(any(test, feature = "test-util"))]
    pause_time: bool,
    default_runtime_handle: tokio::runtime::Handle,
    managed_runtimes: Mutex<HashMap<SharedString, OwnedRuntimeHandle>>,
    start_time: Instant,
    /// We hold on to the owned Runtime to ensure it's dropped when task center is dropped. If this
    /// is None, it means that it's the responsibility of the Handle owner to correctly drop
    /// tokio's runtime after dropping the task center.
    #[allow(dead_code)]
    default_runtime: Option<tokio::runtime::Runtime>,
    global_cancel_token: CancellationToken,
    shutdown_requested: AtomicBool,
    current_exit_code: AtomicI32,
    managed_tasks: Mutex<HashMap<TaskId, Arc<Task>>>,
    global_metadata: OnceLock<Metadata>,
    health: Health,
    cluster_state: ClusterState,
    root_task_context: TaskContext,
    // A callback to be executed after task center finishes shutdown.
    on_shutdown: Mutex<Option<BoxFuture<'static, ()>>>,
}

impl TaskCenterInner {
    fn new(
        default_runtime_handle: tokio::runtime::Handle,
        default_runtime: Option<tokio::runtime::Runtime>,
        // used in tests to start all runtimes with clock paused. Note that this only impacts
        // partition processor runtimes
        #[cfg(any(test, feature = "test-util"))] pause_time: bool,
    ) -> Self {
        metric_definitions::describe_metrics();
        let root_task_context = TaskContext {
            id: TaskId::ROOT,
            name: "::".into(),
            kind: TaskKind::InPlace,
            cancellation_token: CancellationToken::new(),
            partition_id: None,
        };
        Self {
            id: rand::random(),
            start_time: Instant::now(),
            default_runtime_handle,
            default_runtime,
            global_cancel_token: CancellationToken::new(),
            shutdown_requested: AtomicBool::new(false),
            current_exit_code: AtomicI32::new(0),
            managed_tasks: Mutex::new(HashMap::new()),
            global_metadata: OnceLock::new(),
            managed_runtimes: Mutex::new(HashMap::with_capacity(64)),
            root_task_context,
            #[cfg(any(test, feature = "test-util"))]
            pause_time,
            cluster_state: Default::default(),
            health: Health::default(),
            on_shutdown: Mutex::default(),
        }
    }

    /// Attempt to set the global metadata handle. This should be called once
    /// at the startup of the node.
    pub fn try_set_global_metadata(self: &Arc<Self>, metadata: Metadata) -> bool {
        self.global_metadata.set(metadata).is_ok()
    }

    pub fn set_on_shutdown(&self, on_shutdown: BoxFuture<'static, ()>) {
        let mut guard = self.on_shutdown.lock();
        guard.replace(on_shutdown);
    }

    pub fn global_metadata(self: &Arc<Self>) -> Option<&Metadata> {
        self.global_metadata.get()
    }

    pub fn metadata(self: &Arc<Self>) -> Option<Metadata> {
        match OVERRIDES.try_with(|overrides| overrides.metadata.clone()) {
            Ok(Some(o)) => Some(o),
            // No metadata override, use task-center-level metadata
            _ => self.global_metadata.get().cloned(),
        }
    }

    pub fn with_metadata<F, R>(self: &Arc<Self>, f: F) -> Option<R>
    where
        F: FnOnce(&Metadata) -> R,
    {
        OVERRIDES
            .try_with(|overrides| match &overrides.metadata {
                Some(m) => Some(f(m)),
                // No metadata override, use task-center-level metadata
                None => self.global_metadata().map(f),
            })
            .ok()
            .flatten()
    }

    pub fn run_sync<F, O>(self: &Arc<Self>, f: F) -> O
    where
        F: FnOnce() -> O,
    {
        CURRENT_TASK_CENTER.sync_scope(Handle::new(self), || {
            OVERRIDES.sync_scope(OVERRIDES.try_with(Clone::clone).unwrap_or_default(), || {
                TASK_CONTEXT.sync_scope(self.with_task_context(Clone::clone), f)
            })
        })
    }

    /// Sets the current task_center but doesn't create a task. Use this when you need to run a
    /// future within task_center scope.
    pub fn block_on<F, O>(self: &Arc<Self>, future: F) -> O
    where
        F: Future<Output = O>,
    {
        self.default_runtime_handle
            .block_on(future.in_tc(&Handle::new(self)))
    }

    /// Launch a new task
    pub fn spawn<F>(
        self: &Arc<Self>,
        kind: TaskKind,
        name: impl Into<SharedString>,
        future: F,
    ) -> Result<TaskId, ShutdownError>
    where
        F: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        if self.shutdown_requested.load(Ordering::Relaxed) {
            return Err(ShutdownError);
        }
        let name = name.into();

        // spawned tasks get their own unlinked cancellation tokens
        let cancel = CancellationToken::new();
        let (parent_id, parent_name, parent_partition) =
            self.with_task_context(|ctx| (ctx.id, ctx.name.clone(), ctx.partition_id));

        if self.root_task_context.cancellation_token.is_cancelled() {
            debug!(
                kind = ?kind,
                name = %name,
                parent_task = %parent_id,
                parent_name = %parent_name,
                "Cannot start new task \"{}\" because task-center is shutting down.",
                name,
            );
            return Err(ShutdownError);
        }

        let result = self.spawn_inner(
            kind,
            name.clone(),
            parent_id,
            parent_partition,
            cancel,
            future,
        );

        trace!(
            kind = ?kind,
            name = %name,
            parent_task = %parent_id,
            "Task \"{}\" {} spawned \"{}\" {}",
            parent_name, parent_id, name, result
        );
        Ok(result)
    }

    /// Spawn a new task that is a child of the current task. The child task will be cancelled if the parent
    /// task is cancelled. At the moment, the parent task will not automatically wait for children tasks to
    /// finish before completion, but this might change in the future if the need for that arises.
    pub fn spawn_child<F>(
        self: &Arc<Self>,
        kind: TaskKind,
        name: impl Into<SharedString>,
        future: F,
    ) -> Result<TaskId, ShutdownError>
    where
        F: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        if self.shutdown_requested.load(Ordering::Relaxed) {
            return Err(ShutdownError);
        }
        let name = name.into();

        let (parent_id, parent_name, parent_kind, parent_partition, cancel) = self
            .with_task_context(|ctx| {
                (
                    ctx.id,
                    ctx.name.clone(),
                    ctx.kind,
                    ctx.partition_id,
                    ctx.cancellation_token.child_token(),
                )
            });

        if cancel.is_cancelled() {
            debug!(
                kind = ?kind,
                name = %name,
                parent_task = %parent_id,
                parent_name = %parent_name,
                "Cannot start new task {} in \"{}\" {} because parent has been cancelled.",
                name, parent_name, parent_id,
            );
            return Err(ShutdownError);
        }

        let result = self.spawn_inner(
            kind,
            name.clone(),
            parent_id,
            parent_partition,
            cancel,
            future,
        );

        trace!(
            kind = ?parent_kind,
            name = ?parent_name,
            child_kind = ?kind,
            "Task \"{}\" {} spawned \"{}\" {}",
            parent_name, parent_id, name, result
        );
        Ok(result)
    }

    pub fn spawn_unmanaged<F, T>(
        self: &Arc<Self>,
        kind: TaskKind,
        name: &SharedString,
        future: F,
    ) -> Result<TaskHandle<T>, ShutdownError>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        if self.shutdown_requested.load(Ordering::Relaxed) {
            return Err(ShutdownError);
        }
        let (parent_id, parent_name, parent_partition) =
            self.with_task_context(|ctx| (ctx.id, ctx.name.clone(), ctx.partition_id));

        if self.root_task_context.cancellation_token.is_cancelled() {
            debug!(
                kind = ?kind,
                name = %name,
                parent_task = %parent_id,
                parent_name = %parent_name,
                "Cannot start new task \"{}\" because task-center is shutting down.",
                name,
            );
            return Err(ShutdownError);
        }

        let cancel = CancellationToken::new();
        let id = TaskId::default();
        let context = TaskContext {
            id,
            name: name.clone(),
            kind,
            partition_id: parent_partition,
            cancellation_token: cancel.clone(),
        };

        let fut = unmanaged_wrapper(Arc::clone(self), context, future);

        Ok(self.spawn_on_runtime(kind, name, cancel, fut))
    }

    pub fn spawn_unmanaged_child<F, T>(
        self: &Arc<Self>,
        kind: TaskKind,
        name: &SharedString,
        future: F,
    ) -> Result<TaskHandle<T>, ShutdownError>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let (parent_id, parent_name, parent_partition, is_parent_cancelled) = self
            .with_task_context(|ctx| {
                (
                    ctx.id,
                    ctx.name.clone(),
                    ctx.partition_id,
                    ctx.cancellation_token.is_cancelled(),
                )
            });

        if is_parent_cancelled {
            debug!(
                kind = ?kind,
                name = %name,
                parent_task = %parent_id,
                parent_name = %parent_name,
                "Cannot start new task {} in \"{}\" {} because parent has been cancelled.",
                name, parent_name, parent_id,
            );
            return Err(ShutdownError);
        }

        let cancel = CancellationToken::new();
        let id = TaskId::default();
        let context = TaskContext {
            id,
            name: name.clone(),
            kind,
            partition_id: parent_partition,
            cancellation_token: cancel.clone(),
        };

        let fut = unmanaged_wrapper(Arc::clone(self), context, future);

        Ok(self.spawn_on_runtime(kind, name, cancel, fut))
    }

    /// Must be called within a Localset-scoped task, not from a normal spawned task.
    /// If ran from a non-localset task, this will panic.
    pub fn spawn_local<F>(
        self: &Arc<Self>,
        kind: TaskKind,
        name: &SharedString,
        future: F,
    ) -> Result<TaskId, ShutdownError>
    where
        F: Future<Output = anyhow::Result<()>> + 'static,
    {
        let cancellation_token = CancellationToken::new();
        let id = TaskId::default();
        let context = TaskContext {
            id,
            name: name.clone(),
            kind,
            cancellation_token: cancellation_token.clone(),
            // We must be within task-context already. let's get inherit partition_id
            partition_id: self.with_task_context(|c| c.partition_id),
        };

        let task = Arc::new(Task {
            context: context.clone(),
            handle: Mutex::new(None),
        });

        self.managed_tasks
            .lock()
            .insert(context.id, Arc::clone(&task));
        let mut handle_mut = task.handle.lock();

        let fut = wrapper(Arc::clone(self), context, future);

        let tokio_task = tokio::task::Builder::new().name(name);
        let inner_handle = tokio_task
            .spawn_local(fut)
            .expect("must run from a LocalSet");
        *handle_mut = Some(TaskHandle {
            cancellation_token,
            inner_handle,
        });

        // drop the lock
        drop(handle_mut);

        Ok(id)
    }

    /// Starts the `root_future` on a new runtime. The runtime is stopped once the root future
    /// completes.
    pub fn start_runtime<F, R>(
        self: &Arc<Self>,
        root_task_kind: TaskKind,
        runtime_name: impl Into<SharedString>,
        partition_id: Option<PartitionId>,
        root_future: impl FnOnce() -> F + Send + 'static,
    ) -> Result<RuntimeTaskHandle<R>, RuntimeError>
    where
        F: Future<Output = R> + 'static,
        R: Send + 'static,
    {
        if self.shutdown_requested.load(Ordering::Relaxed) {
            return Err(ShutdownError.into());
        }
        let cancel = CancellationToken::new();
        let runtime_name: SharedString = runtime_name.into();

        // hold a lock while creating the runtime to avoid concurrent runtimes with the same name
        let mut runtimes_guard = self.managed_runtimes.lock();
        if runtimes_guard.contains_key(&runtime_name) {
            warn!(
                "Failed to start new runtime, a runtime with name {} already exists",
                runtime_name
            );
            return Err(RuntimeError::AlreadyExists(runtime_name.into_owned()));
        }

        // todo: configure the runtime according to a new runtime kind perhaps?
        let thread_builder = std::thread::Builder::new().name(format!("rt:{runtime_name}"));
        let mut builder = tokio::runtime::Builder::new_current_thread();

        #[cfg(any(test, feature = "test-util"))]
        builder.start_paused(self.pause_time);

        let rt = builder
            .enable_all()
            .build()
            .expect("runtime builder succeeds");
        let tc = self.clone();

        let rt_handle = Arc::new(rt);

        runtimes_guard.insert(
            runtime_name.clone(),
            OwnedRuntimeHandle::new(cancel.clone(), rt_handle.clone()),
        );

        // release the lock.
        drop(runtimes_guard);

        let id = TaskId::default();
        let context = TaskContext {
            id,
            name: runtime_name.clone(),
            kind: root_task_kind,
            cancellation_token: cancel.clone(),
            partition_id,
        };

        let (result_tx, result_rx) = oneshot::channel();

        // start the work on the runtime
        let _ = thread_builder
            .spawn({
                let runtime_name = runtime_name.clone();
                move || {
                    let local_set = LocalSet::new();
                    let result = rt_handle.block_on(local_set.run_until(unmanaged_wrapper(
                        tc.clone(),
                        context,
                        root_future(),
                    )));

                    drop(rt_handle);
                    tc.drop_runtime(runtime_name);

                    // need to use an oneshot here since we cannot await a thread::JoinHandle :-(
                    let _ = result_tx.send(result);
                }
            })
            .unwrap();

        Ok(RuntimeTaskHandle::new(runtime_name, cancel, result_rx))
    }

    /// Runs **only** after the inner main thread has completed work and no other owner exists for
    /// the runtime handle.
    fn drop_runtime(self: &Arc<Self>, name: SharedString) {
        let mut runtimes_guard = self.managed_runtimes.lock();
        if let Some(runtime) = runtimes_guard.remove(&name) {
            // We must be the only owner of runtime at this point.
            debug!("Runtime {} completed", name);
            let owner = Arc::into_inner(runtime.into_inner());
            if let Some(runtime) = owner {
                runtime.shutdown_timeout(Duration::from_secs(2));
                trace!("Runtime {} shutdown completed", name);
            }
        }
    }

    #[cfg(not(tokio_taskdump))]
    async fn dump_tasks(self: &Arc<Self>, _: impl std::io::Write) {
        warn!("Cannot dump tokio tasks; tokio_taskdump was not set at compile time")
    }

    #[cfg(tokio_taskdump)]
    async fn dump_tasks(self: &Arc<Self>, mut writer: impl std::io::Write) {
        let managed_tasks: HashMap<_, _> = self
            .managed_tasks
            .lock()
            .iter()
            .filter_map(|(task_id, task)| {
                Some((
                    task.handle.lock().as_ref()?.inner_handle.id(),
                    (*task_id, task.name().to_string()),
                ))
            })
            .collect();

        let mut managed_runtimes: Vec<_> = self
            .managed_runtimes
            .lock()
            .iter()
            .map(|(name, handle)| (name.to_string(), handle.runtime_handle().clone()))
            .collect();
        managed_runtimes.sort_by(|(left_name, _), (right_name, _)| left_name.cmp(right_name));

        let mut dump = async |name: &str, handle: &tokio::runtime::Handle| {
            if let Ok(Ok(dump)) = tokio::time::timeout(
                // The future produced by Handle::dump may never produce Ready if another runtime worker is blocked for more than 250ms
                std::time::Duration::from_secs(2),
                // It is a requirement for current thread runtimes that the dump is executed *inside* the runtime
                handle.spawn({
                    let handle = handle.clone();
                    async move { handle.dump().await }
                }),
            )
            .await
            {
                let _ = writeln!(writer, "Tokio dump (runtime_name = {name}):");

                for task in dump.tasks().iter() {
                    let id = task.id();
                    let trace = task.trace();
                    if let Some((managed_task_id, task_name)) = managed_tasks.get(&id) {
                        let _ = writeln!(
                            writer,
                            "tokio_task_id = {id} task_center_name = {task_name} task_center_task_id = {managed_task_id}:"
                        );
                    } else {
                        let _ = writeln!(writer, "tokio_task_id = {id}:");
                    }
                    let _ = writeln!(writer, "{trace}\n");
                }
            }
        };

        dump("default", &self.default_runtime_handle).await;
        for (name, handle) in managed_runtimes {
            dump(&name, &handle).await;
        }
    }

    fn with_task_context<F, R>(&self, f: F) -> R
    where
        F: Fn(&TaskContext) -> R,
    {
        TASK_CONTEXT
            .try_with(|ctx| f(ctx))
            .unwrap_or_else(|_| f(&self.root_task_context))
    }

    fn spawn_inner<F>(
        self: &Arc<Self>,
        kind: TaskKind,
        name: SharedString,
        _parent_id: TaskId,
        partition_id: Option<PartitionId>,
        cancel: CancellationToken,
        future: F,
    ) -> TaskId
    where
        F: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        let inner = Arc::clone(self);
        let id = TaskId::default();
        let context = TaskContext {
            id,
            name: name.clone(),
            kind,
            partition_id,
            cancellation_token: cancel.clone(),
        };
        let task = Arc::new(Task {
            context: context.clone(),
            handle: Mutex::new(None),
        });

        inner.managed_tasks.lock().insert(id, Arc::clone(&task));

        let mut handle_mut = task.handle.lock();

        let fut = wrapper(inner, context, future);
        *handle_mut = Some(self.spawn_on_runtime(kind, &name, cancel, fut));
        // drop the lock
        drop(handle_mut);
        // Task is ready
        id
    }

    fn spawn_on_runtime<F, T>(
        self: &Arc<Self>,
        kind: TaskKind,
        name: &str,
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
            crate::AsyncRuntime::Default => &self.default_runtime_handle,
        };
        let inner_handle = tokio_task
            .spawn_on(fut, runtime)
            .expect("runtime can spawn tasks");

        TaskHandle {
            cancellation_token,
            inner_handle,
        }
    }

    async fn on_finish(
        self: &Arc<Self>,
        task_id: TaskId,
        result: std::result::Result<
            anyhow::Result<()>,
            std::boxed::Box<dyn std::any::Any + std::marker::Send>,
        >,
    ) {
        // Remove our entry from the tasks map.
        let Some(task) = self.managed_tasks.lock().remove(&task_id) else {
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
                    counter!(TC_FINISHED, "kind" => kind_str, "status" => STATUS_COMPLETED)
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
                    counter!(TC_FINISHED, "kind" => kind_str, "status" => STATUS_FAILED)
                        .increment(1);
                }
                Err(err) => {
                    counter!(TC_FINISHED, "kind" => kind_str, "status" => STATUS_FAILED)
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
                &format!(
                    "task {}({}) failed and requested a shutdown",
                    task.name(),
                    task.id()
                ),
                EXIT_CODE_FAILURE,
            )
            .await;
        }
    }

    async fn shutdown_node(self: &Arc<Self>, reason: &str, exit_code: i32) {
        if self
            .shutdown_requested
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .unwrap_or_else(|e| e)
        {
            // already shutting down....
            return;
        }

        let start = Instant::now();
        let shutdown_result = tokio::time::timeout(
            Configuration::pinned().common.shutdown_grace_period(),
            self.shutdown_node_inner(reason, exit_code),
        )
        .await;

        if shutdown_result.is_err() {
            warn!(
                "Timeout waiting for graceful shutdown. Shutdown took {:?}",
                start.elapsed()
            );
        } else {
            info!("Restate has gracefully shutdown in {:?}", start.elapsed());
        };
        self.root_task_context.cancel();
        self.global_cancel_token.cancel();
    }

    async fn shutdown_node_inner(self: &Arc<Self>, reason: &str, exit_code: i32) {
        self.health.node_status().merge(NodeStatus::ShuttingDown);
        self.current_exit_code.store(exit_code, Ordering::Relaxed);

        if exit_code != 0 {
            warn!(%reason, "** Shutdown requested");
        } else {
            info!(%reason, "** Shutdown requested");
        }
        self.cancel_tasks(Some(TaskKind::ClusterController), None)
            .await;
        // stop accepting ingress
        self.cancel_tasks(Some(TaskKind::HttpIngressRole), None)
            .await;
        // Worker will shutdown running processors
        self.cancel_tasks(Some(TaskKind::WorkerRole), None).await;
        self.initiate_managed_runtimes_shutdown();
        // Ask bifrost to shutdown providers and loglets
        self.cancel_tasks(Some(TaskKind::BifrostWatchdog), None)
            .await;
        self.shutdown_managed_runtimes();
        // global shutdown trigger
        self.root_task_context.cancel();
        self.cancel_tasks(None, None).await;
        // notify outer components that we have completed the shutdown.
        let on_shutdown = self.on_shutdown.lock().take();
        if let Some(on_shutdown) = on_shutdown {
            on_shutdown.await;
        }
        info!("Task center has stopped");
        self.global_cancel_token.cancel();
    }

    /// Take control over the running task from task-center. This returns None if the task was not
    /// found, completed, or has been cancelled.
    pub fn take_task(self: &Arc<Self>, task_id: TaskId) -> Option<TaskHandle<()>> {
        let task = {
            // find the task
            let mut tasks = self.managed_tasks.lock();
            tasks.remove(&task_id)?
        };

        let mut task_mut = task.handle.lock();
        // Task is not running anymore or a cancellation is already in progress.
        task_mut.take()
    }

    /// Request cancellation of a task. This returns the join handle if the task was found and was
    /// not already cancelled or completed. The returned task will not be awaited by task-center on
    /// shutdown, and it's the responsibility of the caller to join or abort.
    pub fn cancel_task(self: &Arc<Self>, task_id: TaskId) -> Option<TaskHandle<()>> {
        let task = {
            // find the task
            let tasks = self.managed_tasks.lock();
            let task = tasks.get(&task_id)?;
            // request cancellation
            task.cancel();
            Arc::clone(task)
        };

        let mut task_mut = task.handle.lock();
        // Task is not running anymore or a cancellation is already in progress.
        task_mut.take()
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
    async fn cancel_tasks(
        self: &Arc<Self>,
        kind: Option<TaskKind>,
        partition_id: Option<PartitionId>,
    ) {
        let mut victims = Vec::new();

        {
            let tasks = self.managed_tasks.lock();
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

    fn initiate_managed_runtimes_shutdown(self: &Arc<Self>) {
        let runtimes = self.managed_runtimes.lock();
        for (name, runtime) in runtimes.iter() {
            // asking the root task in the runtime to shutdown gracefully.
            runtime.cancel();
            trace!("Asked runtime {} to shutdown gracefully", name);
        }
    }

    fn shutdown_managed_runtimes(self: &Arc<Self>) {
        let mut runtimes = self.managed_runtimes.lock();
        for (_, runtime) in runtimes.drain() {
            if let Some(runtime) = Arc::into_inner(runtime.into_inner()) {
                // This really isn't doing much, but it's left here for completion.
                // The reason is: If the runtime is still running, then it'll hold the Arc until it
                // finishes gracefully, yielding None here. If the runtime completed, it'll
                // self-shutdown prior to reaching this point.
                runtime.shutdown_background();
            }
        }
    }
}

/// This wrapper function runs in a newly-spawned task. It initializes the
/// task-local variables and wraps the inner future.
async fn wrapper<F>(inner: Arc<TaskCenterInner>, context: TaskContext, future: F)
where
    F: Future<Output = anyhow::Result<()>> + 'static,
{
    let id = context.id;
    trace!(kind = ?context.kind, name = ?context.name, "Starting task {}", context.id);

    let result = CURRENT_TASK_CENTER
        .scope(
            Handle::new(&inner),
            OVERRIDES.scope(
                OVERRIDES.try_with(Clone::clone).unwrap_or_default(),
                TASK_CONTEXT.scope(
                    context,
                    // We use AssertUnwindSafe here so that the wrapped function
                    // doesn't need to be UnwindSafe. We should not do anything after
                    // unwinding that'd risk us being in unwind-unsafe behavior.
                    AssertUnwindSafe(future).catch_unwind(),
                ),
            ),
        )
        .await;
    inner.on_finish(id, result).await;
}

/// Like wrapper but doesn't call on_finish nor it catches panics
async fn unmanaged_wrapper<F, T>(inner: Arc<TaskCenterInner>, context: TaskContext, future: F) -> T
where
    F: Future<Output = T> + 'static,
{
    trace!(kind = ?context.kind, name = ?context.name, "Starting task {}", context.id);

    CURRENT_TASK_CENTER
        .scope(
            Handle::new(&inner),
            OVERRIDES.scope(
                OVERRIDES.try_with(Clone::clone).unwrap_or_default(),
                TASK_CONTEXT.scope(context, future),
            ),
        )
        .await
}

/// Access to this node id. This is available in task-center tasks only!
#[track_caller]
pub fn my_node_id() -> GenerationalNodeId {
    // todo: migrate call-sites
    Metadata::with_current(|m| m.my_node_id())
}

/// The current task-center task Id. This returns None if we are not in the scope
/// of a task-center task.
pub fn current_task_id() -> Option<TaskId> {
    TaskContext::try_with_current(|ctx| ctx.id())
}

/// The current partition Id associated to the running task-center task.
pub fn current_task_partition_id() -> Option<PartitionId> {
    TaskContext::try_with_current(|ctx| ctx.partition_id()).flatten()
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
    let res = TaskContext::try_with_current(|ctx| ctx.cancellation_token().clone());

    if cfg!(any(test, feature = "test-util")) {
        // allow in tests to call from non-task-center tasks.
        res.unwrap_or_default()
    } else {
        res.expect("cancellation_token() called in in a task-center task")
    }
}

/// Has the current task been requested to cancel?
pub fn is_cancellation_requested() -> bool {
    TaskContext::try_with_current(|ctx| ctx.cancellation_token().is_cancelled()).unwrap_or_else(
        || {
            if cfg!(any(test, feature = "test-util")) {
                warn!("is_cancellation_requested() called outside task-center context");
            }
            false
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    use googletest::prelude::*;
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
            .build()?
            .into_handle();
        let start = tokio::time::Instant::now();
        tc.spawn(TaskKind::RoleRunner, "worker-role", async {
            info!("Hello async");
            tokio::time::sleep(Duration::from_secs(10)).await;
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
