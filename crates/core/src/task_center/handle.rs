// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::any::Any;
use std::fmt::Debug;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use futures::FutureExt;
use tokio_util::sync::CancellationToken;
use tracing::instrument;

use restate_types::SharedString;
use restate_types::cluster_state::{ClusterState, ClusterStateUpdater};
use restate_types::health::{Health, NodeStatus};
use restate_types::identifiers::PartitionId;

use crate::{Metadata, ShutdownError};

use super::{
    RuntimeError, RuntimeTaskHandle, TaskCenterInner, TaskContext, TaskHandle, TaskId, TaskKind,
};

#[derive(Clone, derive_more::Debug)]
#[debug("TaskCenter({})", inner.id)]
pub struct Handle {
    pub(super) inner: Arc<TaskCenterInner>,
}

static_assertions::assert_impl_all!(Handle: Send, Sync, Clone);

impl Handle {
    pub(super) fn new(inner: &Arc<TaskCenterInner>) -> Self {
        Self {
            inner: Arc::clone(inner),
        }
    }

    pub(crate) fn with_task_context<F, R>(&self, f: F) -> R
    where
        F: Fn(&TaskContext) -> R,
    {
        self.inner.with_task_context(f)
    }

    /// Attempt to access task-level overridden metadata first, if we don't have an override,
    /// fallback to task-center's level metadata.
    pub(crate) fn with_metadata<F, R>(&self, f: F) -> Option<R>
    where
        F: FnOnce(&Metadata) -> R,
    {
        self.inner.with_metadata(f)
    }

    /// Attempt to set the global metadata handle. This should be called once
    /// at the startup of the node.
    pub fn try_set_global_metadata(&self, metadata: Metadata) -> bool {
        self.inner
            .health
            .node_status()
            .merge(NodeStatus::StartingUp);
        self.inner.try_set_global_metadata(metadata)
    }

    pub fn health(&self) -> &Health {
        &self.inner.health
    }

    pub fn cluster_state(&self) -> &ClusterState {
        &self.inner.cluster_state
    }

    pub fn cluster_state_updater(&self) -> ClusterStateUpdater {
        self.inner.cluster_state.clone().updater()
    }

    /// Returns true if the task center was requested to shutdown
    pub fn is_shutdown_requested(&self) -> bool {
        self.inner.shutdown_requested.load(Ordering::Relaxed)
    }

    /// Returns an error if a shutdown has been requested.
    pub fn check_shutdown(&self) -> Result<(), ShutdownError> {
        if self.is_shutdown_requested() {
            return Err(ShutdownError);
        }
        Ok(())
    }

    /// Sets the current task_center but doesn't create a task. Use this when you need to run a
    /// closure within task_center scope.
    ///
    /// # Panics
    /// If your closure tries to spawn tasks with [`TaskKind`] that inherit the runtime, then you
    /// need to call this method from within a Tokio runtime. Otherwise, this method panics.
    pub fn run_sync<F, O>(&self, f: F) -> O
    where
        F: FnOnce() -> O,
    {
        self.inner.run_sync(f)
    }

    /// Sets the current task_center but doesn't create a task. Use this when you need to run a
    /// future within task_center scope.
    pub fn block_on<F, O>(&self, future: F) -> O
    where
        F: Future<Output = O>,
    {
        self.inner.block_on(future)
    }

    pub fn start_runtime<F, R>(
        &self,
        root_task_kind: TaskKind,
        runtime_name: impl Into<SharedString>,
        partition_id: Option<PartitionId>,
        root_future: impl FnOnce() -> F + Send + 'static,
    ) -> Result<RuntimeTaskHandle<R>, RuntimeError>
    where
        F: Future<Output = R> + 'static,
        R: Send + Debug + 'static,
    {
        self.inner
            .start_runtime(root_task_kind, runtime_name, partition_id, root_future)
    }

    /// Launch a new task
    pub fn spawn<F>(
        &self,
        kind: TaskKind,
        name: impl Into<SharedString>,
        future: F,
    ) -> Result<TaskId, ShutdownError>
    where
        F: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        let name = name.into();
        self.inner.spawn(kind, &name, future)
    }

    /// Spawn a new task that is a child of the current task. The child task will be cancelled if the parent
    /// task is cancelled. At the moment, the parent task will not automatically wait for children tasks to
    /// finish before completion, but this might change in the future if the need for that arises.
    pub fn spawn_child<F>(
        &self,
        kind: TaskKind,
        name: impl Into<SharedString>,
        future: F,
    ) -> Result<TaskId, ShutdownError>
    where
        F: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        self.inner.spawn_child(kind, name, future)
    }

    /// An unmanaged task is one that is not automatically cancelled by the task center on
    /// shutdown. Moreover, the task ID will not be registered with task center and therefore
    /// cannot be "taken" by calling [`TaskCenter::take_task`].
    pub fn spawn_unmanaged<F, T>(
        &self,
        kind: TaskKind,
        name: impl Into<SharedString>,
        future: F,
    ) -> Result<TaskHandle<T>, ShutdownError>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let name = name.into();
        self.inner.spawn_unmanaged(kind, &name, future)
    }

    /// Must be called within a Localset-scoped task, not from a normal spawned task.
    /// If ran from a non-localset task, this will panic.
    pub fn spawn_local<F>(
        &self,
        kind: TaskKind,
        name: impl Into<SharedString>,
        future: F,
    ) -> Result<TaskId, ShutdownError>
    where
        F: Future<Output = anyhow::Result<()>> + 'static,
    {
        let name = name.into();
        self.inner.spawn_local(kind, &name, future)
    }

    pub fn metadata(&self) -> Option<Metadata> {
        self.inner.metadata()
    }

    /// Take control over the running task from task-center. This returns None if the task was not
    /// found, completed, or has been cancelled.
    pub fn take_task(&self, task_id: TaskId) -> Option<TaskHandle<()>> {
        self.inner.take_task(task_id)
    }

    /// Request cancellation of a task. This returns the join handle if the task was found and was
    /// not already cancelled or completed. The returned task will not be awaited by task-center on
    /// shutdown, and it's the responsibility of the caller to join or abort.
    pub fn cancel_task(&self, task_id: TaskId) -> Option<TaskHandle<()>> {
        self.inner.cancel_task(task_id)
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
        self.inner.cancel_tasks(kind, partition_id).await
    }

    pub fn shutdown_managed_runtimes(&self) {
        self.inner.shutdown_managed_runtimes()
    }

    pub async fn dump_tasks(&self, writer: impl std::io::Write) {
        self.inner.dump_tasks(writer).await
    }

    /// Triggers a shutdown of the system. All running tasks will be asked gracefully
    /// to cancel but we will only wait for tasks with a TaskKind that has the property
    /// "OnCancel" set to "wait".
    #[instrument(level = "error", skip(self, exit_code))]
    pub async fn shutdown_node(&self, reason: &str, exit_code: i32) {
        self.inner.shutdown_node(reason, exit_code).await;
    }

    /// Use to monitor an on-going shutdown when requested
    pub fn shutdown_token(&self) -> CancellationToken {
        self.inner.global_cancel_token.clone()
    }
}

// Shutsdown
pub struct OwnedHandle {
    inner: Arc<TaskCenterInner>,
}

impl OwnedHandle {
    pub(super) fn new(inner: TaskCenterInner) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }

    pub fn to_handle(&self) -> Handle {
        Handle::new(&self.inner)
    }

    pub fn into_handle(self) -> Handle {
        Handle { inner: self.inner }
    }

    /// Sets the current task_center but doesn't create a task. Use this when you need to run a
    /// future within task_center scope.
    pub fn block_on<F, O>(&self, future: F) -> Result<O, Box<dyn Any + Send + 'static>>
    where
        F: Future<Output = O>,
    {
        // We use AssertUnwindSafe here so that the wrapped function
        // doesn't need to be UnwindSafe. We should not do anything after
        // unwinding that'd risk us being in unwind-unsafe behavior.
        self.inner.block_on(AssertUnwindSafe(future).catch_unwind())
    }

    /// The exit code that the process should exit with.
    pub fn exit_code(&self) -> i32 {
        self.inner.current_exit_code.load(Ordering::Relaxed)
    }
}
