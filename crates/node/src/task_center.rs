// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use futures::FutureExt;
use restate_types::identifiers::PartitionId;
use restate_types::tasks::{TaskId, TaskKind};
use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::Future;
use once_cell::sync::Lazy;
use tokio::task::JoinHandle;
use tokio::task_local;
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};
use tracing::{debug, error, info, instrument, warn};

static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);
static TASK_CENTER: Lazy<TaskCenter> = Lazy::new(TaskCenter::default);
static CURRENT_EXIT_CODE: AtomicI32 = AtomicI32::new(0);

const EXIT_CODE_FAILURE: i32 = 1;
const TASK_CANCELLATION_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, thiserror::Error)]
#[error("system is shutting down")]
pub struct ShutdownError;

#[derive(Default)]
pub struct TaskCenter {
    global_cancel_token: CancellationToken,
}

impl TaskCenter {
    /// Used to monitor an on-going shutdown when requested
    pub fn watch_shutdown() -> WaitForCancellationFutureOwned {
        TASK_CENTER.global_cancel_token.clone().cancelled_owned()
    }

    /// The exit code that the process should exit with.
    pub fn exit_code() -> i32 {
        CURRENT_EXIT_CODE.load(Ordering::Acquire)
    }

    /// Triggers a shutdown of the system. All running tasks will be asked gracefully
    /// to cancel within a given timeout.
    #[instrument(level = "error", skip(exit_code))]
    pub async fn shutdown_node(reason: &str, exit_code: i32) {
        if SHUTDOWN_REQUESTED.load(Ordering::Acquire) {
            // already shutting down....
            return;
        }
        SHUTDOWN_REQUESTED.store(true, Ordering::Release);
        let start = Instant::now();
        CURRENT_EXIT_CODE.store(exit_code, Ordering::Release);

        if exit_code != 0 {
            info!("** Shutdown requested");
        } else {
            warn!("** Shutdown requested");
        }
        // todo: define a controlled shutdown by task kind as needed.
        cancel_tasks(None, None).await;
        // notify outer components that we have completed the shutdown.
        TASK_CENTER.global_cancel_token.cancel();
        info!("** Shutdown completed in {:?}", start.elapsed());
    }
}

static TASKS: Lazy<Mutex<HashMap<TaskId, Arc<Task>>>> = Lazy::new(|| Mutex::new(HashMap::new()));
static NEXT_TASK_ID: AtomicU64 = AtomicU64::new(0);

pub struct Task {
    /// It's nice to have a unique ID for each task.
    id: TaskId,
    name: &'static str,
    // TODO: consider tracking when it was created.
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
}

#[track_caller]
fn spawn_inner<F>(
    kind: TaskKind,
    name: &'static str,
    partition_id: Option<PartitionId>,
    shutdown_node_on_failure: bool,
    cancel: CancellationToken,
    future: F,
) -> TaskId
where
    F: Future<Output = anyhow::Result<()>> + Send + 'static,
{
    let id = TaskId::from(NEXT_TASK_ID.fetch_add(1, Ordering::Relaxed));
    let task = Arc::new(Task {
        id,
        name,
        kind,
        partition_id,
        cancel: cancel.clone(),
        join_handle: Mutex::new(None),
    });

    // TODO: decide which index to keep in
    TASKS.lock().unwrap().insert(id, Arc::clone(&task));

    let mut handle_mut = task.join_handle.lock().unwrap();

    let task_cloned = Arc::clone(&task);
    let join_handle = tokio::spawn(wrapper(
        id,
        kind,
        task_cloned,
        cancel,
        shutdown_node_on_failure,
        future,
    ));
    *handle_mut = Some(join_handle);
    drop(handle_mut);

    // Task is ready
    id
}

/// Launch a new task
/// Note: if shutdown_process_on_error is set to true failure
///   of the task will lead to shutdown of entire process
#[track_caller]
pub fn spawn<F>(
    kind: TaskKind,
    name: &'static str,
    partition_id: Option<PartitionId>,
    shutdown_node_on_failure: bool,
    future: F,
) -> Result<TaskId, ShutdownError>
where
    F: Future<Output = anyhow::Result<()>> + Send + 'static,
{
    if SHUTDOWN_REQUESTED.load(Ordering::Acquire) {
        return Err(ShutdownError);
    }
    Ok(spawn_unchecked(
        kind,
        name,
        partition_id,
        shutdown_node_on_failure,
        future,
    ))
}

#[track_caller]
pub fn spawn_unchecked<F>(
    kind: TaskKind,
    name: &'static str,
    partition_id: Option<PartitionId>,
    shutdown_node_on_failure: bool,
    future: F,
) -> TaskId
where
    F: Future<Output = anyhow::Result<()>> + Send + 'static,
{
    let cancel = CancellationToken::new();
    spawn_inner(
        kind,
        name,
        partition_id,
        shutdown_node_on_failure,
        cancel,
        future,
    )
}

#[track_caller]
pub fn spawn_child<F>(
    kind: TaskKind,
    name: &'static str,
    partition_id: Option<PartitionId>,
    shutdown_node_on_failure: bool,
    future: F,
) -> Result<TaskId, ShutdownError>
where
    F: Future<Output = anyhow::Result<()>> + Send + 'static,
{
    if SHUTDOWN_REQUESTED.load(Ordering::Acquire) {
        return Err(ShutdownError);
    }

    let cancel = cancel_token().child_token();
    Ok(spawn_inner(
        kind,
        name,
        partition_id,
        shutdown_node_on_failure,
        cancel,
        future,
    ))
}

#[track_caller]
pub fn spawn_child_unchecked<F>(
    kind: TaskKind,
    name: &'static str,
    partition_id: Option<PartitionId>,
    shutdown_node_on_failure: bool,
    future: F,
) -> TaskId
where
    F: Future<Output = anyhow::Result<()>> + Send + 'static,
{
    let cancel = cancel_token().child_token();
    spawn_inner(
        kind,
        name,
        partition_id,
        shutdown_node_on_failure,
        cancel,
        future,
    )
}

/// This wrapper function runs in a newly-spawned task. It initializes the
/// task-local variables and calls the payload function.
async fn wrapper<F>(
    task_id: TaskId,
    kind: TaskKind,
    task: Arc<Task>,
    cancel_token: CancellationToken,
    shutdown_process_on_error: bool,
    future: F,
) where
    F: Future<Output = anyhow::Result<()>> + Send + 'static,
{
    debug!(kind = ?kind, name = ?task.name, "Starting task {}", task_id);

    let result = CANCEL_TOKEN
        .scope(
            cancel_token,
            CURRENT_TASK.scope(task, {
                // We use AssertUnwindSafe here so that the wrapped function
                // doesn't need to be UnwindSafe. We should not do anything after
                // unwinding that'd risk us being in unwind-unsafe behavior.
                AssertUnwindSafe(future).catch_unwind()
            }),
        )
        .await;
    on_finish(result, kind, task_id, shutdown_process_on_error).await;
}

async fn on_finish(
    result: std::result::Result<
        anyhow::Result<()>,
        std::boxed::Box<dyn std::any::Any + std::marker::Send>,
    >,
    kind: TaskKind,
    task_id: TaskId,
    shutdown_node_on_failure: bool,
) {
    // Remove our entry from the global hashmap.
    let task = TASKS
        .lock()
        .unwrap()
        .remove(&task_id)
        .expect("task in task center");

    let mut request_node_shutdown = false;
    {
        match result {
            Ok(Ok(())) => {
                debug!(kind = ?kind, name = ?task.name, "Task {} exited normally", task_id);
            }
            Ok(Err(err)) => {
                if shutdown_node_on_failure {
                    error!(kind = ?kind, name = ?task.name,
                        "Shutting down: task {} failed with: {:?}",
                        task_id, err
                    );
                    request_node_shutdown = true;
                } else {
                    error!(kind = ?kind, name = ?task.name, "Task {} failed with: {:?}", task_id, err);
                }
            }
            Err(err) => {
                if shutdown_node_on_failure {
                    error!(kind = ?kind, name = ?task.name, "Shutting down: task {} panicked: {:?}", task_id, err);
                    request_node_shutdown = true;
                } else {
                    error!(kind = ?kind, name = ?task.name, "Task {} panicked: {:?}", task_id, err);
                }
            }
        }
    }

    if request_node_shutdown {
        TaskCenter::shutdown_node(
            &format!("task {} failed and requested a shutdown", task.name),
            EXIT_CODE_FAILURE,
        )
        .await;
    }
}

pub fn cancel_task(task_id: TaskId) -> Result<Option<JoinHandle<()>>, anyhow::Error> {
    let task = {
        // find the task
        let tasks = TASKS.lock().unwrap();
        let task = tasks
            .get(&task_id)
            .ok_or(anyhow::anyhow!("unknown task id"))?;
        // request cancellation
        task.cancel.cancel();
        Arc::clone(task)
    };

    let join_handle = {
        let mut task_mut = task.join_handle.lock().unwrap();
        // Task is not running anymore.
        task_mut.take()
    };
    Ok(join_handle)
}

/// Signal and wait for tasks to stop.
///
///
/// You can select which tasks to cancel. Any None arguments are ignored.
/// For example, to shut down all MetadataSync tasks:
///
///   cancel_tasks(Some(TaskKind::MetadataSync), None)
///
/// Or to shut down all tasks for a particular partition ID:
///
///   cancel_tasks(None, Some(partition_id))
///
pub async fn cancel_tasks(kind: Option<TaskKind>, partition_id: Option<PartitionId>) {
    let mut victims = Vec::new();

    {
        let tasks = TASKS.lock().unwrap();
        for task in tasks.values() {
            if (kind.is_none() || Some(task.kind) == kind)
                && (partition_id.is_none() || task.partition_id == partition_id)
            {
                task.cancel.cancel();
                victims.push((Arc::clone(task), task.kind, task.partition_id));
            }
        }
    }

    let log_all = kind.is_none() && partition_id.is_none();

    for (task, task_kind, partition_id) in victims {
        let join_handle = {
            let mut task_mut = task.join_handle.lock().unwrap();
            // Task is not running anymore.
            task_mut.take()
        };
        if let Some(mut join_handle) = join_handle {
            if log_all {
                if partition_id.is_none() {
                    // global tasks (non partition-id specific)
                    info!(kind = ?task_kind, name = ?task.name, "stopping global task");
                } else {
                    // warn to catch these in tests; there shouldn't be any
                    warn!(kind = ?task_kind, name = ?task.name, partition_id = ?partition_id, "stopping left-over");
                }
            }
            if tokio::time::timeout(TASK_CANCELLATION_TIMEOUT / 2, &mut join_handle)
                .await
                .is_err()
            {
                // allow some time to elapse before logging to give the task a chance to finish
                // before we have to log anything.
                info!(kind = ?task_kind, name = ?task.name, "still waiting for task {} to shutdown", task.id);
                if tokio::time::timeout(TASK_CANCELLATION_TIMEOUT / 2, &mut join_handle)
                    .await
                    .is_err()
                {
                    warn!(kind = ?task_kind, name = ?task.name, "timed out waiting for task {} to finish gracefully, ignoring!", task.id);
                } else {
                    info!(kind = ?task_kind, name = ?task.name, "task {} completed", task.id);
                }
            }
        } else {
            // Possibly one of:
            //  * The task had not even fully started yet.
            //  * It was shut down concurrently and already exited (or failed)
        }
    }
}

pub fn current_task_kind() -> Option<TaskKind> {
    CURRENT_TASK.try_with(|ct| ct.kind).ok()
}

pub fn current_task_id() -> Option<TaskId> {
    CURRENT_TASK.try_with(|ct| ct.id).ok()
}

pub fn current_task_partition_id() -> Option<PartitionId> {
    CURRENT_TASK.try_with(|ct| ct.partition_id).ok().flatten()
}

/// A Future that can be used to check if the current task has been requested to
/// shut down.
pub async fn cancel_watcher() {
    let token = CANCEL_TOKEN
        .try_with(|t| t.clone())
        .expect("cancel_watcher() called in a task-center task");

    token.cancelled().await;
}

/// Clone the current task's cancellation token, which can be moved across tasks.
///
/// When the task which is currently executing is shutdown, the cancellation token will be
/// cancelled. It can however be moved to other tasks, such as `tokio::task::spawn_blocking` or
/// `tokio::task::JoinSet::spawn`.
pub fn cancel_token() -> CancellationToken {
    let res = CANCEL_TOKEN.try_with(|t| t.clone());

    if cfg!(test) {
        // allow in tests to call from non-task-center tasks.
        res.unwrap_or_default()
    } else {
        res.expect("cancel_token() called in in a task-center task")
    }
}

/// Has the current task been requested to cancel?
pub fn is_cancellation_requested() -> bool {
    if let Ok(cancel) = CANCEL_TOKEN.try_with(|t| t.clone()) {
        cancel.is_cancelled()
    } else {
        if !cfg!(test) {
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
    use tracing_test::traced_test;

    #[tokio::test(start_paused = true)]
    #[traced_test]
    async fn test_basic_lifecycle() -> Result<()> {
        let start = tokio::time::Instant::now();
        let tid = spawn(TaskKind::RoleRunner, "worker-role", None, false, async {
            info!("Hello async");
            tokio::time::sleep(Duration::from_secs(10)).await;
            assert_eq!(TaskKind::RoleRunner, current_task_kind().unwrap());
            info!("Bye async");
            Ok(())
        })
        .unwrap();

        println!("tid: {}", tid);
        cancel_task(tid).unwrap().unwrap().await.unwrap();
        assert!(logs_contain("Hello async"));
        assert!(logs_contain("Bye async"));
        assert!(start.elapsed() >= Duration::from_secs(10));
        Ok(())
    }
}
