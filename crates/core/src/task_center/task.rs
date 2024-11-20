// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;

use restate_types::identifiers::PartitionId;

use super::{TaskId, TaskKind};
use crate::{Metadata, ShutdownError};

#[derive(Clone)]
pub(super) struct TaskContext {
    /// It's nice to have a unique ID for each task.
    pub(super) id: TaskId,
    pub(super) name: &'static str,
    pub(super) kind: TaskKind,
    /// cancel this token to request cancelling this task.
    pub(super) cancellation_token: CancellationToken,
    /// Tasks associated with a specific partition ID will have this set. This allows
    /// for cancellation of tasks associated with that partition.
    pub(super) partition_id: Option<PartitionId>,
    /// Access to a locally-cached metadata view.
    pub(super) metadata: Option<Metadata>,
}

pub(super) struct Task<R = ()> {
    pub(super) context: TaskContext,
    pub(super) handle: Mutex<Option<TaskHandle<R>>>,
}

impl<R> Task<R> {
    pub fn id(&self) -> TaskId {
        self.context.id
    }

    pub fn name(&self) -> &'static str {
        self.context.name
    }

    pub fn kind(&self) -> TaskKind {
        self.context.kind
    }

    pub fn partition_id(&self) -> Option<PartitionId> {
        self.context.partition_id
    }

    pub fn cancel(&self) {
        self.context.cancellation_token.cancel()
    }
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
