// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::Future;
use std::pin::Pin;

use pin_project_lite::pin_project;
use tokio::task::futures::TaskLocalFuture;
use tokio_util::sync::CancellationToken;

use restate_types::SharedString;

use crate::Metadata;
use crate::task_center::TaskContext;

use super::{
    CURRENT_TASK_CENTER, GlobalOverrides, Handle, OVERRIDES, TASK_CONTEXT, TaskCenter, TaskId,
    TaskKind,
};

type TaskCenterFuture<F> =
    TaskLocalFuture<Handle, TaskLocalFuture<GlobalOverrides, TaskLocalFuture<TaskContext, F>>>;

/// Adds the ability to override task-center for a future and all its children
pub trait TaskCenterFutureExt: Sized {
    /// Ensures that a future will run within a task-center context. This will inherit the current
    /// task context (if there is one). Otherwise, it'll run in the context of the root task (task-id=0).
    fn in_tc(self, task_center: &Handle) -> WithTaskCenter<Self>;

    /// Lets task-center treat this future as a pseudo-task. It gets its own TaskId and an
    /// independent cancellation token. However, task-center will not spawn this as a task nor
    /// manage its lifecycle.
    fn in_tc_as_task<S>(
        self,
        task_center: &Handle,
        kind: TaskKind,
        name: S,
    ) -> WithTaskCenter<Self>
    where
        S: Into<SharedString>;

    /// Ensures that a future will run within the task-center in current scope. This will inherit the current
    /// task context (if there is one). Otherwise, it'll run in the context of the root task (task-id=0).
    ///
    /// This is useful when running dispatching a future as a task on an external runtime/thread,
    /// or when running a future on tokio's JoinSet without representing those tokio tasks as
    /// task-center tasks. However, in the latter case, it's preferred to use
    /// [`Self::in_current_ts_as_task`] instead.
    fn in_current_tc(self) -> WithTaskCenter<Self>;

    /// Attaches current task-center and lets it treat the future as a pseudo-task. It gets its own TaskId and an
    /// independent cancellation token. However, task-center will not spawn this as a task nor
    /// manage its lifecycle.
    fn in_current_tc_as_task<S>(self, kind: TaskKind, name: S) -> WithTaskCenter<Self>
    where
        S: Into<SharedString>;
}

pin_project! {
    pub struct WithTaskCenter<F> {
        #[pin]
        inner_fut: TaskCenterFuture<F>,
    }
}

impl<F, O> TaskCenterFutureExt for F
where
    F: Future<Output = O>,
{
    fn in_tc(self, task_center: &Handle) -> WithTaskCenter<Self> {
        let ctx = task_center.with_task_context(Clone::clone);

        let inner = CURRENT_TASK_CENTER.scope(
            task_center.clone(),
            OVERRIDES.scope(
                OVERRIDES.try_with(Clone::clone).unwrap_or_default(),
                TASK_CONTEXT.scope(ctx, self),
            ),
        );
        WithTaskCenter { inner_fut: inner }
    }

    fn in_tc_as_task<S>(self, task_center: &Handle, kind: TaskKind, name: S) -> WithTaskCenter<Self>
    where
        S: Into<SharedString>,
    {
        let name = name.into();
        let ctx = task_center.with_task_context(move |parent| TaskContext {
            id: TaskId::default(),
            name: name.clone(),
            kind,
            cancellation_token: CancellationToken::new(),
            partition_id: parent.partition_id,
        });

        let inner = CURRENT_TASK_CENTER.scope(
            task_center.clone(),
            OVERRIDES.scope(
                OVERRIDES.try_with(Clone::clone).unwrap_or_default(),
                TASK_CONTEXT.scope(ctx, self),
            ),
        );
        WithTaskCenter { inner_fut: inner }
    }

    /// Ensures that a future will run within a task-center context. This will inherit the current
    /// task context (if there is one). Otherwise, it'll run in the context of the root task (task-id=0).
    #[track_caller]
    fn in_current_tc(self) -> WithTaskCenter<Self> {
        TaskCenter::with_current(|tc| self.in_tc(tc))
    }

    #[track_caller]
    fn in_current_tc_as_task<S>(self, kind: TaskKind, name: S) -> WithTaskCenter<Self>
    where
        S: Into<SharedString>,
    {
        TaskCenter::with_current(|tc| self.in_tc_as_task(tc, kind, name))
    }
}

impl<T: Future> Future for WithTaskCenter<T> {
    type Output = T::Output;

    fn poll(
        self: Pin<&mut Self>,
        ctx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.project();
        this.inner_fut.poll(ctx)
    }
}

/// Adds the ability to override Metadata for a future and all its children
pub trait MetadataFutureExt: Sized {
    /// Attaches restate's Metadata as an override on a future and all children futures or
    /// task-center tasks spawned from it.
    fn with_metadata(self, metadata: &Metadata) -> WithMetadata<Self>;
}

pin_project! {
    pub struct WithMetadata<F> {
        #[pin]
        inner_fut: TaskLocalFuture<GlobalOverrides, F>,
    }
}

impl<F, O> MetadataFutureExt for F
where
    F: Future<Output = O>,
{
    fn with_metadata(self, metadata: &Metadata) -> WithMetadata<Self> {
        // temporary mute until overrides include more fields
        #[allow(unused_variables)]
        let current_overrides = OVERRIDES.try_with(Clone::clone).unwrap_or_default();
        // temporary mute until overrides include more fields
        #[allow(clippy::needless_update)]
        let overrides = GlobalOverrides {
            metadata: Some(metadata.clone()),
            ..current_overrides
        };
        let inner = OVERRIDES.scope(overrides, self);
        WithMetadata { inner_fut: inner }
    }
}

impl<T: Future> Future for WithMetadata<T> {
    type Output = T::Output;

    fn poll(
        self: Pin<&mut Self>,
        ctx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.project();
        this.inner_fut.poll(ctx)
    }
}
