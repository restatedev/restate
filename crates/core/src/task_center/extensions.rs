// Copyright (c) 2023 - 2025  Restate Software, Inc., Restate GmbH.
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

use crate::Metadata;

use super::{GlobalOverrides, TaskCenter, CURRENT_TASK_CENTER, OVERRIDES};

/// Adds the ability to override task-center for a future and all its children
pub trait TaskCenterFutureExt: Sized {
    fn with_task_center(self, task_center: &TaskCenter) -> WithTaskCenter<Self>;
    fn in_current_task_center(self) -> WithTaskCenter<Self>;
}

pin_project! {
    pub struct WithTaskCenter<F> {
        #[pin]
        inner_fut: TaskLocalFuture<TaskCenter, TaskLocalFuture<GlobalOverrides, F>>,
    }
}

impl<F, O> TaskCenterFutureExt for F
where
    F: Future<Output = O>,
{
    fn with_task_center(self, task_center: &TaskCenter) -> WithTaskCenter<Self> {
        let inner = CURRENT_TASK_CENTER.scope(
            task_center.clone(),
            OVERRIDES.scope(OVERRIDES.try_with(Clone::clone).unwrap_or_default(), self),
        );
        WithTaskCenter { inner_fut: inner }
    }

    fn in_current_task_center(self) -> WithTaskCenter<Self> {
        TaskCenter::with_current(|tc| self.with_task_center(tc))
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
