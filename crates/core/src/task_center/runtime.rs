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
use std::task::{ready, Context, Poll};

use futures::FutureExt;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

/// A handle for a dedicated runtime managed by task-center
pub struct RuntimeRootTaskHandle<T> {
    pub(crate) cancellation_token: CancellationToken,
    pub(crate) inner_handle: oneshot::Receiver<T>,
}

impl<T> RuntimeRootTaskHandle<T> {
    /// Trigger graceful shutdown of the runtime root task. Shutdown is not guaranteed, it depends
    /// on whether the root task awaits the cancellation token or not.
    pub fn cancel(&self) {
        self.cancellation_token.cancel()
    }

    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation_token.clone()
    }
}

impl<T> std::future::Future for RuntimeRootTaskHandle<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(
            ready!(self.inner_handle.poll_unpin(cx)).expect("runtime panicked unexpectedly"),
        )
    }
}
