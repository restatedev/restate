// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};

use futures::FutureExt;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use restate_types::SharedString;
/// A handle for a dedicated runtime managed by task-center
pub struct RuntimeTaskHandle<T> {
    name: SharedString,
    cancellation_token: CancellationToken,
    inner_handle: oneshot::Receiver<T>,
}

impl<T> RuntimeTaskHandle<T> {
    pub fn new(
        name: SharedString,
        cancellation_token: CancellationToken,
        result_receiver: oneshot::Receiver<T>,
    ) -> Self {
        Self {
            name,
            cancellation_token,
            inner_handle: result_receiver,
        }
    }
    // The runtime  name
    pub fn name(&self) -> &SharedString {
        &self.name
    }
    /// Trigger graceful shutdown of the runtime root task. Shutdown is not guaranteed, it depends
    /// on whether the root task awaits the cancellation token or not.
    pub fn cancel(&self) {
        self.cancellation_token.cancel()
    }

    pub fn cancellation_token(&self) -> &CancellationToken {
        &self.cancellation_token
    }
}

impl<T> std::future::Future for RuntimeTaskHandle<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(
            ready!(self.inner_handle.poll_unpin(cx)).expect("runtime panicked unexpectedly"),
        )
    }
}

pub(super) struct OwnedRuntimeHandle {
    cancellation_token: CancellationToken,
    inner: Arc<tokio::runtime::Runtime>,
}

impl OwnedRuntimeHandle {
    pub fn new(
        cancellation_token: CancellationToken,
        runtime: Arc<tokio::runtime::Runtime>,
    ) -> Self {
        Self {
            cancellation_token,
            inner: runtime,
        }
    }

    // The runtime name
    pub fn runtime_handle(&self) -> &tokio::runtime::Handle {
        self.inner.handle()
    }

    /// Trigger graceful shutdown of the runtime root task. Shutdown is not guaranteed, it depends
    /// on whether the root task awaits the cancellation token or not.
    pub fn cancel(&self) {
        self.cancellation_token.cancel()
    }

    pub fn into_inner(self) -> Arc<tokio::runtime::Runtime> {
        self.inner
    }
}
