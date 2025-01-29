// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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
use std::task::{Context, Poll};
use std::time::Duration;

use pin_project_lite::pin_project;
use tokio::time::Instant;

/// Adds the ability to override task-center for a future and all its children
pub trait OverdueFutureExt: Future + Sized {
    /// Runs a function a single if future takes longer than duration. The execution
    /// latency is measured from the first poll of the inner future.
    ///
    /// It runs the `complete_op` function only if the duration exceeded the limit
    /// and only after the `op` function has been called.
    fn with_overdue_action<O1, O2>(
        self,
        after: Duration,
        op: O1,
        complete_op: O2,
    ) -> WithOverdue<Self, O1, O2>
    where
        O1: FnMut(Duration),
        O2: FnMut(Duration),
    {
        WithOverdue {
            future: self,
            duration: after,
            start: None,
            has_triggered: false,
            op,
            complete_op,
        }
    }
}

impl<F: Future> OverdueFutureExt for F {}

pin_project! {
    pub struct WithOverdue<F, O1, O2> {
        #[pin]
        future: F,
        duration: Duration,
        start: Option<Instant>,
        has_triggered: bool,
        op: O1,
        complete_op: O2,
    }
}

impl<F: Future, O1, O2> Future for WithOverdue<F, O1, O2>
where
    O1: FnMut(Duration),
    O2: FnMut(Duration),
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // projecting so we can operate on futures that are not Unpinned.
        let this = self.project();
        // only set the start at the first poll.
        if this.start.is_none() {
            *this.start = Some(Instant::now());
        }
        // Check how long we've been waiting so far.
        let elapsed = this.start.unwrap().elapsed();
        if !*this.has_triggered && elapsed > *this.duration {
            (this.op)(elapsed);
            // Ensures we only log once.
            *this.has_triggered = true;
        }

        // Forward the poll to the underlying pinned future.
        let res = this.future.poll(cx);
        if res.is_ready() && *this.has_triggered {
            // if we have logged, we should also say that we completed
            (this.complete_op)(elapsed);
        }
        res
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test(start_paused = true)]
    async fn overdue_action_passed() {
        let mut overdue = false;
        let mut completed = false;
        let future = tokio::time::sleep(Duration::from_secs(2)).with_overdue_action(
            Duration::from_secs(1),
            |elapsed| {
                overdue = true;
                assert!(elapsed >= Duration::from_secs(1));
            },
            |elapsed| {
                completed = true;
                assert!(elapsed >= Duration::from_secs(1));
            },
        );
        future.await;
        assert!(overdue);
        assert!(completed);
    }

    #[tokio::test(start_paused = true)]
    async fn overdue_action_not_passed() {
        let mut overdue = false;
        let mut completed = false;
        let future = tokio::time::sleep(Duration::from_millis(500)).with_overdue_action(
            Duration::from_secs(1),
            |elapsed| {
                overdue = true;
                assert!(elapsed >= Duration::from_secs(1));
            },
            |elapsed| {
                completed = true;
                assert!(elapsed >= Duration::from_secs(1));
            },
        );
        future.await;
        assert!(!overdue);
        assert!(!completed);
    }
}
