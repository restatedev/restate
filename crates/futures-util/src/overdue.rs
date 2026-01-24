// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Display;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::time::Duration;

use pin_project_lite::pin_project;
use tokio::time::{Instant, Sleep};
use tracing::{Level, debug, error, info, trace, warn};

const MAX_REPEAT_DURATION: Duration = const { Duration::from_secs(30) };

/// Adds the ability to override task-center for a future and all its children
pub trait OverdueLoggingExt: Future + Sized {
    /// Logs a message at the given level if the future takes longer than the given duration after
    /// its creation. After the first log, it repeats logging with exponential backoff duration
    /// clamped at 30 seconds.
    fn log_slow_after<M>(
        self,
        after: Duration,
        level: Level,
        message: M,
    ) -> WithOverdueLogging<Self, M>
    where
        M: Display + Send + Sync + 'static,
    {
        WithOverdueLogging {
            future: self,
            slow_level: level,
            overdue_level: level,
            repeat_duration_iter: ExponentialBackoff::new(after),
            overdue_after: None,
            is_already_overdue: false,
            started_at: Instant::now(),
            has_logged: false,
            delay: tokio::time::sleep_until(Instant::now() + after),
            message,
        }
    }
}

struct ExponentialBackoff {
    current: Duration,
}

impl ExponentialBackoff {
    /// Create a new ExponentialBackoff iterator with the given start
    fn new(start: Duration) -> Self {
        ExponentialBackoff { current: start }
    }

    fn advance(&mut self) {
        // Attempt to multiply by factor=2; clamp at `MAX_DURATION`
        self.current = self
            .current
            .saturating_mul(2)
            .clamp(self.current, MAX_REPEAT_DURATION);
    }
}

impl<F: Future> OverdueLoggingExt for F {}

pin_project! {
    pub struct WithOverdueLogging<F, M> {
        #[pin]
        future: F,
        slow_level: Level,
        started_at: Instant,
        repeat_duration_iter:ExponentialBackoff,

        overdue_level: Level,
        overdue_after: Option<Duration>,
        is_already_overdue: bool,
        has_logged: bool,

        #[pin]
        delay: Sleep,
        message: M,
    }
}

impl<F, M> WithOverdueLogging<F, M>
where
    F: Future,
    M: Display + Send + Sync + 'static,
{
    /// Marks this future as overdue and logs at `level` from this point onwards.
    /// Panics if duration is smaller that the slow logging duration
    pub fn with_overdue(mut self, duration: Duration, level: Level) -> Self {
        assert!(
            duration >= self.repeat_duration_iter.current,
            "log_overdue_after accepts durations that are >= initial slow duration"
        );
        self.overdue_after = Some(duration);
        self.overdue_level = level;
        self
    }
}

impl<F: Future, M> Future for WithOverdueLogging<F, M>
where
    M: Display + Send + Sync + 'static,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // projecting so we can operate on futures that are not Unpinned.
        let mut this = self.project();

        let mut should_log = false;
        // fixing (now) as one time reference that everything is calculated from to avoid drifts.
        let now = Instant::now();
        // Check how long we've been waiting so far.
        let elapsed_since_start = now.saturating_duration_since(*this.started_at);
        let (mut level, mut label) = if *this.is_already_overdue {
            (*this.overdue_level, "overdue")
        } else {
            (*this.slow_level, "slow")
        };

        // are we overdue and we haven't logged overdue yet?
        if !*this.is_already_overdue
            && this
                .overdue_after
                .is_some_and(|overdue| elapsed_since_start >= overdue)
        {
            level = *this.overdue_level;
            label = "overdue";
            should_log = true;
            *this.is_already_overdue = true;
            this.delay
                .as_mut()
                .reset(now + this.repeat_duration_iter.current);
        }

        // Are we slow? If yes, should we log again?
        if this.delay.is_elapsed() || should_log {
            *this.has_logged = true;
            log_message(this.message, level, elapsed_since_start, label);
        }

        // Forward the poll to the underlying pinned future.
        match this.future.poll(cx) {
            Poll::Ready(res) if *this.has_logged => {
                // if we have logged, we should also say that we completed
                // Note that we log at the last level used (i.e. overdue's level) if we are already
                // overdue. This is to ensure that we have symmetry in logging. If we logged on a WARN
                // level that an operation is overdue, we'd also want to see that the operation has
                // been completed on the same level, otherwise we may never know that it did.
                log_completion(this.message, level, elapsed_since_start);
                return Poll::Ready(res);
            }
            Poll::Ready(res) => {
                return Poll::Ready(res);
            }
            Poll::Pending => {}
        }

        // Wait for the delay
        ready!(this.delay.as_mut().poll(cx));

        this.repeat_duration_iter.advance();
        let normal_next_point = now + this.repeat_duration_iter.current;
        let new_deadline = if !*this.is_already_overdue && this.overdue_after.is_some() {
            let overdue_at = now
                + this
                    .overdue_after
                    .unwrap()
                    .saturating_sub(elapsed_since_start);
            // rationale here is, if we are already at the overdue point, we should have printed
            // the overdue message above and is_already_overdue should be true.
            // everything is calculated from a fixed `now` to make checks like these possible.
            debug_assert!(overdue_at > now);
            std::cmp::min(overdue_at, normal_next_point)
        } else {
            // we are overdue already
            normal_next_point
        };
        this.delay.as_mut().reset(new_deadline);
        // to make sure we register the waker
        let r = this.delay.poll(cx);
        if !r.is_pending() {
            // we need to wake up the future to make sure it's polled again
            cx.waker().wake_by_ref();
        }
        Poll::Pending
    }
}

fn log_message<M: Display>(message: &M, level: Level, elapsed: Duration, label: &str) {
    match level {
        Level::ERROR => error!(?elapsed, "[{label}] {message}"),
        Level::WARN => warn!(?elapsed, "[{label}] {message}"),
        Level::INFO => info!(?elapsed, "[{label}] {message}"),
        Level::DEBUG => debug!(?elapsed, "[{label}] {message}"),
        Level::TRACE => trace!(?elapsed, "[{label}] {message}"),
    }
}

fn log_completion<M: Display>(message: &M, level: Level, elapsed: Duration) {
    match level {
        Level::ERROR => error!(?elapsed, "[completed] {message}"),
        Level::WARN => warn!(?elapsed, "[completed] {message}"),
        Level::INFO => info!(?elapsed, "[completed] {message}"),
        Level::DEBUG => debug!(?elapsed, "[completed] {message}"),
        Level::TRACE => trace!(?elapsed, "[completed] {message}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::Duration;

    use tracing_test::traced_test;

    #[tokio::test(start_paused = true)]
    #[traced_test]
    async fn no_log_slow() {
        let future = tokio::time::sleep(Duration::from_millis(500)).log_slow_after(
            Duration::from_secs(2),
            Level::DEBUG,
            "sleep operation",
        );
        future.await;
        logs_assert(|lines: &[&str]| {
            match lines
                .iter()
                .filter(|line| line.contains("sleep operation"))
                .count()
            {
                0 => Ok(()),
                n => Err(format!("Expected no matching logs, but found {n}")),
            }
        });
    }

    #[tokio::test(start_paused = true)]
    #[traced_test]
    async fn log_slow() {
        let future = tokio::time::sleep(Duration::from_secs(2)).log_slow_after(
            Duration::from_millis(500),
            Level::DEBUG,
            "sleep operation",
        );
        future.await;
        assert!(logs_contain("[slow] sleep operation elapsed=500ms"));
        assert!(logs_contain("[slow] sleep operation elapsed=1.5s"));
        assert!(logs_contain("[completed] sleep operation elapsed=2s"));
        logs_assert(|lines: &[&str]| {
            match lines
                .iter()
                .filter(|line| line.contains("sleep operation"))
                .count()
            {
                3 => Ok(()),
                n => Err(format!("Expected 3 matching logs, but found {n}")),
            }
        });
    }

    #[tokio::test(start_paused = true)]
    #[traced_test]
    async fn log_overdue_dedup() {
        // 10 seconds of total sleep
        let future = tokio::time::sleep(Duration::from_secs(10))
            .log_slow_after(Duration::from_millis(500), Level::DEBUG, "sleep operation")
            .with_overdue(Duration::from_millis(3200), Level::WARN);
        future.await;

        assert!(logs_contain("[slow] sleep operation elapsed=500ms"));
        // 1s later
        assert!(logs_contain("[slow] sleep operation elapsed=1.5s"));
        // 3.2 (the overdue point) is closer than 1.5+2=3.5, so we should see overdue sooner than 4.5 elapsed time
        assert!(logs_contain("[overdue] sleep operation elapsed=3.2s"));
        // we use the next (unused) duration from the previous run (2s)
        // we expect that 3.2s+2s = 5.2s is our next notification point
        assert!(logs_contain("[overdue] sleep operation elapsed=5.2s"));
        // back to normal, next point is 4s after 5.2s = 9.2s
        assert!(logs_contain("[overdue] sleep operation elapsed=9.2s"));
        // operation finishes before the next tick which is after 8s (9.2+8=17.2s)
        assert!(logs_contain("[completed] sleep operation elapsed=10s"));
        logs_assert(|lines: &[&str]| {
            match lines
                .iter()
                .filter(|line| line.contains("sleep operation"))
                .count()
            {
                6 => Ok(()),
                n => Err(format!("Expected 6 matching logs, but found {n}")),
            }
        });
    }

    #[tokio::test(start_paused = true)]
    #[traced_test]
    async fn log_overdue_normal() {
        // 5 minutes sleep
        let future = async {
            tokio::time::sleep(Duration::from_millis(35900)).await;
        }
        .log_slow_after(Duration::from_millis(500), Level::DEBUG, "sleep operation")
        .with_overdue(Duration::from_secs(10), Level::WARN);

        future.await;
        assert!(logs_contain("[slow] sleep operation elapsed=500ms"));
        // 1s
        assert!(logs_contain("[slow] sleep operation elapsed=1.5s"));
        // 2s
        assert!(logs_contain("[slow] sleep operation elapsed=3.5s"));
        // 4s
        assert!(logs_contain("[slow] sleep operation elapsed=7.5s"));
        // over due at 10s
        assert!(logs_contain("[overdue] sleep operation elapsed=10s"));
        // 8s
        assert!(logs_contain("[overdue] sleep operation elapsed=18s"));
        // 16s
        assert!(logs_contain("[overdue] sleep operation elapsed=34s"));
        assert!(logs_contain("[completed] sleep operation elapsed=35.9s"));
        logs_assert(|lines: &[&str]| {
            match lines
                .iter()
                .filter(|line| line.contains("sleep operation"))
                .count()
            {
                8 => Ok(()),
                n => Err(format!("Expected 8 matching logs, but found {n}")),
            }
        });
    }
}
