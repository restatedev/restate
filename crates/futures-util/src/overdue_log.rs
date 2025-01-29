// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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
use std::task::{Context, Poll};
use std::time::Duration;

use pin_project_lite::pin_project;
use tokio::time::Instant;
use tracing::{debug, error, info, trace, warn, Level};

const MAX_REPEAT_DURATION: Duration = const { Duration::from_secs(30) };

/// Adds the ability to override task-center for a future and all its children
pub trait OverdueLoggingExt: Future + Sized {
    /// Logs a message at the given level if the future takes longer than the given duration after
    /// its first poll. After the first log, it repeats logging with exponential backoff duration
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
            started_at: None,
            last_logged: None,
            message,
        }
    }
}

struct ExponentialBackoff {
    current: Duration,
}

impl ExponentialBackoff {
    /// Create a new ExponentialBackoff iterator with the given start, max, and factor.
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
        started_at: Option<Instant>,
        repeat_duration_iter:ExponentialBackoff,

        overdue_level: Level,
        overdue_after: Option<Duration>,
        is_already_overdue: bool,

        last_logged: Option<Instant>,
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
        let this = self.project();
        // only set the start at the first poll.
        if this.started_at.is_none() {
            *this.started_at = Some(Instant::now());
        }

        let (mut level, mut label) = if *this.is_already_overdue {
            (*this.overdue_level, "overdue")
        } else {
            (*this.slow_level, "slow")
        };

        // Check how long we've been waiting so far.
        let elapsed_since_start = this.started_at.unwrap().elapsed();

        // are we overdue and we haven't logged overdue yet?
        if !*this.is_already_overdue
            && this
                .overdue_after
                .is_some_and(|overdue| elapsed_since_start > overdue)
        {
            level = *this.overdue_level;
            label = "overdue";
            log_message(this.message, level, elapsed_since_start, label);
            *this.last_logged = Some(Instant::now());
            *this.is_already_overdue = true;
        }

        // Are we slow? If yes, should we log again?
        if this
            .last_logged
            .is_some_and(|last_logged| last_logged.elapsed() > this.repeat_duration_iter.current)
            || (elapsed_since_start > this.repeat_duration_iter.current
                && this.last_logged.is_none())
        {
            log_message(this.message, level, elapsed_since_start, label);
            *this.last_logged = Some(Instant::now());
            this.repeat_duration_iter.advance();
        }

        // Forward the poll to the underlying pinned future.
        let res = this.future.poll(cx);
        if res.is_ready() && this.last_logged.is_some() {
            // if we have logged, we should also say that we completed
            // Note that we log at the last level used (i.e. overdue's level) if we are already
            // overdue. This is to ensure that we have symmetry in logging. If we logged on a WARN
            // level that an operation is overdue, we'd also want to see that the operation has
            // been completed on the same level, otherwise we may never know that it did.
            log_completion(this.message, level, elapsed_since_start);
        }
        res
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
            "false op",
        );
        future.await;
        logs_assert(|lines: &[&str]| {
            match lines
                .iter()
                .filter(|line| line.contains("sleep operation"))
                .count()
            {
                0 => Ok(()),
                n => Err(format!("Expected no matching logs, but found {}", n)),
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
        logs_contain("[slow] sleep operation elapsed=2s");
        logs_contain("[completed] sleep operation elapsed=2s");
        logs_assert(|lines: &[&str]| {
            match lines
                .iter()
                .filter(|line| line.contains("sleep operation"))
                .count()
            {
                2 => Ok(()),
                n => Err(format!("Expected two matching logs, but found {}", n)),
            }
        });
    }

    #[tokio::test(start_paused = true)]
    #[traced_test]
    async fn log_overdue_dedup() {
        // 5 minutes sleep
        let future = tokio::time::sleep(Duration::from_secs(5 * 60))
            .log_slow_after(Duration::from_millis(500), Level::DEBUG, "sleep operation")
            .with_overdue(Duration::from_secs(10), Level::WARN);
        future.await;

        // in this test, tokio will only poll after the 300s, so we want to make sure we have only
        // printed the overdue and now the slow
        logs_contain("[overdue] sleep operation elapsed=300s");
        logs_contain("[completed] sleep operation elapsed=300s");
        logs_assert(|lines: &[&str]| {
            match lines
                .iter()
                .filter(|line| line.contains("sleep operation"))
                .count()
            {
                2 => Ok(()),
                n => Err(format!("Expected 3 matching logs, but found {}", n)),
            }
        });
    }

    #[tokio::test(start_paused = true)]
    #[traced_test]
    async fn log_overdue_normal() {
        // 5 minutes sleep
        let future = async {
            tokio::time::sleep(Duration::from_millis(200)).await;
            // crosses the slow threshold, but not overdue.
            tokio::time::sleep(Duration::from_millis(500)).await;
            // still slow, but should remind again
            tokio::time::sleep(Duration::from_millis(1200)).await;
            // no reminder, next one is if total time crossed 2000
            tokio::time::sleep(Duration::from_millis(500)).await;
            // we just did
            tokio::time::sleep(Duration::from_millis(1500)).await;
            // now overdue
            tokio::time::sleep(Duration::from_secs(11)).await;
            // capped at 30 seconds, we should see another overdue
            tokio::time::sleep(Duration::from_secs(20)).await;
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        .log_slow_after(Duration::from_millis(500), Level::DEBUG, "sleep operation")
        .with_overdue(Duration::from_secs(10), Level::WARN);

        future.await;
        // in this test, tokio will only poll after the 300s, so we want to make sure we have only
        // printed the overdue and now the slow
        logs_contain("[slow] sleep operation elapsed=700ms");
        logs_contain("[slow] sleep operation elapsed=1.9s");
        logs_contain("[overdue] sleep operation elapsed=14.9s");
        logs_contain("[overdue] sleep operation elapsed=34.9s");
        logs_contain("[completed] sleep operation elapsed=35.9s");
        logs_assert(|lines: &[&str]| {
            match lines
                .iter()
                .filter(|line| line.contains("sleep operation"))
                .count()
            {
                5 => Ok(()),
                n => Err(format!("Expected 3 matching logs, but found {}", n)),
            }
        });
    }
}
