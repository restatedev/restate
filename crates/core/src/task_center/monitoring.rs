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
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use enum_map::EnumMap;
use metrics::{counter, gauge};
use pin_project_lite::pin_project;
use tokio::runtime::RuntimeMetrics;

use restate_platform::prelude::ReString;

use super::{Handle, TaskKind};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DefaultRuntimeTaskSnapshot {
    /// Total TaskCenter tasks scheduled with this provenance.
    pub spawned: u64,
    /// TaskCenter tasks that have not completed or been dropped yet.
    pub active: u64,
    /// Highest active count observed since the previous TaskCenter sample.
    pub peak_active: u64,
    /// Total outer future polls for TaskCenter tasks with this provenance.
    pub poll_count: u64,
    /// Cumulative wall duration spent inside outer future polls for this provenance.
    ///
    /// This is attribution evidence, not literal executor CPU occupancy: it can include time in
    /// `block_in_place` and other synchronous work performed during a poll.
    pub poll_wall_duration: Duration,
}

/// A best-effort, racy sample of default-runtime TaskCenter-context provenance.
///
/// This does not prove the executor for tasks that carry a TaskCenter context through `in_tc` or
/// pseudo-task helpers. Tokio and TaskCenter counters are sampled independently, so their
/// difference is only an approximate unattributed-task signal.
#[derive(Debug)]
pub struct DefaultRuntimeTaskStats {
    pub tokio_alive_tasks: usize,
    pub tracked_active_tasks: u64,
    pub task_kinds: EnumMap<TaskKind, DefaultRuntimeTaskSnapshot>,
}

struct TaskKindStats {
    spawned: AtomicU64,
    active: AtomicU64,
    peak_active: AtomicU64,
    poll_count: AtomicU64,
    poll_wall_duration_nanos: AtomicU64,
}

/// Tracks tasks carrying default-runtime TaskCenter provenance at TaskCenter scheduling points.
pub(super) struct DefaultRuntimeTaskTracker {
    task_kinds: EnumMap<TaskKind, TaskKindStats>,
}

pub(super) struct DefaultRuntimeTaskGuard {
    // A queued future can outlive its scheduler call, so the guard owns the tracker. This temporary
    // diagnostic pays one Arc clone per TaskCenter task lifetime for safe cancellation accounting.
    tracker: Arc<DefaultRuntimeTaskTracker>,
    kind: TaskKind,
}

pin_project! {
    /// Counts outer polls while retaining the task-lifetime guard.
    pub(super) struct DefaultRuntimeTaskFuture<F> {
        #[pin]
        future: F,
        guard: DefaultRuntimeTaskGuard,
    }
}

impl DefaultRuntimeTaskTracker {
    pub(super) fn new() -> Self {
        Self {
            task_kinds: EnumMap::from_fn(|_| TaskKindStats {
                spawned: AtomicU64::new(0),
                active: AtomicU64::new(0),
                peak_active: AtomicU64::new(0),
                poll_count: AtomicU64::new(0),
                poll_wall_duration_nanos: AtomicU64::new(0),
            }),
        }
    }

    pub(super) fn track(self: &Arc<Self>, kind: TaskKind) -> DefaultRuntimeTaskGuard {
        let stats = &self.task_kinds[kind];
        stats.spawned.fetch_add(1, Ordering::Relaxed);
        let active = stats.active.fetch_add(1, Ordering::Relaxed) + 1;
        let mut peak = stats.peak_active.load(Ordering::Relaxed);
        while active > peak {
            match stats.peak_active.compare_exchange_weak(
                peak,
                active,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(current) => peak = current,
            }
        }

        DefaultRuntimeTaskGuard {
            tracker: Arc::clone(self),
            kind,
        }
    }

    fn record_poll(&self, kind: TaskKind) {
        self.task_kinds[kind]
            .poll_count
            .fetch_add(1, Ordering::Relaxed);
    }

    fn record_poll_wall_duration(&self, kind: TaskKind, duration: Duration) {
        let duration_nanos = u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX);
        let total = &self.task_kinds[kind].poll_wall_duration_nanos;
        let _ = total.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            Some(current.saturating_add(duration_nanos))
        });
    }

    fn snapshot(&self, tokio_alive_tasks: usize) -> DefaultRuntimeTaskStats {
        let task_kinds = EnumMap::from_fn(|kind| {
            let stats = &self.task_kinds[kind];
            DefaultRuntimeTaskSnapshot {
                spawned: stats.spawned.load(Ordering::Relaxed),
                active: stats.active.load(Ordering::Relaxed),
                peak_active: stats.peak_active.swap(0, Ordering::Relaxed),
                poll_count: stats.poll_count.load(Ordering::Relaxed),
                poll_wall_duration: Duration::from_nanos(
                    stats.poll_wall_duration_nanos.load(Ordering::Relaxed),
                ),
            }
        });
        let tracked_active_tasks = task_kinds.iter().map(|(_, stats)| stats.active).sum();

        DefaultRuntimeTaskStats {
            tokio_alive_tasks,
            tracked_active_tasks,
            task_kinds,
        }
    }
}

impl<F> DefaultRuntimeTaskFuture<F> {
    pub(super) fn new(future: F, guard: DefaultRuntimeTaskGuard) -> Self {
        Self { future, guard }
    }
}

impl<F: Future> Future for DefaultRuntimeTaskFuture<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        this.guard.tracker.record_poll(this.guard.kind);
        let poll_started = Instant::now();
        let result = this.future.poll(cx);
        this.guard
            .tracker
            .record_poll_wall_duration(this.guard.kind, poll_started.elapsed());
        result
    }
}

impl Drop for DefaultRuntimeTaskGuard {
    fn drop(&mut self) {
        let stats = &self.tracker.task_kinds[self.kind];
        let previous = stats.active.fetch_sub(1, Ordering::Relaxed);
        debug_assert!(previous > 0);
    }
}

pub trait TaskCenterMonitoring {
    fn default_runtime_metrics(&self) -> RuntimeMetrics;

    fn default_runtime_task_stats(&self) -> DefaultRuntimeTaskStats;

    fn managed_runtime_metrics(&self) -> Vec<(ReString, RuntimeMetrics)>;

    fn managed_runtime_metric(&self, runtime: &str) -> Option<RuntimeMetrics>;

    /// How long has the task-center been running?
    fn age(&self) -> Duration;

    /// Submit telemetry for all runtimes to metrics recorder
    fn submit_metrics(&self);
}

impl TaskCenterMonitoring for Handle {
    fn default_runtime_metrics(&self) -> RuntimeMetrics {
        self.inner.default_runtime_handle.metrics()
    }

    fn default_runtime_task_stats(&self) -> DefaultRuntimeTaskStats {
        self.inner
            .default_runtime_task_tracker
            .snapshot(self.default_runtime_metrics().num_alive_tasks())
    }

    fn managed_runtime_metrics(&self) -> Vec<(ReString, RuntimeMetrics)> {
        let guard = self.inner.managed_runtimes.lock();
        guard
            .iter()
            .map(|(k, v)| (k.clone(), v.runtime_handle().metrics()))
            .collect()
    }

    fn managed_runtime_metric(&self, runtime: &str) -> Option<RuntimeMetrics> {
        let guard = self.inner.managed_runtimes.lock();
        guard
            .get(runtime)
            .map(|runtime| runtime.runtime_handle().metrics())
    }

    /// How long has the task-center been running?
    fn age(&self) -> Duration {
        self.inner.start_time.elapsed()
    }

    /// Submit telemetry for all runtimes to metrics recorder
    fn submit_metrics(&self) {
        submit_runtime_metrics("default", self.default_runtime_metrics());

        // Partition processor runtimes
        let processor_runtimes = self.managed_runtime_metrics();
        for (task_name, metrics) in processor_runtimes {
            submit_runtime_metrics(task_name, metrics);
        }
    }
}

fn submit_runtime_metrics(runtime: impl Into<ReString>, stats: RuntimeMetrics) {
    let runtime: ReString = runtime.into();
    #[cfg(debug_assertions)]
    {
        let labels = [("runtime", runtime.clone())];
        gauge!("restate.tokio.num_workers", &labels).set(stats.num_workers() as f64);
        gauge!("restate.tokio.blocking_threads", &labels).set(stats.num_blocking_threads() as f64);
        gauge!("restate.tokio.blocking_queue_depth", &labels)
            .set(stats.blocking_queue_depth() as f64);
        gauge!("restate.tokio.num_alive_tasks", &labels).set(stats.num_alive_tasks() as f64);
        gauge!("restate.tokio.io_driver_ready_count", &labels)
            .set(stats.io_driver_ready_count() as f64);
        counter!("restate.tokio.remote_schedule_count", &labels)
            .absolute(stats.remote_schedule_count());
    }
    // per worker stats
    for idx in 0..stats.num_workers() {
        let labels = [
            ("runtime", runtime.clone()),
            ("worker", idx.to_string().into()),
        ];
        #[cfg(debug_assertions)]
        {
            counter!("restate.tokio.worker_overflow_count", &labels)
                .absolute(stats.worker_overflow_count(idx));
            counter!("restate.tokio.worker_park_count", &labels)
                .absolute(stats.worker_park_count(idx));
            counter!("restate.tokio.worker_noop_count", &labels)
                .absolute(stats.worker_noop_count(idx));
            counter!("restate.tokio.worker_steal_count", &labels)
                .absolute(stats.worker_steal_count(idx));
            gauge!("restate.tokio.worker_total_busy_duration_seconds", &labels)
                .set(stats.worker_total_busy_duration(idx).as_secs_f64());
        }
        // Main metrics we want in non-debug mode
        counter!("restate.tokio.worker_poll_count", &labels).absolute(stats.worker_poll_count(idx));
        gauge!("restate.tokio.worker_mean_poll_time", &labels)
            .set(stats.worker_mean_poll_time(idx).as_secs_f64());
    }
}

#[cfg(test)]
mod tests {
    use std::future;
    use std::sync::Arc;
    use std::task::{Context, Poll};
    use std::time::Duration;

    use futures::task::noop_waker_ref;

    use super::{DefaultRuntimeTaskFuture, DefaultRuntimeTaskTracker, TaskKind};

    #[test]
    fn queued_default_task_is_counted_before_its_first_poll() {
        let tracker = Arc::new(DefaultRuntimeTaskTracker::new());
        let guard = tracker.track(TaskKind::H2ServerStream);
        let queued = async move {
            let _guard = guard;
            future::pending::<()>().await;
        };

        let stats = tracker.snapshot(1);
        let h2 = &stats.task_kinds[TaskKind::H2ServerStream];
        assert_eq!(h2.spawned, 1);
        assert_eq!(h2.active, 1);
        assert_eq!(h2.peak_active, 1);
        assert_eq!(h2.poll_count, 0);
        assert_eq!(stats.tracked_active_tasks, 1);

        drop(queued);

        let stats = tracker.snapshot(0);
        let h2 = &stats.task_kinds[TaskKind::H2ServerStream];
        assert_eq!(h2.active, 0);
        assert_eq!(h2.peak_active, 0);
        assert_eq!(stats.tracked_active_tasks, 0);
    }

    #[test]
    fn tracks_interval_peak_active_tasks() {
        let tracker = Arc::new(DefaultRuntimeTaskTracker::new());
        let first = tracker.track(TaskKind::SequencerAppender);
        let second = tracker.track(TaskKind::SequencerAppender);

        let stats = tracker.snapshot(2);
        let appender = &stats.task_kinds[TaskKind::SequencerAppender];
        assert_eq!(appender.active, 2);
        assert_eq!(appender.peak_active, 2);

        drop((first, second));
    }

    #[test]
    fn tracks_default_task_polls_after_first_poll() {
        let tracker = Arc::new(DefaultRuntimeTaskTracker::new());
        let mut task = Box::pin(DefaultRuntimeTaskFuture::new(
            future::poll_fn(|_| {
                std::thread::sleep(Duration::from_millis(1));
                Poll::<()>::Pending
            }),
            tracker.track(TaskKind::H2ServerStream),
        ));
        let mut cx = Context::from_waker(noop_waker_ref());

        let before_first_poll = tracker.snapshot(1);
        assert_eq!(
            before_first_poll.task_kinds[TaskKind::H2ServerStream].poll_count,
            0
        );
        assert_eq!(
            before_first_poll.task_kinds[TaskKind::H2ServerStream].poll_wall_duration,
            Duration::default()
        );

        assert!(task.as_mut().poll(&mut cx).is_pending());
        let after_first_poll = tracker.snapshot(1);
        assert_eq!(
            after_first_poll.task_kinds[TaskKind::H2ServerStream].poll_count,
            1
        );
        assert!(
            after_first_poll.task_kinds[TaskKind::H2ServerStream].poll_wall_duration
                >= Duration::from_millis(1)
        );
        assert_eq!(
            after_first_poll.task_kinds[TaskKind::H2ServerStream]
                .poll_count
                .saturating_sub(before_first_poll.task_kinds[TaskKind::H2ServerStream].poll_count),
            1
        );
        assert!(
            after_first_poll.task_kinds[TaskKind::H2ServerStream]
                .poll_wall_duration
                .saturating_sub(
                    before_first_poll.task_kinds[TaskKind::H2ServerStream].poll_wall_duration
                )
                >= Duration::from_millis(1)
        );

        assert!(task.as_mut().poll(&mut cx).is_pending());
        let after_second_poll = tracker.snapshot(1);
        assert_eq!(
            after_second_poll.task_kinds[TaskKind::H2ServerStream].poll_count,
            2
        );
        assert!(
            after_second_poll.task_kinds[TaskKind::H2ServerStream].poll_wall_duration
                >= Duration::from_millis(2)
        );
        assert_eq!(
            after_second_poll.task_kinds[TaskKind::H2ServerStream]
                .poll_count
                .saturating_sub(after_first_poll.task_kinds[TaskKind::H2ServerStream].poll_count),
            1
        );
        assert!(
            after_second_poll.task_kinds[TaskKind::H2ServerStream]
                .poll_wall_duration
                .saturating_sub(
                    after_first_poll.task_kinds[TaskKind::H2ServerStream].poll_wall_duration
                )
                >= Duration::from_millis(1)
        );
    }
}
