// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Collects RocksDB perf counters for a named operation.
//!
//! RocksDB keeps its perf counters in a thread-local context, so a measurement
//! only sees what happens on the current thread while it is active. Work is
//! measured in *sections* that never cross an `.await`: a sync guard is one
//! section, and a [`RocksDbPerf`] future opens one section per poll. Each section
//! adds what changed in the counters to its [`Collector`], which reports the
//! totals once at the end.
//!
//! Sections nest: only the outermost one on a thread turns perf stats on and off
//! and resets the counters. An inner section diffs against a snapshot instead, so
//! it doesn't clobber the outer one.

use std::cell::{Cell, RefCell};
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use metrics::{counter, histogram};
use pin_project_lite::pin_project;
use rocksdb::perf::PerfStatsLevel as RocksLevel;
use rocksdb::{PerfContext, PerfMetric};

use restate_types::config::{Configuration, PerfStatsLevel};
use restate_util_random as rand;

use crate::metric_definitions::{OP_NAME, TOTAL_DURATION};

/// Upper bound on the number of metrics a [`PerfSpec`] can track.
const MAX_METRICS: usize = 20;

type Counters = [u64; MAX_METRICS];

thread_local! {
    /// Reads RocksDB's thread-local perf context. Creating a `PerfContext` allocates
    /// a C wrapper, so it is created once per thread rather than per section.
    static CONTEXT: RefCell<PerfContext> = RefCell::new(PerfContext::default());
    /// How many sections are open on this thread.
    static DEPTH: Cell<u32> = const { Cell::new(0) };
}

#[derive(Clone, Copy)]
pub(super) enum Unit {
    /// Reported as a counter.
    Count,
    /// Reported as a histogram, in seconds.
    Nanos,
}

pub(super) struct MetricSpec {
    pub metric: PerfMetric,
    pub name: &'static str,
    pub unit: Unit,
}

/// What to measure for a kind of operation, and how often.
pub(super) struct PerfSpec {
    /// Only one in `weight` operations is measured, and its values are reported
    /// `weight` times.
    pub weight: usize,
    pub metrics: &'static [MetricSpec],
}

impl PerfSpec {
    pub(super) const fn new(weight: usize, metrics: &'static [MetricSpec]) -> Self {
        assert!(weight > 0);
        assert!(metrics.len() <= MAX_METRICS);
        Self { weight, metrics }
    }

    fn should_sample(&self) -> bool {
        rand::pseudo_random() < u64::MAX / self.weight as u64
    }
}

/// Totals for one sampled operation, across all of its sections. It holds no
/// thread-local state, so it can move between threads between sections.
pub(super) struct Collector {
    spec: &'static PerfSpec,
    name: &'static str,
    level: RocksLevel,
    counters: Counters,
    /// Time spent inside sections. Time spent parked at an `.await` isn't counted.
    busy: Duration,
}

/// An open section. It must be passed back to [`Collector::exit`] on the same
/// thread.
pub(super) struct Section {
    started_at: Instant,
    /// Counters when the section started; `None` for the outermost section,
    /// which reset them to zero.
    base: Option<Counters>,
    _not_send: PhantomData<*const ()>,
}

impl Collector {
    /// Returns `None` if this operation isn't sampled.
    pub(super) fn sampled(spec: &'static PerfSpec, name: &'static str) -> Option<Self> {
        spec.should_sample().then(|| Self {
            spec,
            name,
            level: convert_perf_level(Configuration::pinned().common.rocksdb_perf_level),
            counters: [0; MAX_METRICS],
            busy: Duration::ZERO,
        })
    }

    pub(super) fn enter(&self) -> Section {
        let depth = DEPTH.get();
        DEPTH.set(depth + 1);
        let base = if depth == 0 {
            rocksdb::perf::set_perf_stats(self.level);
            CONTEXT.with_borrow_mut(PerfContext::reset);
            None
        } else {
            Some(self.read())
        };
        Section {
            started_at: Instant::now(),
            base,
            _not_send: PhantomData,
        }
    }

    pub(super) fn exit(&mut self, section: Section) {
        let now = self.read();
        for (i, total) in self.counters[..self.spec.metrics.len()]
            .iter_mut()
            .enumerate()
        {
            let base = section.base.map_or(0, |base| base[i]);
            *total += now[i].saturating_sub(base);
        }
        self.busy += section.started_at.elapsed();

        let depth = DEPTH.get() - 1;
        DEPTH.set(depth);
        if depth == 0 {
            rocksdb::perf::set_perf_stats(RocksLevel::Disable);
        }
    }

    fn read(&self) -> Counters {
        let mut out = [0; MAX_METRICS];
        CONTEXT.with_borrow(|ctx| {
            for (slot, spec) in out.iter_mut().zip(self.spec.metrics) {
                *slot = ctx.metric(spec.metric);
            }
        });
        out
    }

    pub(super) fn report(&self) {
        let labels = [(OP_NAME, self.name)];
        let weight = self.spec.weight;
        // RocksDB reports times in nanoseconds in this API, compared to
        // microseconds in Statistics/Properties. Prometheus wants seconds.
        histogram!(TOTAL_DURATION, &labels).record_many(self.busy, weight);
        for (spec, &v) in self.spec.metrics.iter().zip(&self.counters) {
            if v == 0 {
                continue;
            }
            match spec.unit {
                Unit::Count => counter!(spec.name, &labels).increment(v * weight as u64),
                Unit::Nanos => histogram!(spec.name, &labels).record_many(n_to_s(v), weight),
            }
        }
    }
}

/// A sync guard: one section, from creation to drop. Shared by
/// `RocksDbReadPerfGuard` and `RocksDbWritePerfGuard`.
pub(super) struct SyncGuard {
    collector: Collector,
    // `Option` only so that `drop` can move it out.
    section: Option<Section>,
    report: bool,
}

impl SyncGuard {
    pub(super) fn new(spec: &'static PerfSpec, name: &'static str) -> Option<Self> {
        let collector = Collector::sampled(spec, name)?;
        let section = Some(collector.enter());
        Some(Self {
            collector,
            section,
            report: true,
        })
    }

    pub(super) fn forget(&mut self) {
        self.report = false;
    }
}

impl Drop for SyncGuard {
    fn drop(&mut self) {
        if let Some(section) = self.section.take() {
            self.collector.exit(section);
        }
        if self.report {
            self.collector.report();
        }
    }
}

pin_project! {
    /// A future that measures the RocksDB work it does, one section per poll.
    /// Work done by other tasks on the same thread between polls isn't counted.
    ///
    /// Metrics are reported when the future completes. A future dropped before
    /// completing reports nothing.
    ///
    /// Unlike the sync guards, this is `Send` if the wrapped future is: no
    /// thread-local state is held between polls.
    pub struct RocksDbPerf<F> {
        #[pin]
        inner: F,
        collector: Option<Collector>,
    }
}

impl<F> RocksDbPerf<F> {
    pub(super) fn new(inner: F, spec: &'static PerfSpec, name: &'static str) -> Self {
        Self {
            inner,
            collector: Collector::sampled(spec, name),
        }
    }
}

impl<F: Future> Future for RocksDbPerf<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        let this = self.project();
        let Some(collector) = this.collector.as_mut() else {
            return this.inner.poll(cx);
        };

        // Closes the section even if the inner future panics.
        struct Exit<'a>(&'a mut Collector, Option<Section>);
        impl Drop for Exit<'_> {
            fn drop(&mut self) {
                if let Some(section) = self.1.take() {
                    self.0.exit(section);
                }
            }
        }

        let section = collector.enter();
        let poll = {
            let _exit = Exit(collector, Some(section));
            this.inner.poll(cx)
        };
        if poll.is_ready()
            && let Some(collector) = this.collector.take()
        {
            collector.report();
        }
        poll
    }
}

static_assertions::assert_impl_all!(RocksDbPerf<std::future::Ready<()>>: Send);

/// Measures the RocksDB work done by a future. See [`RocksDbPerf`].
pub trait RocksDbPerfExt: Future + Sized {
    /// Reads, like `RocksDbReadPerfGuard`.
    fn with_read_perf(self, name: &'static str) -> RocksDbPerf<Self> {
        RocksDbPerf::new(self, &super::read_perf::READ_SPEC, name)
    }

    /// Writes, like `RocksDbWritePerfGuard`.
    fn with_write_perf(self, name: &'static str) -> RocksDbPerf<Self> {
        RocksDbPerf::new(self, &super::write_perf::WRITE_SPEC, name)
    }
}

impl<F: Future> RocksDbPerfExt for F {}

fn convert_perf_level(input: PerfStatsLevel) -> RocksLevel {
    match input {
        PerfStatsLevel::Disable => RocksLevel::Disable,
        PerfStatsLevel::EnableCount => RocksLevel::EnableCount,
        PerfStatsLevel::EnableTimeExceptForMutex => RocksLevel::EnableTimeExceptForMutex,
        PerfStatsLevel::EnableTimeAndCPUTimeExceptForMutex => {
            RocksLevel::EnableTimeAndCPUTimeExceptForMutex
        }
        PerfStatsLevel::EnableTime => RocksLevel::EnableTime,
    }
}

/// nanos to seconds
#[inline]
fn n_to_s(v: u64) -> f64 {
    v as f64 / 1_000_000_000.0
}

#[cfg(test)]
mod tests {
    use std::pin::pin;
    use std::task::Waker;

    use rocksdb::DB;

    use super::*;

    /// Always sampled, with one metric that goes up by exactly one per memtable
    /// `get` at the default `EnableCount` level.
    static SPEC: PerfSpec = PerfSpec::new(
        1,
        &[MetricSpec {
            metric: PerfMetric::GetFromMemtableCount,
            name: "test_get_from_memtable",
            unit: Unit::Count,
        }],
    );

    fn collector() -> Collector {
        Collector::sampled(&SPEC, "test").expect("weight 1 is always sampled")
    }

    fn raw_count() -> u64 {
        CONTEXT.with_borrow(|ctx| ctx.metric(PerfMetric::GetFromMemtableCount))
    }

    #[test]
    fn sections_nest_and_accumulate_across_polls() {
        let dir = restate_test_util::tempdir().unwrap();
        let db = DB::open_default(dir.path()).unwrap();
        db.put(b"k", b"v").unwrap();
        let get = || assert!(db.get(b"k").unwrap().is_some());

        // An inner section neither resets nor disables the outer one, and the
        // outer one includes the inner one's work.
        let (mut outer, mut inner) = (collector(), collector());
        let o = outer.enter();
        get();
        let i = inner.enter();
        get();
        inner.exit(i);
        assert_eq!(DEPTH.get(), 1);
        get();
        outer.exit(o);
        assert_eq!(DEPTH.get(), 0);
        assert_eq!(inner.counters[0], 1);
        assert_eq!(outer.counters[0], 3);

        // Leaving the outermost section turns perf stats off.
        let before = raw_count();
        get();
        assert_eq!(raw_count(), before);

        // A future adds up the work from every poll, and holds no section
        // while it is parked.
        let mut yielded = 0;
        let mut fut = pin!(RocksDbPerf {
            inner: std::future::poll_fn(|cx| {
                get();
                if yielded < 2 {
                    yielded += 1;
                    cx.waker().wake_by_ref();
                    Poll::Pending
                } else {
                    Poll::Ready(())
                }
            }),
            collector: Some(collector()),
        });
        let mut cx = Context::from_waker(Waker::noop());
        for polls in 1..=2 {
            assert!(fut.as_mut().poll(&mut cx).is_pending());
            assert_eq!(DEPTH.get(), 0);
            // Work outside the future's polls isn't counted.
            get();
            let collector = fut.collector.as_ref().unwrap();
            assert_eq!(collector.counters[0], polls);
        }
        assert!(fut.as_mut().poll(&mut cx).is_ready());
        assert!(fut.collector.is_none(), "reported on completion");
    }
}
