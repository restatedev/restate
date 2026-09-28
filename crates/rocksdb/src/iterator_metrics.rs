// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::Instant;

use parking_lot::Mutex;

use crate::IterAction;

/// Unsampled work performed by background iterators, independent of any query engine.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct IteratorStats {
    /// The scan path supports accounting, even if it did not need an iterator.
    pub supported: bool,
    pub iterators: u64,
    /// Iterators that have stopped, including callback-requested early termination.
    pub completed_iterators: u64,
    pub keys_visited: u64,
    /// Includes initial positioning and seeks that reach the end of the iterator.
    pub seeks: u64,
    pub nexts: u64,
    pub prevs: u64,
    /// Key + value bytes presented to callbacks, not physical disk-read bytes.
    pub bytes_visited: u64,
    /// Iterator lifetime, including time blocked on consumers.
    pub wall_time_ns: u64,
}

/// Query-owned aggregate. Publication is batched; the per-key path uses local counters.
#[derive(Debug, Default, Clone)]
pub struct IteratorMetrics(Arc<Mutex<IteratorStats>>);

impl IteratorMetrics {
    /// Called by an instrumented scan operation before executing its plan.
    pub fn mark_supported(&self) {
        self.0.lock().supported = true;
    }

    pub fn snapshot(&self) -> IteratorStats {
        *self.0.lock()
    }

    fn publish(&self, delta: IteratorStats) {
        let mut total = self.0.lock();
        total.supported |= delta.supported;
        total.iterators += delta.iterators;
        total.completed_iterators += delta.completed_iterators;
        total.keys_visited += delta.keys_visited;
        total.seeks += delta.seeks;
        total.nexts += delta.nexts;
        total.prevs += delta.prevs;
        total.bytes_visited += delta.bytes_visited;
        total.wall_time_ns += delta.wall_time_ns;
    }
}

pub(crate) struct IteratorProbe {
    metrics: IteratorMetrics,
    pending: IteratorStats,
    last_flush: Instant,
    finished: bool,
}

impl IteratorProbe {
    pub(crate) fn new(metrics: IteratorMetrics) -> Self {
        metrics.mark_supported();
        metrics.publish(IteratorStats {
            iterators: 1,
            ..IteratorStats::default()
        });
        Self {
            metrics,
            pending: IteratorStats::default(),
            last_flush: Instant::now(),
            finished: false,
        }
    }

    pub(crate) fn action(&mut self, action: &IterAction) {
        match action {
            IterAction::Seek(_) | IterAction::SeekToFirst | IterAction::SeekToLast => {
                self.pending.seeks += 1
            }
            IterAction::Next => self.pending.nexts += 1,
            IterAction::Prev => self.pending.prevs += 1,
            IterAction::Stop => {}
        }
    }

    pub(crate) fn visit(&mut self, bytes: usize) {
        self.pending.keys_visited += 1;
        self.pending.bytes_visited += bytes as u64;
        if self.pending.keys_visited >= 128 {
            self.flush();
        }
    }

    fn flush(&mut self) {
        let now = Instant::now();
        self.pending.wall_time_ns = now
            .duration_since(self.last_flush)
            .as_nanos()
            .min(u64::MAX as u128) as u64;
        self.last_flush = now;
        self.metrics.publish(std::mem::take(&mut self.pending));
    }

    /// Must precede the callback/channel teardown that exposes EOF to the consumer.
    pub(crate) fn finish(&mut self) {
        if !self.finished {
            self.finished = true;
            self.pending.completed_iterators = 1;
            self.flush();
        }
    }
}

impl Drop for IteratorProbe {
    fn drop(&mut self) {
        // Publish partial work on abnormal teardown, without claiming completion.
        if !self.finished {
            self.flush();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn progress_is_batched_and_abandoned_iterators_are_not_complete() {
        let metrics = IteratorMetrics::default();
        let mut first = IteratorProbe::new(metrics.clone());
        first.action(&IterAction::SeekToFirst);
        for _ in 0..128 {
            first.visit(10);
        }
        assert_eq!(metrics.snapshot().keys_visited, 128);
        first.visit(10);
        first.finish();
        first.finish(); // Finalization must not double-count.
        drop(first);
        let mut abandoned = IteratorProbe::new(metrics.clone());
        abandoned.visit(7);
        drop(abandoned);
        let snapshot = metrics.snapshot();
        assert!(snapshot.supported);
        assert_eq!(snapshot.iterators, 2);
        assert_eq!(snapshot.completed_iterators, 1);
        assert_eq!(snapshot.keys_visited, 130);
        assert_eq!(snapshot.bytes_visited, 1297);
        assert_eq!(snapshot.seeks, 1);
    }
}
