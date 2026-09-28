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
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream};
use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, MetricBuilder, MetricType, Time,
};
use futures::Stream;

use restate_partition_store::IteratorMetrics;
use restate_types::net::remote_query_scanner::ScannerMetrics;
use restate_types::sharding::PartitionId;

/// Accounting for one physical scan. DataFusion's partition attribute is the
/// logical execution partition; the physical Restate partition is a label.
#[derive(Debug, Clone)]
pub struct ScanMetrics {
    pub(crate) elapsed_compute: Time,
    iterators: Count,
    completed_iterators: Count,
    keys_visited: Count,
    seeks: Count,
    nexts: Count,
    prevs: Count,
    bytes_visited: Count,
    wall_time: Time,
    records_emitted: Count,
    reports: Count,
    completed_reports: Count,
    iterator_metrics: IteratorMetrics,
    complete: Arc<AtomicBool>,
}

impl ScanMetrics {
    pub(crate) fn new(
        metrics: &ExecutionPlanMetricsSet,
        logical_partition: usize,
        partition_id: PartitionId,
        elapsed_compute: Time,
    ) -> Self {
        let builder = || {
            MetricBuilder::new(metrics)
                .with_type(MetricType::Summary)
                .with_new_label("restate_partition", partition_id.to_string())
        };
        let counter = |name| builder().counter(name, logical_partition);
        counter("storage_scans").add(1);
        Self {
            elapsed_compute,
            iterators: counter("storage_iterators"),
            completed_iterators: counter("storage_iterators_completed"),
            keys_visited: counter("storage_keys_visited"),
            seeks: counter("storage_seek_count"),
            nexts: counter("storage_next_count"),
            prevs: counter("storage_prev_count"),
            bytes_visited: counter("storage_iterator_bytes"),
            wall_time: builder().subset_time("storage_iterator_wall_time", logical_partition),
            records_emitted: counter("storage_records_emitted"),
            reports: counter("storage_scans_reported"),
            completed_reports: counter("storage_scans_completed"),
            iterator_metrics: IteratorMetrics::default(),
            complete: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Server-side collection has no coordinator execution plan to register with.
    pub(crate) fn remote(partition_id: PartitionId) -> Self {
        Self::new(
            &ExecutionPlanMetricsSet::new(),
            0,
            partition_id,
            Time::new(),
        )
    }

    pub(crate) fn iterator_metrics(&self) -> IteratorMetrics {
        self.iterator_metrics.clone()
    }

    pub(crate) fn record_emitted(&self) {
        self.records_emitted.add(1);
    }

    pub(crate) fn finish(&self) {
        self.complete.store(true, Ordering::Release);
    }

    pub(crate) fn snapshot(&self) -> ScannerMetrics {
        // Acquire completion before reading counters published by the producer.
        let complete = self.complete.load(Ordering::Acquire);
        let iterator = self.iterator_metrics.snapshot();
        ScannerMetrics {
            iterators: iterator.iterators,
            completed_iterators: iterator.completed_iterators,
            keys_visited: iterator.keys_visited,
            seeks: iterator.seeks,
            nexts: iterator.nexts,
            prevs: iterator.prevs,
            bytes_visited: iterator.bytes_visited,
            wall_time_ns: iterator.wall_time_ns,
            records_emitted: self.records_emitted.value() as u64,
            available: iterator.supported,
            complete: complete && iterator.iterators == iterator.completed_iterators,
        }
    }

    /// Apply cumulative snapshots by high-water mark. Repeated/older reports do
    /// not double-count. Each physical scanner has its own handles and one reader.
    pub(crate) fn update(&self, report: ScannerMetrics) {
        fn set_at_least(counter: &Count, value: u64) {
            let value = usize::try_from(value).unwrap_or(usize::MAX);
            counter.add(value.saturating_sub(counter.value()));
        }
        set_at_least(&self.records_emitted, report.records_emitted);
        if !report.available {
            return;
        }
        set_at_least(&self.reports, 1);
        set_at_least(&self.completed_reports, u64::from(report.complete));
        set_at_least(&self.iterators, report.iterators);
        set_at_least(&self.completed_iterators, report.completed_iterators);
        set_at_least(&self.keys_visited, report.keys_visited);
        set_at_least(&self.seeks, report.seeks);
        set_at_least(&self.nexts, report.nexts);
        set_at_least(&self.prevs, report.prevs);
        set_at_least(&self.bytes_visited, report.bytes_visited);
        let elapsed = report
            .wall_time_ns
            .saturating_sub(self.wall_time.value() as u64);
        if elapsed > 0 {
            self.wall_time.add_duration(Duration::from_nanos(elapsed));
        }
    }

    pub(crate) fn observe(&self, stream: SendableRecordBatchStream) -> SendableRecordBatchStream {
        Box::pin(ObservedScan {
            stream,
            metrics: self.clone(),
        })
    }
}

struct ObservedScan {
    stream: SendableRecordBatchStream,
    metrics: ScanMetrics,
}

impl Stream for ObservedScan {
    type Item = datafusion::common::Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let result = self.stream.as_mut().poll_next(cx);
        self.metrics.update(self.metrics.snapshot());
        result
    }
}

impl RecordBatchStream for ObservedScan {
    fn schema(&self) -> SchemaRef {
        self.stream.schema()
    }
}

impl Drop for ObservedScan {
    fn drop(&mut self) {
        // A parent LIMIT may drop us before the producer finishes. Publish the
        // last available progress, without waiting or claiming final accounting.
        self.metrics.update(self.metrics.snapshot());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cumulative_remote_reports_are_idempotent_and_keep_scans_separate() {
        let set = ExecutionPlanMetricsSet::new();
        let first = ScanMetrics::new(&set, 0, PartitionId::from(1), Time::new());
        let second = ScanMetrics::new(&set, 0, PartitionId::from(2), Time::new());
        // Legacy/unsupported peers must not look like completed zero-work scans.
        first.update(ScannerMetrics::default());
        assert_eq!(first.reports.value(), 0);
        assert_eq!(first.completed_reports.value(), 0);
        let progress = ScannerMetrics {
            available: true,
            iterators: 1,
            keys_visited: 100,
            seeks: 3,
            wall_time_ns: 1000,
            ..Default::default()
        };
        first.update(progress);
        first.update(progress);
        first.update(ScannerMetrics {
            keys_visited: 50,
            ..progress
        });
        assert_eq!(first.keys_visited.value(), 100);
        assert_eq!(first.reports.value(), 1);
        assert_eq!(first.completed_reports.value(), 0);
        let final_report = ScannerMetrics {
            complete: true,
            completed_iterators: 1,
            keys_visited: 150,
            records_emitted: 10,
            wall_time_ns: 1500,
            ..progress
        };
        first.update(final_report);
        first.update(final_report);
        second.update(final_report);
        assert_eq!(first.keys_visited.value(), 150);
        assert_eq!(second.keys_visited.value(), 150);
        assert_eq!(first.records_emitted.value(), 10);
        assert_eq!(first.wall_time.value(), 1500);
        assert_eq!(first.completed_reports.value(), 1);
        assert_eq!(second.completed_reports.value(), 1);

        // Dropping a consumer before EOF publishes progress, not a final report.
        let partial = ScanMetrics::new(&set, 0, PartitionId::from(3), Time::new());
        partial.iterator_metrics().mark_supported();
        let stream = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
            Arc::new(datafusion::arrow::datatypes::Schema::empty()),
            futures::stream::pending::<datafusion::common::Result<RecordBatch>>(),
        );
        drop(partial.observe(Box::pin(stream)));
        assert_eq!(partial.reports.value(), 1);
        assert_eq!(partial.completed_reports.value(), 0);
    }
}
