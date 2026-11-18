// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! How long a task's partition captures took, for `EXPLAIN ANALYZE`.
//!
//! Before a lane of a [`PartitionReadExec`] reads a partition, it asks the
//! partition's processor for a *capture*: a storage snapshot and the live
//! state its sources need. The processor admits it between two of its turns.
//! Each step is stamped on the owner's monotonic clock, where the lane and the
//! processor both run, so every duration is measured within one process:
//!
//! ```text
//! task installed
//!   │  capture_request_age: the task's age when a lane asks
//!   ▼
//! lane asks ──▶ processor admits ──▶ snapshot taken ──▶ invoker rows read ──▶ lane resumes
//!   │  capture_queued      │  snapshot_create  │    capture_return, less the invoker  │
//!   │                      │                   │    read (invoker_capture)            │
//!   └────────────────────────────────── capture ────────────────────────────────────────┘
//! ```
//!
//! For example, `capture_queued_max=40ms, snapshot_create_max=200µs` says that
//! one partition's request waited 40ms for its processor, say behind a commit,
//! and that taking its snapshot took no more than 200µs.
//!
//! A task reports these per owner, next to `task_elapsed`, as a count with an
//! average and a maximum of each step over the captures that took it.
//! Partitions read in parallel lanes wait at the same time, so none of these
//! adds up to the query's latency. A capture that failed, or that its lane gave
//! up, isn't a sample: only `captures` are.
//!
//! [`PartitionReadExec`]: super::partition_reads::PartitionReadExec

use std::sync::Arc;
use std::time::Duration;

use datafusion::physical_plan::Metric;
use datafusion::physical_plan::metrics::{
    Count, CustomMetricValue, MetricCategory, MetricType, MetricValue,
};
use datafusion_distributed::{AvgLatencyMetric, MaxLatencyMetric, MinLatencyMetric};
use tokio::time::Instant;

use restate_worker_api::CaptureTiming;

/// One partition's capture as its lane saw it: when the lane asked, what the
/// capturing processor stamped, and when the lane had it back.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CaptureSample {
    pub(crate) requested: Instant,
    pub(crate) timing: CaptureTiming,
    pub(crate) resumed: Instant,
}

/// What a task's captures took so far, in constant space.
#[derive(Debug, Default)]
pub(super) struct Captures {
    /// The task's age when it asked for its first and its last capture.
    requested: Option<(Duration, Duration)>,
    capture: Step,
    queued: Step,
    snapshot: Step,
    returned: Step,
    invoker: Step,
}

/// A step's count, total and maximum.
#[derive(Debug, Default, Clone, Copy)]
struct Step {
    count: usize,
    nanos: usize,
    max: usize,
}

impl Step {
    fn add(&mut self, duration: Duration) {
        let nanos = usize::try_from(duration.as_nanos()).unwrap_or(usize::MAX);
        self.count += 1;
        self.nanos = self.nanos.saturating_add(nanos);
        self.max = self.max.max(nanos);
    }
}

impl Captures {
    /// Adds the capture `sample` of a task installed at `installed`.
    pub(super) fn add(&mut self, installed: Instant, sample: &CaptureSample) {
        let CaptureSample {
            requested,
            timing,
            resumed,
        } = *sample;
        let age = requested.saturating_duration_since(installed);
        self.requested = Some(match self.requested {
            Some((first, last)) => (first.min(age), last.max(age)),
            None => (age, age),
        });
        self.capture
            .add(resumed.saturating_duration_since(requested));
        self.queued
            .add(timing.admitted.saturating_duration_since(requested));
        let answered = match timing.snapshot {
            Some(snapshot) => {
                self.snapshot.add(snapshot.duration());
                snapshot.end
            }
            None => timing.admitted,
        };
        let invoker = timing.invoker.map(|invoker| invoker.duration());
        if let Some(invoker) = invoker {
            self.invoker.add(invoker);
        }
        self.returned.add(
            resumed
                .saturating_duration_since(answered)
                .saturating_sub(invoker.unwrap_or_default()),
        );
    }

    /// The task's metrics, none without a capture: a count, and each step's
    /// average and maximum over the captures that took it.
    pub(super) fn metrics(&self) -> Vec<Metric> {
        let Some((first, last)) = self.requested else {
            return Vec::new();
        };
        let count = Count::new();
        count.add(self.capture.count);
        let mut metrics = vec![
            Metric::new(
                MetricValue::Count {
                    name: "captures".into(),
                    count,
                },
                None,
            )
            .with_type(MetricType::Summary)
            .with_category(MetricCategory::Uncategorized),
        ];
        let nanos = |age: Duration| usize::try_from(age.as_nanos()).unwrap_or(usize::MAX);
        metrics.extend([
            latency(
                "capture_request_age_min",
                MinLatencyMetric::from_nanos(nanos(first)),
                MetricType::Dev,
            ),
            latency(
                "capture_request_age_max",
                MaxLatencyMetric::from_nanos(nanos(last)),
                MetricType::Dev,
            ),
        ]);
        for (name, step, level) in [
            ("capture", self.capture, MetricType::Summary),
            ("capture_queued", self.queued, MetricType::Summary),
            ("snapshot_create", self.snapshot, MetricType::Summary),
            ("capture_return", self.returned, MetricType::Dev),
            ("invoker_capture", self.invoker, MetricType::Dev),
        ] {
            if step.count > 0 {
                metrics.extend([
                    latency(
                        format!("{name}_avg"),
                        AvgLatencyMetric::from_raw(step.nanos, step.count),
                        level,
                    ),
                    latency(
                        format!("{name}_max"),
                        MaxLatencyMetric::from_nanos(step.max),
                        level,
                    ),
                ]);
            }
        }
        metrics
    }
}

fn latency(
    name: impl Into<std::borrow::Cow<'static, str>>,
    value: impl CustomMetricValue + 'static,
    level: MetricType,
) -> Metric {
    Metric::new(
        MetricValue::Custom {
            name: name.into(),
            value: Arc::new(value),
        },
        None,
    )
    .with_type(level)
    .with_category(MetricCategory::Timing)
}

#[cfg(test)]
mod tests {
    use datafusion::physical_plan::metrics::MetricsSet;

    use restate_worker_api::CaptureSpan;

    use super::super::report::{Budget, Encoded, decode, encode};
    use super::*;

    /// Two captures, 5ms and 60ms after install. The first waited 40ms for its
    /// processor, took a 200µs snapshot and a 7ms invoker read; the second was
    /// live only.
    fn samples(installed: Instant) -> [CaptureSample; 2] {
        let at = |micros: u64| installed + Duration::from_micros(micros);
        [
            CaptureSample {
                requested: at(5_000),
                timing: CaptureTiming {
                    admitted: at(45_000),
                    snapshot: Some(CaptureSpan {
                        start: at(45_000),
                        end: at(45_200),
                    }),
                    invoker: Some(CaptureSpan {
                        start: at(46_000),
                        end: at(53_000),
                    }),
                },
                resumed: at(54_000),
            },
            CaptureSample {
                requested: at(60_000),
                timing: CaptureTiming::admitted_at(at(61_000)),
                resumed: at(62_000),
            },
        ]
    }

    /// Each step ends where the next starts, the snapshot and invoker steps
    /// count only the captures that took them, and the task's numbers keep
    /// their averages and maxima through a report, also when two tasks of a
    /// stage are shown together.
    #[test]
    fn capture_steps_split_at_their_boundaries() {
        let installed = Instant::now();
        assert!(
            Captures::default().metrics().is_empty(),
            "no capture, no sample"
        );
        let mut captures = Captures::default();
        for sample in &samples(installed) {
            captures.add(installed, sample);
        }
        let shown = |metrics: &MetricsSet| {
            metrics
                .iter()
                .map(|metric| metric.to_string())
                .collect::<Vec<_>>()
        };
        let reported = |captures: &Captures| {
            let mut budget = Budget::for_task(0);
            let mut metrics = MetricsSet::new();
            for metric in captures.metrics() {
                let Encoded::Record(record) = encode(&metric, &mut budget) else {
                    panic!("{metric} is carried");
                };
                let decoded = decode(&record).unwrap();
                assert_eq!(
                    (decoded.metric_type(), decoded.metric_category()),
                    (metric.metric_type(), metric.metric_category()),
                    "{metric}"
                );
                metrics.push(Arc::new(decoded));
            }
            metrics
        };
        let task = reported(&captures);
        assert_eq!(
            shown(&task),
            [
                "captures=2",
                "capture_request_age_min=5.00ms",
                "capture_request_age_max=60.00ms",
                "capture_avg=25.50ms",
                "capture_max=49.00ms",
                "capture_queued_avg=20.50ms",
                "capture_queued_max=40.00ms",
                "snapshot_create_avg=200.00µs",
                "snapshot_create_max=200.00µs",
                "capture_return_avg=1.40ms",
                "capture_return_max=1.80ms",
                "invoker_capture_avg=7.00ms",
                "invoker_capture_max=7.00ms",
            ]
        );
        let summary = task.clone().filter_by_metric_types(&[MetricType::Summary]);
        assert_eq!(summary.iter().count(), 7, "{:?}", shown(&summary));

        // A second task of the same stage with the first sample only: shown
        // together, counts add, averages weigh every capture, maxima stay.
        let mut other = Captures::default();
        other.add(installed, &samples(installed)[0]);
        let mut both = task;
        for metric in reported(&other).iter() {
            both.push(Arc::clone(metric));
        }
        let mut together = shown(&both.aggregate_by_name());
        together.sort();
        assert_eq!(
            together,
            [
                "capture_avg=33.33ms",
                "capture_max=49.00ms",
                "capture_queued_avg=27.00ms",
                "capture_queued_max=40.00ms",
                "capture_request_age_max=60.00ms",
                "capture_request_age_min=5.00ms",
                "capture_return_avg=1.53ms",
                "capture_return_max=1.80ms",
                "captures=3",
                "invoker_capture_avg=7.00ms",
                "invoker_capture_max=7.00ms",
                "snapshot_create_avg=200.00µs",
                "snapshot_create_max=200.00µs",
            ]
        );
    }
}
