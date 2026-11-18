// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! DataFusion metrics in a task report ([`QueryTaskReport`]), within a budget.
//!
//! A worker's report of what a task did holds at most [`MAX_RECORDS`] metric
//! records in at most [`MAX_BYTES`] of encoded records. Each record is charged
//! an upper bound of its encoded size, computed from the metric's own strings
//! *before* anything is copied ([`charge`]), so a metric that doesn't fit costs
//! nothing; the report counts it instead. Records keep everything `EXPLAIN
//! ANALYZE` selects and displays by: the value and its kind, the labels, the
//! output partition, the metric type (its `analyze_level`) and its category.
//!
//! Custom metric values aren't carried, and their encodings aren't bounded in
//! general: they are unsupported, which the caller counts. Restate's own scan
//! and read metrics, and the operators of today's static owner-bound tasks, use
//! standard kinds; the distributed runtime's worker task timings (`plan_*_at`)
//! are custom, but reports carry their own task timings instead.
//!
//! [`QueryTaskReport`]: restate_types::net::distributed_query::QueryTaskReport

use std::borrow::Cow;
use std::time::Duration;

use datafusion::arrow::temporal_conversions::timestamp_ns_to_datetime;
use datafusion::physical_plan::Metric;
use datafusion::physical_plan::metrics::{
    Count, Gauge, Label, MetricCategory, MetricType, MetricValue, PruningMetrics,
    RatioMergeStrategy, RatioMetrics, Time, Timestamp,
};

use restate_types::net::distributed_query::{
    MetricCategoryRecord, MetricLabel, MetricLevel, MetricRecord, MetricRecordValue, NamedPruning,
    NamedRatio, NamedValue, QueryTaskReport, RatioMerge, Timestamp as TimestampRecord,
};

/// The most metric records one task report holds.
pub(crate) const MAX_RECORDS: usize = 1024;
/// The most bytes of encoded metric records one task report holds.
pub(crate) const MAX_BYTES: usize = 64 * 1024;
/// The most a field's key and a length or number can take in bilrost: a key of
/// at most two bytes for tags below 2,048, and a varint of at most ten bytes.
const FIELD: usize = 12;
/// What a record costs at most besides its strings: its own key and length in
/// the enclosing list, the value's key and length, up to four numbers in the
/// value, the partition, the level and the category.
const RECORD: usize = FIELD + FIELD + 4 * FIELD + FIELD + 2 * FIELD;
/// What a label costs at most besides its name and value: its key and length,
/// and the keys and lengths of its two strings.
const LABEL: usize = 3 * FIELD;

/// What is left of a report's allowance.
#[derive(Debug)]
pub(crate) struct Budget {
    records: usize,
    bytes: usize,
}

impl Budget {
    /// The allowance of one task report, less `reserved` bytes kept for what the
    /// report adds afterwards, such as its markers.
    pub(crate) fn for_task(reserved: usize) -> Self {
        Self {
            records: MAX_RECORDS,
            bytes: MAX_BYTES.saturating_sub(reserved),
        }
    }

    /// Takes `bytes` that aren't a record from the allowance, if they fit.
    pub(crate) fn take_bytes(&mut self, bytes: usize) -> bool {
        if bytes > self.bytes {
            return false;
        }
        self.bytes -= bytes;
        true
    }

    /// Takes `bytes` and one record from the allowance, if both fit.
    pub(crate) fn take(&mut self, bytes: usize) -> bool {
        if self.records == 0 || bytes > self.bytes {
            return false;
        }
        self.records -= 1;
        self.bytes -= bytes;
        true
    }
}

/// What became of one metric.
#[derive(Debug)]
pub(crate) enum Encoded {
    Record(MetricRecord),
    /// A kind a report can't carry.
    Unsupported,
    /// It didn't fit in what was left of the budget; nothing was taken.
    OverBudget,
}

/// `metric` as a record, if its kind is supported and it fits in `budget`.
pub(crate) fn encode(metric: &Metric, budget: &mut Budget) -> Encoded {
    let value = metric.value();
    if matches!(value, MetricValue::Custom { .. }) {
        return Encoded::Unsupported;
    }
    // Counted before its labels are walked: a metric with more of them than a
    // record keeps doesn't fit.
    if metric.labels().len() > MAX_LABELS {
        return Encoded::OverBudget;
    }
    // A set timestamp beyond what nanoseconds since the epoch can hold.
    if let MetricValue::StartTimestamp(at) | MetricValue::EndTimestamp(at) = value
        && at
            .value()
            .is_some_and(|at| at.timestamp_nanos_opt().is_none())
    {
        return Encoded::Unsupported;
    }
    if !budget.take(charge(metric)) {
        return Encoded::OverBudget;
    }
    let named_value = |name: &Cow<'static, str>, value: usize| NamedValue {
        name: name.as_ref().into(),
        value: value as u64,
    };
    // Representable, as checked above.
    let timestamp = |timestamp: &Timestamp| TimestampRecord {
        nanos: timestamp.value().and_then(|at| at.timestamp_nanos_opt()),
    };
    let value = match value {
        MetricValue::OutputRows(count) => MetricRecordValue::OutputRows(count.value() as u64),
        MetricValue::ElapsedCompute(time) => {
            MetricRecordValue::ElapsedComputeNanos(time.value() as u64)
        }
        MetricValue::SpillCount(count) => MetricRecordValue::SpillCount(count.value() as u64),
        MetricValue::SpilledBytes(count) => MetricRecordValue::SpilledBytes(count.value() as u64),
        MetricValue::OutputBytes(count) => MetricRecordValue::OutputBytes(count.value() as u64),
        MetricValue::OutputBatches(count) => MetricRecordValue::OutputBatches(count.value() as u64),
        MetricValue::SpilledRows(count) => MetricRecordValue::SpilledRows(count.value() as u64),
        MetricValue::CurrentMemoryUsage(gauge) => {
            MetricRecordValue::CurrentMemoryUsage(gauge.value() as u64)
        }
        MetricValue::Count { name, count } => {
            MetricRecordValue::Count(named_value(name, count.value()))
        }
        MetricValue::Gauge { name, gauge } => {
            MetricRecordValue::Gauge(named_value(name, gauge.value()))
        }
        MetricValue::PeakMemoryUsage { name, gauge } => {
            MetricRecordValue::PeakMemoryUsage(named_value(name, gauge.value()))
        }
        MetricValue::Time { name, time } => {
            MetricRecordValue::TimeNanos(named_value(name, time.value()))
        }
        MetricValue::StartTimestamp(at) => MetricRecordValue::StartTimestamp(timestamp(at)),
        MetricValue::EndTimestamp(at) => MetricRecordValue::EndTimestamp(timestamp(at)),
        MetricValue::PruningMetrics {
            name,
            pruning_metrics,
        } => MetricRecordValue::Pruning(NamedPruning {
            name: name.as_ref().into(),
            pruned: pruning_metrics.pruned() as u64,
            matched: pruning_metrics.matched() as u64,
            fully_matched: pruning_metrics.fully_matched() as u64,
        }),
        MetricValue::Ratio {
            name,
            ratio_metrics,
        } => MetricRecordValue::Ratio(NamedRatio {
            name: name.as_ref().into(),
            part: ratio_metrics.part() as u64,
            total: ratio_metrics.total() as u64,
            merge: match ratio_metrics.merge_strategy() {
                RatioMergeStrategy::AddPartAddTotal => RatioMerge::AddPartAddTotal,
                RatioMergeStrategy::AddPartSetTotal => RatioMerge::AddPartSetTotal,
                RatioMergeStrategy::SetPartAddTotal => RatioMerge::SetPartAddTotal,
            },
            display_raw_values: ratio_metrics.display_raw_values(),
        }),
        MetricValue::Custom { .. } => unreachable!("custom values are unsupported"),
    };
    Encoded::Record(MetricRecord {
        value,
        labels: metric
            .labels()
            .iter()
            .map(|label| MetricLabel {
                name: label.name().into(),
                value: label.value().into(),
            })
            .collect(),
        partition: metric.partition().map(|partition| partition as u64),
        level: match metric.metric_type() {
            MetricType::Summary => MetricLevel::Summary,
            MetricType::Dev => MetricLevel::Dev,
        },
        category: match metric.metric_category() {
            None => MetricCategoryRecord::None,
            Some(MetricCategory::Rows) => MetricCategoryRecord::Rows,
            Some(MetricCategory::Bytes) => MetricCategoryRecord::Bytes,
            Some(MetricCategory::Timing) => MetricCategoryRecord::Timing,
            Some(MetricCategory::Uncategorized) => MetricCategoryRecord::Uncategorized,
        },
    })
}

/// An upper bound of `metric`'s encoded record, from its name (for the kinds
/// that carry one) and labels, saturating rather than overflowing.
pub(crate) fn charge(metric: &Metric) -> usize {
    let value = metric.value();
    let named = !matches!(
        value,
        MetricValue::OutputRows(_)
            | MetricValue::ElapsedCompute(_)
            | MetricValue::SpillCount(_)
            | MetricValue::SpilledBytes(_)
            | MetricValue::OutputBytes(_)
            | MetricValue::OutputBatches(_)
            | MetricValue::SpilledRows(_)
            | MetricValue::CurrentMemoryUsage(_)
            | MetricValue::StartTimestamp(_)
            | MetricValue::EndTimestamp(_)
    );

    let string = |text: &str| FIELD.saturating_add(text.len());
    let name = if named {
        string(metric.value().name())
    } else {
        0
    };
    metric
        .labels()
        .iter()
        .fold(RECORD.saturating_add(name), |total, label| {
            total
                .saturating_add(LABEL)
                .saturating_add(label.name().len())
                .saturating_add(label.value().len())
        })
}

/// The most nodes a report's shape describes.
pub(crate) const MAX_NODES: usize = 1024;
/// The most labels a kept metric has.
pub(crate) const MAX_LABELS: usize = 64;

/// A task's report from the bytes its `Closed` carries, or why it can't be
/// used. The size is checked before anything is decoded, so a report allocates
/// at most what [`MAX_BYTES`] of input can describe; then its counts are
/// checked before anyone keeps it. A worker's report is built within the same
/// bytes ([`Budget`]), so a valid one always fits.
pub(crate) fn read_report(bytes: &[u8]) -> Result<QueryTaskReport, &'static str> {
    if bytes.len() > MAX_BYTES {
        return Err("oversized");
    }
    let report =
        <QueryTaskReport as bilrost::OwnedMessage>::decode(bytes).map_err(|_| "malformed")?;
    let records = report
        .nodes
        .iter()
        .map(|node| node.metrics.len())
        .sum::<usize>()
        + report.task.len();
    if report.shape.len() > MAX_NODES
        || report.nodes.len() > report.shape.len()
        || records > MAX_RECORDS
    {
        return Err("over bounds");
    }
    Ok(report)
}

/// FNV-1a 64 of `bytes`, the same on every node.
pub(crate) fn digest(bytes: &[u8]) -> u64 {
    let mut hash = Fnv::default();
    hash.write(bytes);
    hash.0
}

/// FNV-1a 64, fed bytes or formatted text without allocating.
pub(crate) struct Fnv(pub(crate) u64);

impl Default for Fnv {
    fn default() -> Self {
        Self(0xcbf2_9ce4_8422_2325)
    }
}

impl Fnv {
    pub(crate) fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 = (self.0 ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
}

impl std::fmt::Write for Fnv {
    fn write_str(&mut self, text: &str) -> std::fmt::Result {
        self.write(text.as_bytes());
        Ok(())
    }
}

/// The metric `record` holds, or `None` for a value or level this binary
/// doesn't know, or a number that doesn't fit this platform.
pub(crate) fn decode(record: &MetricRecord) -> Option<Metric> {
    let size = |value: u64| usize::try_from(value).ok();
    let count = |value: u64| {
        let count = Count::new();
        count.add(size(value)?);
        Some(count)
    };
    let gauge = |value: u64| {
        let gauge = Gauge::new();
        gauge.set(size(value)?);
        Some(gauge)
    };
    // A fresh time is zero; adding even zero would count one nanosecond.
    let time = |nanos: u64| {
        let time = Time::new();
        if size(nanos)? > 0 {
            time.add_duration(Duration::from_nanos(nanos));
        }
        Some(time)
    };
    let timestamp = |record: &TimestampRecord| {
        let timestamp = Timestamp::new();
        if let Some(nanos) = record.nanos {
            timestamp.set(timestamp_ns_to_datetime(nanos)?.and_utc());
        }
        Some(timestamp)
    };
    let name = |named: &str| Cow::Owned(named.to_owned());
    let value = match &record.value {
        MetricRecordValue::Unknown
        | MetricRecordValue::Latency(_)
        | MetricRecordValue::BytesCounter(_)
        | MetricRecordValue::MaxGauge(_) => return None,
        MetricRecordValue::OutputRows(value) => MetricValue::OutputRows(count(*value)?),
        MetricRecordValue::ElapsedComputeNanos(nanos) => MetricValue::ElapsedCompute(time(*nanos)?),
        MetricRecordValue::SpillCount(value) => MetricValue::SpillCount(count(*value)?),
        MetricRecordValue::SpilledBytes(value) => MetricValue::SpilledBytes(count(*value)?),
        MetricRecordValue::OutputBytes(value) => MetricValue::OutputBytes(count(*value)?),
        MetricRecordValue::OutputBatches(value) => MetricValue::OutputBatches(count(*value)?),
        MetricRecordValue::SpilledRows(value) => MetricValue::SpilledRows(count(*value)?),
        MetricRecordValue::CurrentMemoryUsage(value) => {
            MetricValue::CurrentMemoryUsage(gauge(*value)?)
        }
        MetricRecordValue::Count(named) => MetricValue::Count {
            name: name(&named.name),
            count: count(named.value)?,
        },
        MetricRecordValue::Gauge(named) => MetricValue::Gauge {
            name: name(&named.name),
            gauge: gauge(named.value)?,
        },
        MetricRecordValue::PeakMemoryUsage(named) => MetricValue::PeakMemoryUsage {
            name: name(&named.name),
            gauge: gauge(named.value)?,
        },
        MetricRecordValue::TimeNanos(named) => MetricValue::Time {
            name: name(&named.name),
            time: time(named.value)?,
        },
        MetricRecordValue::StartTimestamp(at) => MetricValue::StartTimestamp(timestamp(at)?),
        MetricRecordValue::EndTimestamp(at) => MetricValue::EndTimestamp(timestamp(at)?),
        MetricRecordValue::Pruning(pruning) => {
            let metrics = PruningMetrics::new();
            metrics.add_pruned(size(pruning.pruned)?);
            metrics.add_matched(size(pruning.matched)?);
            metrics.add_fully_matched(size(pruning.fully_matched)?);
            MetricValue::PruningMetrics {
                name: name(&pruning.name),
                pruning_metrics: metrics,
            }
        }
        MetricRecordValue::Ratio(ratio) => {
            let metrics = RatioMetrics::new()
                .with_merge_strategy(match ratio.merge {
                    RatioMerge::AddPartAddTotal => RatioMergeStrategy::AddPartAddTotal,
                    RatioMerge::AddPartSetTotal => RatioMergeStrategy::AddPartSetTotal,
                    RatioMerge::SetPartAddTotal => RatioMergeStrategy::SetPartAddTotal,
                })
                .with_display_raw_values(ratio.display_raw_values);
            metrics.add_part(size(ratio.part)?);
            metrics.add_total(size(ratio.total)?);
            MetricValue::Ratio {
                name: name(&ratio.name),
                ratio_metrics: metrics,
            }
        }
    };
    let level = match record.level {
        MetricLevel::Summary => MetricType::Summary,
        MetricLevel::Dev => MetricType::Dev,
        MetricLevel::Unknown => return None,
    };
    let labels = record
        .labels
        .iter()
        .map(|label| Label::new(label.name.to_string(), label.value.to_string()))
        .collect();
    let partition = match record.partition {
        Some(partition) => Some(size(partition)?),
        None => None,
    };
    let metric = Metric::new_with_labels(value, partition, labels).with_type(level);
    Some(match record.category {
        MetricCategoryRecord::None => metric,
        MetricCategoryRecord::Rows => metric.with_category(MetricCategory::Rows),
        MetricCategoryRecord::Bytes => metric.with_category(MetricCategory::Bytes),
        MetricCategoryRecord::Timing => metric.with_category(MetricCategory::Timing),
        MetricCategoryRecord::Uncategorized => metric.with_category(MetricCategory::Uncategorized),
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bilrost::Message;
    use datafusion::physical_plan::metrics::CustomMetricValue;

    use restate_types::net::distributed_query::NodeMetrics;

    use super::*;

    fn count(value: usize) -> Count {
        let count = Count::new();
        count.add(value);
        count
    }

    fn gauge(value: usize) -> Gauge {
        let gauge = Gauge::new();
        gauge.set(value);
        gauge
    }

    fn time(nanos: u64) -> Time {
        let time = Time::new();
        if nanos > 0 {
            time.add_duration(Duration::from_nanos(nanos));
        }
        time
    }

    fn timestamp(nanos: Option<i64>) -> Timestamp {
        let timestamp = Timestamp::new();
        if let Some(nanos) = nanos {
            timestamp.set(timestamp_ns_to_datetime(nanos).unwrap().and_utc());
        }
        timestamp
    }

    /// One metric of each standard kind, zero times included, with `name` for
    /// the named kinds and `n` for every number.
    fn every_kind(name: &str, n: usize) -> Vec<MetricValue> {
        let name = || Cow::Owned(name.to_owned());
        let pruning = PruningMetrics::new();
        pruning.add_pruned(n);
        pruning.add_matched(n);
        pruning.add_fully_matched(n);
        let ratio = |merge| {
            let ratio = RatioMetrics::new()
                .with_merge_strategy(merge)
                .with_display_raw_values(true);
            ratio.add_part(n);
            ratio.add_total(n);
            ratio
        };
        vec![
            MetricValue::OutputRows(count(n)),
            MetricValue::ElapsedCompute(time(0)),
            MetricValue::SpillCount(count(n)),
            MetricValue::SpilledBytes(count(n)),
            MetricValue::OutputBytes(count(n)),
            MetricValue::OutputBatches(count(n)),
            MetricValue::SpilledRows(count(n)),
            MetricValue::CurrentMemoryUsage(gauge(n)),
            MetricValue::Count {
                name: name(),
                count: count(n),
            },
            MetricValue::Gauge {
                name: name(),
                gauge: gauge(n),
            },
            MetricValue::PeakMemoryUsage {
                name: name(),
                gauge: gauge(n),
            },
            MetricValue::Time {
                name: name(),
                time: time(0),
            },
            MetricValue::Time {
                name: name(),
                time: time(1_500),
            },
            MetricValue::StartTimestamp(timestamp(Some(1_700_000_000_123_456_789))),
            MetricValue::EndTimestamp(timestamp(None)),
            MetricValue::PruningMetrics {
                name: name(),
                pruning_metrics: pruning,
            },
            MetricValue::Ratio {
                name: name(),
                ratio_metrics: ratio(RatioMergeStrategy::AddPartAddTotal),
            },
            MetricValue::Ratio {
                name: name(),
                ratio_metrics: ratio(RatioMergeStrategy::AddPartSetTotal),
            },
            MetricValue::Ratio {
                name: name(),
                ratio_metrics: ratio(RatioMergeStrategy::SetPartAddTotal),
            },
        ]
    }

    /// Every standard kind, zero times included, survives encoding and decoding
    /// with its value, labels, partition, level and category. A custom value, a
    /// timestamp beyond nanosecond range and an unknown level aren't carried,
    /// and a metric that doesn't fit takes nothing from the budget.
    #[test]
    fn records_keep_what_analyze_selects_by_within_a_budget() {
        let mut budget = Budget::for_task(0);
        for (i, value) in every_kind("read_rows", 11).into_iter().enumerate() {
            let metric = Metric::new_with_labels(value, Some(3), vec![Label::new("owner", "N2:1")])
                .with_type(if i % 2 == 0 {
                    MetricType::Summary
                } else {
                    MetricType::Dev
                });
            let metric = if i % 3 == 0 {
                metric.with_category(MetricCategory::Rows)
            } else {
                metric
            };
            let Encoded::Record(record) = encode(&metric, &mut budget) else {
                panic!("{metric} is supported and fits");
            };
            let decoded = decode(&record).unwrap();
            assert_eq!(decoded.to_string(), metric.to_string(), "{metric}");
            assert_eq!(
                decoded.value().as_usize(),
                metric.value().as_usize(),
                "{metric}"
            );
            if let (
                MetricValue::Ratio {
                    ratio_metrics: a, ..
                },
                MetricValue::Ratio {
                    ratio_metrics: b, ..
                },
            ) = (decoded.value(), metric.value())
            {
                assert_eq!(
                    format!("{:?}", a.merge_strategy()),
                    format!("{:?}", b.merge_strategy())
                );
                assert_eq!(a.display_raw_values(), b.display_raw_values());
            }
            if let (
                MetricValue::PruningMetrics {
                    pruning_metrics: a, ..
                },
                MetricValue::PruningMetrics {
                    pruning_metrics: b, ..
                },
            ) = (decoded.value(), metric.value())
            {
                assert_eq!(
                    (a.pruned(), a.matched(), a.fully_matched()),
                    (b.pruned(), b.matched(), b.fully_matched())
                );
            }
            assert_eq!(decoded.partition(), metric.partition(), "{metric}");
            assert_eq!(decoded.metric_type(), metric.metric_type(), "{metric}");
            assert_eq!(
                decoded.metric_category(),
                metric.metric_category(),
                "{metric}"
            );
            assert_eq!(
                decoded
                    .labels()
                    .iter()
                    .map(|label| (label.name().to_owned(), label.value().to_owned()))
                    .collect::<Vec<_>>(),
                vec![("owner".to_owned(), "N2:1".to_owned())],
                "{metric}"
            );
        }

        #[derive(Debug, Default)]
        struct Custom;
        impl std::fmt::Display for Custom {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "custom")
            }
        }
        impl CustomMetricValue for Custom {
            fn new_empty(&self) -> Arc<dyn CustomMetricValue> {
                Arc::new(Custom)
            }
            fn aggregate(&self, _: Arc<dyn CustomMetricValue + 'static>) {}
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }
            fn as_usize(&self) -> usize {
                0
            }
            fn is_eq(&self, _: &Arc<dyn CustomMetricValue>) -> bool {
                true
            }
        }
        let unsupported = [
            Metric::new(
                MetricValue::Custom {
                    name: "custom".into(),
                    value: Arc::new(Custom),
                },
                None,
            ),
            // Beyond the year 2262, nanoseconds since the epoch overflow.
            {
                let far = Timestamp::new();
                far.set(
                    datafusion::arrow::temporal_conversions::timestamp_s_to_datetime(
                        10_000_000_000,
                    )
                    .unwrap()
                    .and_utc(),
                );
                Metric::new(MetricValue::EndTimestamp(far), None)
            },
        ];
        let mut spent = Budget::for_task(MAX_BYTES - 1_000);
        for metric in &unsupported {
            assert!(
                matches!(encode(metric, &mut spent), Encoded::Unsupported),
                "{metric}"
            );
        }
        // A known value with an unknown level decodes to nothing.
        let Encoded::Record(mut record) = encode(
            &Metric::new(MetricValue::OutputRows(count(1)), None),
            &mut spent,
        ) else {
            panic!("fits");
        };
        record.level = MetricLevel::Unknown;
        assert!(decode(&record).is_none());
        assert!(decode(&MetricRecord::default()).is_none());

        // A long label doesn't fit a nearly spent budget, and takes nothing.
        let mut tight = Budget::for_task(MAX_BYTES - 300);
        let long = Metric::new_with_labels(
            MetricValue::OutputRows(Count::new()),
            None,
            vec![Label::new("owner", "x".repeat(200))],
        );
        assert!(matches!(encode(&long, &mut tight), Encoded::OverBudget));
        let short = Metric::new(MetricValue::OutputRows(Count::new()), None);
        assert!(matches!(encode(&short, &mut tight), Encoded::Record(_)));
        // Records run out at 1,024, however small they are.
        let mut records = Budget {
            records: MAX_RECORDS,
            bytes: usize::MAX,
        };
        for _ in 0..MAX_RECORDS {
            assert!(matches!(encode(&short, &mut records), Encoded::Record(_)));
        }
        assert!(matches!(encode(&short, &mut records), Encoded::OverBudget));
    }

    /// A record's charge bounds its encoded size in a report, framing included,
    /// at the varint length boundaries of its strings and with the largest
    /// numbers.
    #[test]
    fn charges_bound_encoded_records() {
        let framed = |record: MetricRecord| {
            let empty = NodeMetrics::default().encoded_len();
            NodeMetrics {
                ordinal: 0,
                metrics: vec![record],
            }
            .encoded_len()
                - empty
        };
        for length in [1, 127, 128, 16_383, 16_384, 60_000] {
            let name = "n".repeat(length);
            for value in every_kind(&name, usize::MAX) {
                for labels in [vec![], vec![Label::new(name.clone(), name.clone())]] {
                    let metric = Metric::new_with_labels(value.clone(), Some(usize::MAX), labels)
                        .with_type(MetricType::Summary)
                        .with_category(MetricCategory::Uncategorized);
                    let mut budget = Budget {
                        records: 1,
                        bytes: usize::MAX,
                    };
                    let Encoded::Record(record) = encode(&metric, &mut budget) else {
                        panic!("{metric} is supported");
                    };
                    let charged = usize::MAX - budget.bytes;
                    let encoded = framed(record);
                    assert!(encoded <= charged, "{metric}: {encoded} > {charged}");
                }
            }
        }
    }
}
