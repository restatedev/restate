// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! What a task did, for `EXPLAIN ANALYZE`: a report the worker returns when
//! the task closes, if the coordinator asked for one when installing it.
//!
//! ```text
//! coordinator                                  worker
//! Install{.., analyze: Some} ────────────────▶ remembers the request
//!                           ◀──────────────── Installed{.., analyze: Some}
//! Execute / Next … (unchanged)
//! Close ─────────────────────────────────────▶ builds one bounded report
//!                           ◀──────────────── Closed{report: Some(..)}
//! ```
//!
//! Every field is new and optional: a worker that predates reports ignores the
//! request, answers `Installed` without the acknowledgment and `Closed` without
//! a report, exactly as before. The report holds the shape of the plan the
//! worker executed, so the coordinator attaches metrics only to the same nodes,
//! and the metrics of each node by its position in that shape.

use bytes::Bytes;

use restate_encoding::BilrostNewType;
use restate_util_string::ReString;

/// Asks for a report when installing a task, and acknowledges the request in
/// `Installed`. Its presence is the request.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, bilrost::Message)]
pub struct QueryTaskAnalyze {}

/// The answer to `Close`: the task's report, if it was asked for and built.
#[derive(Debug, Clone, Default, PartialEq, bilrost::Message)]
pub struct QueryTaskClosed {
    /// A [`QueryTaskReport`], encoded on its own. The coordinator decodes it
    /// only after checking its size, so a malformed or oversized report spoils
    /// only itself, not the reply that carries it. On the wire it's the same as
    /// the report inline.
    #[bilrost(1)]
    pub report: Option<Bytes>,
}

/// What a task did. Built within a budget: whatever didn't fit is counted in
/// `left_out`, never dropped silently, and positions never shift.
#[derive(Debug, Clone, Default, PartialEq, bilrost::Message)]
pub struct QueryTaskReport {
    #[bilrost(1)]
    pub outcome: TaskOutcome,
    /// The executed plan's nodes in pre-order. Empty if the shape didn't fit.
    #[bilrost(2)]
    pub shape: Vec<PlanNodeShape>,
    /// The metrics of the nodes that have any, by their position in `shape`.
    #[bilrost(3)]
    pub nodes: Vec<NodeMetrics>,
    /// Metrics of the task as a whole.
    #[bilrost(4)]
    pub task: Vec<MetricRecord>,
    #[bilrost(5)]
    pub left_out: LeftOut,
    /// Metrics of kinds the report can't carry, such as unknown custom ones.
    #[bilrost(6)]
    pub unsupported: u32,
    /// Lanes still pulling rows when the task closed: their last batch may be
    /// missing from the metrics.
    #[bilrost(7)]
    pub unsettled_lanes: u32,
    /// Nodes that occur more than once in the plan and share one set of
    /// metrics: their numbers can't be told apart, so none are reported.
    #[bilrost(8)]
    pub ambiguous_nodes: u32,
    /// FNV-1a of the plan bytes the task was installed with, so that the report
    /// attaches only to the plan the coordinator sent for that task.
    #[bilrost(9)]
    pub plan_digest: u64,
}

/// How a task's execution ended, as far as its streams tell. Open-ended, like
/// every number-coded field of a report: a value this node doesn't know, for
/// example from a newer node, decodes and reads as unknown. Never renumber or
/// reuse.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, BilrostNewType)]
pub struct TaskOutcome(pub u32);

impl TaskOutcome {
    pub const UNKNOWN: Self = Self(0);
    /// Every output stream ended without an error.
    pub const COMPLETED: Self = Self(1);
    /// Not every stream ended and none failed: an early LIMIT, a cancellation
    /// or another task's failure, which the worker can't tell apart.
    pub const INCOMPLETE: Self = Self(2);
    /// A stream ended with an error.
    pub const FAILED: Self = Self(3);
}

/// One node of an executed plan: its name, how many children it has, and a
/// hash of its configuration as displayed (predicate, order, fetch, source),
/// so that two nodes of the same kind can't be swapped unnoticed.
#[derive(Debug, Clone, Default, PartialEq, Eq, bilrost::Message)]
pub struct PlanNodeShape {
    #[bilrost(1)]
    pub name: ReString,
    #[bilrost(2)]
    pub children: u32,
    #[bilrost(3)]
    pub identity: u64,
}

/// The metrics of the node at pre-order position `ordinal`.
#[derive(Debug, Clone, Default, PartialEq, bilrost::Message)]
pub struct NodeMetrics {
    #[bilrost(1)]
    pub ordinal: u32,
    #[bilrost(2)]
    pub metrics: Vec<MetricRecord>,
}

/// What the report's budget left out.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, bilrost::Message)]
pub struct LeftOut {
    /// The whole shape didn't fit: no node metrics are attached.
    #[bilrost(1)]
    pub shape: bool,
    #[bilrost(2)]
    pub nodes: u32,
    #[bilrost(3)]
    pub metrics: u32,
}

/// One DataFusion metric: its value, labels, partition, type and category.
#[derive(Debug, Clone, Default, PartialEq, bilrost::Message)]
pub struct MetricRecord {
    #[bilrost(oneof(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19))]
    pub value: MetricRecordValue,
    #[bilrost(20)]
    pub labels: Vec<MetricLabel>,
    #[bilrost(21)]
    pub partition: Option<u64>,
    #[bilrost(22)]
    pub level: MetricLevel,
    #[bilrost(23)]
    pub category: MetricCategoryRecord,
}

/// A metric's value. Counts, gauges, bytes and rows are plain numbers; times are
/// nanoseconds; timestamps are nanoseconds since the Unix epoch, absent if
/// unset.
#[derive(Debug, Clone, Default, PartialEq, bilrost::Oneof)]
pub enum MetricRecordValue {
    #[default]
    Unknown,
    #[bilrost(1)]
    OutputRows(u64),
    #[bilrost(2)]
    ElapsedComputeNanos(u64),
    #[bilrost(3)]
    SpillCount(u64),
    #[bilrost(4)]
    SpilledBytes(u64),
    #[bilrost(5)]
    OutputBytes(u64),
    #[bilrost(6)]
    OutputBatches(u64),
    #[bilrost(7)]
    SpilledRows(u64),
    #[bilrost(8)]
    CurrentMemoryUsage(u64),
    #[bilrost(9)]
    Count(NamedValue),
    #[bilrost(10)]
    Gauge(NamedValue),
    #[bilrost(11)]
    PeakMemoryUsage(NamedValue),
    #[bilrost(12)]
    TimeNanos(NamedValue),
    #[bilrost(13)]
    StartTimestamp(Timestamp),
    #[bilrost(14)]
    EndTimestamp(Timestamp),
    #[bilrost(15)]
    Pruning(NamedPruning),
    #[bilrost(16)]
    Ratio(NamedRatio),
    /// The distributed runtime's latency metrics, in nanoseconds.
    #[bilrost(17)]
    Latency(NamedLatency),
    /// The distributed runtime's byte counter.
    #[bilrost(18)]
    BytesCounter(NamedValue),
    /// The distributed runtime's maximum gauge.
    #[bilrost(19)]
    MaxGauge(NamedValue),
}

/// A named number.
#[derive(Debug, Clone, Default, PartialEq, Eq, bilrost::Message)]
pub struct NamedValue {
    #[bilrost(1)]
    pub name: ReString,
    #[bilrost(2)]
    pub value: u64,
}

/// A timestamp in nanoseconds since the Unix epoch, absent if unset.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, bilrost::Message)]
pub struct Timestamp {
    #[bilrost(1)]
    pub nanos: Option<i64>,
}

/// Pruning counts.
#[derive(Debug, Clone, Default, PartialEq, Eq, bilrost::Message)]
pub struct NamedPruning {
    #[bilrost(1)]
    pub name: ReString,
    #[bilrost(2)]
    pub pruned: u64,
    #[bilrost(3)]
    pub matched: u64,
    #[bilrost(4)]
    pub fully_matched: u64,
}

/// A ratio's two parts, how it merges and how it displays.
#[derive(Debug, Clone, Default, PartialEq, Eq, bilrost::Message)]
pub struct NamedRatio {
    #[bilrost(1)]
    pub name: ReString,
    #[bilrost(2)]
    pub part: u64,
    #[bilrost(3)]
    pub total: u64,
    #[bilrost(4)]
    pub merge: RatioMerge,
    #[bilrost(5)]
    pub display_raw_values: bool,
}

/// How a ratio merges with another, named as DataFusion's `RatioMergeStrategy`.
/// Open-ended (see [`TaskOutcome`]).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, BilrostNewType)]
pub struct RatioMerge(pub u32);

impl RatioMerge {
    pub const ADD_PART_ADD_TOTAL: Self = Self(0);
    pub const ADD_PART_SET_TOTAL: Self = Self(1);
    pub const SET_PART_ADD_TOTAL: Self = Self(2);
}

/// One of the distributed runtime's latency metrics.
#[derive(Debug, Clone, Default, PartialEq, Eq, bilrost::Message)]
pub struct NamedLatency {
    #[bilrost(1)]
    pub name: ReString,
    #[bilrost(2)]
    pub kind: LatencyKind,
    /// The latency for min, max and first; the sum for avg.
    #[bilrost(3)]
    pub nanos: u64,
    /// How many latencies an avg was taken over; zero for the others.
    #[bilrost(4)]
    pub count: u64,
}

/// Which latency metric. Open-ended (see [`TaskOutcome`]).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, BilrostNewType)]
pub struct LatencyKind(pub u32);

impl LatencyKind {
    pub const UNKNOWN: Self = Self(0);
    pub const MIN: Self = Self(1);
    pub const MAX: Self = Self(2);
    pub const AVG: Self = Self(3);
    pub const FIRST: Self = Self(4);
}

/// A metric label.
#[derive(Debug, Clone, Default, PartialEq, Eq, bilrost::Message)]
pub struct MetricLabel {
    #[bilrost(1)]
    pub name: ReString,
    #[bilrost(2)]
    pub value: ReString,
}

/// DataFusion's metric type: which `analyze_level` shows it. Open-ended (see
/// [`TaskOutcome`]).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, BilrostNewType)]
pub struct MetricLevel(pub u32);

impl MetricLevel {
    pub const UNKNOWN: Self = Self(0);
    pub const SUMMARY: Self = Self(1);
    pub const DEV: Self = Self(2);
}

/// DataFusion's metric category, `NONE` if the metric has none. Open-ended (see
/// [`TaskOutcome`]).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, BilrostNewType)]
pub struct MetricCategoryRecord(pub u32);

impl MetricCategoryRecord {
    pub const NONE: Self = Self(0);
    pub const ROWS: Self = Self(1);
    pub const BYTES: Self = Self(2);
    pub const TIMING: Self = Self(3);
    pub const UNCATEGORIZED: Self = Self(4);
}
