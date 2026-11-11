// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use http::HeaderMap;

use restate_clock::UniqueTimestamp;
use restate_util_string::ReString;

use crate::NodeWarning;
use crate::metrics::MetricsSet;

/// Information known before result streaming starts.
#[derive(Debug, Clone)]
pub struct QueryMetadata {
    pub session_id: ReString,
    /// Query timestamp, used together with `session_id` to identify an execution.
    /// A fresh timestamp is allocated before planning from the environment's shared clock.
    /// This does not yet establish a storage snapshot or a cluster-wide read cutoff.
    pub query_ts: UniqueTimestamp,
    /// Allow-listed request headers retained from the session, including repeated values.
    pub headers: HeaderMap,
    /// Diagnostic SQL with literal values replaced by `?`, or an omission marker.
    /// Identifiers, aliases, and type parameters are retained. Never used for execution.
    pub redacted_sql: ReString,
    /// Parsing, logical planning, optimization, and physical planning.
    pub planning_duration: Duration,
}

/// Query-scoped observations, independent of the result consumer and transport.
pub trait QueryDiagnostics: Send + Sync {
    /// Reads allocation-free output counters and timings without traversing the plan.
    fn snapshot(&self) -> QueryStats;

    /// Reads coordinator operator metrics on demand. Each call walks the plan afresh;
    /// partitions can register metrics lazily. Native DataFusion metric sets preserve
    /// names, labels, partitions, and custom values. Their counters remain live;
    /// only the set membership is snapshotted. Remote scanner internals require
    /// separate reporting.
    fn plan_metrics(&self) -> QueryOperatorStats;

    /// Copies accumulated warnings without draining them or traversing the plan.
    fn warnings(&self) -> Vec<NodeWarning>;
}

/// State of the output stream, not an acknowledgement of remote-task cleanup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryStatus {
    /// The stream has not yet ended or been dropped.
    Running,
    /// Normal end of stream. Warnings can still indicate partial node results.
    Completed,
    /// The output stream yielded an execution error.
    Failed,
    /// The consumer dropped the stream before observing its end or an error.
    Cancelled,
}

/// Lightweight summary, independent of detailed operator metrics and warnings.
#[derive(Debug, Clone, Copy)]
pub struct QueryStats {
    pub status: QueryStatus,
    /// Wall time from entry to `QuerySession::execute`, including planning and
    /// consumer backpressure. Stops when the output stream ends or is dropped;
    /// does not include session creation or response serialization after that point.
    pub total_duration: Duration,
    /// Wall time since execution started, including consumer backpressure,
    /// excluding planning. Stops when the stream ends or is dropped.
    pub execution_duration: Duration,
    /// Rows and batches yielded by the output stream, not sums over operators.
    pub output_rows: u64,
    pub output_batches: u64,
}

/// Preserves the operator tree, partition identities, and labels without aggregation.
#[derive(Debug, Clone)]
pub struct QueryOperatorStats {
    pub name: ReString,
    pub metrics: Option<MetricsSet>,
    pub children: Vec<QueryOperatorStats>,
}
