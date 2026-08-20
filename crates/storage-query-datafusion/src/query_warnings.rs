// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use parking_lot::Mutex;
use tracing::warn;

use restate_types::PlainNodeId;
use restate_types::identifiers::PartitionId;

/// The scan a warning refers to.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum WarningOrigin {
    /// A cluster node that did not answer a node-scoped fan-out scan.
    Node(PlainNodeId),
    /// A Restate partition that could not be scanned.
    Partition(PartitionId),
}

impl Display for WarningOrigin {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Node(node_id) => write!(f, "{node_id}"),
            Self::Partition(partition_id) => write!(f, "partition {partition_id}"),
        }
    }
}

/// A scan that was skipped instead of failing the query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryWarning {
    pub origin: WarningOrigin,
    pub message: String,
}

/// Per-query accumulator of scans that were skipped.
///
/// Installed as a [`datafusion::prelude::SessionConfig`] extension by
/// `QueryContext::execute`. Because a `TaskContext` inherits the session config, the same
/// instance is reachable both while the query is planned (where an unroutable partition is
/// dropped from the plan) and while it executes (where a partition that became unroutable
/// in between is skipped).
///
/// A non-empty collection means the result is **incomplete**: rows from the listed origins
/// are missing, so aggregates are lower bounds and a lookup may find nothing for a record
/// that exists.
#[derive(Debug, Default)]
pub struct QueryWarnings {
    // Keyed by origin so that repeated failures of the same origin collapse into one
    // warning (a point-read query produces one physical partition per requested key, all
    // resolving to the same Restate partition), and so the output order is deterministic.
    warnings: Mutex<BTreeMap<WarningOrigin, String>>,
}

impl QueryWarnings {
    pub(crate) fn record(&self, origin: WarningOrigin, message: String) {
        warn!(%origin, %message, "Skipping a scan that cannot be served; query results will be incomplete");
        self.warnings.lock().insert(origin, message);
    }

    /// The skipped scans, ordered by origin. Only meaningful once the query's record-batch
    /// stream has been consumed.
    pub fn collect(&self) -> Vec<QueryWarning> {
        self.warnings
            .lock()
            .iter()
            .map(|(origin, message)| QueryWarning {
                origin: origin.clone(),
                message: message.clone(),
            })
            .collect()
    }

    pub fn is_empty(&self) -> bool {
        self.warnings.lock().is_empty()
    }
}

/// Records `origin` in `warnings` if a sink is present, reporting whether it was recorded.
///
/// Returns `false` when there is no sink, in which case the caller must propagate the
/// original error: a scan may only be skipped when the skip can be reported.
pub(crate) fn try_record(
    warnings: Option<&Arc<QueryWarnings>>,
    origin: WarningOrigin,
    message: impl Into<String>,
) -> bool {
    match warnings {
        Some(warnings) => {
            warnings.record(origin, message.into());
            true
        }
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dedups_by_origin_and_orders_deterministically() {
        let warnings = QueryWarnings::default();
        assert!(warnings.is_empty());

        let p7 = WarningOrigin::Partition(PartitionId::from(7));
        let p2 = WarningOrigin::Partition(PartitionId::from(2));

        warnings.record(p7.clone(), "unroutable".to_owned());
        warnings.record(p2.clone(), "unroutable".to_owned());
        // the same partition failing twice must collapse into a single warning
        warnings.record(p7.clone(), "unroutable again".to_owned());

        let collected = warnings.collect();
        assert!(!warnings.is_empty());
        assert_eq!(
            collected,
            vec![
                QueryWarning {
                    origin: p2,
                    message: "unroutable".to_owned()
                },
                QueryWarning {
                    origin: p7,
                    message: "unroutable again".to_owned()
                },
            ]
        );
        assert_eq!("partition 7", collected[1].origin.to_string());
    }

    #[test]
    fn try_record_requires_a_sink() {
        let warnings = Arc::new(QueryWarnings::default());
        let origin = WarningOrigin::Partition(PartitionId::from(1));

        assert!(try_record(Some(&warnings), origin.clone(), "skipped"));
        assert_eq!(1, warnings.collect().len());

        // without a sink there is nowhere to report the skip, so it must not be swallowed
        assert!(!try_record(None, origin, "skipped"));
    }
}
