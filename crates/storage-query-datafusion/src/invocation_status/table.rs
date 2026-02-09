// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Debug;
use std::ops::{ControlFlow, RangeInclusive};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use datafusion::logical_expr::Operator;
use datafusion::physical_expr::split_conjunction;
use datafusion::physical_expr_common::physical_expr::snapshot_physical_expr;
use datafusion::physical_plan::PhysicalExpr;
use datafusion::physical_plan::expressions::{BinaryExpr, Column, IsNullExpr};
use datafusion::scalar::ScalarValue;

use enumset::EnumSet;
use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_storage_api::StorageError;
use restate_storage_api::invocation_status_table::{
    InvocationStatusDiscriminants, InvocationStatusFilter, InvocationStatusRange,
    ScanInvocationStatusTable,
};
use restate_storage_api::protobuf_types::v1::lazy::{
    InvocationStatusLazyFilter, InvocationStatusV2Lazy,
};
use restate_types::errors::ConversionError;
use restate_types::identifiers::{InvocationId, PartitionKey};

use crate::context::{QueryContext, SelectPartitions};
use crate::filter::{
    FirstMatchingPartitionKeyExtractor, InList, InvocationIdFilter, extract_column_literal,
};
use crate::invocation_status::row::append_invocation_status_row;
use crate::invocation_status::schema::{
    SysInvocationStatusBuilder, sys_invocation_status_sort_order,
};
use crate::partition_store_scanner::{
    LocalPartitionsScanner, ScanLocalPartition, ScanLocalPartitionFilter,
};
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::statistics::{
    DEPLOYMENT_ROW_ESTIMATE, RowEstimate, SERVICE_ROW_ESTIMATE, TableStatisticsBuilder,
};
use crate::table_providers::{PartitionedTableProvider, ScanPartition};

const NAME: &str = "sys_invocation_status";

pub(crate) fn register_self(
    ctx: &QueryContext,
    partition_selector: impl SelectPartitions,
    partition_store_manager: Arc<PartitionStoreManager>,
    remote_scanner_manager: &RemoteScannerManager,
) -> datafusion::common::Result<()> {
    let local_scanner = Arc::new(LocalPartitionsScanner::new(
        partition_store_manager,
        StatusScanner,
    )) as Arc<dyn ScanPartition>;

    let schema = SysInvocationStatusBuilder::schema();
    let statistics = TableStatisticsBuilder::new(schema.clone())
        .with_num_rows_estimate(RowEstimate::Large)
        .with_partition_key()
        .with_primary_key("id")
        .with_foreign_key("pinned_deployment_id", DEPLOYMENT_ROW_ESTIMATE)
        .with_foreign_key("target_service_name", SERVICE_ROW_ESTIMATE);

    let status_table = PartitionedTableProvider::new(
        partition_selector,
        schema,
        sys_invocation_status_sort_order(),
        remote_scanner_manager.create_distributed_scanner(NAME, local_scanner),
        FirstMatchingPartitionKeyExtractor::default()
            .with_service_key("target_service_key")
            .with_invocation_id("id"),
    )
    .with_statistics(statistics.build());
    ctx.register_partitioned_table(NAME, Arc::new(status_table))
}

#[derive(Debug, Clone)]
struct StatusScanner;

impl ScanLocalPartition for StatusScanner {
    type Builder = SysInvocationStatusBuilder;
    type Item<'a> = (InvocationId, &'a InvocationStatusV2Lazy<'a>);
    type ConversionError = ConversionError;
    type Filter = InvocationStatusScanFilter;

    fn for_each_row<
        F: for<'a> FnMut(Self::Item<'a>) -> ControlFlow<Result<(), Self::ConversionError>>
            + Send
            + Sync
            + 'static,
    >(
        partition_store: &PartitionStore,
        mut filter: InvocationStatusScanFilter,
        mut f: F,
    ) -> Result<impl Future<Output = Result<(), StorageError>> + Send, StorageError> {
        let lazy_filter = filter.lazy_filter();
        partition_store.for_each_invocation_status_lazy(
            InvocationStatusFilter::new(filter.range(), lazy_filter),
            move |item| {
                filter.maybe_refresh();
                f(item)
            },
        )
    }

    fn append_row<'a>(
        row_builder: &mut Self::Builder,
        (invocation_id, invocation_status): Self::Item<'a>,
    ) -> Result<(), ConversionError> {
        append_invocation_status_row(row_builder, invocation_id, invocation_status)
    }
}

/// Wraps an [`InvocationStatusLazyFilter`] derived from a DataFusion predicate,
/// combined with the key range extracted from the `InvocationIdFilter`.
/// The filter is built once with `Arc<AtomicU64>` timestamp thresholds. The
/// storage layer receives a clone (sharing the same Arcs), and `refresh()`
/// periodically re-evaluates the predicate to update the atomics in-place.
pub struct InvocationStatusScanFilter {
    id_filter: InvocationIdFilter,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    filter: InvocationStatusLazyFilter,
    rows_since_refresh: u8,
}

impl ScanLocalPartitionFilter for InvocationStatusScanFilter {
    fn new(range: RangeInclusive<PartitionKey>, predicate: Option<Arc<dyn PhysicalExpr>>) -> Self {
        let id_filter = InvocationIdFilter::new(range, predicate.clone());

        let lazy_filter = predicate
            .as_ref()
            .filter(|_| !std::env::var("ROW_FILTER").is_ok_and(|v| v == "0"))
            .and_then(build_lazy_filter)
            .unwrap_or_default();

        Self {
            id_filter,
            filter: lazy_filter,
            predicate,
            rows_since_refresh: 0,
        }
    }
}

impl InvocationStatusScanFilter {
    /// Return the key range for this scan.
    pub fn range(&self) -> InvocationStatusRange {
        if let Some(invocation_ids) = self.id_filter.invocation_ids.clone() {
            InvocationStatusRange::InvocationId(invocation_ids)
        } else {
            InvocationStatusRange::PartitionKey(self.id_filter.partition_keys.clone())
        }
    }

    /// Clone the lazy filter for the storage layer. The clone shares
    /// the same `Arc<AtomicU64>` thresholds, so in-place updates from
    /// `refresh()` are visible to the storage layer.
    pub fn lazy_filter(&self) -> InvocationStatusLazyFilter {
        self.filter.clone()
    }

    /// Re-evaluate the predicate and update the atomic thresholds in-place.
    pub fn maybe_refresh(&mut self) {
        if let Some(predicate) = &self.predicate {
            self.rows_since_refresh = self.rows_since_refresh.wrapping_add(1);
            if self.rows_since_refresh == 0 {
                refresh_lazy_filter(&self.filter, predicate);
            }
        }
    }
}

/// Build an [`InvocationStatusLazyFilter`] from a physical expression.
/// Creates `Arc<AtomicU64>` for timestamp thresholds so they can be
/// updated in-place later via `refresh_lazy_filter`.
fn build_lazy_filter(predicate: &Arc<dyn PhysicalExpr>) -> Option<InvocationStatusLazyFilter> {
    let mut statuses: EnumSet<InvocationStatusDiscriminants> = EnumSet::all();
    let created_after = Arc::new(AtomicU64::new(0));
    let modified_after = Arc::new(AtomicU64::new(0));

    let Ok(predicate) = snapshot_physical_expr(predicate.clone()) else {
        return None;
    };

    for conjunct in split_conjunction(&predicate) {
        if let Some(status_filter) = extract_status_filter(conjunct) {
            statuses = statuses.intersection(status_filter);
        } else if let Some((col_name, millis)) = extract_timestamp_lower_bound(conjunct.as_ref()) {
            let target = match col_name {
                "created_at" => &created_after,
                "modified_at" => &modified_after,
                _ => continue,
            };
            target.fetch_max(millis, Ordering::Relaxed);
        }
    }

    Some(InvocationStatusLazyFilter {
        statuses,
        created_after: Some(created_after),
        modified_after: Some(modified_after),
    })
}

/// Re-evaluate the predicate and update the atomic thresholds in the
/// existing filter in-place. Status filters are static and not refreshed.
fn refresh_lazy_filter(filter: &InvocationStatusLazyFilter, predicate: &Arc<dyn PhysicalExpr>) {
    let Ok(predicate) = snapshot_physical_expr(predicate.clone()) else {
        return;
    };
    for conjunct in split_conjunction(&predicate) {
        if let Some((col_name, millis)) = extract_timestamp_lower_bound(conjunct.as_ref()) {
            let target = match col_name {
                "created_at" => &filter.created_after,
                "modified_at" => &filter.modified_after,
                _ => continue,
            };
            if let Some(atomic) = target {
                atomic.store(millis, Ordering::Relaxed);
            }
        }
    }
}

/// Try to extract a status filter from a single predicate conjunct.
/// Delegates to [`InList::parse`] for expression traversal, then converts
/// the matched literal values to an [`EnumSet`] of status discriminants.
fn extract_status_filter(
    conjunct: &Arc<dyn PhysicalExpr>,
) -> Option<EnumSet<InvocationStatusDiscriminants>> {
    // We allow quite a large max depth here because the status filter expressions
    // that get pushed through the invocation_status join are pretty ugly.
    let in_list = InList::parse(conjunct, 10)?;
    if in_list.col.name() != "status" {
        return None;
    }

    let mut set = if in_list.negated {
        EnumSet::all()
    } else {
        EnumSet::empty()
    };
    for value in &in_list.list {
        let s = value.try_as_str()??;
        let Ok(status) = s.parse::<InvocationStatusDiscriminants>() else {
            return None;
        };
        if in_list.negated {
            set.remove(status);
        } else {
            set.insert(status);
        }
    }
    Some(set)
}

/// Try to extract a timestamp lower bound from a predicate.
fn extract_timestamp_lower_bound(predicate: &dyn PhysicalExpr) -> Option<(&str, u64)> {
    let binary = predicate.as_any().downcast_ref::<BinaryExpr>()?;

    match binary.op() {
        Operator::Or => {
            if let Some(null) = binary.left().as_any().downcast_ref::<IsNullExpr>()
                && let Some(col) = null.arg().as_any().downcast_ref::<Column>()
                && let Some((r_col, ms)) = extract_timestamp_lower_bound(binary.right().as_ref())
                && r_col == col.name()
            {
                // ORDER BY creates dynamic filters like:
                // created_at IS NULL OR created_at > x
                // and created_at can never be null so we can ignore the null condition
                return Some((r_col, ms));
            }

            let (l_col, l_ms) = extract_timestamp_lower_bound(binary.left().as_ref())?;
            let (r_col, r_ms) = extract_timestamp_lower_bound(binary.right().as_ref())?;
            if l_col == r_col {
                Some((l_col, l_ms.min(r_ms)))
            } else {
                None
            }
        }
        Operator::And => {
            match (
                extract_timestamp_lower_bound(binary.left().as_ref()),
                extract_timestamp_lower_bound(binary.right().as_ref()),
            ) {
                (Some((l_col, l_ms)), Some((r_col, r_ms))) if l_col == r_col => {
                    Some((l_col, l_ms.max(r_ms)))
                }
                (some @ Some(_), None) | (None, some @ Some(_)) => some,
                _ => None,
            }
        }
        _ => extract_simple_timestamp_bound(binary),
    }
}

/// Extract a lower bound from a simple comparison.
fn extract_simple_timestamp_bound(binary: &BinaryExpr) -> Option<(&str, u64)> {
    let (col, lit, flip) =
        if let Some((col, lit)) = extract_column_literal(binary.left(), binary.right()) {
            Some((col, lit, false))
        } else if let Some((col, lit)) = extract_column_literal(binary.right(), binary.left()) {
            Some((col, lit, true))
        } else {
            None
        }?;

    let millis = match lit.value() {
        ScalarValue::TimestampMillisecond(Some(ms), _) => u64::try_from(*ms).ok(),
        _ => None,
    }?;

    match (binary.op(), flip) {
        (Operator::Gt, false) | (Operator::Lt, true) => {
            Some((col.name(), millis.saturating_add(1)))
        }
        (Operator::GtEq, false) | (Operator::LtEq, true) => Some((col.name(), millis)),
        (Operator::Eq, _) => Some((col.name(), millis)),
        _ => None,
    }
}
