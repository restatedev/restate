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
use datafusion::physical_plan::expressions::{BinaryExpr, Column, InListExpr, IsNullExpr, Literal};
use datafusion::scalar::ScalarValue;

use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_storage_api::StorageError;
use restate_storage_api::invocation_status_table::ScanInvocationStatusTable;
use restate_storage_api::protobuf_types::v1::lazy::{
    InvocationStatusLazyFilter, InvocationStatusV2Lazy, StatusFilter,
};
use restate_types::errors::ConversionError;
use restate_types::identifiers::{InvocationId, PartitionKey};

use crate::context::{QueryContext, SelectPartitions};
use crate::invocation_status::row::append_invocation_status_row;
use crate::invocation_status::schema::{
    SysInvocationStatusBuilder, sys_invocation_status_sort_order,
};
use crate::partition_filter::FirstMatchingPartitionKeyExtractor;
use crate::partition_store_scanner::{LocalPartitionsScanner, ScanLocalPartition};
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

    fn create_filter(predicate: &Arc<dyn PhysicalExpr>) -> Self::Filter {
        InvocationStatusScanFilter::from_predicate(predicate)
    }

    fn for_each_row<
        F: for<'a> FnMut(Self::Item<'a>) -> ControlFlow<Result<(), Self::ConversionError>>
            + Send
            + Sync
            + 'static,
    >(
        partition_store: &PartitionStore,
        range: RangeInclusive<PartitionKey>,
        mut filter: InvocationStatusScanFilter,
        mut f: F,
    ) -> Result<impl Future<Output = Result<(), StorageError>> + Send, StorageError> {
        let lazy_filter = filter.lazy_filter();
        partition_store.for_each_invocation_status_lazy(range, lazy_filter, move |item| {
            filter.maybe_refresh();
            f(item)
        })
    }

    fn append_row<'a>(
        row_builder: &mut Self::Builder,
        (invocation_id, invocation_status): Self::Item<'a>,
    ) -> Result<(), ConversionError> {
        append_invocation_status_row(row_builder, invocation_id, invocation_status)
    }
}

/// Wraps an [`InvocationStatusLazyFilter`] derived from a DataFusion predicate.
/// The filter is built once with `Arc<AtomicU64>` timestamp thresholds. The
/// storage layer receives a clone (sharing the same Arcs), and `refresh()`
/// periodically re-evaluates the predicate to update the atomics in-place.
#[derive(Default)]
pub struct InvocationStatusScanFilter {
    predicate: Option<Arc<dyn PhysicalExpr>>,
    filter: InvocationStatusLazyFilter,
    rows_since_refresh: u8,
}

impl InvocationStatusScanFilter {
    /// Analyze a physical expression and build a lazy filter.
    /// Disabled when `ROW_FILTER=0` is set in the environment.
    pub fn from_predicate(predicate: &Arc<dyn PhysicalExpr>) -> Self {
        if std::env::var("ROW_FILTER").is_ok_and(|v| v == "0") {
            return Self::default();
        }
        Self {
            filter: build_lazy_filter(predicate),
            predicate: Some(Arc::clone(predicate)),
            rows_since_refresh: 0,
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
        self.rows_since_refresh = self.rows_since_refresh.wrapping_add(1);
        if self.rows_since_refresh == 0
            && let Some(predicate) = &self.predicate
        {
            refresh_lazy_filter(&self.filter, predicate);
        }
    }
}

/// Build an [`InvocationStatusLazyFilter`] from a physical expression.
/// Creates `Arc<AtomicU64>` for timestamp thresholds so they can be
/// updated in-place later via `refresh_lazy_filter`.
fn build_lazy_filter(predicate: &Arc<dyn PhysicalExpr>) -> InvocationStatusLazyFilter {
    let mut statuses: Option<StatusFilter> = None;
    let created_after = Arc::new(AtomicU64::new(0));
    let modified_after = Arc::new(AtomicU64::new(0));

    for conjunct in split_conjunction(predicate) {
        if let Some(status_filter) = extract_status_filter(conjunct.as_ref()) {
            statuses = Some(match statuses {
                None => status_filter,
                Some(existing) => existing.intersect(status_filter),
            });
        } else if let Some((col_name, millis)) = extract_timestamp_lower_bound(conjunct.as_ref()) {
            let target = match col_name {
                "created_at" => &created_after,
                "modified_at" => &modified_after,
                _ => continue,
            };
            target.fetch_max(millis, Ordering::Relaxed);
        }
    }

    InvocationStatusLazyFilter {
        statuses,
        created_after: Some(created_after),
        modified_after: Some(modified_after),
    }
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

/// Try to extract a status filter from a single predicate.
/// Handles: `status = 'x'`, `status != 'x'`, `status IN ('a', 'b')`,
/// `status NOT IN ('a', 'b')`, `(status = 'x') IS [NOT] DISTINCT FROM true`,
/// and OR trees of status filters.
fn extract_status_filter(predicate: &dyn PhysicalExpr) -> Option<StatusFilter> {
    // Handle IN list: status IN ('a', 'b', ...)
    if let Some(in_list) = predicate.as_any().downcast_ref::<InListExpr>() {
        let col = in_list.expr().as_any().downcast_ref::<Column>()?;
        if col.name() != "status" {
            return None;
        }
        let mut filter = if in_list.negated() {
            // base case is no status excluded
            StatusFilter::all()
        } else {
            // base case is no status included
            StatusFilter::none()
        };
        for item in in_list.list() {
            let lit = item.as_any().downcast_ref::<Literal>()?;
            let ScalarValue::LargeUtf8(Some(s)) = lit.value() else {
                return None;
            };
            let Ok(status) = s.parse() else {
                // Don't try and understand comparisons to status that we don't recognise
                return None;
            };
            if in_list.negated() {
                filter = filter.intersect(StatusFilter::exclude(status));
            } else {
                filter = filter.union(StatusFilter::include(status));
            }
        }
        return Some(filter);
    }

    let binary = predicate.as_any().downcast_ref::<BinaryExpr>()?;

    match binary.op() {
        // These weird constructs come from sys_invocation CASE pushdown. Because status cannot be null:
        // `(status = 'x') IS NOT DISTINCT FROM true` is equivalent to `status = 'x'`
        // `(status = 'x') IS DISTINCT FROM true` is equivalent to `status != 'x'`
        Operator::IsNotDistinctFrom | Operator::IsDistinctFrom => {
            let ScalarValue::Boolean(Some(true)) = extract_literal_value(binary)? else {
                return None;
            };
            let mut filter = extract_status_filter(binary.left().as_ref())?;
            if *binary.op() == Operator::IsDistinctFrom {
                filter = filter.negate();
            }
            Some(filter)
        }
        // Handle OR trees: union both sides if both yield status filters.
        Operator::Or => {
            let left = extract_status_filter(binary.left().as_ref())?;
            let right = extract_status_filter(binary.right().as_ref())?;
            Some(left.union(right))
        }
        // Handle simple equality: status = 'x' or status != 'x'
        Operator::Eq | Operator::NotEq => {
            let (col, lit) = extract_column_literal(binary)?;
            if col.name() != "status" {
                return None;
            }
            let ScalarValue::LargeUtf8(Some(s)) = lit.value() else {
                return None;
            };
            let Ok(status) = s.parse() else {
                // Don't try and understand comparisons to status that we don't recognise
                return None;
            };
            if *binary.op() == Operator::Eq {
                Some(StatusFilter::include(status))
            } else {
                Some(StatusFilter::exclude(status))
            }
        }
        _ => None,
    }
}

/// Try to extract a timestamp lower bound from a predicate.
///
/// Handles simple comparisons (`col > ts`, `col >= ts`, `col = ts`,
/// `ts < col`, `ts <= col`), IS NULL short-circuits, and compound
/// expressions produced by DataFusion for keyset pagination:
///
/// ```text
/// (IS NULL(created_at) OR created_at > X)
///   OR (created_at = X AND id < Y)
/// ```
///
/// For OR, we take the weaker (minimum) bound from each side. Each side
/// is split by AND and the best simple bound is extracted from the
/// conjuncts. Returns `(column_name, millis_u64)`.
fn extract_timestamp_lower_bound(predicate: &dyn PhysicalExpr) -> Option<(&str, u64)> {
    let binary = predicate.as_any().downcast_ref::<BinaryExpr>()?;

    match binary.op() {
        Operator::Or => {
            // IS NULL(col) OR expr — IS NULL is trivially false for non-nullable
            // timestamp columns, so the bound is just the right side.
            if let Some(null) = binary.left().as_any().downcast_ref::<IsNullExpr>()
                && let Some(col) = null.arg().as_any().downcast_ref::<Column>()
                && let Some((r_col, ms)) = extract_timestamp_lower_bound(binary.right().as_ref())
                && r_col == col.name()
            {
                return Some((r_col, ms));
            }

            // General OR: both branches can match, so take the weaker (min) bound.
            let (l_col, l_ms) = extract_timestamp_lower_bound(binary.left().as_ref())?;
            let (r_col, r_ms) = extract_timestamp_lower_bound(binary.right().as_ref())?;
            if l_col == r_col {
                Some((l_col, l_ms.min(r_ms)))
            } else {
                None
            }
        }
        Operator::And => {
            // General AND: both branches must match, so take the stronger (max) bound
            // We can ignore anything we don't parse
            let left_bound = extract_timestamp_lower_bound(binary.left().as_ref());
            let right_bound = extract_timestamp_lower_bound(binary.right().as_ref());
            if let (Some((l_col, l_ms)), Some((r_col, r_ms))) = (left_bound, right_bound) {
                if l_col == r_col {
                    Some((l_col, l_ms.max(r_ms)))
                } else {
                    None
                }
            } else if left_bound.is_some() {
                left_bound
            } else if right_bound.is_some() {
                right_bound
            } else {
                None
            }
        }
        _ => extract_simple_timestamp_bound(binary),
    }
}

/// Extract a lower bound from a simple comparison: `col >/>=  lit`, `col = lit`,
/// or `lit </<=  col`.
fn extract_simple_timestamp_bound(binary: &BinaryExpr) -> Option<(&str, u64)> {
    // Try col op literal
    if let Some((col, lit)) = extract_column_literal(binary) {
        let millis = extract_timestamp_millis(lit.value())?;
        return match binary.op() {
            Operator::Gt => Some((col.name(), millis.saturating_add(1))),
            Operator::GtEq | Operator::Eq => Some((col.name(), millis)),
            _ => None,
        };
    }

    // Try literal op col (reversed operand order)
    if let Some((col, lit)) = extract_literal_column(binary) {
        let millis = extract_timestamp_millis(lit.value())?;
        return match binary.op() {
            Operator::Lt => Some((col.name(), millis.saturating_add(1))),
            Operator::LtEq | Operator::Eq => Some((col.name(), millis)),
            _ => None,
        };
    }

    None
}

/// Extract (Column, Literal) from a BinaryExpr where left=Column, right=Literal.
fn extract_column_literal(binary: &BinaryExpr) -> Option<(&Column, &Literal)> {
    let col = binary.left().as_any().downcast_ref::<Column>()?;
    let lit = binary.right().as_any().downcast_ref::<Literal>()?;
    Some((col, lit))
}

/// Extract the ScalarValue from whichever side of a BinaryExpr is a Literal.
fn extract_literal_value(binary: &BinaryExpr) -> Option<&ScalarValue> {
    binary
        .right()
        .as_any()
        .downcast_ref::<Literal>()
        .or_else(|| binary.left().as_any().downcast_ref::<Literal>())
        .map(|lit| lit.value())
}

/// Extract (Column, Literal) from a BinaryExpr where left=Literal, right=Column.
fn extract_literal_column(binary: &BinaryExpr) -> Option<(&Column, &Literal)> {
    let lit = binary.left().as_any().downcast_ref::<Literal>()?;
    let col = binary.right().as_any().downcast_ref::<Column>()?;
    Some((col, lit))
}

/// Extract milliseconds as u64 from a ScalarValue timestamp.
fn extract_timestamp_millis(value: &ScalarValue) -> Option<u64> {
    match value {
        ScalarValue::TimestampMillisecond(Some(ms), _) => u64::try_from(*ms).ok(),
        _ => None,
    }
}
