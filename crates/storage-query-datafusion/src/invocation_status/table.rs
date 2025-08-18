// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::Ordering;
use std::collections::HashSet;
use std::fmt::Debug;
use std::ops::{ControlFlow, RangeInclusive};
use std::sync::Arc;

use datafusion::logical_expr::Operator;
use datafusion::physical_expr::split_conjunction;
use datafusion::physical_expr::utils::collect_columns;
use datafusion::physical_expr_common::physical_expr::{
    snapshot_generation, snapshot_physical_expr,
};
use datafusion::physical_plan::PhysicalExpr;
use datafusion::physical_plan::expressions::{BinaryExpr, Column, IsNullExpr, Literal};
use datafusion::scalar::ScalarValue;
use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_storage_api::StorageError;
use restate_storage_api::invocation_status_table::{
    FilterInvocationStatus, FilterInvocationTime, InvocationStatus, ScanInvocationStatusTable,
};
use restate_types::identifiers::{InvocationId, PartitionKey};

use crate::context::{QueryContext, SelectPartitions};
use crate::invocation_status::row::append_invocation_status_row;
use crate::invocation_status::schema::{
    SysInvocationStatusBuilder, sys_invocation_status_sort_order,
};
use crate::partition_filter::FirstMatchingPartitionKeyExtractor;
use crate::partition_store_scanner::{LocalPartitionsScanner, ScanLocalPartitionInPlace};
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::table_providers::{PartitionedTableProvider, ScanPartition};

const NAME: &str = "sys_invocation_status";

pub(crate) fn register_self(
    ctx: &QueryContext,
    partition_selector: impl SelectPartitions,
    local_partition_store_manager: Option<PartitionStoreManager>,
    remote_scanner_manager: &RemoteScannerManager,
) -> datafusion::common::Result<()> {
    let local_scanner = local_partition_store_manager.map(|partition_store_manager| {
        Arc::new(LocalPartitionsScanner::new_in_place(
            partition_store_manager,
            StatusScanner,
        )) as Arc<dyn ScanPartition>
    });
    let status_table = PartitionedTableProvider::new(
        partition_selector,
        SysInvocationStatusBuilder::schema(),
        sys_invocation_status_sort_order(),
        remote_scanner_manager.create_distributed_scanner(NAME, local_scanner),
        FirstMatchingPartitionKeyExtractor::default()
            .with_service_key("target_service_key")
            .with_invocation_id("id"),
    );
    ctx.register_partitioned_table(NAME, Arc::new(status_table))
}

#[derive(Debug)]
struct Filter<T>(Ordering, T);

impl<T: Ord + PartialEq> Filter<T> {
    fn filter(&self, value: &T) -> bool {
        return self.0 == value.cmp(&self.1);
    }
}

fn parse_timestamp_filter_from_snapshot(
    column_name: &str,
    predicate: &Arc<dyn PhysicalExpr>,
) -> Option<Filter<u64>> {
    for predicate in split_conjunction(predicate) {
        if let Some(filter) = parse_timestamp_filter_from_expr(column_name, predicate) {
            return Some(filter);
        }
    }
    None
}

fn parse_timestamp_filter_from_expr(
    column_name: &str,
    predicate: &Arc<dyn PhysicalExpr>,
) -> Option<Filter<u64>> {
    let binary = predicate.as_any().downcast_ref::<BinaryExpr>()?;

    let binary = match binary.op() {
        // in the desc case, nulls go at the top, so the expr is `modified_at IS NULL OR modified_at > 1234`
        // modified_at is nullable, but we actually never set a null, so the left side is always false
        Operator::Or => {
            let is_null = binary.left().as_any().downcast_ref::<IsNullExpr>()?;
            let is_null_column = is_null.arg().as_any().downcast_ref::<Column>()?;
            if is_null_column.name() != column_name {
                return None;
            }
            binary.right().as_any().downcast_ref::<BinaryExpr>()?
        }
        // in the asc case, nulls go at the bottom, so the expr is `modified_at < 1234`
        Operator::Lt => binary,
        _ => return None,
    };

    let left_column = binary.left().as_any().downcast_ref::<Column>()?;
    if left_column.name() != column_name {
        return None;
    }

    let lit = binary.right().as_any().downcast_ref::<Literal>()?;

    let ScalarValue::TimestampMillisecond(Some(filter_millis), _) = lit.value() else {
        return None;
    };

    let filter_millis = u64::try_from(*filter_millis).unwrap_or(0);

    let filter = match binary.op() {
        Operator::Lt => Some(Filter(Ordering::Less, filter_millis)),
        Operator::Gt => Some(Filter(Ordering::Greater, filter_millis)),
        _ => None,
    };
    filter
}

fn extract_filters(predicate: &Arc<dyn PhysicalExpr>) -> Option<FilterInvocationStatus> {
    let columns = collect_columns(predicate);
    let column_names: HashSet<&str> = columns.iter().map(|c| c.name()).collect();

    let (time_field, column_name) = if column_names.contains("modified_at") {
        (FilterInvocationTime::Modification, "modified_at")
    } else if column_names.contains("created_at") {
        (FilterInvocationTime::Creation, "created_at")
    } else {
        return None;
    };

    return Some(FilterInvocationStatus::Time(
        time_field,
        Box::new({
            let predicate = predicate.clone();

            let mut iterations = 0;
            let mut seen_generation = snapshot_generation(&predicate);
            let mut filter = parse_timestamp_filter_from_snapshot(
                column_name,
                &snapshot_physical_expr(predicate.clone()).ok()?,
            );

            move |millis_since_epoch| {
                // getting the generation is fairly expensive; don't do it for every row
                if iterations % 64 == 0 {
                    let current_generation = snapshot_generation(&predicate);
                    if current_generation != seen_generation {
                        // filters have changed, snapshot and parse new filters
                        seen_generation = current_generation;
                        filter =
                            snapshot_physical_expr(predicate.clone())
                                .ok()
                                .and_then(|snapshot| {
                                    parse_timestamp_filter_from_snapshot(column_name, &snapshot)
                                })
                    }
                }

                iterations += 1;

                return match &filter {
                    Some(filter) => filter.filter(&millis_since_epoch.as_u64()),
                    None => true,
                };
            }
        }),
    ));
}

#[derive(Debug, Clone)]
struct StatusScanner;

impl ScanLocalPartitionInPlace for StatusScanner {
    type Builder = SysInvocationStatusBuilder;
    type Item = (InvocationId, InvocationStatus);

    fn for_each_row<F: FnMut(Self::Item) -> ControlFlow<()> + Send + Sync + 'static>(
        partition_store: &PartitionStore,
        range: RangeInclusive<PartitionKey>,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        f: F,
    ) -> Result<impl Future<Output = Result<(), StorageError>> + Send, StorageError> {
        let filter = if let Some(ref predicate) = predicate {
            extract_filters(predicate)
        } else {
            None
        };

        partition_store.for_each_invocation_status(range, filter, f)
    }

    fn append_row(
        row_builder: &mut Self::Builder,
        string_buffer: &mut String,
        (invocation_id, invocation_status): Self::Item,
    ) {
        append_invocation_status_row(row_builder, string_buffer, invocation_id, invocation_status)
    }
}
