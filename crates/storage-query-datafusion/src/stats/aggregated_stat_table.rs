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

use datafusion::datasource::{TableProvider, ViewTable, provider_as_source};
use datafusion::functions_aggregate::expr_fn::sum;
use datafusion::logical_expr::{LogicalPlanBuilder, col};

const NUM_ENTRIES_COLUMN: &str = "num_entries";

/// Builds the public view of a partition-local gauge statistic.
///
/// Every raw column other than `num_entries` is a dimension. The raw provider is embedded directly in
/// the view rather than registered in the catalog. This keeps physical partition details private
/// while allowing DataFusion to push dimension filters through the aggregate and into the
/// partitioned scan.
pub(crate) fn gauge_stat_sum_view(
    name: &str,
    raw_table: Arc<dyn TableProvider>,
) -> datafusion::common::Result<ViewTable> {
    let schema = raw_table.schema();
    let group_expr = schema
        .fields()
        .iter()
        .filter(|field| field.name() != NUM_ENTRIES_COLUMN)
        .map(|field| col(field.name()));
    let logical_plan = LogicalPlanBuilder::scan(
        format!("__{name}_partitioned"),
        provider_as_source(raw_table),
        None,
    )?
    .aggregate(
        group_expr,
        [sum(col(NUM_ENTRIES_COLUMN)).alias(NUM_ENTRIES_COLUMN)],
    )?
    .build()?;

    Ok(ViewTable::new(logical_plan, None))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{LargeStringArray, UInt64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::logical_expr::LogicalPlan;
    use datafusion::prelude::SessionContext;

    use restate_storage_api::filter::{Filter, ValuePredicate};
    use restate_storage_api::stats::service_load::{
        ServiceLoad, ServiceLoadClause, ServiceLoadField,
    };
    use restate_types::sharding::KeyRange;
    use restate_util_string::ReString;

    use crate::partition_store_scanner::ScanLocalPartitionFilter;
    use crate::stats::service_stats::schema::SysServiceStatsBuilder;

    use super::{NUM_ENTRIES_COLUMN, gauge_stat_sum_view};

    #[tokio::test]
    async fn optimized_sql_in_list_preserves_storage_constraints() {
        let ctx = SessionContext::new();
        let schema = SysServiceStatsBuilder::schema();
        let table = gauge_stat_sum_view(
            "sys_service_stats",
            Arc::new(EmptyTable::new(schema.clone())),
        )
        .unwrap();
        ctx.register_table("sys_service_stats", Arc::new(table))
            .unwrap();
        let plan = ctx.sql("select * from sys_service_stats where service_name in ('A', 'LargeState', 'Zorder')")
            .await.unwrap().into_optimized_plan().unwrap();
        let mut nodes = vec![&plan];
        let filter = loop {
            let node = nodes.pop().expect("expected an optimized filter");
            if let LogicalPlan::Filter(filter) = node {
                break filter;
            }
            nodes.extend(node.inputs());
        };
        let expression = ctx
            .state()
            .create_physical_expr(filter.predicate.clone(), filter.input.schema())
            .unwrap();
        let remote = crate::decode_expr(
            &ctx.task_ctx(),
            &schema,
            &crate::encode_expr(&expression).unwrap(),
        )
        .unwrap();
        for expression in [expression, remote] {
            let Filter::Predicates(fields) =
                Filter::<ServiceLoad>::new(KeyRange::FULL, Some(expression.clone()))
            else {
                panic!("lost IN constraints after optimization: {expression}")
            };
            let [ServiceLoadClause::ServiceName(ValuePredicate::In(values))] =
                fields.for_field(ServiceLoadField::ServiceName)
            else {
                panic!("expected exact service-name membership")
            };
            let mut names: Vec<_> = values.iter().map(ReString::as_str).collect();
            names.sort_unstable();
            assert_eq!(names, ["A", "LargeState", "Zorder"]);
        }
    }

    #[tokio::test]
    async fn gauge_view_sums_matching_dimensions_across_input_partitions() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("dimension", DataType::LargeUtf8, true),
            Field::new(NUM_ENTRIES_COLUMN, DataType::UInt64, true),
        ]));
        let batch = |dimension: &[&str], value: &[u64]| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(LargeStringArray::from(dimension.to_vec())),
                    Arc::new(UInt64Array::from(value.to_vec())),
                ],
            )
            .unwrap()
        };
        let raw_table = Arc::new(
            MemTable::try_new(
                schema.clone(),
                vec![
                    vec![batch(&["shared", "left"], &[2, 3])],
                    vec![batch(&["shared", "right"], &[5, 7])],
                ],
            )
            .unwrap(),
        );

        let ctx = SessionContext::new();
        ctx.register_table(
            "stats",
            Arc::new(gauge_stat_sum_view("stats", raw_table).unwrap()),
        )
        .unwrap();

        let mut batches = ctx
            .sql("SELECT dimension, num_entries FROM stats ORDER BY dimension")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let batch = batches.pop().unwrap();
        let dimensions = batch
            .column_by_name("dimension")
            .unwrap()
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap();
        let values = batch
            .column_by_name(NUM_ENTRIES_COLUMN)
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        assert_eq!(
            dimensions.iter().flatten().collect::<Vec<_>>(),
            ["left", "right", "shared"]
        );
        assert_eq!(values.values(), &[3, 7, 7]);
    }
}
