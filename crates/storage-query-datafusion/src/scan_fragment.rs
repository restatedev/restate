// Copyright (c) 2023 - 2026 Restate Software Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Pushdown of stable row-wise operators into partition placement branches.
//!
//! `QueryContext` installs [`ScanFragmentPushdown`] after DataFusion's final
//! filter-pushdown pass. The rule finds a maximal chain of `FilterExec` and
//! `ProjectionExec` operators directly above a location-aware or remote scan,
//! validates every operator with `remote_fragment`'s remote-safety checks, and
//! converts the chain into one `RemoteFragment`. Local placement branches bind
//! that fragment as ordinary DataFusion operators; remote branches serialize it
//! for execution by the partition owner.
//!
//! Stateful dynamic expressions—most importantly the predicate produced by
//! TopK optimization—are not part of a serialized fragment because worker and
//! coordinator copies could diverge. The preceding filter-pushdown pass instead
//! places that shared expression in `PartitionScanExec`, whose remote scanner
//! protocol forwards changed predicate generations independently.

use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{Result, internal_err};
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;

use crate::partitioned_scan::{LocationAwareScanExec, RemoteNodeExec};
use crate::remote_fragment::{
    FragmentLeafExec, RemoteFragment, bind_unary_operators, is_remote_safe_operator,
    is_row_wise_operator,
};

/// Pushes a maximal stable filter/projection pipeline to each partition owner.
///
/// The rule runs after DataFusion's final filter-pushdown pass, so mutable TopK
/// filters have already reached the raw scan. Dynamic expressions are rejected
/// here because a serialized fragment cannot share their coordinator-side state.
#[derive(Debug, Default)]
pub(crate) struct ScanFragmentPushdown;

impl PhysicalOptimizerRule for ScanFragmentPushdown {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_down(rewrite_scan_fragment).data()
    }

    fn name(&self) -> &str {
        "ScanFragmentPushdown"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Rewrites the maximal `[FilterExec | ProjectionExec]+` prefix at `plan`.
/// Returning `Continue` for an unsafe prefix lets a safe suffix closer to the
/// scan remain independently eligible.
fn rewrite_scan_fragment(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let (operators, input) = collect_row_wise_operators(&plan);
    if operators.is_empty() || !operators.iter().all(is_remote_safe_operator) {
        return Ok(Transformed::no(plan));
    }

    let rewritten = if let Some(scan) = input.downcast_ref::<LocationAwareScanExec>() {
        if !scan.supports_fragment_pushdown() {
            return Ok(Transformed::no(plan));
        }
        let Some(fragment) = build_fragment(&operators, scan.schema()) else {
            return Ok(Transformed::no(plan));
        };
        scan.with_fragment(Arc::new(fragment))?
    } else if let Some(remote) = input.downcast_ref::<RemoteNodeExec>() {
        if !remote.can_accept_fragment() {
            return Ok(Transformed::no(plan));
        }
        let Some(fragment) = build_fragment(&operators, remote.schema()) else {
            return Ok(Transformed::no(plan));
        };
        remote.with_fragment(Arc::new(fragment))?
    } else {
        return Ok(Transformed::no(plan));
    };

    if rewritten.schema() != plan.schema() {
        return internal_err!(
            "scan fragment pushdown changed the plan schema: expected {:?}, got {:?}",
            plan.schema(),
            rewritten.schema()
        );
    }
    Ok(Transformed::new(rewritten, true, TreeNodeRecursion::Jump))
}

/// Collects a unary row-wise chain from root to leaf and returns its input.
fn collect_row_wise_operators(
    plan: &Arc<dyn ExecutionPlan>,
) -> (Vec<Arc<dyn ExecutionPlan>>, Arc<dyn ExecutionPlan>) {
    let mut operators = Vec::new();
    let mut input = Arc::clone(plan);
    while is_row_wise_operator(&input) {
        operators.push(Arc::clone(&input));
        input = Arc::clone(input.children()[0]);
    }
    (operators, input)
}

fn build_fragment(
    operators: &[Arc<dyn ExecutionPlan>],
    input_schema: datafusion::arrow::datatypes::SchemaRef,
) -> Option<RemoteFragment> {
    let leaf = Arc::new(FragmentLeafExec::new(input_schema)) as Arc<dyn ExecutionPlan>;
    RemoteFragment::try_new(bind_unary_operators(operators, leaf)?).ok()
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::execution::context::{SessionConfig, SessionContext};
    use futures::TryStreamExt;

    use super::*;
    use crate::filter::FirstMatchingPartitionKeyExtractor;
    use crate::mocks::{LocatedTestScanner, TwoPartitions};
    use crate::table_providers::PartitionedTableProvider;

    #[tokio::test]
    async fn pushes_stable_filter_and_computed_projection_without_aggregation() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batch = |values| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(values))],
            )
            .expect("test batch")
        };
        let provider = PartitionedTableProvider::new(
            TwoPartitions,
            Arc::clone(&schema),
            Vec::new(),
            LocatedTestScanner::new(batch(vec![1, 3]), batch(vec![2, 4])),
            FirstMatchingPartitionKeyExtractor::default(),
        );
        let context =
            SessionContext::new_with_config(SessionConfig::new().with_target_partitions(2));
        context
            .register_table("test_values", Arc::new(provider))
            .expect("register test table");

        let plan = context
            .sql("SELECT value * 2 AS doubled FROM test_values WHERE value > 2")
            .await
            .expect("row-wise query")
            .create_physical_plan()
            .await
            .expect("row-wise physical plan");
        let optimized = ScanFragmentPushdown
            .optimize(plan, &ConfigOptions::new())
            .expect("scan fragment pushdown");
        let display = datafusion::physical_plan::displayable(optimized.as_ref())
            .indent(true)
            .to_string();
        assert!(
            display.contains("fragment=[ProjectionExec")
                && display.contains(" -> FilterExec: value@0 > 2"),
            "{display}"
        );
        assert!(display.contains("predicate=value@0 > 2"), "{display}");

        let mut values = datafusion::physical_plan::execute_stream(optimized, context.task_ctx())
            .expect("execute optimized plan")
            .try_fold(Vec::<i64>::new(), |mut values, batch| async move {
                values.extend(
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("computed integer values")
                        .values(),
                );
                Ok(values)
            })
            .await
            .expect("collect optimized rows");
        values.sort_unstable();
        assert_eq!(values, [6, 8]);

        let volatile = context
            .sql("SELECT random(), value FROM test_values WHERE value > 2")
            .await
            .expect("volatile projection query")
            .create_physical_plan()
            .await
            .expect("volatile projection plan");
        let volatile = ScanFragmentPushdown
            .optimize(volatile, &ConfigOptions::new())
            .expect("safe suffix remains pushable");
        let display = datafusion::physical_plan::displayable(volatile.as_ref())
            .indent(true)
            .to_string();
        assert!(display.contains("fragment=[FilterExec"), "{display}");
        assert!(!display.contains("fragment=[ProjectionExec"), "{display}");
    }
}
