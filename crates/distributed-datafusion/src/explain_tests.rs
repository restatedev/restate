// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use datafusion::arrow::array::{RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::common::DataFusionError;
use datafusion::common::config::ConfigOptions;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;

use restate_clock::MockClock;
use restate_core::test_env::TestCoreEnv;
use restate_storage_query_api::{QueryOptions, SessionOptions};
use restate_types::errors::GenericError;
use restate_types::identifiers::PartitionId;
use restate_types::partition_table::{Partition, PartitionTable};
use restate_types::{GenerationalNodeId, Version};

use crate::context::{DataFusionQueryEngine, SelectPartitions};
use crate::mocks::MockPartitionSelector;
use crate::placement::PartitionPlacement;
use crate::source_registry::{PartitionLocation, PartitionLocator, SourceRegistry};
use crate::{DataFusionEnv, UserTables};

use super::*;

#[path = "explain_analyze_tests.rs"]
mod analyze;

fn environment() -> DataFusionEnv {
    DataFusionEnv::new(64 * 1024 * 1024, None, Some(1), &HashMap::new())
        .unwrap()
        .with_mock_clock(MockClock::new())
        .unwrap()
}

async fn plans(ctx: &SessionContext, sql: &str, native: bool) -> Vec<(String, String)> {
    let state = ctx.state();
    let logical = state.create_logical_plan(sql).await.unwrap();
    let logical = state.optimize(&logical).unwrap();
    let physical = if native {
        DefaultPhysicalPlanner::default()
            .create_physical_plan(&logical, &state)
            .await
            .unwrap()
    } else {
        ExplainPlanner
            .create_physical_plan(&logical, &state)
            .await
            .unwrap()
    };
    let batches = collect(physical, ctx.task_ctx()).await.unwrap();
    batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(|row| {
                (
                    array_value_to_string(batch.column(0), row).unwrap(),
                    array_value_to_string(batch.column(1), row).unwrap(),
                )
            })
        })
        .collect()
}

struct RemoteOwner;

impl PartitionLocator for RemoteOwner {
    fn get_partition_target_node(
        &self,
        _: PartitionId,
        _: PartitionPlacement,
    ) -> anyhow::Result<PartitionLocation> {
        Ok(PartitionLocation::Remote {
            node_id: GenerationalNodeId::new(2, 1),
        })
    }
}

#[restate_core::test]
async fn explain_large_in_across_catalog_and_distributed_fragments() {
    let core = TestCoreEnv::create_with_single_node(1, 1).await;
    let env = environment().with_distributed_execution(core.networking);
    let sources = SourceRegistry::new(Arc::new(RemoteOwner), core.metadata, Default::default());
    // Planning needs the real providers and view, but no storage or remote worker.
    env.register_tables(UserTables::new(MockPartitionSelector, sources))
        .await
        .unwrap();
    let ctx = env.create_session(&SessionOptions::default()).unwrap();
    let sql = include_str!("explain_test.sql");
    let query = format!("EXPLAIN {sql}");
    let before = plans(&ctx, &query, true).await;
    let after = plans(&ctx, &query, false).await;
    assert_eq!(before.len(), 2);
    assert_eq!(after.len(), 2);
    for ((before_kind, before), (after_kind, after)) in before.iter().zip(&after) {
        assert_eq!(before_kind, after_kind);
        assert!(after.len() < before.len() / 4, "{after}");
        assert_eq!(
            after.matches("<set#1: 250 values, 250 distinct").count(),
            4,
            "{after}"
        );
        assert!(!after.contains("<set#2"), "{after}");
        assert_eq!(after.matches("inv_").count(), 12, "{after}");
        assert!(after.contains("Promises"), "{after}");
        assert_eq!(before.lines().count(), after.lines().count());
        eprintln!(
            "{after_kind}: {} -> {} bytes; {} -> {} characters\n{after}",
            before.len(),
            after.len(),
            before.chars().count(),
            after.chars().count(),
        );
    }
    for node in ["Filter:", "TableScan:"] {
        assert!(after[0].1.contains(node), "{}", after[0].1);
    }
    for node in [
        "DistributedExec",
        "NetworkCoalesceExec",
        "FilterExec:",
        "MultiGetExec: table=sys_invocation_status",
        "TableScanExec: table=sys_invocation_state",
        "keys=250",
        "owner=N2:1",
    ] {
        assert!(after[1].1.contains(node), "{}", after[1].1);
    }
    let verbose = format!("EXPLAIN VERBOSE {sql}");
    assert_eq!(
        plans(&ctx, &verbose, false).await,
        plans(&ctx, &verbose, true).await,
        "verbose retains complete native values and optimizer history"
    );

    // Exercise the public session boundary too, including schema-dump filtering.
    let session = DataFusionQueryEngine::from_env(env, None)
        .create_session(SessionOptions::default())
        .unwrap();
    let batches: Vec<_> = session
        .execute(&query, QueryOptions {})
        .await
        .unwrap()
        .stream
        .try_collect()
        .await
        .unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    for (row, (_, expected)) in after.iter().enumerate() {
        assert_eq!(
            array_value_to_string(batches[0].column(1), row).unwrap(),
            *expected
        );
    }
}

#[derive(Clone, Debug)]
struct ThreeOwners;

#[async_trait]
impl SelectPartitions for ThreeOwners {
    async fn get_live_partitions(
        &self,
    ) -> std::result::Result<Vec<(PartitionId, Partition)>, GenericError> {
        Ok(
            PartitionTable::with_equally_sized_partitions(Version::MIN, 8)
                .iter()
                .map(|(id, partition)| (*id, partition.clone()))
                .collect(),
        )
    }
}

impl PartitionLocator for ThreeOwners {
    fn get_partition_target_node(
        &self,
        partition: PartitionId,
        _: PartitionPlacement,
    ) -> anyhow::Result<PartitionLocation> {
        Ok(PartitionLocation::Remote {
            node_id: GenerationalNodeId::new(u32::from(partition) % 3 + 2, 1),
        })
    }
}

#[restate_core::test]
async fn explain_state_values_preserves_trailing_conditions_across_three_owners() {
    let core = TestCoreEnv::create_with_single_node(1, 1).await;
    let env = environment().with_distributed_execution(core.networking);
    let sources = SourceRegistry::new(Arc::new(ThreeOwners), core.metadata, Default::default());
    env.register_tables(UserTables::new(ThreeOwners, sources))
        .await
        .unwrap();
    let ctx = env.create_session(&SessionOptions::default()).unwrap();
    let sql = include_str!("explain_state_test.sql");
    let before = plans(&ctx, &format!("EXPLAIN {sql}"), true).await;
    let after = plans(&ctx, &format!("EXPLAIN {sql}"), false).await;
    assert_eq!(after.len(), 2);
    for ((kind, before), (_, after)) in before.iter().zip(&after) {
        assert!(after.len() < before.len() / 4, "{after}");
        assert!(!after.contains("<set#2"), "{after}");
        assert!(!after.contains("999799"), "{after}");
        assert_eq!(before.lines().count(), after.lines().count());
        eprintln!(
            "state {kind}: {} -> {} bytes; {} -> {} characters\n{after}",
            before.len(),
            after.len(),
            before.chars().count(),
            after.chars().count(),
        );
    }
    let physical = &after[1].1;
    assert_eq!(
        physical.matches("NetworkCoalesceExec").count(),
        3,
        "{physical}"
    );
    assert_eq!(physical.matches("FilterExec:").count(), 3, "{physical}");
    for table in ["state.legacy", "state.scoped"] {
        assert_eq!(
            physical
                .matches(&format!("TableScanExec: table={table}"))
                .count(),
            3,
            "{physical}"
        );
    }
    let predicates: Vec<_> = physical
        .lines()
        .filter(|line| line.contains("<set#1"))
        .collect();
    assert_eq!(predicates.len(), 9, "{physical}");
    for line in predicates {
        assert!(line.contains("300 values, 300 distinct"), "{line}");
        assert!(
            line.contains("AND scope@0 IS NULL AND value_length@5 <= 2048"),
            "{line}"
        );
    }
    let verbose = format!("EXPLAIN VERBOSE {sql}");
    assert_eq!(
        plans(&ctx, &verbose, false).await,
        plans(&ctx, &verbose, true).await
    );
}

#[tokio::test]
async fn explain_preserves_nested_not_in_escaped_literals_and_small_predicates() {
    let env = environment();
    let ctx = env.create_session(&SessionOptions::default()).unwrap();
    let long_value = "é".repeat(100);
    let values = [
        "comma, bracket]), quote' and \"double\"",
        "雪☃🦀\\path\nnext",
        &long_value,
        "v3",
        "v4",
        "v5",
        "v6",
        "v7",
        "v8",
        "v9",
    ];
    let list = values
        .iter()
        .map(|s| format!("'{}'", s.replace('\'', "''")))
        .collect::<Vec<_>>()
        .join(", ");
    let reversed = values
        .iter()
        .rev()
        .map(|s| format!("'{}'", s.replace('\'', "''")))
        .collect::<Vec<_>>()
        .join(", ");
    ctx.register_batch(
        "items",
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Utf8, true),
                Field::new("service", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec![
                    Some(values[0]),
                    Some("other"),
                    None,
                ])),
                Arc::new(StringArray::from(vec!["Promises", "Promises", "Other"])),
            ],
        )
        .unwrap(),
    )
    .unwrap();
    let query = format!(
        "SELECT id FROM items WHERE \
         (id NOT IN ({list}) OR (id IN ({reversed}) AND service LIKE 'Other%')) \
         AND service = 'Promises'"
    );
    let explain = format!("EXPLAIN {query}");
    let before = plans(&ctx, &explain, true).await;
    let after = plans(&ctx, &explain, false).await;
    for ((_, before), (_, after)) in before.iter().zip(&after) {
        assert!(after.contains("NOT IN"), "{after}");
        assert!(after.contains(" OR "), "{after}");
        assert!(after.contains(" AND "), "{after}");
        assert!(after.contains("Promises"), "{after}");
        assert!(after.contains("<set#1: 10 values, 10 distinct"), "{after}");
        assert!(!after.contains("<set#2"), "{after}");
        assert!(!after.contains(&"é".repeat(100)), "{after}");
        assert!(after.contains(r#"雪☃🦀\\path\nnext"#), "{after}");
        assert!(after.contains(r#"quote' and \"double\""#), "{after}");
        assert!(after.len() < before.len(), "{after}");
    }
    let verbose = format!("EXPLAIN VERBOSE {query}");
    assert_eq!(
        plans(&ctx, &verbose, false).await,
        plans(&ctx, &verbose, true).await
    );
    let result = ctx.sql(&query).await.unwrap().collect().await.unwrap();
    assert_eq!(result.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    assert_eq!(
        array_value_to_string(result[0].column(0), 0).unwrap(),
        "other"
    );

    for query in [
        "EXPLAIN SELECT id FROM items WHERE id IN ('x', '雪') AND service = 'Promises'",
        "EXPLAIN SELECT id FROM items WHERE id NOT IN ('x', NULL)",
        "EXPLAIN SELECT id FROM items WHERE id IN (service, '1', '2', '3', '4', '5', '6', '7', '8')",
        "EXPLAIN (FORMAT GRAPHVIZ) SELECT id FROM items",
    ] {
        assert_eq!(
            plans(&ctx, query, false).await,
            plans(&ctx, query, true).await
        );
    }

    // Identical samples/cardinalities must not conflate different sets.
    let changed = list.replace("'v9'", "'different-last-value'");
    let query =
        format!("EXPLAIN SELECT id FROM items WHERE id IN ({list}) OR service IN ({changed})");
    for (_, plan) in plans(&ctx, &query, false).await {
        assert!(plan.contains("<set#1"), "{plan}");
        assert!(plan.contains("<set#2"), "{plan}");
    }

    // Respect mode options, including physical schema/statistics decorations.
    ctx.sql("SET datafusion.explain.show_schema = true")
        .await
        .unwrap();
    ctx.sql("SET datafusion.explain.show_statistics = true")
        .await
        .unwrap();
    let small = "EXPLAIN SELECT id FROM items WHERE id = 'other'";
    assert_eq!(
        plans(&ctx, small, false).await,
        plans(&ctx, small, true).await
    );
    for setting in ["logical_plan_only", "physical_plan_only"] {
        ctx.sql(&format!("SET datafusion.explain.{setting} = true"))
            .await
            .unwrap();
        let output = plans(&ctx, &explain, false).await;
        assert_eq!(output.len(), 1);
        assert!(output[0].1.contains("<set#1"), "{}", output[0].1);
        ctx.sql(&format!("SET datafusion.explain.{setting} = false"))
            .await
            .unwrap();
    }

    // Native physical literals are unquoted. Preserve an ambiguous spelling,
    // and protect an unchanged root containing another root's display text.
    assert_eq!(
        substitute(
            "a IN ([1, 2]), prefix a IN ([1, 2]) suffix",
            vec![
                (String::new(), String::new()),
                ("a IN ([1, 2])".into(), "<set#1>".into()),
                (
                    "prefix a IN ([1, 2]) suffix".into(),
                    "prefix a IN ([1, 2]) suffix".into(),
                ),
            ],
            &Schema::empty(),
        ),
        "a IN ([1, 2]), prefix a IN ([1, 2]) suffix"
    );
    assert_eq!(
        substitute(
            "same, same",
            vec![
                ("same".into(), "<set#1>".into()),
                ("same".into(), "same".into()),
            ],
            &Schema::empty(),
        ),
        "same, same"
    );
}

#[tokio::test]
async fn explain_preserves_expression_shaped_aliases_and_schema_names() {
    let ctx = environment()
        .create_session(&SessionOptions::default())
        .unwrap();
    ctx.register_batch(
        "items",
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)])),
            vec![Arc::new(StringArray::from(vec!["a"]))],
        )
        .unwrap(),
    )
    .unwrap();
    let expression = "id IN ('a','b','c','d','e','f','g','h','i')";
    let probe = ctx
        .sql(&format!("SELECT {expression} AS probe FROM items"))
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let spelling = probe.downcast_ref::<ProjectionExec>().unwrap().expr()[0]
        .expr
        .to_string();
    eprintln!("native expression spelling: {spelling}");
    ctx.sql("SET datafusion.explain.analyze_categories = 'rows'")
        .await
        .unwrap();

    for show_schema in [false, true] {
        ctx.sql(&format!(
            "SET datafusion.explain.show_schema = {show_schema}"
        ))
        .await
        .unwrap();
        for alias in [spelling.clone(), format!("label 雪 {spelling} suffix")] {
            let sql = format!(
                "EXPLAIN SELECT {expression} AS \"{}\" FROM items",
                alias.replace('"', "\"\""),
            );
            let native = plans(&ctx, &sql, true).await;
            let compact = plans(&ctx, &sql, false).await;
            eprintln!(
                "show_schema={show_schema}, alias={alias:?}\nnative:\n{}\ncompact:\n{}",
                native[1].1, compact[1].1
            );
            assert_eq!(
                native[1], compact[1],
                "ambiguous physical expression/alias spelling must retain native output",
            );
            assert_eq!(
                native[0].1.matches(&alias).count(),
                compact[0].1.matches(&alias).count()
            );
            let sql = sql.replacen("EXPLAIN ", "EXPLAIN ANALYZE ", 1);
            let native = plans(&ctx, &sql, true).await;
            let compact = plans(&ctx, &sql, false).await;
            assert_eq!(
                native, compact,
                "ANALYZE preserves expression-shaped aliases"
            );
            eprintln!(
                "ANALYZE show_schema={show_schema}, alias={alias:?}\nnative:\n{}\ncompact:\n{}",
                native[0].1, compact[0].1
            );
        }
    }
}

#[derive(Debug)]
struct ExplainCountingTable {
    fail: bool,
    scans: Arc<AtomicUsize>,
}

#[async_trait]
impl TableProvider for ExplainCountingTable {
    fn schema(&self) -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]))
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _: &dyn Session,
        _: Option<&Vec<usize>>,
        _: &[Expr],
        _: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.scans.fetch_add(1, Ordering::Relaxed);
        if self.fail {
            return Err(DataFusionError::Plan("explain scan failure".into()));
        }
        Ok(Arc::new(EmptyExec::new(self.schema())))
    }
}

#[derive(Debug)]
struct ExplainFailingRule {
    schema_mismatch: bool,
    calls: Arc<AtomicUsize>,
}

impl PhysicalOptimizerRule for ExplainFailingRule {
    fn optimize(
        &self,
        _: Arc<dyn ExecutionPlan>,
        _: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if self.schema_mismatch {
            Ok(Arc::new(EmptyExec::new(Arc::new(Schema::empty()))))
        } else {
            Err(DataFusionError::Plan("explain optimizer failure".into()))
        }
    }

    fn name(&self) -> &str {
        "explain_failing_rule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[tokio::test]
async fn explain_preserves_native_planning_failures_without_replanning() {
    for (phase, fail_scan, schema_mismatch) in [
        ("initial plan", true, false),
        ("optimizer", false, false),
        ("optimizer schema check", false, true),
    ] {
        let scans = Arc::new(AtomicUsize::new(0));
        let calls = Arc::new(AtomicUsize::new(0));
        let state =
            SessionStateBuilder::new_from_existing(environment().build_session_state().unwrap())
                .with_physical_optimizer_rule(Arc::new(ExplainFailingRule {
                    schema_mismatch,
                    calls: Arc::clone(&calls),
                }))
                .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_table(
            "items",
            Arc::new(ExplainCountingTable {
                fail: fail_scan,
                scans: Arc::clone(&scans),
            }),
        )
        .unwrap();
        for verbose in ["", "VERBOSE "] {
            let sql = format!("EXPLAIN {verbose}SELECT id FROM items");
            let state = ctx.state();
            let logical = state.create_logical_plan(&sql).await.unwrap();
            let logical = state.optimize(&logical).unwrap();
            let native = DefaultPhysicalPlanner::default()
                .create_physical_plan(&logical, &state)
                .await
                .unwrap();
            let compact = ExplainPlanner
                .create_physical_plan(&logical, &state)
                .await
                .unwrap();
            let native_explain = native.downcast_ref::<ExplainExec>().unwrap();
            let compact_explain = compact.downcast_ref::<ExplainExec>().unwrap();
            assert_eq!(
                native_explain.stringified_plans(),
                compact_explain.stringified_plans(),
                "complete native diagnostic history for {phase}, including hidden plans"
            );
            let visible: Vec<_> = native_explain
                .stringified_plans()
                .iter()
                .filter(|plan| plan.should_display(native_explain.verbose()))
                .map(|plan| plan.plan_type.to_string())
                .collect();
            eprintln!(
                "phase={phase}, verbose={verbose:?}\nnative visible rows: {visible:?}\n\
                 native terminal diagnostic: {:#?}\ncompact: identical history and rows",
                native_explain.stringified_plans().last().unwrap(),
            );
            assert_eq!(scans.swap(0, Ordering::Relaxed), 2, "one scan per planner");
            assert_eq!(
                calls.swap(0, Ordering::Relaxed),
                if fail_scan { 0 } else { 2 }
            );
            assert_eq!(
                collect(native, ctx.task_ctx()).await.unwrap(),
                collect(compact, ctx.task_ctx()).await.unwrap(),
                "native diagnostic rows for {phase}"
            );
        }
    }
}
