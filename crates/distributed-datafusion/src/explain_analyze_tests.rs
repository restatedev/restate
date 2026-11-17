// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use datafusion::common::stats::Precision;
use datafusion::physical_plan::RecordBatchStream;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, MetricBuilder, MetricCategory, MetricType,
};
use futures::{Stream, StreamExt};
use tokio::sync::Notify;

use super::*;

const LIST: &str = "'a','b','c','d','e','f','g','h','i'";
const SUMMARY: &str = r#"<set#1: 9 values, 9 distinct, sample=["a", "b", "c", …]>"#;

#[derive(Clone, Copy, Debug)]
enum Behavior {
    Rows,
    ExecuteError,
    StreamError,
    Pending,
}

#[derive(Debug, Default)]
struct Counts {
    scans: AtomicUsize,
    executions: AtomicUsize,
    rows: AtomicUsize,
    drops: AtomicUsize,
    waiting: Notify,
    dropped: Notify,
}

#[derive(Debug)]
struct AnalyzeSource {
    properties: Arc<PlanProperties>,
    batch: RecordBatch,
    behavior: Behavior,
    counts: Arc<Counts>,
    metrics: ExecutionPlanMetricsSet,
}

impl AnalyzeSource {
    fn new(behavior: Behavior, counts: Arc<Counts>) -> Self {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
        Self {
            properties: Arc::clone(EmptyExec::new(Arc::clone(&schema)).properties()),
            batch: RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["a", "z"]))])
                .unwrap(),
            behavior,
            counts,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

#[async_trait]
impl TableProvider for AnalyzeSource {
    fn schema(&self) -> Arc<Schema> {
        self.batch.schema()
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
        self.counts.scans.fetch_add(1, Ordering::Relaxed);
        Ok(Arc::new(Self::new(self.behavior, Arc::clone(&self.counts))))
    }
}

impl DisplayAs for AnalyzeSource {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AnalyzeSource")
    }
}

impl ExecutionPlan for AnalyzeSource {
    fn name(&self) -> &str {
        "AnalyzeSource"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(&self, partition: usize, _: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        assert_eq!(partition, 0);
        self.counts.executions.fetch_add(1, Ordering::Relaxed);
        if matches!(self.behavior, Behavior::ExecuteError) {
            return Err(DataFusionError::Execution("analyze execute failure".into()));
        }
        let rows = MetricBuilder::new(&self.metrics)
            .with_type(MetricType::Summary)
            .output_rows(partition);
        let bytes = MetricBuilder::new(&self.metrics)
            .with_category(MetricCategory::Bytes)
            .counter("source_bytes", partition);
        Ok(Box::pin(AnalyzeSourceStream {
            schema: self.batch.schema(),
            batch: Some(self.batch.clone()),
            behavior: self.behavior,
            counts: Arc::clone(&self.counts),
            rows,
            bytes,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics_from_inputs(
        &self,
        _: &[Arc<Statistics>],
        _: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        let mut stats = Statistics::new_unknown(self.batch.schema().as_ref());
        stats.num_rows = Precision::Exact(self.batch.num_rows());
        Ok(Arc::new(stats))
    }
}

struct AnalyzeSourceStream {
    schema: Arc<Schema>,
    batch: Option<RecordBatch>,
    behavior: Behavior,
    counts: Arc<Counts>,
    rows: Count,
    bytes: Count,
}

impl Stream for AnalyzeSourceStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(batch) = self.batch.take() {
            self.counts
                .rows
                .fetch_add(batch.num_rows(), Ordering::Relaxed);
            self.rows.add(batch.num_rows());
            self.bytes.add(batch.get_array_memory_size());
            return Poll::Ready(Some(Ok(batch)));
        }
        match self.behavior {
            Behavior::StreamError => {
                self.behavior = Behavior::Rows;
                Poll::Ready(Some(Err(DataFusionError::Execution(
                    "analyze stream failure".into(),
                ))))
            }
            Behavior::Pending => {
                self.counts.waiting.notify_one();
                Poll::Pending
            }
            _ => Poll::Ready(None),
        }
    }
}

impl RecordBatchStream for AnalyzeSourceStream {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }
}

impl Drop for AnalyzeSourceStream {
    fn drop(&mut self) {
        self.counts.drops.fetch_add(1, Ordering::Relaxed);
        self.counts.dropped.notify_one();
    }
}

fn context(behavior: Behavior) -> (SessionContext, Arc<Counts>) {
    let ctx = environment()
        .create_session(&SessionOptions::default())
        .unwrap();
    let counts = Arc::new(Counts::default());
    ctx.register_table(
        "items",
        Arc::new(AnalyzeSource::new(behavior, Arc::clone(&counts))),
    )
    .unwrap();
    (ctx, counts)
}

async fn physical(ctx: &SessionContext, sql: &str, native: bool) -> Result<Arc<dyn ExecutionPlan>> {
    let state = ctx.state();
    let logical = state.create_logical_plan(sql).await?;
    let logical = state.optimize(&logical)?;
    if native {
        DefaultPhysicalPlanner::default()
            .create_physical_plan(&logical, &state)
            .await
    } else {
        ExplainPlanner.create_physical_plan(&logical, &state).await
    }
}

// Cross-execution comparisons use deterministic metric categories. The separate
// wall-clock Duration row has no stable value, but must remain present/nonempty.
fn without_duration(rows: &[(String, String)]) -> Vec<&(String, String)> {
    rows.iter()
        .filter(|(kind, value)| {
            if kind == "Duration" {
                assert!(!value.is_empty());
                false
            } else {
                true
            }
        })
        .collect()
}

#[tokio::test]
async fn four_modes_preserve_execution_counts_and_native_verbose() {
    let (ctx, counts) = context(Behavior::Rows);
    ctx.sql("SET datafusion.explain.analyze_categories = 'rows,bytes'")
        .await
        .unwrap();
    let query = format!(
        "SELECT id FROM items WHERE id IN ({LIST}) \
         UNION ALL SELECT id FROM items WHERE id NOT IN ({LIST})"
    );
    for (mode, analyze, verbose) in [
        ("EXPLAIN", false, false),
        ("EXPLAIN VERBOSE", false, true),
        ("EXPLAIN ANALYZE", true, false),
        ("EXPLAIN ANALYZE VERBOSE", true, true),
    ] {
        let sql = format!("{mode} {query}");
        let mut outputs = Vec::new();
        for native in [true, false] {
            let rows = plans(&ctx, &sql, native).await;
            assert_eq!(counts.scans.swap(0, Ordering::Relaxed), 2, "{mode}");
            assert_eq!(
                counts.executions.swap(0, Ordering::Relaxed),
                if analyze { 2 } else { 0 },
                "{mode}"
            );
            assert_eq!(
                counts.rows.swap(0, Ordering::Relaxed),
                if analyze { 4 } else { 0 },
                "{mode}"
            );
            assert_eq!(
                counts.drops.swap(0, Ordering::Relaxed),
                if analyze { 2 } else { 0 },
                "{mode}"
            );
            for (kind, text) in &rows {
                if analyze || matches!(kind.as_str(), "logical_plan" | "physical_plan") {
                    eprintln!("{mode}, native={native}, {kind}:\n{text}");
                }
            }
            outputs.push(rows);
        }
        let [native, compact] = outputs.as_slice() else {
            unreachable!()
        };
        assert_eq!(
            native.iter().map(|r| &r.0).collect::<Vec<_>>(),
            compact.iter().map(|r| &r.0).collect::<Vec<_>>()
        );
        if verbose {
            assert_eq!(without_duration(native), without_duration(compact));
            assert!(compact.iter().all(|(_, text)| !text.contains("<set#")));
        } else {
            let text = &compact.last().unwrap().1;
            assert_eq!(text.matches(SUMMARY).count(), 2, "{text}");
            assert!(text.contains("NOT IN"), "{text}");
            assert!(!text.contains("<set#2"), "{text}");
            if analyze {
                assert_eq!(compact.len(), 1);
                assert_eq!(compact[0].0, "Plan with Metrics");
                assert!(
                    text.contains("output_rows=2") && text.contains("source_bytes="),
                    "{text}"
                );
                assert_eq!(
                    text.replace(SUMMARY, "a, b, c, d, e, f, g, h, i"),
                    native[0].1
                );
            }
        }
        if analyze && verbose {
            assert_eq!(
                compact.iter().map(|r| r.0.as_str()).collect::<Vec<_>>(),
                [
                    "Plan with Metrics",
                    "Plan with Full Metrics",
                    "Output Rows",
                    "Duration"
                ]
            );
            assert_eq!(compact[2].1, "2");
        }
    }
}

#[tokio::test]
async fn analyze_preserves_metric_filters_statistics_and_same_run_timing() {
    let (ctx, _) = context(Behavior::Rows);
    ctx.sql("SET datafusion.explain.show_statistics = true")
        .await
        .unwrap();
    ctx.sql("SET datafusion.explain.analyze_level = 'summary'")
        .await
        .unwrap();
    ctx.sql("SET datafusion.explain.analyze_categories = 'bytes'")
        .await
        .unwrap();
    for (options, has_rows, has_bytes) in [
        ("ANALYZE", false, false),
        ("(ANALYZE, METRICS 'rows,bytes', LEVEL dev)", true, true),
        (
            "(ANALYZE, METRICS 'rows,bytes', LEVEL summary)",
            true,
            false,
        ),
        ("(ANALYZE, METRICS 'none', LEVEL dev)", false, false),
    ] {
        let sql = format!("EXPLAIN {options} SELECT id FROM items WHERE id IN ({LIST})");
        let native = plans(&ctx, &sql, true).await;
        let compact = plans(&ctx, &sql, false).await;
        let text = &compact[0].1;
        assert!(text.contains("statistics=[Rows=Exact(2)"), "{text}");
        assert_eq!(text.contains("output_rows="), has_rows, "{text}");
        assert_eq!(text.contains("source_bytes="), has_bytes, "{text}");
        assert_eq!(
            text.replace(SUMMARY, "a, b, c, d, e, f, g, h, i"),
            native[0].1
        );
        eprintln!(
            "options={options}\nnative:\n{}\ncompact:\n{text}",
            native[0].1
        );
    }

    // Timings vary across executions. Compare against the native formatter on the
    // very same completed input, rather than deleting/rounding timing metrics.
    ctx.sql("SET datafusion.explain.analyze_level = 'dev'")
        .await
        .unwrap();
    ctx.sql("SET datafusion.explain.analyze_categories = 'all'")
        .await
        .unwrap();
    let plan = physical(
        &ctx,
        &format!("EXPLAIN ANALYZE SELECT id FROM items WHERE id IN ({LIST})"),
        false,
    )
    .await
    .unwrap();
    let input = Arc::clone(plan.downcast_ref::<AnalyzeExec>().unwrap().input());
    let batches = collect(plan, ctx.task_ctx()).await.unwrap();
    let compact = array_value_to_string(batches[0].column(1), 0).unwrap();
    let native = DisplayableExecutionPlan::with_metrics(input.downcast_delegate().unwrap())
        .set_show_statistics(true)
        .indent(false)
        .to_string();
    assert!(compact.contains("elapsed_compute="), "{compact}");
    assert_eq!(
        compact.replace(SUMMARY, "a, b, c, d, e, f, g, h, i"),
        native
    );
    eprintln!("same-execution timing/statistics\nnative:\n{native}\ncompact:\n{compact}");

    // The alternate format remains the native plan, without a display wrapper.
    ctx.sql("SET datafusion.explain.show_statistics = false")
        .await
        .unwrap();
    let plan = physical(
        &ctx,
        &format!("EXPLAIN (ANALYZE, FORMAT PGJSON) SELECT id FROM items WHERE id IN ({LIST})"),
        false,
    )
    .await
    .unwrap();
    assert!(
        plan.downcast_ref::<AnalyzeExec>()
            .unwrap()
            .input()
            .downcast_delegate()
            .is_none()
    );
}

#[tokio::test]
async fn analyze_preserves_failures_and_cancels_native_input() {
    for behavior in [Behavior::ExecuteError, Behavior::StreamError] {
        for verbose in ["", "VERBOSE "] {
            let mut errors = Vec::new();
            for native in [true, false] {
                let (ctx, counts) = context(behavior);
                let plan = physical(
                    &ctx,
                    &format!("EXPLAIN ANALYZE {verbose}SELECT id FROM items WHERE id IN ({LIST})"),
                    native,
                )
                .await
                .unwrap();
                let error = collect(plan, ctx.task_ctx()).await.unwrap_err();
                assert_eq!(counts.scans.load(Ordering::Relaxed), 1);
                assert_eq!(counts.executions.load(Ordering::Relaxed), 1);
                errors.push(error.strip_backtrace());
            }
            assert_eq!(errors[0], errors[1]);
            eprintln!(
                "{behavior:?}, verbose={verbose:?}: native=compact={}",
                errors[0]
            );
        }
    }
    for native in [true, false] {
        let (ctx, counts) = context(Behavior::Pending);
        let plan = physical(
            &ctx,
            &format!("EXPLAIN ANALYZE SELECT id FROM items WHERE id IN ({LIST})"),
            native,
        )
        .await
        .unwrap();
        let mut stream = plan.execute(0, ctx.task_ctx()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::select! {
                _ = counts.waiting.notified() => {},
                result = stream.next() => panic!("ANALYZE must await its pending input: {result:?}"),
            }
        }).await.unwrap();
        drop(stream);
        // Retain the plan/diagnostics: only dropping execution should cancel work.
        tokio::time::timeout(Duration::from_secs(5), counts.dropped.notified())
            .await
            .unwrap();
        assert_eq!(counts.scans.load(Ordering::Relaxed), 1);
        assert_eq!(counts.executions.load(Ordering::Relaxed), 1);
        assert_eq!(counts.drops.load(Ordering::Relaxed), 1);
        eprintln!("native={native}: dropping ANALYZE cancelled one pending source; plan retained");
        drop(plan);
    }

    for (fail_scan, schema_mismatch) in [(true, false), (false, false), (false, true)] {
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
            let sql = format!("EXPLAIN ANALYZE {verbose}SELECT id FROM items");
            let native = physical(&ctx, &sql, true).await.unwrap_err();
            let compact = physical(&ctx, &sql, false).await.unwrap_err();
            assert_eq!(
                std::mem::discriminant(&native),
                std::mem::discriminant(&compact)
            );
            assert_eq!(native.strip_backtrace(), compact.strip_backtrace());
            assert_eq!(scans.swap(0, Ordering::Relaxed), 2);
            assert_eq!(
                calls.swap(0, Ordering::Relaxed),
                if fail_scan { 0 } else { 2 }
            );
            eprintln!(
                "ANALYZE planning failure, scan={fail_scan}, schema_mismatch={schema_mismatch}, verbose={verbose:?}: {native}"
            );
        }
    }
}
