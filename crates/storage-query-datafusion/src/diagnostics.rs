// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::{ExecutionPlan, RecordBatchStream};
use futures::{Stream, StreamExt};
use tokio::time::Instant;

use restate_platform::sync::Mutex;
use restate_storage_query_api::{
    NodeWarning, NodeWarnings, QueryDiagnostics, QueryOperatorStats, QueryStats, QueryStatus,
};

struct DataFusionQueryDiagnostics {
    // Read metrics afresh: operators may register metrics lazily during execution.
    plan: Arc<dyn ExecutionPlan>,
    warnings: Vec<NodeWarnings>,
    query_started: Instant,
    execution_started: Instant,
    progress: Mutex<QueryProgress>,
}

#[derive(Clone, Copy)]
struct QueryProgress {
    status: QueryStatus,
    finished: Option<Instant>,
    output_rows: u64,
    output_batches: u64,
}

impl QueryDiagnostics for DataFusionQueryDiagnostics {
    fn snapshot(&self) -> QueryStats {
        let progress = *self.progress.lock();
        let at = progress.finished.unwrap_or_else(Instant::now);
        QueryStats {
            status: progress.status,
            total_duration: at.duration_since(self.query_started),
            execution_duration: at.duration_since(self.execution_started),
            output_rows: progress.output_rows,
            output_batches: progress.output_batches,
        }
    }

    fn plan_metrics(&self) -> QueryOperatorStats {
        snapshot_plan(self.plan.as_ref())
    }

    fn warnings(&self) -> Vec<NodeWarning> {
        self.warnings
            .iter()
            .flat_map(|w| w.lock().clone())
            .collect()
    }
}

/// Owns the execution stream independently of its observers. Retaining diagnostics
/// retains the plan, but must not keep execution alive after this stream is dropped.
pub(crate) struct QueryDiagnosticStream {
    inner: Option<SendableRecordBatchStream>,
    schema: SchemaRef,
    diagnostics: Arc<DataFusionQueryDiagnostics>,
}

impl QueryDiagnosticStream {
    pub(crate) fn wrap(
        inner: SendableRecordBatchStream,
        plan: Arc<dyn ExecutionPlan>,
        warnings: Vec<NodeWarnings>,
        query_started: Instant,
        execution_started: Instant,
    ) -> (SendableRecordBatchStream, Arc<dyn QueryDiagnostics>) {
        let diagnostics = Arc::new(DataFusionQueryDiagnostics {
            plan,
            warnings,
            query_started,
            execution_started,
            progress: Mutex::new(QueryProgress {
                status: QueryStatus::Running,
                finished: None,
                output_rows: 0,
                output_batches: 0,
            }),
        });
        let stream = Self {
            schema: inner.schema(),
            inner: Some(inner),
            diagnostics: Arc::clone(&diagnostics),
        };
        (Box::pin(stream), diagnostics)
    }

    fn finish(&mut self, status: QueryStatus) {
        if let Some(inner) = self.inner.take() {
            // Some operators update metrics on Drop. Release execution before
            // publishing its terminal status. Remote cleanup can still be asynchronous.
            drop(inner);
            let mut progress = self.diagnostics.progress.lock();
            progress.status = status;
            progress.finished = Some(Instant::now());
        }
    }
}

impl Stream for QueryDiagnosticStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let Some(inner) = self.inner.as_mut() else {
            return Poll::Ready(None);
        };
        let poll = inner.poll_next_unpin(cx);
        match &poll {
            Poll::Ready(Some(Ok(batch))) => {
                let mut progress = self.diagnostics.progress.lock();
                progress.output_rows += batch.num_rows() as u64;
                progress.output_batches += 1;
            }
            Poll::Ready(Some(Err(_))) => self.finish(QueryStatus::Failed),
            Poll::Ready(None) => self.finish(QueryStatus::Completed),
            Poll::Pending => {}
        }
        poll
    }
}

impl RecordBatchStream for QueryDiagnosticStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Drop for QueryDiagnosticStream {
    fn drop(&mut self) {
        self.finish(QueryStatus::Cancelled);
    }
}

fn snapshot_plan(plan: &dyn ExecutionPlan) -> QueryOperatorStats {
    QueryOperatorStats {
        name: plan.name().into(),
        metrics: plan.metrics(),
        children: plan
            .children()
            .iter()
            .map(|child| snapshot_plan(child.as_ref()))
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use std::fmt;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use datafusion::arrow::datatypes::Schema;
    use datafusion::common::DataFusionError;
    use datafusion::common::tree_node::TreeNodeRecursion;
    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::{DisplayAs, DisplayFormatType, PhysicalExpr, PlanProperties};

    use restate_storage_query_api::metrics::{
        ExecutionPlanMetricsSet, Label, MetricBuilder, MetricsSet,
    };

    use super::*;

    #[derive(Debug)]
    struct ObservedPlan {
        inner: EmptyExec,
        metrics: ExecutionPlanMetricsSet,
        reads: AtomicUsize,
    }

    impl DisplayAs for ObservedPlan {
        fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(self.name())
        }
    }

    impl ExecutionPlan for ObservedPlan {
        fn name(&self) -> &str {
            "ObservedPlan"
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            self.inner.properties()
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn apply_expressions(
            &self,
            f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
        ) -> Result<TreeNodeRecursion> {
            self.inner.apply_expressions(f)
        }

        fn with_new_children(
            self: Arc<Self>,
            _: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(self)
        }

        fn execute(
            &self,
            partition: usize,
            context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            self.inner.execute(partition, context)
        }

        fn metrics(&self) -> Option<MetricsSet> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            Some(self.metrics.clone_inner())
        }
    }

    struct FailingStream {
        schema: SchemaRef,
        dropped: Arc<AtomicBool>,
        warnings: NodeWarnings,
    }

    impl Stream for FailingStream {
        type Item = Result<RecordBatch>;

        fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Poll::Ready(Some(Err(DataFusionError::Execution("test failure".into()))))
        }
    }

    impl RecordBatchStream for FailingStream {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
    }

    impl Drop for FailingStream {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::Relaxed);
            self.warnings.lock().push(NodeWarning {
                node_id: "N1".into(),
                message: "recorded on drop".into(),
            });
        }
    }

    #[tokio::test]
    async fn terminal_states_release_execution_before_observation() {
        for fail in [true, false] {
            let schema = Arc::new(Schema::empty());
            let dropped = Arc::new(AtomicBool::new(false));
            let warnings = Arc::new(Mutex::new(Vec::new()));
            let plan = Arc::new(ObservedPlan {
                inner: EmptyExec::new(Arc::clone(&schema)),
                metrics: ExecutionPlanMetricsSet::new(),
                reads: AtomicUsize::new(0),
            });
            let query_started = Instant::now();
            let (mut stream, diagnostics) = QueryDiagnosticStream::wrap(
                Box::pin(FailingStream {
                    schema: Arc::clone(&schema),
                    dropped: Arc::clone(&dropped),
                    warnings: Arc::clone(&warnings),
                }),
                plan.clone(),
                vec![warnings],
                query_started,
                Instant::now(),
            );
            assert!(diagnostics.warnings().is_empty());
            if fail {
                assert!(stream.next().await.unwrap().is_err());
                assert!(dropped.load(Ordering::Relaxed));
                assert!(stream.next().await.is_none());
            }
            drop(stream);
            assert!(dropped.load(Ordering::Relaxed));
            let stats = diagnostics.snapshot();
            assert_eq!(
                stats.status,
                if fail {
                    QueryStatus::Failed
                } else {
                    QueryStatus::Cancelled
                }
            );
            assert_eq!(stats.output_rows, 0);
            let warnings = diagnostics.warnings();
            assert_eq!(warnings.len(), 1);
            // Observers do not consume each other's warnings.
            assert_eq!(diagnostics.warnings().len(), 1);
            assert_eq!(warnings[0].message.as_str(), "recorded on drop");
            assert_eq!(plan.reads.load(Ordering::Relaxed), 0);
            assert!(stats.total_duration >= stats.execution_duration);

            // Detailed reads are opt-in and preserve the native metric identity.
            let count = MetricBuilder::new(&plan.metrics)
                .with_label(Label::new("source", "scanner"))
                .counter("records_scanned", 3);
            count.add(7);
            let metrics = diagnostics.plan_metrics().metrics.unwrap();
            let metric = metrics.iter().next().unwrap();
            assert_eq!(metric.value().name(), "records_scanned");
            assert_eq!(metric.partition(), Some(3));
            assert_eq!(metric.labels()[0].name(), "source");
            assert_eq!(metric.labels()[0].value(), "scanner");
            assert_eq!(metric.value().as_usize(), 7);
            // Native counters stay live; a fresh call also discovers new registrations.
            count.add(1);
            assert_eq!(metric.value().as_usize(), 8);
            MetricBuilder::new(&plan.metrics)
                .counter("late_metric", 3)
                .add(1);
            assert_eq!(metrics.iter().count(), 1);
            assert_eq!(
                diagnostics.plan_metrics().metrics.unwrap().iter().count(),
                2
            );
            assert_eq!(plan.reads.load(Ordering::Relaxed), 2);
        }
    }
}
