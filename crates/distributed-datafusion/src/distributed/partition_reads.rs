// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A compact, single-partition template executed within bounded admitted scopes.
//! Native operators inside the template share its TaskContext and snapshot; an
//! index-to-primary pipeline can use the same capability for both accesses.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Result, Statistics, exec_err, plan_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalSortExpr};
use datafusion::physical_plan::execution_plan::{
    Boundedness, EmissionType, SchedulingType, reset_plan_states,
};
use datafusion::physical_plan::limit::LimitStream;
use datafusion::physical_plan::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::statistics::{ChildStats, StatisticsArgs};
use datafusion::physical_plan::stream::{RecordBatchReceiverStream, RecordBatchStreamAdapter};
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PhysicalExpr, PlanProperties,
    execute_stream,
};
use futures::StreamExt;

use restate_types::GenerationalNodeId;
use restate_types::identifiers::PartitionId;
use restate_types::net::distributed_query::{QueryFailureKind, QueryTaskError};

use crate::source_registry::PartitionLocation;
use crate::table_providers::PartitionReadContext;

use super::{LocalBinding, SourceWork, external, source_plan};

#[derive(Clone, Debug)]
pub(super) struct PartitionReadExec {
    input: Arc<dyn ExecutionPlan>,
    partitions: Arc<[PartitionId]>,
    owner: Option<GenerationalNodeId>,
    parallelism: usize,
    fetch: Option<usize>,
    /// The order of each lane's rows ([`Self::lane_ordering`]).
    ordering: Vec<PhysicalSortExpr>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl PartitionReadExec {
    pub(super) fn new(
        inputs: Vec<Arc<dyn ExecutionPlan>>,
        parallelism: usize,
        fetch: Option<usize>,
    ) -> Result<Self> {
        let input = UnionExec::try_new(inputs)?;
        let mut owner = None;
        let mut partitions = BTreeSet::new();
        input.apply(|node| {
            if let Some(source) = source_plan(node.as_ref()) {
                if owner.is_some_and(|owner| owner != source.descriptor.owner) {
                    return plan_err!("partition sources must share an owner");
                }
                owner = Some(source.descriptor.owner);
                match &source.descriptor.work {
                    SourceWork::MultiGet(work) => {
                        partitions.extend(work.reads.iter().map(|read| read.partition))
                    }
                    work => match work.scans() {
                        Some(work) => {
                            partitions.extend(work.reads.iter().map(|read| read.partition))
                        }
                        None => return plan_err!("partition read cannot contain a node source"),
                    },
                }
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        if partitions.is_empty() {
            return plan_err!("partition read has no storage partitions");
        }
        let parallelism = parallelism.max(1).min(partitions.len());
        let ordering = Self::lane_ordering(&input, parallelism, partitions.len());
        Ok(Self {
            properties: Self::properties(&input, &ordering, parallelism),
            input,
            partitions: partitions.into_iter().collect(),
            owner: owner.flatten(),
            parallelism,
            fetch,
            ordering,
            metrics: Default::default(),
        })
    }

    /// The order of each lane's rows: the template's own output order, when every
    /// lane reads exactly one storage partition. The order holds within a storage
    /// partition, not across the partitions a lane concatenates, so lanes that
    /// concatenate keep none. The configured lanes still bound how many partitions
    /// are read at once.
    fn lane_ordering(
        input: &Arc<dyn ExecutionPlan>,
        lanes: usize,
        partitions: usize,
    ) -> Vec<PhysicalSortExpr> {
        let properties = input.properties();
        if lanes != partitions || properties.output_partitioning().partition_count() != 1 {
            return Vec::new();
        }
        properties
            .output_ordering()
            .map(|ordering| ordering.iter().cloned().collect())
            .unwrap_or_default()
    }

    fn properties(
        input: &Arc<dyn ExecutionPlan>,
        ordering: &[PhysicalSortExpr],
        parallelism: usize,
    ) -> Arc<PlanProperties> {
        Arc::new(
            PlanProperties::new(
                EquivalenceProperties::new_with_orderings(input.schema(), [ordering.to_vec()]),
                Partitioning::UnknownPartitioning(parallelism),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )
            .with_scheduling_type(SchedulingType::Cooperative),
        )
    }

    pub(super) fn owner(&self) -> Option<GenerationalNodeId> {
        self.owner
    }

    pub(super) fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub(super) fn to_proto(&self) -> PartitionReadsProto {
        PartitionReadsProto {
            parallelism: Some(self.parallelism as u64),
            fetch: self.fetch.map(|n| n as u64),
        }
    }

    async fn admit(
        input: &Arc<dyn ExecutionPlan>,
        partition: PartitionId,
        context: &TaskContext,
    ) -> Result<PartitionReadContext> {
        let mut requirements = restate_worker_api::PartitionCaptureRequirements::default();
        let mut require_leader = false;
        let mut registry = None;
        // Only participating leaves contribute requirements. All accesses within
        // this subtree receive the same resulting capability, including lookups.
        input.apply(|node| {
            if let Some(source) = source_plan(node.as_ref()) {
                let Some(LocalBinding::Partition {
                    manager,
                    placement,
                    reads,
                }) = &source.binding
                else {
                    return exec_err!("unbound partition read reached execution");
                };
                if let Ok(index) = reads.binary_search_by_key(&partition, |read| read.partition) {
                    if !matches!(
                        manager
                            .get_partition_target_node(partition, *placement)
                            .map_err(external)?,
                        PartitionLocation::Local
                    ) {
                        return exec_err!("storage ownership changed for partition {partition}");
                    }
                    requirements.include(reads[index].read.capture_requirements());
                    require_leader |= placement.requires_leader();
                    registry = Some(manager.clone());
                }
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        if requirements.storage || requirements.needs_live_state() {
            let registry = registry.expect("participating storage source");
            let query_io = registry.query_io().map_err(external)?;
            Ok(PartitionReadContext::from_capture(
                registry
                    .capture_partition(
                        partition,
                        require_leader,
                        context.session_config().get_extension(),
                        requirements,
                    )
                    .await
                    .map_err(external)?,
            )
            .with_query_io(query_io))
        } else {
            Ok(PartitionReadContext::live(partition))
        }
    }
}

#[derive(Clone, PartialEq, prost::Message)]
pub(super) struct PartitionReadsProto {
    #[prost(uint64, optional, tag = "201")]
    pub parallelism: Option<u64>,
    #[prost(uint64, optional, tag = "202")]
    pub fetch: Option<u64>,
}

/// Combine complementary physical layouts on the same owner before ordinary
/// optimization. Repeated SQL UNION ALL branches retain their multiplicity.
pub(super) fn group_sources(
    plan: Arc<dyn ExecutionPlan>,
    parallelism: usize,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let Some(union) = plan.downcast_ref::<UnionExec>() else {
        return Ok(Transformed::no(plan));
    };
    fn collect(
        plan: &Arc<dyn ExecutionPlan>,
        owners: &mut BTreeMap<Option<GenerationalNodeId>, Vec<Arc<dyn ExecutionPlan>>>,
    ) -> bool {
        if let Some(union) = plan.downcast_ref::<UnionExec>() {
            union.inputs().iter().all(|input| collect(input, owners))
        } else if let Some(group) = plan.downcast_ref::<PartitionReadExec>()
            && group.fetch.is_none()
        {
            owners
                .entry(group.owner())
                .or_default()
                .push(Arc::clone(&group.input));
            true
        } else {
            false
        }
    }
    let mut owners = BTreeMap::new();
    if !union
        .inputs()
        .iter()
        .all(|input| collect(input, &mut owners))
    {
        return Ok(Transformed::no(plan));
    }
    let inputs = owners
        .into_values()
        .map(|inputs| {
            Ok(Arc::new(PartitionReadExec::new(inputs, parallelism, None)?)
                as Arc<dyn ExecutionPlan>)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Transformed::yes(UnionExec::try_new(inputs)?))
}

impl DisplayAs for PartitionReadExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "PartitionReadExec: partitions={}, parallelism={}",
            self.partitions.len(),
            self.parallelism
        )?;
        if let Some(fetch) = self.fetch {
            write!(f, ", fetch={fetch}")?;
        }
        Ok(())
    }
}

impl ExecutionPlan for PartitionReadExec {
    fn name(&self) -> &str {
        "PartitionReadExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input]: [Arc<dyn ExecutionPlan>; 1] = children.try_into().map_err(|_| {
            datafusion::common::plan_datafusion_err!("partition read requires one template")
        })?;
        // Requirement enforcement temporarily hides union branches behind opaque
        // planning inputs. Preserve the selected partition universe across these
        // native rewrites rather than rediscovering it through those wrappers.
        let mut plan = self.as_ref().clone();
        plan.ordering = Self::lane_ordering(&input, self.parallelism, self.partitions.len());
        plan.properties = Self::properties(&input, &plan.ordering, self.parallelism);
        plan.input = input;
        Ok(Arc::new(plan))
    }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn with_fetch(&self, fetch: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        let fetch = self.fetch.into_iter().chain(fetch).min();
        // Source bounds are safe through concatenation, but not through residuals
        // or other native operators inside a future partition-local pipeline.
        fn bound(
            plan: Arc<dyn ExecutionPlan>,
            fetch: Option<usize>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            if let Some(union) = plan.downcast_ref::<UnionExec>() {
                UnionExec::try_new(
                    union
                        .inputs()
                        .iter()
                        .map(|input| bound(Arc::clone(input), fetch))
                        .collect::<Result<_>>()?,
                )
            } else {
                Ok(plan.with_fetch(fetch).unwrap_or(plan))
            }
        }
        let mut plan = self.clone();
        plan.input = bound(Arc::clone(&self.input), fetch).ok()?;
        plan.fetch = fetch;
        Some(Arc::new(plan))
    }
    fn fetch(&self) -> Option<usize> {
        self.fetch
    }
    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
    fn child_stats_requests(&self, _: Option<usize>) -> Vec<ChildStats> {
        vec![ChildStats::At(None)]
    }
    fn statistics_from_inputs(
        &self,
        inputs: &[Arc<Statistics>],
        args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        let stats = if args.partition().is_some() {
            crate::statistics::estimate_source_statistics(
                &inputs[0],
                1.0 / self.parallelism as f64,
                None,
            )?
        } else {
            Arc::clone(&inputs[0])
        };
        Ok(Arc::new(stats.as_ref().clone().with_fetch(
            self.fetch,
            0,
            if args.partition().is_some() {
                1
            } else {
                self.parallelism
            },
        )?))
    }

    fn execute(&self, lane: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        if lane >= self.parallelism {
            return exec_err!("invalid partition read lane {lane}");
        }
        let input = Arc::clone(&self.input);
        let partitions = Arc::clone(&self.partitions);
        let parallelism = self.parallelism;
        let fetch = self.fetch;
        let opened = MetricBuilder::new(&self.metrics).counter("partition_scopes", lane);
        let mut output = RecordBatchReceiverStream::builder(self.schema(), 1);
        let tx = output.tx();
        output.spawn(async move {
            let result = async {
                let mut remaining = fetch.unwrap_or(usize::MAX);
                for index in (lane..partitions.len()).step_by(parallelism) {
                    if remaining == 0 || tx.is_closed() {
                        break;
                    }
                    let scope = Self::admit(&input, partitions[index], &context).await?;
                    opened.add(1);
                    let closed = scope.snapshot.as_ref().map(|read| read.closed());
                    let cancel = scope
                        .snapshot
                        .as_ref()
                        .map(|read| read.cancellation_token());
                    let _cancel_on_drop = cancel.as_ref().map(|cancel| cancel.clone().drop_guard());
                    let mut config = context.session_config().clone();
                    config.set_extension(Arc::new(scope));
                    let scoped_context = Arc::new(TaskContext::new(
                        context.task_id(),
                        context.session_id(),
                        config,
                        context.scalar_functions().clone(),
                        context.higher_order_functions().clone(),
                        context.aggregate_functions().clone(),
                        context.window_functions().clone(),
                        context.runtime_env(),
                    ));
                    let input = Arc::clone(&input);
                    let tx = tx.clone();
                    let run = async move {
                        let mut stream = execute_stream(reset_plan_states(input)?, scoped_context)?;
                        while remaining > 0
                            && let Some(batch) = stream.next().await
                        {
                            let mut batch = batch?;
                            if batch.num_rows() > remaining {
                                batch = batch.slice(0, remaining);
                            }
                            remaining -= batch.num_rows();
                            if tx.send(Ok(batch)).await.is_err() {
                                return Ok(0);
                            }
                        }
                        Ok::<_, datafusion::common::DataFusionError>(remaining)
                    };
                    remaining = if let Some(cancel) = cancel {
                        // Poll cancellation independently of the consumer, including
                        // while blocked sending a full output buffer. Dropping the
                        // native subtree closes its storage batch channels.
                        match cancel.run_until_cancelled(run).await {
                            Some(result) if !cancel.is_cancelled() => result?,
                            Some(_) | None => return Err(read_cancelled()),
                        }
                    } else {
                        run.await?
                    };
                    // Async stream completion can precede raw iterator teardown. A
                    // scheduling lane does not open its next partition until both
                    // the subtree and its storage jobs have released this scope.
                    drop(_cancel_on_drop);
                    if let Some(closed) = closed {
                        closed.await;
                    }
                }
                Ok(())
            }
            .await;
            // Keep terminal errors behind batches already sent by this producer.
            // Returning an error from the task would race the buffered channel.
            if let Err(error) = result {
                let _ = tx.send(Err(error)).await;
            }
            Ok(())
        });
        Ok(Box::pin(LimitStream::new(
            Box::pin(RecordBatchStreamAdapter::new(self.schema(), output.build())),
            0,
            self.fetch,
            BaselineMetrics::new(&self.metrics, lane),
        )))
    }
}

/// The error of a read whose partition scope was cancelled, for example by its
/// stopping processor: a typed `CANCELLED` task failure, so that a worker reports
/// it as such.
fn read_cancelled() -> datafusion::common::DataFusionError {
    datafusion::common::DataFusionError::External(Box::new(QueryTaskError::new(
        QueryFailureKind::CANCELLED,
        "partition query read cancelled",
    )))
}
