// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Explicit, owner-bound stages backed by datafusion-distributed's task runtime.

mod pushdown;
mod requirements;
mod source;
mod transport;
mod worker;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod multi_owner_tests;

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Result, exec_err};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::coop::CooperativeExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion::prelude::SessionConfig;
use datafusion_distributed::{
    DistributedConfig, DistributedExec, DistributedExt, NetworkBoundary, NetworkCoalesceExec,
    RouteTaskEvent, RouteTaskEventResponse, RouteTaskHandler, Stage, WorkerResolver,
};
use url::Url;
use uuid::Uuid;

use restate_core::network::NetworkSender;
use restate_types::GenerationalNodeId;

pub(crate) use source::SourceExec;
pub use worker::DistributedQueryServer;

#[derive(Debug)]
pub(crate) struct DistributedExecution {
    pub operator_pushdown: bool,
}

/// Runs last so EXPLAIN captures the same mandatory stages that queries execute.
#[derive(Debug)]
pub(crate) struct DistributedPlanRule {
    pub operator_pushdown: bool,
}

impl PhysicalOptimizerRule for DistributedPlanRule {
    fn optimize(
        &self,
        input: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan_with_pushdown(input, self.operator_pushdown, config)
    }

    fn name(&self) -> &str {
        "restate_distributed_stages"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

pub(crate) fn configure(config: &mut SessionConfig, network: impl NetworkSender) {
    config.set_extension(Arc::new(DistributedExecution {
        operator_pushdown: true,
    }));
    config.set_distributed_option_extension(runtime_config());
    config.set_distributed_channel_resolver(transport::RestateChannelResolver::new(network));
    config.set_distributed_worker_resolver(SourceOwnersOnly);
    config.set_distributed_route_task_handler(SourceOwnersOnly);
    config.set_distributed_user_codec(source::SourceCodec::default());
}

fn runtime_config() -> DistributedConfig {
    let mut config = DistributedConfig::default();
    config.collect_metrics = false;
    config.collect_dynamic_filters = false;
    config.remote_dynamic_filters = false;
    config.max_coordinator_channel_retries = 0;
    config
}

/// Install boundaries *after* ordinary optimization: the library's automatic
/// finalizer intentionally elides 1:1 boundaries, which cannot express ownership.
pub(crate) fn plan(plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    plan_with_pushdown(plan, true, &ConfigOptions::default())
}

fn plan_with_pushdown(
    plan: Arc<dyn ExecutionPlan>,
    operator_pushdown: bool,
    config: &ConfigOptions,
) -> Result<Arc<dyn ExecutionPlan>> {
    if !plan.exists(|plan| Ok(plan.is::<SourceExec>()))? {
        return Ok(plan);
    }
    // Normalize native requirements against available source lanes before
    // forming stages. This removes speculative fan-out that can otherwise
    // hide owner inputs from partition-local operators.
    let plan = requirements::prepare(plan, config)?;
    let query_id = Uuid::new_v4();
    let mut stage_number = 0;
    let plan = plan
        .transform_up(|plan| {
            if !plan.is::<SourceExec>() {
                if operator_pushdown && let Some(plan) = pushdown::push_into_sources(&plan)? {
                    return Ok(Transformed::yes(plan));
                }
                return Ok(Transformed::no(plan));
            }
            stage_number += 1;
            Ok(Transformed::yes(owner_boundary(
                plan,
                query_id,
                stage_number,
            )?))
        })?
        .data;
    if stage_number == 0 {
        return Ok(plan);
    }
    // Each owner is optimized with its own input budget; the coordinator sees
    // only completed stage outputs and uses their combined budget.
    let plan = requirements::finalize(plan, config)?;
    let plan = if plan.output_partitioning().partition_count() == 1 {
        plan
    } else {
        Arc::new(CoalescePartitionsExec::new(plan))
    };
    Ok(Arc::new(DistributedExec::new(plan)))
}

fn owner_boundary(
    plan: Arc<dyn ExecutionPlan>,
    query_id: Uuid,
    number: usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    // The library replaces a task-root repartition with its producer head.
    // A coalesce boundary uses ProducerHead::None, so protect owner-local
    // redistribution from being mistaken for a removable network exchange.
    let plan = if plan.is::<RepartitionExec>() {
        Arc::new(CooperativeExec::new(plan)) as Arc<dyn ExecutionPlan>
    } else {
        plan
    };
    let boundary = NetworkCoalesceExec::try_new(plan, 1, 1)?;
    let mut stage = boundary.input_stage().clone();
    if let Stage::Local(local) = &mut stage {
        local.query_id = query_id;
        local.num = number;
    }
    Ok(boundary.with_input_stage(stage)? as Arc<dyn ExecutionPlan>)
}

fn worker_url(owner: GenerationalNodeId) -> Result<Url> {
    Url::parse(&format!("restate-query:///{owner}"))
        .map_err(|err| datafusion::error::DataFusionError::External(Box::new(err)))
}

struct SourceOwnersOnly;

impl WorkerResolver for SourceOwnersOnly {
    fn get_urls(&self) -> Result<Vec<Url>> {
        // Assignment is exclusively through the mandatory handler below.
        Ok(vec![])
    }
}

#[async_trait]
impl RouteTaskHandler for SourceOwnersOnly {
    async fn handle(&self, ev: RouteTaskEvent<'_>) -> Option<Result<RouteTaskEventResponse>> {
        Some(
            async {
                if ev.task_count != 1 || ev.task_key.task_number != 0 {
                    return exec_err!("owner-bound stages must have exactly one task");
                }
                let mut owner = None;
                ev.task_specialized_plan.apply(|plan| {
                    if let Some(source) = plan.downcast_ref::<SourceExec>() {
                        if owner.is_some_and(|owner| owner != source.owner()) {
                            return exec_err!("task contains conflicting source owners");
                        }
                        owner = Some(source.owner());
                    }
                    Ok(TreeNodeRecursion::Continue)
                })?;
                let Some(owner) = owner else {
                    return exec_err!("task has no source owner");
                };
                ev.dialer.dial(worker_url(owner)?).await
            }
            .await,
        )
    }
}
