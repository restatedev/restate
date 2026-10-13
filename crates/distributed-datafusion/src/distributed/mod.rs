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

mod source;
mod transport;
mod worker;

#[cfg(test)]
mod tests;

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Result, exec_err, plan_err};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
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

pub(crate) use source::StorageScanExec;
pub use worker::DistributedQueryServer;

#[derive(Debug)]
pub(crate) struct DistributedExecution;

/// Runs last so EXPLAIN captures the same mandatory stages that queries execute.
#[derive(Debug)]
pub(crate) struct DistributedPlanRule;

impl PhysicalOptimizerRule for DistributedPlanRule {
    fn optimize(
        &self,
        input: Arc<dyn ExecutionPlan>,
        _: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan(input)
    }

    fn name(&self) -> &str {
        "restate_distributed_stages"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

pub(crate) fn configure(config: &mut SessionConfig, network: impl NetworkSender) {
    config.set_extension(Arc::new(DistributedExecution));
    config.set_distributed_option_extension(runtime_config());
    config.set_distributed_channel_resolver(transport::RestateChannelResolver::new(network));
    config.set_distributed_worker_resolver(StorageOwnersOnly);
    config.set_distributed_route_task_handler(StorageOwnersOnly);
    config.set_distributed_user_codec(source::StorageCodec::default());
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
    let query_id = Uuid::new_v4();
    let mut stage_number = 0;
    let mut owner = None;
    let plan = plan
        .transform_up(|plan| {
            let Some(source) = plan.downcast_ref::<StorageScanExec>() else {
                return Ok(Transformed::no(plan));
            };
            if owner.is_some_and(|owner| owner != source.owner()) {
                return plan_err!(
                    "the distributed prototype currently requires a single storage owner"
                );
            }
            owner = Some(source.owner());
            stage_number += 1;
            let boundary = NetworkCoalesceExec::try_new(plan, 1, 1)?;
            let mut stage = boundary.input_stage().clone();
            if let Stage::Local(local) = &mut stage {
                local.query_id = query_id;
                local.num = stage_number;
            }
            Ok(Transformed::yes(
                boundary.with_input_stage(stage)? as Arc<dyn ExecutionPlan>
            ))
        })?
        .data;
    if stage_number == 0 {
        return Ok(plan);
    }
    let plan = if plan.output_partitioning().partition_count() == 1 {
        plan
    } else {
        Arc::new(CoalescePartitionsExec::new(plan))
    };
    Ok(Arc::new(DistributedExec::new(plan)))
}

fn worker_url(owner: GenerationalNodeId) -> Result<Url> {
    Url::parse(&format!("restate-query:///{owner}"))
        .map_err(|err| datafusion::error::DataFusionError::External(Box::new(err)))
}

struct StorageOwnersOnly;

impl WorkerResolver for StorageOwnersOnly {
    fn get_urls(&self) -> Result<Vec<Url>> {
        // Assignment is exclusively through the mandatory handler below.
        Ok(vec![])
    }
}

#[async_trait]
impl RouteTaskHandler for StorageOwnersOnly {
    async fn handle(&self, ev: RouteTaskEvent<'_>) -> Option<Result<RouteTaskEventResponse>> {
        Some(
            async {
                if ev.task_count != 1 || ev.task_key.task_number != 0 {
                    return exec_err!("owner-bound stages must have exactly one task");
                }
                let mut owner = None;
                ev.task_specialized_plan.apply(|plan| {
                    if let Some(source) = plan.downcast_ref::<StorageScanExec>() {
                        if owner.is_some_and(|owner| owner != source.owner()) {
                            return exec_err!("task contains conflicting storage owners");
                        }
                        owner = Some(source.owner());
                    }
                    Ok(TreeNodeRecursion::Continue)
                })?;
                let Some(owner) = owner else {
                    return exec_err!("task has no storage owner");
                };
                ev.dialer.dial(worker_url(owner)?).await
            }
            .await,
        )
    }
}
