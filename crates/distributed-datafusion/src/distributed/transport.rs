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
use std::time::Duration;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result, exec_datafusion_err, exec_err};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_distributed::{
    ChannelResolver, CoordinatorToWorkerMsg, ExecuteTaskRequest, GetWorkerInfoRequest,
    GetWorkerInfoResponse, MaybeEncoded, ProducerHead, SetPlanRequest, TaskKey, WorkerChannel,
    WorkerToCoordinatorMsg,
};
use futures::stream::BoxStream;
use futures::{StreamExt, stream};
use http::HeaderMap;
use url::Url;

use restate_clock::UniqueTimestamp;
use restate_core::network::{Connection, NetworkSender, Swimlane};
use restate_core::{TaskCenter, TaskCenterFutureExt, TaskKind, task_center};
use restate_platform::sync::Mutex;
use restate_types::GenerationalNodeId;
use restate_types::net::ProtocolVersion;
use restate_types::net::distributed_query::{
    QueryTaskAnalyze, QueryTaskClose, QueryTaskClosed, QueryTaskExecute, QueryTaskId,
    QueryTaskInstall, QueryTaskInstalled, QueryTaskNext, QueryTaskReplyBody, QueryTaskRequest,
};

use crate::{decode_record_batch, decode_schema};

use super::analyze::{Arrival, ReportRegistry};
use super::report::{digest, read_report};

type Tasks = Arc<Mutex<HashMap<TaskKey, RemoteTask>>>;

pub(super) struct RestateChannelResolver<N> {
    network: N,
    tasks: Tasks,
    task_center: task_center::Handle,
}

impl<N> RestateChannelResolver<N> {
    pub fn new(network: N) -> Self {
        Self {
            network,
            tasks: Default::default(),
            task_center: TaskCenter::current(),
        }
    }
}

#[async_trait]
impl<N: NetworkSender> ChannelResolver for RestateChannelResolver<N> {
    async fn get_worker_client_for_url(&self, url: &Url) -> Result<Box<dyn WorkerChannel>> {
        if url.scheme() != "restate-query" || url.host().is_some() {
            return exec_err!("invalid Restate query owner URL");
        }
        let node: GenerationalNodeId = url
            .path()
            .trim_start_matches('/')
            .parse()
            .map_err(|err| exec_datafusion_err!("invalid query owner: {err}"))?;
        let connection = self
            .network
            .get_connection(node, Swimlane::Datafusion)
            .in_tc_as_task(
                &self.task_center,
                TaskKind::InPlace,
                "distributed-query-connect",
            )
            .await
            .map_err(|err| DataFusionError::External(Box::new(err)))?;
        if connection.protocol_version() < ProtocolVersion::V7 {
            return exec_err!("node {node} does not support distributed queries");
        }
        Ok(Box::new(RestateWorkerChannel {
            connection,
            owner: node,
            tasks: Arc::clone(&self.tasks),
        }))
    }
}

#[derive(Clone)]
struct RemoteTask {
    id: QueryTaskId,
    connection: Connection,
    schema: SchemaRef,
    partitions: usize,
}

struct RestateWorkerChannel {
    connection: Connection,
    owner: GenerationalNodeId,
    tasks: Tasks,
}

/// Tells a test what the coordinator decided about each task's telemetry:
/// whether its worker will report, what became of a report it read, and a
/// task closed on cleanup without its report being read.
#[cfg(test)]
pub(super) static TELEMETRY: std::sync::Mutex<
    Option<tokio::sync::mpsc::UnboundedSender<(QueryTaskId, String)>>,
> = std::sync::Mutex::new(None);

#[cfg(test)]
fn telemetry(id: &QueryTaskId, what: String) {
    if let Some(sender) = &*TELEMETRY.lock().unwrap() {
        let _ = sender.send((id.clone(), what));
    }
}

fn task_id(key: TaskKey, ctx: &TaskContext) -> Result<QueryTaskId> {
    let Some(query_ts) = ctx.session_config().get_extension::<UniqueTimestamp>() else {
        return exec_err!("distributed query has no native query timestamp");
    };
    Ok(QueryTaskId {
        session_id: ctx.session_id().into(),
        query_ts: *query_ts,
        runtime_query_id: *key.query_id.as_bytes(),
        stage: key.stage_id as u64,
        task: key.task_number as u64,
    })
}

pub(super) async fn rpc(
    connection: &Connection,
    request: QueryTaskRequest,
) -> Result<QueryTaskReplyBody> {
    let peer = connection.peer();
    // todo: make this configurable
    tokio::time::timeout(Duration::from_secs(600), async {
        let permit = connection
            .reserve()
            .await
            .ok_or_else(|| exec_datafusion_err!("query task connection to {peer} closed"))?;
        let response = permit
            .send_rpc(request, None)
            .map_err(|err| {
                exec_datafusion_err!("sending a query task request to {peer} failed: {err}")
            })?
            .await
            .map_err(|err| exec_datafusion_err!("query task request to {peer} failed: {err}"))?;
        response.into_result().map_err(|mut error| {
            // A worker sets the origin; name the peer if one didn't.
            error.origin.get_or_insert(peer);
            DataFusionError::External(Box::new(error))
        })
    })
    .await
    .map_err(|_| exec_datafusion_err!("query task request to {peer} timed out"))?
}

#[async_trait]
impl WorkerChannel for RestateWorkerChannel {
    async fn coordinator_channel(
        &mut self,
        headers: HeaderMap,
        request: SetPlanRequest,
        mut c2w: BoxStream<'static, CoordinatorToWorkerMsg>,
        _: ExecutionPlanMetricsSet,
        ctx: &Arc<TaskContext>,
    ) -> Result<BoxStream<'static, Result<WorkerToCoordinatorMsg>>> {
        if request.task_count != 1
            || !request.work_unit_feed_declarations.is_empty()
            || !request.dynamic_filter_remote_producer_ids.is_empty()
        {
            return exec_err!("unsupported distributed task capability");
        }
        let (schema, partitions) = match &request.plan {
            MaybeEncoded::Decoded(plan) => {
                (plan.schema(), plan.output_partitioning().partition_count())
            }
            MaybeEncoded::Encoded(_) => {
                return exec_err!("expected a task-specialized plan before transport encoding");
            }
        };
        let id = task_id(request.task_key, ctx)?;
        let registry = ctx.session_config().get_extension::<ReportRegistry>();
        let report_key = (request.task_key.stage_id, request.task_key.task_number);
        let metadata = ctx
            .session_config()
            .get_extension::<restate_storage_query_api::QueryMetadata>()
            .ok_or_else(|| exec_datafusion_err!("distributed query has no execution metadata"))?;
        // Own cleanup before dispatch, including cancellation while awaiting installation.
        let mut guard = TaskGuard {
            connection: Some(self.connection.clone()),
            id: id.clone(),
            key: request.task_key,
            tasks: Arc::clone(&self.tasks),
        };
        let install = QueryTaskInstall {
            id: id.clone(),
            plan: request.plan.encode(ctx)?.into(),
            options: ctx
                .session_config()
                .options()
                .entries()
                .into_iter()
                .filter(|entry| entry.key.starts_with("datafusion."))
                .filter_map(|entry| entry.value.map(|value| (entry.key.into(), value.into())))
                .collect(),
            runtime_headers: headers
                .iter()
                .map(|(name, value)| {
                    Ok((
                        name.as_str().into(),
                        value
                            .to_str()
                            .map_err(|err| exec_datafusion_err!("invalid runtime header: {err}"))?
                            .into(),
                    ))
                })
                .collect::<Result<_>>()?,
            query_start_time_ns: request.query_start_time_ns as u64,
            query_headers: metadata
                .headers
                .iter()
                .map(
                    |(name, value)| restate_types::net::distributed_query::QueryTaskHeader {
                        name: name.as_str().into(),
                        value: value.as_bytes().to_vec().into(),
                    },
                )
                .collect(),
            redacted_sql: metadata.redacted_sql.clone(),
            planning_duration: metadata.planning_duration,
            // Only an EXPLAIN ANALYZE asks for the task's report.
            analyze: registry.as_ref().map(|_| QueryTaskAnalyze {}),
        };
        let plan_digest = digest(&install.plan);
        let reply = rpc(&self.connection, QueryTaskRequest::Install(install)).await?;
        // The registry waits for the task's report only if its worker will send one.
        let registry = match reply {
            QueryTaskReplyBody::Installed(QueryTaskInstalled {
                schema: actual,
                analyze,
            }) => {
                if decode_schema(&actual).map_err(|err| DataFusionError::External(err.into()))?
                    != *schema
                {
                    return exec_err!(
                        "installed task output schema differs from its planned schema"
                    );
                }
                registry.filter(|registry| {
                    registry.installed(report_key, analyze.is_some(), plan_digest);
                    #[cfg(test)]
                    telemetry(&id, format!("installed report={}", analyze.is_some()));
                    analyze.is_some()
                })
            }
            _ => return exec_err!("worker did not acknowledge task installation"),
        };
        self.tasks.lock().insert(
            request.task_key,
            RemoteTask {
                id,
                connection: self.connection.clone(),
                schema,
                partitions,
            },
        );

        // Static source tasks have no work-unit or sampling traffic. The library's
        // request-stream EOS owns task lifetime; keep it alive until that EOS arrives.
        let owner = self.owner;
        Ok(stream::once(async move {
            while let Some(message) = c2w.next().await {
                if !matches!(message, CoordinatorToWorkerMsg::WorkUnitEos) {
                    return exec_err!("unsupported coordinator control message");
                }
            }
            match registry {
                // The report is optional: waiting for it is bounded by the query's
                // one deadline, and failing to get it only marks it missing. A
                // worker without reports closes as before.
                Some(registry) => {
                    let closed = tokio::time::timeout_at(registry.deadline(), guard.close()).await;
                    registry.received(
                        report_key,
                        match closed {
                            Ok(Ok(QueryTaskClosed {
                                report: Some(bytes),
                            })) => match read_report(&bytes) {
                                Ok(report) => {
                                    #[cfg(test)]
                                    telemetry(&guard.id, "received".to_owned());
                                    Arrival::Report(owner, report)
                                }
                                Err(reason) => {
                                    tracing::debug!(%owner, reason, "Query task report unusable");
                                    #[cfg(test)]
                                    telemetry(&guard.id, "invalid".to_owned());
                                    Arrival::Invalid
                                }
                            },
                            Ok(Ok(QueryTaskClosed { report: None }) | Err(_)) | Err(_) => {
                                #[cfg(test)]
                                telemetry(&guard.id, "missing".to_owned());
                                Arrival::Missing
                            }
                        },
                    );
                }
                None => {
                    guard.close().await?;
                    #[cfg(test)]
                    telemetry(&guard.id, "closed".to_owned());
                }
            }
            Ok(WorkerToCoordinatorMsg::LoadInfoEos)
        })
        .boxed())
    }

    async fn execute_task(
        &mut self,
        _: HeaderMap,
        request: ExecuteTaskRequest,
        _: ExecutionPlanMetricsSet,
        ctx: &Arc<TaskContext>,
    ) -> Result<Vec<BoxStream<'static, Result<RecordBatch>>>> {
        if !matches!(request.producer_head, ProducerHead::None) {
            return exec_err!("the V2 query transport supports coalesce boundaries only");
        }
        let task = self
            .tasks
            .lock()
            .get(&request.task_key)
            .cloned()
            .ok_or_else(|| exec_datafusion_err!("query task was not installed"))?;
        if task.id != task_id(request.task_key, ctx)? {
            return exec_err!("query execution identity mismatch");
        }
        if request.target_partition_start >= request.target_partition_end
            || request.target_partition_end > task.partitions
        {
            return exec_err!("invalid task output partition range");
        }
        let execute = QueryTaskExecute {
            id: task.id.clone(),
            partition_start: request.target_partition_start as u64,
            partition_end: request.target_partition_end as u64,
        };
        if !matches!(
            rpc(&task.connection, QueryTaskRequest::Execute(execute)).await?,
            QueryTaskReplyBody::Executing(())
        ) {
            return exec_err!("unexpected task execution response");
        }
        Ok(
            (request.target_partition_start..request.target_partition_end)
                .map(|partition| {
                    stream::try_unfold(task.clone(), move |task| async move {
                        let reply = rpc(
                            &task.connection,
                            QueryTaskRequest::Next(QueryTaskNext {
                                id: task.id.clone(),
                                partition: partition as u64,
                            }),
                        )
                        .await?;
                        match reply {
                            QueryTaskReplyBody::End(()) => Ok(None),
                            QueryTaskReplyBody::Batch(bytes) => {
                                let batch = decode_record_batch(&bytes)?;
                                if batch.schema() != task.schema {
                                    return exec_err!(
                                        "remote task returned a different output schema"
                                    );
                                }
                                Ok(Some((batch, task)))
                            }
                            _ => exec_err!("unexpected task output response"),
                        }
                    })
                    .boxed()
                })
                .collect(),
        )
    }

    async fn get_worker_info(&mut self, _: GetWorkerInfoRequest) -> Result<GetWorkerInfoResponse> {
        exec_err!("Restate task compatibility is negotiated during installation")
    }
}

struct TaskGuard {
    connection: Option<Connection>,
    id: QueryTaskId,
    key: TaskKey,
    tasks: Tasks,
}

impl TaskGuard {
    /// Closes the task; its reply, which may carry its encoded report.
    async fn close(&mut self) -> Result<QueryTaskClosed> {
        let Some(connection) = &self.connection else {
            return Ok(QueryTaskClosed::default());
        };
        let QueryTaskReplyBody::Closed(closed) = rpc(
            connection,
            QueryTaskRequest::Close(QueryTaskClose {
                id: self.id.clone(),
            }),
        )
        .await?
        else {
            return exec_err!("unexpected task close response");
        };
        self.connection = None;
        Ok(closed)
    }
}

impl Drop for TaskGuard {
    fn drop(&mut self) {
        self.tasks.lock().remove(&self.key);
        if let Some(connection) = self.connection.take() {
            let id = self.id.clone();
            tokio::spawn(async move {
                #[cfg(test)]
                let closed = id.clone();
                if let Err(error) =
                    rpc(&connection, QueryTaskRequest::Close(QueryTaskClose { id })).await
                {
                    tracing::debug!(%error, "Query task cleanup RPC failed");
                }
                #[cfg(test)]
                telemetry(&closed, "closed unread".to_owned());
            });
        }
    }
}
