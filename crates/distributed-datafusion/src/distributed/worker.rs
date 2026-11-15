// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use datafusion::common::{DataFusionError, Result, exec_datafusion_err, exec_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion::prelude::SessionConfig;
use datafusion_distributed::{
    DistributedConfig, DistributedExt, ExecuteTaskRequest, MaybeEncoded, ProducerHead,
    SetPlanRequest, TaskKey, Worker, WorkerPlanRewriteEvent, WorkerPlanRewriteEventResponse,
    WorkerQueryContext, WorkerToCoordinatorMsg,
};
use futures::future::{BoxFuture, Shared};
use futures::{FutureExt, StreamExt, TryFutureExt, TryStreamExt, stream};
use http::{HeaderMap, HeaderName, HeaderValue};
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use restate_clock::UniqueTimestamp;
use restate_core::cancellation_watcher;
use restate_core::network::{
    BackPressureMode, MessageRouterBuilder, ServiceMessage, ServiceReceiver, Verdict,
};
use restate_platform::sync::Mutex;
use restate_storage_query_api::QueryMetadata;
use restate_types::GenerationalNodeId;
use restate_types::net::distributed_query::{
    DistributedQueryService, PartitionReadError, QueryTaskError, QueryTaskExecute, QueryTaskId,
    QueryTaskInstall, QueryTaskInstalled, QueryTaskReply, QueryTaskReplyBody, QueryTaskRequest,
};
use restate_types::net::{ProtocolVersion, RpcRequest};

use crate::environment::DataFusionEnv;
use crate::source_registry::SourceRegistry;
use crate::{encode_record_batch, encode_schema};

use super::source::SourceCodec;
use super::worker_url;

type Lane = Arc<tokio::sync::Mutex<Option<SendableRecordBatchStream>>>;

/// Opt-in service for task installation and pull-based output over Restate RPC.
/// The library owns the installed plan and its shared execution state; this service
/// owns transport handles and their lifetimes.
pub struct DistributedQueryServer {
    env: DataFusionEnv,
    scanners: SourceRegistry,
    receiver: ServiceReceiver<DistributedQueryService>,
    #[cfg(test)]
    pub(super) observations: Arc<Mutex<Vec<TaskObservation>>>,
}

#[cfg(test)]
pub(super) struct TaskObservation {
    pub id: QueryTaskId,
    pub context: Arc<TaskContext>,
    pub partitions: usize,
    pub owner: GenerationalNodeId,
    pub plan: String,
    pub output: Arc<Mutex<TaskOutput>>,
}

#[cfg(test)]
#[derive(Default, Clone, Copy, Debug)]
pub(super) struct TaskOutput {
    pub rows: usize,
    pub bytes: usize,
}

impl DistributedQueryServer {
    pub fn new(
        env: DataFusionEnv,
        scanners: SourceRegistry,
        router: &mut MessageRouterBuilder,
    ) -> Self {
        Self {
            env,
            scanners,
            receiver: router.register_service(BackPressureMode::Lossy),
            #[cfg(test)]
            observations: Default::default(),
        }
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let mut receiver = self.receiver.start();
        let shutdown = cancellation_watcher();
        tokio::pin!(shutdown);
        let mut tasks: HashMap<(GenerationalNodeId, QueryTaskId), Arc<InstalledTask>> =
            HashMap::new();
        let mut requests = JoinSet::new();
        let mut reap = tokio::time::interval(Duration::from_secs(60));
        loop {
            tokio::select! {
                _ = &mut shutdown => break,
                _ = reap.tick() => {
                    tasks.retain(|_, task| {
                        let keep = task.last_used.lock().elapsed() < Duration::from_secs(300);
                        if !keep { task.cancel.cancel(); }
                        keep
                    });
                }
                Some(result) = requests.join_next(), if !requests.is_empty() => {
                    if let Err(error) = result { tracing::warn!(%error, "Distributed query request failed"); }
                }
                message = receiver.next() => {
                    let Some(message) = message else { break; };
                    let ServiceMessage::Rpc(message) = message else { message.fail(Verdict::MessageUnrecognized); continue; };
                    if message.msg_type() != QueryTaskRequest::TYPE { message.fail(Verdict::MessageUnrecognized); continue; }
                    if message.protocol_version() < ProtocolVersion::V7 {
                        message.fail(Verdict::MessageUnrecognized);
                        continue;
                    }
                    let peer = message.peer();
                    let (reply, request) = message.into_typed::<QueryTaskRequest>().split();
                    // The receiver starts before node identity is published.
                    // Resolve the origin only when a query request arrives.
                    let origin = self.scanners.node_id();
                    match request {
                        QueryTaskRequest::Install(request) => {
                            let key = (peer, request.id.clone());
                            if tasks.contains_key(&key) {
                                reply.send(QueryTaskReplyBody::Failure("query task is already installed".into()).into());
                                continue;
                            }
                            // Installation builds state and decodes once, without polling storage.
                            match InstalledTask::install(&self.env, &self.scanners, request).await {
                                Ok(task) => {
                                    let schema = encode_schema(&task.plan.schema()).into();
                                    #[cfg(test)]
                                    self.observations.lock().push(TaskObservation {
                                        id: key.1.clone(), context: Arc::clone(&task.context), partitions: task.plan.output_partitioning().partition_count(),
                                        owner: self.scanners.node_id(), plan: datafusion::physical_plan::displayable(task.plan.as_ref()).indent(true).to_string(), output: Arc::clone(&task.output),
                                    });
                                    tasks.insert(key, Arc::new(task));
                                    reply.send(QueryTaskReplyBody::Installed(QueryTaskInstalled { schema }).into());
                                }
                                Err(error) => reply.send(task_reply(Err(error), origin)),
                            }
                        }
                        QueryTaskRequest::Close(request) => {
                            if let Some(task) = tasks.remove(&(peer, request.id)) { task.cancel.cancel(); }
                            reply.send(QueryTaskReplyBody::Closed(()).into());
                        }
                        QueryTaskRequest::Execute(request) => {
                            let task = tasks.get(&(peer, request.id.clone())).cloned();
                            requests.spawn(async move {
                                let result = match task {
                                    Some(task) => task.execute(request).await,
                                    None => exec_err!("query task is not installed"),
                                };
                                reply.send(task_reply(result, origin));
                            });
                        }
                        QueryTaskRequest::Next(request) => {
                            let task = tasks.get(&(peer, request.id)).cloned();
                            requests.spawn(async move {
                                let result = match task {
                                    Some(task) => task.next(request.partition as usize).await,
                                    None => exec_err!("query task is not installed"),
                                };
                                reply.send(task_reply(result, origin));
                            });
                        }
                        QueryTaskRequest::Unknown => reply.send(QueryTaskReplyBody::Failure("unknown query task request".into()).into()),
                    }
                }
            }
        }
        for task in tasks.values() {
            task.cancel.cancel();
        }
        // Aborting on JoinSet drop does not wait for request futures to release
        // their installed tasks and storage owners. Drain them before returning.
        requests.shutdown().await;
        Ok(())
    }
}

pub(super) fn task_reply(
    reply: Result<QueryTaskReplyBody>,
    origin: GenerationalNodeId,
) -> QueryTaskReply {
    let error = match reply {
        Ok(body) => return body.into(),
        Err(error) => error,
    };
    let mut cause: Option<&(dyn Error + 'static)> = Some(&error);
    while let Some(current) = cause {
        if let Some(remote) = current.downcast_ref::<QueryTaskError>() {
            // Preserve the original code, details (including unknown fields), and
            // origin across forwarding. Typed interpretation is only a view.
            return QueryTaskReply::failure(remote.clone());
        }
        if let Some(admission) = current.downcast_ref::<PartitionReadError>() {
            let mut metadata = admission.to_metadata();
            metadata.origin = Some(origin);
            return QueryTaskReply::failure(QueryTaskError {
                message: error.to_string().into(),
                metadata: Some(metadata),
            });
        }
        cause = current.source();
    }
    QueryTaskReplyBody::Failure(error.to_string().into()).into()
}

struct InstalledTask {
    worker: Worker,
    key: TaskKey,
    context: Arc<TaskContext>,
    plan: Arc<dyn ExecutionPlan>,
    started: Mutex<HashSet<usize>>,
    lanes: Mutex<HashMap<usize, Lane>>,
    control: Shared<BoxFuture<'static, Result<(), Arc<DataFusionError>>>>,
    cancel: CancellationToken,
    last_used: Mutex<Instant>,
    #[cfg(test)]
    output: Arc<Mutex<TaskOutput>>,
}

impl InstalledTask {
    async fn install(
        env: &DataFusionEnv,
        scanners: &SourceRegistry,
        request: QueryTaskInstall,
    ) -> Result<Self> {
        if request.id.session_id.is_empty()
            || request.id.task != 0
            || request.id.stage == 0
            || request.id.runtime_query_id == [0; 16]
        {
            return exec_err!("invalid owner-bound task identity");
        }
        let mut headers = HeaderMap::new();
        for (name, value) in &request.runtime_headers {
            headers.insert(
                HeaderName::from_bytes(name.as_bytes())
                    .map_err(|err| exec_datafusion_err!("invalid runtime header: {err}"))?,
                HeaderValue::from_str(value)
                    .map_err(|err| exec_datafusion_err!("invalid runtime header: {err}"))?,
            );
        }
        let config = SessionConfig::new()
            .with_distributed_option_extension_from_headers::<DistributedConfig>(&headers)?;
        let config = DistributedConfig::from_session_config(&config)?;
        if config.collect_metrics
            || config.collect_dynamic_filters
            || config.remote_dynamic_filters
            || config.dynamic_task_count
        {
            return exec_err!("unsupported task control capability");
        }
        let key = TaskKey {
            query_id: Uuid::from_bytes(request.id.runtime_query_id),
            stage_id: request.id.stage as usize,
            task_number: request.id.task as usize,
        };
        let target_worker_url = worker_url(scanners.node_id())?;
        let plan = Arc::new(OnceLock::new());
        let env = env.clone();
        let scanners = scanners.clone();
        let captured_plan = Arc::clone(&plan);
        let mut query_headers = HeaderMap::new();
        for header in &request.query_headers {
            query_headers.append(
                HeaderName::from_bytes(header.name.as_bytes())
                    .map_err(|err| exec_datafusion_err!("invalid query header: {err}"))?,
                HeaderValue::from_bytes(&header.value)
                    .map_err(|err| exec_datafusion_err!("invalid query header: {err}"))?,
            );
        }
        let metadata = Arc::new(QueryMetadata {
            session_id: request.id.session_id.clone(),
            query_ts: request.id.query_ts,
            headers: query_headers,
            redacted_sql: request.redacted_sql.clone(),
            planning_duration: request.planning_duration,
        });
        let worker = Worker::from_session_builder(move |ctx: WorkerQueryContext| {
            let env = env.clone();
            let scanners = scanners.clone();
            let id = request.id.clone();
            let options = request.options.clone();
            let plan = Arc::clone(&captured_plan);
            let metadata = Arc::clone(&metadata);
            async move {
                let builder = ctx
                    .builder
                    .with_session_id(id.session_id.to_string())
                    .with_distributed_user_codec(SourceCodec {
                        manager: Some(scanners),
                    })
                    .with_distributed_worker_plan_rewrite_handler(
                        move |ev: WorkerPlanRewriteEvent<'_>| {
                            plan.set(Arc::clone(&ev.plan)).map_err(|_| {
                                exec_datafusion_err!("task plan was installed twice")
                            })?;
                            Ok(WorkerPlanRewriteEventResponse::new(ev.plan))
                        },
                    );
                let mut state = env.build_worker_state(builder)?;
                for (name, value) in options {
                    if !name.starts_with("datafusion.") {
                        return exec_err!("invalid task option {name}");
                    }
                    state.config_mut().options_mut().set(&name, &value)?;
                }
                state.config_mut().set_extension(Arc::new(id.query_ts));
                state.config_mut().set_extension(metadata);
                Ok(state)
            }
        });
        let cancel = CancellationToken::new();
        let guard = cancel.clone().drop_guard();
        let channel = worker
            .coordinator_channel(
                headers,
                SetPlanRequest {
                    task_key: key,
                    task_count: 1,
                    plan: MaybeEncoded::Encoded(request.plan.to_vec()),
                    dynamic_filter_remote_producer_ids: vec![],
                    work_unit_feed_declarations: vec![],
                    target_worker_url,
                    query_start_time_ns: request.query_start_time_ns as usize,
                },
                stream::pending()
                    .take_until(cancel.clone().cancelled_owned())
                    .boxed(),
            )
            .await?;
        // Installation has completed, but the control stream lives until task closure,
        // even with reporting disabled. Poll it alongside execution to propagate errors
        // without waiting for channel EOS before acknowledging installation.
        let control = channel
            .stream
            .try_for_each(|message| async move {
                if !matches!(message, WorkerToCoordinatorMsg::LoadInfoEos) {
                    return exec_err!("unsupported worker control message");
                }
                Ok(())
            })
            .map_err(Arc::new)
            .boxed()
            .shared();
        let plan = plan
            .get()
            .cloned()
            .ok_or_else(|| exec_datafusion_err!("worker did not install a plan"))?;
        guard.disarm();
        Ok(Self {
            worker,
            key,
            context: channel.task_ctx,
            plan,
            started: Default::default(),
            lanes: Default::default(),
            control,
            cancel,
            last_used: Mutex::new(Instant::now()),
            #[cfg(test)]
            output: Default::default(),
        })
    }

    async fn execute(&self, request: QueryTaskExecute) -> Result<QueryTaskReplyBody> {
        *self.last_used.lock() = Instant::now();
        let start = request.partition_start as usize;
        let end = request.partition_end as usize;
        if start >= end || end > self.plan.output_partitioning().partition_count() {
            return exec_err!("invalid task partition range");
        }
        {
            let mut started = self.started.lock();
            if (start..end).any(|p| started.contains(&p)) {
                return exec_err!("task output partition already started");
            }
            started.extend(start..end);
        }
        let (streams, context) = self
            .while_running(self.worker.execute_task(ExecuteTaskRequest {
                task_key: self.key,
                target_partition_start: start,
                target_partition_end: end,
                producer_head: ProducerHead::None,
            }))
            .await?;
        if !Arc::ptr_eq(&self.context, &context)
            || context.session_id() != request.id.session_id.as_str()
            || context
                .session_config()
                .get_extension::<UniqueTimestamp>()
                .as_deref()
                != Some(&request.id.query_ts)
            || streams.len() != end - start
        {
            return exec_err!("worker did not preserve shared task state and execution identity");
        }
        self.lanes
            .lock()
            .extend((start..end).zip(streams).map(|(partition, stream)| {
                (partition, Arc::new(tokio::sync::Mutex::new(Some(stream))))
            }));
        Ok(QueryTaskReplyBody::Executing(()))
    }

    async fn next(&self, partition: usize) -> Result<QueryTaskReplyBody> {
        *self.last_used.lock() = Instant::now();
        let lane = self
            .lanes
            .lock()
            .get(&partition)
            .cloned()
            .ok_or_else(|| exec_datafusion_err!("task output partition is not running"))?;
        self.while_running(async {
            let mut lane = lane
                .try_lock()
                .map_err(|_| exec_datafusion_err!("concurrent pull for the same task partition"))?;
            let Some(stream) = lane.as_mut() else {
                return exec_err!("task output partition already ended");
            };
            match stream.next().await {
                Some(Ok(batch)) => {
                    #[cfg(test)]
                    let rows = batch.num_rows();
                    let encoded = encode_record_batch(&self.plan.schema(), batch)?;
                    #[cfg(test)]
                    {
                        let mut output = self.output.lock();
                        output.rows += rows;
                        output.bytes += encoded.len();
                    }
                    Ok(QueryTaskReplyBody::Batch(encoded.into()))
                }
                Some(Err(error)) => {
                    *lane = None;
                    Err(error)
                }
                None => {
                    *lane = None;
                    Ok(QueryTaskReplyBody::End(()))
                }
            }
        })
        .await
    }

    async fn while_running<T>(&self, work: impl Future<Output = Result<T>>) -> Result<T> {
        self.cancel
            .run_until_cancelled(async {
                tokio::select! {
                    biased;
                    result = self.control.clone() => {
                        result.map_err(DataFusionError::Shared)?;
                        exec_err!("worker control channel ended before task closure")
                    }
                    result = work => result,
                }
            })
            .await
            .ok_or_else(|| exec_datafusion_err!("query task closed"))?
    }
}

impl Drop for InstalledTask {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
