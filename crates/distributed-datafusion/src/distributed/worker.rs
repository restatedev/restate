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
use restate_storage_api::StorageError;
use restate_storage_query_api::QueryMetadata;
use restate_types::GenerationalNodeId;
use restate_types::net::distributed_query::{
    DistributedQueryService, PartitionReadError, QueryFailureKind, QueryTaskAnalyze,
    QueryTaskClosed, QueryTaskError, QueryTaskExecute, QueryTaskId, QueryTaskInstall,
    QueryTaskInstalled, QueryTaskReply, QueryTaskReplyBody, QueryTaskRequest, TaskOutcome,
};
use restate_types::net::{ProtocolVersion, RpcRequest};

use crate::environment::DataFusionEnv;
use crate::source_registry::SourceRegistry;
use crate::{encode_record_batch, encode_schema};

use super::analyze::{AnalyzeCollector, Shape, build_report, shape};
use super::report::digest;
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
    #[cfg(test)]
    pub(super) tamper: Arc<Mutex<Tamper>>,
}

/// How a test worker misbehaves.
#[cfg(test)]
#[derive(Default)]
pub(super) struct Tamper {
    /// Ignores the request for a report, as a worker from before reports.
    pub legacy: bool,
    /// Sends these bytes as every report.
    pub report: Option<Vec<u8>>,
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
            #[cfg(test)]
            tamper: Default::default(),
        }
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let mut receiver = self.receiver.start();
        let shutdown = cancellation_watcher();
        tokio::pin!(shutdown);
        let mut tasks: HashMap<(GenerationalNodeId, QueryTaskId), Arc<InstalledTask>> =
            HashMap::new();
        // Tasks closing with a report: a repeated Close shares the first one's
        // result, until it's sent.
        let mut closing: HashMap<
            (GenerationalNodeId, QueryTaskId),
            Shared<BoxFuture<'static, QueryTaskClosed>>,
        > = HashMap::new();
        // Each request's task, which yields the key of a closing it ended.
        let mut requests: JoinSet<Option<(GenerationalNodeId, QueryTaskId)>> = JoinSet::new();
        let mut reap = tokio::time::interval(Duration::from_secs(60));
        loop {
            tokio::select! {
                _ = &mut shutdown => break,
                _ = reap.tick() => {
                    tasks.retain(|_, task| {
                        let keep = task
                            .activity
                            .idle_for()
                            .is_none_or(|idle| idle < IDLE_TIMEOUT);
                        if !keep {
                            // The same closing as a Close's; nobody waits for the report.
                            close_unwaited(task, &mut requests);
                        }
                        keep
                    });
                }
                Some(result) = requests.join_next(), if !requests.is_empty() => {
                    match result {
                        Ok(Some(closed)) => { closing.remove(&closed); }
                        Ok(None) => {}
                        Err(error) => tracing::warn!(%error, "Distributed query request failed"),
                    }
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
                                reply.send(task_reply(Err(protocol_error("query task is already installed")), origin));
                                continue;
                            }
                            #[cfg(test)]
                            let request = QueryTaskInstall {
                                analyze: request.analyze.filter(|_| !self.tamper.lock().legacy),
                                ..request
                            };
                            // Installation builds state and decodes once, without polling storage.
                            match InstalledTask::install(&self.env, &self.scanners, request).await {
                                Ok(task) => {
                                    let schema = encode_schema(&task.plan.schema()).into();
                                    #[cfg(test)]
                                    self.observations.lock().push(TaskObservation {
                                        id: key.1.clone(), context: Arc::clone(&task.context), partitions: task.plan.output_partitioning().partition_count(),
                                        owner: self.scanners.node_id(), plan: datafusion::physical_plan::displayable(task.plan.as_ref()).indent(true).to_string(), output: Arc::clone(&task.output),
                                    });
                                    let analyze = task.collector.as_ref().map(|_| QueryTaskAnalyze {});
                                    tasks.insert(key, Arc::new(task));
                                    reply.send(QueryTaskReplyBody::Installed(QueryTaskInstalled { schema, analyze }).into());
                                }
                                Err(error) => reply.send(task_reply(Err(error), origin)),
                            }
                        }
                        QueryTaskRequest::Close(request) => {
                            let key = (peer, request.id);
                            match tasks.remove(&key) {
                                // Settling and building the report waits: not in this loop.
                                Some(task) if task.collector.is_some() => {
                                    let result = async move { task.close().await }.boxed().shared();
                                    closing.insert(key.clone(), result.clone());
                                    #[cfg(test)]
                                    let tampered = self.tamper.lock().report.clone();
                                    requests.spawn(async move {
                                        #[cfg_attr(not(test), expect(unused_mut))]
                                        let mut closed = result.await;
                                        #[cfg(test)]
                                        if let Some(bytes) = tampered {
                                            closed.report = Some(bytes.into());
                                        }
                                        reply.send(QueryTaskReplyBody::Closed(closed).into());
                                        Some(key)
                                    });
                                }
                                Some(task) => {
                                    task.abort();
                                    reply.send(QueryTaskReplyBody::Closed(QueryTaskClosed::default()).into());
                                }
                                None => match closing.get(&key).cloned() {
                                    Some(result) => {
                                        requests.spawn(async move {
                                            reply.send(QueryTaskReplyBody::Closed(result.await).into());
                                            None
                                        });
                                    }
                                    None => reply.send(QueryTaskReplyBody::Closed(QueryTaskClosed::default()).into()),
                                },
                            }
                        }
                        QueryTaskRequest::Execute(request) => {
                            let task = tasks.get(&(peer, request.id.clone())).cloned();
                            // Mark the request in flight here, in the loop that also
                            // reaps, so that a task can't be reaped between dispatch
                            // and the request's first poll. It stays marked until the
                            // reply is sent.
                            let in_flight = task.as_ref().map(|task| task.activity.begin());
                            requests.spawn(async move {
                                let result = match task {
                                    Some(task) => task.execute(request).await,
                                    None => Err(protocol_error("query task is not installed")),
                                };
                                reply.send(task_reply(result, origin));
                                drop(in_flight);
                                None
                            });
                        }
                        QueryTaskRequest::Next(request) => {
                            let task = tasks.get(&(peer, request.id)).cloned();
                            let in_flight = task.as_ref().map(|task| task.activity.begin());
                            requests.spawn(async move {
                                let result = match task {
                                    Some(task) => task.next(request.partition as usize).await,
                                    None => Err(protocol_error("query task is not installed")),
                                };
                                reply.send(task_reply(result, origin));
                                drop(in_flight);
                                None
                            });
                        }
                        QueryTaskRequest::Unknown => reply.send(task_reply(Err(protocol_error("unknown query task request")), origin)),
                    }
                }
            }
        }
        for task in tasks.values() {
            close_unwaited(task, &mut requests);
        }
        // Closings settle within their deadline; give them that long, then abort
        // what's left. Aborting on JoinSet drop does not wait for request futures
        // to release their installed tasks and storage owners: drain them.
        let _ = tokio::time::timeout(2 * SETTLE_DEADLINE, async {
            while requests.join_next().await.is_some() {}
        })
        .await;
        requests.shutdown().await;
        Ok(())
    }
}

/// Closes `task` with nobody waiting for its report, as for an idle task or a
/// shutdown: a task asked for a report closes the same way as for a Close, and
/// its report is dropped.
fn close_unwaited(
    task: &Arc<InstalledTask>,
    requests: &mut JoinSet<Option<(GenerationalNodeId, QueryTaskId)>>,
) {
    if task.collector.is_some() {
        let task = Arc::clone(task);
        requests.spawn(async move {
            task.close().await;
            None
        });
    } else {
        task.abort();
    }
}

pub(super) fn task_reply(
    reply: Result<QueryTaskReplyBody>,
    origin: GenerationalNodeId,
) -> QueryTaskReply {
    match reply {
        Ok(body) => body.into(),
        Err(error) => QueryTaskReply::failure(task_error(&error, origin)),
    }
}

/// The task failure reported for `error`. A failure from another node is
/// forwarded unchanged, with its origin, kind and details (including fields a
/// later release may add). Otherwise this node is the origin, and the kind comes
/// from the cause: an admission failure, a storage error, a cancelled read, or
/// else a failure while executing the plan or, for DataFusion's internal errors,
/// a bug. The message is the cause's own, without DataFusion's wrapper prefixes.
fn task_error(error: &DataFusionError, origin: GenerationalNodeId) -> QueryTaskError {
    let mut cause: Option<&(dyn Error + 'static)> = Some(error);
    while let Some(current) = cause {
        if let Some(remote) = current.downcast_ref::<QueryTaskError>() {
            let mut remote = remote.clone();
            remote.origin.get_or_insert(origin);
            return remote;
        }
        if let Some(admission) = current.downcast_ref::<PartitionReadError>() {
            return QueryTaskError {
                origin: Some(origin),
                ..admission.to_task_error()
            };
        }
        if let Some(storage) = current.downcast_ref::<StorageError>() {
            let kind = match storage {
                StorageError::ScanInterrupted => QueryFailureKind::CANCELLED,
                _ => QueryFailureKind::STORAGE,
            };
            return QueryTaskError {
                origin: Some(origin),
                ..QueryTaskError::new(kind, storage.to_string())
            };
        }
        cause = current.source();
    }
    let root = error.find_root();
    let kind = match root {
        DataFusionError::Internal(_) => QueryFailureKind::INTERNAL,
        _ => QueryFailureKind::EXECUTION,
    };
    let message = match root {
        DataFusionError::External(error) => error.to_string(),
        DataFusionError::Internal(message) => message.clone(),
        root => root.message().into_owned(),
    };
    QueryTaskError {
        origin: Some(origin),
        ..QueryTaskError::new(kind, message)
    }
}

/// A request to a task that is closing or closed.
fn closed_error() -> DataFusionError {
    DataFusionError::External(Box::new(QueryTaskError::new(
        QueryFailureKind::CANCELLED,
        "query task closed",
    )))
}

/// A request that doesn't fit the task's state, as a `PROTOCOL` task failure.
fn protocol_error(message: impl Into<restate_util_string::ReString>) -> DataFusionError {
    DataFusionError::External(Box::new(QueryTaskError::new(
        QueryFailureKind::PROTOCOL,
        message,
    )))
}

/// How long a task may sit idle before the server cancels and removes it.
const IDLE_TIMEOUT: Duration = Duration::from_secs(300);

/// When a task was last used, and how many requests it serves now. A task
/// serving a request is never idle, however long the request takes, for example
/// a sort that reads its whole input before its first batch.
struct Activity(Mutex<ActivityState>, tokio::sync::Notify);

struct ActivityState {
    /// Requests dispatched and not yet answered.
    active: usize,
    /// When the last request was dispatched or answered.
    last_used: Instant,
}

impl Activity {
    fn new() -> Self {
        Self(
            Mutex::new(ActivityState {
                active: 0,
                last_used: Instant::now(),
            }),
            tokio::sync::Notify::new(),
        )
    }

    /// Marks a request in flight until the returned guard drops.
    fn begin(self: &Arc<Self>) -> InFlight {
        let mut state = self.0.lock();
        state.active += 1;
        state.last_used = Instant::now();
        InFlight(Arc::clone(self))
    }

    /// How long the task has been idle: `None` while it serves a request.
    fn idle_for(&self) -> Option<Duration> {
        let state = self.0.lock();
        (state.active == 0).then(|| state.last_used.elapsed())
    }

    /// Waits until the task serves no request, or until `deadline`. Whether it
    /// became idle.
    async fn idle(&self, deadline: Instant) -> bool {
        loop {
            let notified = self.1.notified();
            if self.0.lock().active == 0 {
                return true;
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return self.0.lock().active == 0;
            }
        }
    }
}

/// A request in flight; see [`Activity::begin`].
struct InFlight(Arc<Activity>);

impl Drop for InFlight {
    fn drop(&mut self) {
        let mut state = self.0.0.lock();
        state.active -= 1;
        state.last_used = Instant::now();
        if state.active == 0 {
            self.0.1.notify_waiters();
        }
    }
}

/// How long a closing task waits for its in-flight requests and executed
/// copies to settle before its report is frozen.
const SETTLE_DEADLINE: Duration = Duration::from_secs(1);

struct InstalledTask {
    worker: Worker,
    key: TaskKey,
    context: Arc<TaskContext>,
    plan: Arc<dyn ExecutionPlan>,
    /// Of the plan's bytes as installed, for the report.
    plan_digest: u64,
    /// The plan's shape as installed, for the report: only when asked for one.
    shape: Option<Shape>,
    started: Mutex<HashSet<usize>>,
    /// The running output partitions; `None` once the task closes, so that no
    /// Execute still in flight can publish more.
    lanes: Mutex<Option<HashMap<usize, Lane>>>,
    control: Shared<BoxFuture<'static, Result<(), Arc<DataFusionError>>>>,
    /// Interrupts the task's work.
    cancel: CancellationToken,
    /// Ends the runtime's control input, which releases the task there.
    control_end: CancellationToken,
    /// Present when the task was asked for a report.
    collector: Option<Arc<AnalyzeCollector>>,
    /// Output partitions that ended, and whether any failed.
    ended: std::sync::atomic::AtomicUsize,
    failed: std::sync::atomic::AtomicBool,
    activity: Arc<Activity>,
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
            return Err(protocol_error("invalid owner-bound task identity"));
        }
        let mut headers = HeaderMap::new();
        for (name, value) in &request.runtime_headers {
            headers.insert(
                HeaderName::from_bytes(name.as_bytes())
                    .map_err(|err| protocol_error(format!("invalid runtime header: {err}")))?,
                HeaderValue::from_str(value)
                    .map_err(|err| protocol_error(format!("invalid runtime header: {err}")))?,
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
            return Err(protocol_error("unsupported task control capability"));
        }
        let collector = request
            .analyze
            .is_some()
            .then(|| Arc::new(AnalyzeCollector::default()));
        let key = TaskKey {
            query_id: Uuid::from_bytes(request.id.runtime_query_id),
            stage_id: request.id.stage as usize,
            task_number: request.id.task as usize,
        };
        let target_worker_url = worker_url(scanners.node_id())?;
        let plan_digest = digest(&request.plan);
        let plan = Arc::new(OnceLock::new());
        let env = env.clone();
        let scanners = scanners.clone();
        let captured_plan = Arc::clone(&plan);
        let captured_collector = collector.clone();
        let mut query_headers = HeaderMap::new();
        for header in &request.query_headers {
            query_headers.append(
                HeaderName::from_bytes(header.name.as_bytes())
                    .map_err(|err| protocol_error(format!("invalid query header: {err}")))?,
                HeaderValue::from_bytes(&header.value)
                    .map_err(|err| protocol_error(format!("invalid query header: {err}")))?,
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
            let collector = captured_collector.clone();
            async move {
                let builder = ctx
                    .builder
                    .with_session_id(id.session_id.to_string())
                    .with_distributed_user_codec(SourceCodec {
                        manager: Some(scanners),
                    })
                    .with_distributed_worker_plan_rewrite_handler(
                        move |ev: WorkerPlanRewriteEvent<'_>| {
                            plan.set(Arc::clone(&ev.plan))
                                .map_err(|_| protocol_error("task plan was installed twice"))?;
                            Ok(WorkerPlanRewriteEventResponse::new(ev.plan))
                        },
                    );
                let mut state = env.build_worker_state(builder)?;
                for (name, value) in options {
                    if !name.starts_with("datafusion.") {
                        return Err(protocol_error(format!("invalid task option {name}")));
                    }
                    state.config_mut().options_mut().set(&name, &value)?;
                }
                state.config_mut().set_extension(Arc::new(id.query_ts));
                state.config_mut().set_extension(metadata);
                if let Some(collector) = collector {
                    state.config_mut().set_extension(collector);
                }
                Ok(state)
            }
        });
        let cancel = CancellationToken::new();
        let control_end = CancellationToken::new();
        let guard = control_end.clone().drop_guard();
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
                    .take_until(control_end.clone().cancelled_owned())
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
                    return Err(protocol_error("unsupported worker control message"));
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
            shape: collector.as_ref().and_then(|_| shape(&plan)),
            plan,
            plan_digest,
            started: Default::default(),
            lanes: Mutex::new(Some(HashMap::new())),
            control,
            cancel,
            control_end,
            collector,
            ended: Default::default(),
            failed: Default::default(),
            activity: Arc::new(Activity::new()),
            #[cfg(test)]
            output: Default::default(),
        })
    }

    async fn execute(&self, request: QueryTaskExecute) -> Result<QueryTaskReplyBody> {
        let start = request.partition_start as usize;
        let end = request.partition_end as usize;
        if start >= end || end > self.plan.output_partitioning().partition_count() {
            return Err(protocol_error("invalid task partition range"));
        }
        {
            let mut started = self.started.lock();
            if (start..end).any(|p| started.contains(&p)) {
                return Err(protocol_error("task output partition already started"));
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
        let mut lanes = self.lanes.lock();
        let Some(lanes) = lanes.as_mut() else {
            return Err(closed_error());
        };
        lanes.extend((start..end).zip(streams).map(|(partition, stream)| {
            (partition, Arc::new(tokio::sync::Mutex::new(Some(stream))))
        }));
        Ok(QueryTaskReplyBody::Executing(()))
    }

    async fn next(&self, partition: usize) -> Result<QueryTaskReplyBody> {
        let lane = self
            .lanes
            .lock()
            .as_ref()
            .and_then(|lanes| lanes.get(&partition))
            .cloned()
            .ok_or_else(|| protocol_error("task output partition is not running"))?;
        self.while_running(async {
            let mut lane = lane
                .try_lock()
                .map_err(|_| protocol_error("concurrent pull for the same task partition"))?;
            let Some(stream) = lane.as_mut() else {
                return Err(protocol_error("task output partition already ended"));
            };
            match stream.next().await {
                Some(Ok(batch)) => {
                    let rows = batch.num_rows();
                    let encoded = encode_record_batch(&self.plan.schema(), batch)?;
                    if let Some(collector) = &self.collector {
                        collector.sent(rows, encoded.len());
                    }
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
                    self.failed
                        .store(true, std::sync::atomic::Ordering::Relaxed);
                    Err(error)
                }
                None => {
                    *lane = None;
                    self.ended
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    Ok(QueryTaskReplyBody::End(()))
                }
            }
        })
        .await
    }

    /// Stops the task's work and releases it, without a report.
    fn abort(&self) {
        self.cancel.cancel();
        self.control_end.cancel();
    }

    /// Closes the task, with its report if it was asked for one: stops its
    /// work, waits up to [`SETTLE_DEADLINE`] for in-flight requests and
    /// executed copies to settle, drops its output streams, freezes what was
    /// collected, then releases the task. A copy still unsettled at the
    /// deadline keeps running down on its own, and the report says so.
    async fn close(&self) -> QueryTaskClosed {
        let Some(collector) = &self.collector else {
            self.abort();
            return QueryTaskClosed::default();
        };
        self.cancel.cancel();
        let deadline = Instant::now() + SETTLE_DEADLINE;
        let idle = self.activity.idle(deadline).await;
        self.lanes.lock().take();
        let settled = collector.settle(deadline).await;
        let mut collected = collector.freeze();
        collected.unsettled = unsettled_lanes(collected.unsettled, idle);
        let started = self.started.lock().len();
        let outcome = if self.failed.load(std::sync::atomic::Ordering::Relaxed) {
            TaskOutcome::FAILED
        } else if started > 0
            && self.ended.load(std::sync::atomic::Ordering::Relaxed) == started
            && settled
        {
            TaskOutcome::COMPLETED
        } else {
            TaskOutcome::INCOMPLETE
        };
        let mut report = build_report(self.shape.as_ref(), collected, outcome);
        report.plan_digest = self.plan_digest;
        self.control_end.cancel();
        QueryTaskClosed {
            report: Some(bilrost::Message::encode_to_vec(&report).into()),
        }
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
            .ok_or_else(closed_error)?
    }
}

impl Drop for InstalledTask {
    fn drop(&mut self) {
        self.abort();
    }
}

/// How many of a closing task's lanes may still change its numbers: the
/// collector's `unsettled` copies (still running, cut short or not merged), and
/// at least one while a request is still running at the deadline. Such a
/// request usually polls one of those copies, which already counts it once.
fn unsettled_lanes(unsettled: usize, idle: bool) -> usize {
    if idle { unsettled } else { unsettled.max(1) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_failures_carry_their_origin_kind_and_own_message() {
        let origin = GenerationalNodeId::new(2, 1);
        let not_ready = "'service_stats/1' is not ready on partition 12: no maintained index";
        let storage =
            DataFusionError::External(Box::new(StorageError::Generic(anyhow::anyhow!(not_ready))));
        let failure = task_error(&storage, origin);
        assert_eq!(
            (failure.origin, failure.kind),
            (Some(origin), QueryFailureKind::STORAGE)
        );
        // The coordinator reports it once, naming the node and the kind.
        assert_eq!(
            DataFusionError::External(Box::new(failure)).to_string(),
            format!("External error: N2:1: storage: generic storage error: {not_ready}")
        );
        // A DataFusion internal error is a bug, without DataFusion's prefixes.
        let bug = task_error(
            &DataFusionError::Internal("unexpected plan".to_owned()),
            origin,
        );
        assert_eq!(bug.to_string(), "N2:1: internal: unexpected plan");
        let resources = task_error(
            &DataFusionError::ResourcesExhausted("memory limit".to_owned()),
            origin,
        );
        assert_eq!(resources.kind, QueryFailureKind::EXECUTION);
        // Another node's failure is forwarded unchanged.
        let remote = QueryTaskError {
            origin: Some(GenerationalNodeId::new(3, 1)),
            ..QueryTaskError::new(QueryFailureKind::PROTOCOL, "query task is not installed")
        };
        let forwarded = DataFusionError::External(Box::new(remote.clone()));
        assert_eq!(task_error(&forwarded, origin), remote);
        let unknown = protocol_error("unknown query task request");
        assert_eq!(
            task_error(&unknown, origin).to_string(),
            "N2:1: protocol: unknown query task request"
        );
    }

    /// A request still running at the close deadline makes the report
    /// partial, but a lane whose copy already counts isn't counted twice.
    #[test]
    fn a_stalled_lane_counts_once() {
        assert_eq!(super::unsettled_lanes(0, true), 0);
        assert_eq!(super::unsettled_lanes(2, true), 2);
        assert_eq!(
            super::unsettled_lanes(0, false),
            1,
            "a request without a pending copy"
        );
        assert_eq!(
            super::unsettled_lanes(1, false),
            1,
            "the request polls the pending copy"
        );
        assert_eq!(super::unsettled_lanes(3, false), 3);
    }

    #[restate_core::test(start_paused = true)]
    async fn a_task_serving_a_request_never_idles() {
        let activity = Arc::new(Activity::new());
        // Dispatch marks the request before its future first runs, and the mark
        // lives as long as the request.
        let in_flight = activity.begin();
        let mut requests = JoinSet::new();
        requests.spawn(async move {
            let _in_flight = in_flight;
            std::future::pending::<()>().await
        });
        tokio::time::advance(IDLE_TIMEOUT * 2).await;
        assert_eq!(activity.idle_for(), None, "a long request keeps the task");
        requests.shutdown().await;
        assert_eq!(
            activity.idle_for(),
            Some(Duration::ZERO),
            "idle from its end"
        );
        tokio::time::advance(IDLE_TIMEOUT + Duration::from_secs(1)).await;
        assert!(activity.idle_for().unwrap() > IDLE_TIMEOUT, "then reaped");
    }
}
