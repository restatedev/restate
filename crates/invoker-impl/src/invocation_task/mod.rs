// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod service_protocol_runner;
mod service_protocol_runner_v4;

use super::Notification;

use std::collections::HashSet;
use std::convert::Infallible;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::{FutureExt, Stream, StreamExt};
use http::response::Parts as ResponseParts;
use http::{HeaderName, HeaderValue, Response};
use http_body::{Body, Frame};
use metrics::histogram;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::task::AbortOnDropHandle;
use tracing::{debug, instrument};

use restate_invoker_api::invocation_reader::{
    EagerState, InvocationReader, InvocationReaderTransaction,
};
use restate_invoker_api::{EntryEnricher, InvokeInputJournal};
use restate_service_client::{Request, ResponseBody, ServiceClient, ServiceClientError};
use restate_types::deployment::PinnedDeployment;
use restate_types::identifiers::{InvocationId, PartitionLeaderEpoch};
use restate_types::invocation::InvocationTarget;
use restate_types::journal::EntryIndex;
use restate_types::journal::enriched::EnrichedRawEntry;
use restate_types::journal_v2;
use restate_types::journal_v2::raw::RawNotification;
use restate_types::journal_v2::{CommandIndex, NotificationId};
use restate_types::live::Live;
use restate_types::schema::deployment::DeploymentResolver;
use restate_types::schema::invocation_target::InvocationTargetResolver;
use restate_types::service_protocol::ServiceProtocolVersion;

use crate::TokenBucket;
use crate::error::InvokerError;
use crate::invocation_task::service_protocol_runner::ServiceProtocolRunner;
use crate::metric_definitions::{ID_LOOKUP, INVOKER_TASK_DURATION};

// Clippy false positive, might be caused by Bytes contained within HeaderValue.
// https://github.com/rust-lang/rust/issues/40543#issuecomment-1212981256
#[allow(clippy::declare_interior_mutable_const)]
const SERVICE_PROTOCOL_VERSION_V1: HeaderValue =
    HeaderValue::from_static("application/vnd.restate.invocation.v1");

#[allow(clippy::declare_interior_mutable_const)]
const SERVICE_PROTOCOL_VERSION_V2: HeaderValue =
    HeaderValue::from_static("application/vnd.restate.invocation.v2");

#[allow(clippy::declare_interior_mutable_const)]
const SERVICE_PROTOCOL_VERSION_V3: HeaderValue =
    HeaderValue::from_static("application/vnd.restate.invocation.v3");

#[allow(clippy::declare_interior_mutable_const)]
const SERVICE_PROTOCOL_VERSION_V4: HeaderValue =
    HeaderValue::from_static("application/vnd.restate.invocation.v4");

#[allow(clippy::declare_interior_mutable_const)]
const SERVICE_PROTOCOL_VERSION_V5: HeaderValue =
    HeaderValue::from_static("application/vnd.restate.invocation.v5");

#[allow(clippy::declare_interior_mutable_const)]
const SERVICE_PROTOCOL_VERSION_V6: HeaderValue =
    HeaderValue::from_static("application/vnd.restate.invocation.v6");

#[allow(clippy::declare_interior_mutable_const)]
const X_RESTATE_SERVER: HeaderName = HeaderName::from_static("x-restate-server");

/// Collects state entries from an [`EagerState`] stream, respecting an optional size limit.
///
/// Returns a tuple of `(is_partial, entries)` where:
/// - `is_partial` is true if the state was already partial or if collection stopped due to size limit
/// - `entries` contains the collected and mapped key-value bytes
///
/// TODO: It would be better to estimate state size before reading, so we can
/// send no state entries directly instead of reading until we exceed the limit.
async fn collect_eager_state<S, E, T>(
    state: Option<EagerState<S>>,
    size_limit: Option<usize>,
    mut mapper: impl FnMut((Bytes, Bytes)) -> T,
) -> Result<(bool, Vec<T>), InvokerError>
where
    S: Stream<Item = Result<(Bytes, Bytes), E>> + Send,
    E: std::error::Error + Send + Sync + 'static,
{
    let Some(state) = state else {
        return Ok((true, Vec::new()));
    };

    let mut is_partial = state.is_partial();
    let mut entries = Vec::new();
    let mut total_size: usize = 0;

    let mut stream = std::pin::pin!(state.into_inner());
    while let Some(result) = stream.next().await {
        let (key, value) = result.map_err(|e| InvokerError::StateReader(e.into()))?;
        let entry_size = key.len() + value.len();

        // Check if adding this entry would exceed the limit
        if let Some(limit) = size_limit
            && total_size.saturating_add(entry_size) > limit
        {
            // We've hit the limit - mark as partial and stop
            debug!(
                "Eager state size limit reached ({} bytes, limit: {} bytes), \
                 sending partial state with {} entries",
                total_size,
                limit,
                entries.len()
            );
            is_partial = true;
            break;
        }

        total_size = total_size.saturating_add(entry_size);
        entries.push(mapper((key, value)));
    }

    Ok((is_partial, entries))
}

pub(super) struct InvocationTaskOutput {
    pub(super) partition: PartitionLeaderEpoch,
    pub(super) invocation_id: InvocationId,
    pub(super) inner: InvocationTaskOutputInner,
}

pub(super) enum InvocationTaskOutputInner {
    // `has_changed` indicates if we believe this is a freshly selected endpoint or not.
    PinnedDeployment(PinnedDeployment, /* has_changed: */ bool),
    ServerHeaderReceived(String),
    NewEntry {
        entry_index: EntryIndex,
        entry: Box<EnrichedRawEntry>,
        /// If true, the SDK requested to be notified when the entry is correctly stored.
        ///
        /// When reading the entry from the storage this flag will always be false, as we never need to send acks for entries sent during a journal replay.
        ///
        /// See https://github.com/restatedev/service-protocol/blob/main/service-invocation-protocol.md#acknowledgment-of-stored-entries
        requires_ack: bool,
    },
    NewCommand {
        command_index: CommandIndex,
        command: journal_v2::raw::RawCommand,
        /// If true, the SDK requested to be notified when the entry is correctly stored.
        ///
        /// When reading the entry from the storage this flag will always be false, as we never need to send acks for entries sent during a journal replay.
        ///
        /// See https://github.com/restatedev/service-protocol/blob/main/service-invocation-protocol.md#acknowledgment-of-stored-entries
        requires_ack: bool,
    },
    NewNotificationProposal {
        notification: RawNotification,
    },
    Closed,
    Suspended(HashSet<EntryIndex>),
    SuspendedV2(HashSet<NotificationId>),
    Failed(InvokerError),
}

impl From<InvokerError> for InvocationTaskOutputInner {
    fn from(value: InvokerError) -> Self {
        InvocationTaskOutputInner::Failed(value)
    }
}

type InvokerBodyStream =
    http_body_util::StreamBody<ReceiverStream<Result<Frame<Bytes>, Infallible>>>;

type InvokerRequestStreamSender = mpsc::Sender<Result<Frame<Bytes>, Infallible>>;

/// Represents an open invocation stream
pub(super) struct InvocationTask<EE, DMR> {
    // Shared client
    client: ServiceClient,

    // Connection params
    partition: PartitionLeaderEpoch,
    invocation_id: InvocationId,
    invocation_target: InvocationTarget,
    inactivity_timeout: Duration,
    abort_timeout: Duration,
    eager_state_size_limit: Option<usize>,
    message_size_warning: NonZeroUsize,
    message_size_limit: NonZeroUsize,
    retry_count_since_last_stored_entry: u32,

    // Invoker tx/rx
    entry_enricher: EE,
    schemas: Live<DMR>,
    invoker_tx: mpsc::UnboundedSender<InvocationTaskOutput>,
    invoker_rx: mpsc::UnboundedReceiver<Notification>,

    // throttling
    action_token_bucket: Option<TokenBucket>,
}

/// This is needed to split the run_internal in multiple loop functions and have shortcircuiting.
enum TerminalLoopState<T> {
    Continue(T),
    Closed,
    Suspended(HashSet<EntryIndex>),
    SuspendedV2(HashSet<NotificationId>),
    Failed(InvokerError),
}

impl<T, E: Into<InvokerError>> From<Result<T, E>> for TerminalLoopState<T> {
    fn from(value: Result<T, E>) -> Self {
        match value {
            Ok(v) => TerminalLoopState::Continue(v),
            Err(e) => TerminalLoopState::Failed(e.into()),
        }
    }
}

/// Could be replaced by ? operator if we had Try stable. See [`InvocationTask::select_protocol_version_and_run`]
#[macro_export]
macro_rules! shortcircuit {
    ($value:expr) => {
        match TerminalLoopState::from($value) {
            TerminalLoopState::Continue(v) => v,
            TerminalLoopState::Closed => return TerminalLoopState::Closed,
            TerminalLoopState::Suspended(v) => return TerminalLoopState::Suspended(v),
            TerminalLoopState::SuspendedV2(v) => return TerminalLoopState::SuspendedV2(v),
            TerminalLoopState::Failed(e) => return TerminalLoopState::Failed(e),
        }
    };
}

impl<EE, Schemas> InvocationTask<EE, Schemas>
where
    EE: EntryEnricher,
    Schemas: DeploymentResolver + InvocationTargetResolver,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        client: ServiceClient,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        invocation_target: InvocationTarget,
        default_inactivity_timeout: Duration,
        default_abort_timeout: Duration,
        eager_state_size_limit: Option<usize>,
        message_size_warning: NonZeroUsize,
        message_size_limit: NonZeroUsize,
        retry_count_since_last_stored_entry: u32,
        entry_enricher: EE,
        deployment_metadata_resolver: Live<Schemas>,
        invoker_tx: mpsc::UnboundedSender<InvocationTaskOutput>,
        invoker_rx: mpsc::UnboundedReceiver<Notification>,
        action_token_bucket: Option<TokenBucket>,
    ) -> Self {
        Self {
            client,
            partition,
            invocation_id,
            invocation_target,
            inactivity_timeout: default_inactivity_timeout,
            abort_timeout: default_abort_timeout,
            eager_state_size_limit,
            entry_enricher,
            schemas: deployment_metadata_resolver,
            invoker_tx,
            invoker_rx,
            message_size_limit,
            message_size_warning,
            retry_count_since_last_stored_entry,
            action_token_bucket,
        }
    }

    /// Loop opening the request to deployment and consuming the stream
    #[instrument(
        level = "info",
        name = "invoker_invocation_task",
        fields(
            rpc.system = "restate",
            rpc.service = %self.invocation_target.service_name(),
            restate.invocation.id = %self.invocation_id,
            restate.invocation.target = %self.invocation_target,
            otel.name = "invocation-task: run",
        ),
        skip_all,
    )]
    pub async fn run<IR>(mut self, input_journal: InvokeInputJournal, mut invocation_reader: IR)
    where
        IR: InvocationReader,
    {
        let start = Instant::now();
        // Execute the task
        let terminal_state = self
            .select_protocol_version_and_run(input_journal, &mut invocation_reader)
            .await;

        // Sanity check of the final state
        let inner = match terminal_state {
            TerminalLoopState::Continue(_) => {
                unreachable!("This is not supposed to happen. This is a runtime bug")
            }
            TerminalLoopState::Closed => InvocationTaskOutputInner::Closed,
            TerminalLoopState::Suspended(v) => InvocationTaskOutputInner::Suspended(v),
            TerminalLoopState::SuspendedV2(v) => InvocationTaskOutputInner::SuspendedV2(v),
            TerminalLoopState::Failed(e) => InvocationTaskOutputInner::Failed(e),
        };

        self.send_invoker_tx(inner);
        histogram!(INVOKER_TASK_DURATION, "partition_id" => ID_LOOKUP.get(self.partition.0))
            .record(start.elapsed());
    }

    async fn select_protocol_version_and_run<IR>(
        &mut self,
        input_journal: InvokeInputJournal,
        invocation_reader: &mut IR,
    ) -> TerminalLoopState<()>
    where
        IR: InvocationReader,
    {
        let mut txn = invocation_reader.transaction();

        // Get journal metadata and cached items (if any)
        let (journal_metadata, cached_journal_items) = match input_journal {
            InvokeInputJournal::NoCachedJournal => {
                let metadata = shortcircuit!(
                    txn.read_journal_metadata(&self.invocation_id)
                        .await
                        .map_err(|e| InvokerError::JournalReader(e.into()))
                        .and_then(|opt| opt.ok_or_else(|| InvokerError::NotInvoked))
                );
                (metadata, None)
            }
            InvokeInputJournal::CachedJournal(metadata, items) => (metadata, Some(items)),
        };

        // Resolve the deployment metadata
        let schemas = self.schemas.live_load();
        let (deployment, chosen_service_protocol_version, deployment_changed) =
            if let Some(pinned_deployment) = &journal_metadata.pinned_deployment {
                // We have a pinned deployment that we can't change even if newer
                // deployments have been registered for the same service.
                let deployment_metadata = shortcircuit!(
                    schemas
                        .get_deployment(&pinned_deployment.deployment_id)
                        .ok_or_else(|| InvokerError::UnknownDeployment(
                            pinned_deployment.deployment_id
                        ))
                );

                // todo: We should support resuming an invocation with a newer protocol version if
                //  the endpoint supports it
                if !pinned_deployment
                    .service_protocol_version
                    .is_supported_for_inflight_invocation()
                {
                    shortcircuit!(Err(InvokerError::ResumeWithWrongServiceProtocolVersion(
                        pinned_deployment.service_protocol_version
                    )));
                }

                (
                    deployment_metadata,
                    pinned_deployment.service_protocol_version,
                    /* has_changed= */ false,
                )
            } else {
                // We can choose the freshest deployment for the latest revision
                // of the registered service.
                let deployment = shortcircuit!(
                    schemas
                        .resolve_latest_deployment_for_service(
                            self.invocation_target.service_name()
                        )
                        .ok_or(InvokerError::NoDeploymentForService)
                );

                let chosen_service_protocol_version = shortcircuit!(
                    ServiceProtocolVersion::pick(&deployment.supported_protocol_versions,)
                        .ok_or_else(|| {
                            InvokerError::IncompatibleServiceEndpoint(
                                deployment.id,
                                deployment.supported_protocol_versions.clone(),
                            )
                        })
                );

                (
                    deployment,
                    chosen_service_protocol_version,
                    /* has_changed= */ true,
                )
            };

        let invocation_attempt_options = schemas
            .resolve_invocation_attempt_options(
                &deployment.id,
                self.invocation_target.service_name(),
                self.invocation_target.handler_name(),
            )
            .unwrap_or_default();

        // Override the inactivity timeout and abort timeout, if available
        if let Some(inactivity_timeout) = invocation_attempt_options.inactivity_timeout {
            self.inactivity_timeout = inactivity_timeout;
        }
        if let Some(abort_timeout) = invocation_attempt_options.abort_timeout {
            self.abort_timeout = abort_timeout;
        }

        if chosen_service_protocol_version < ServiceProtocolVersion::V4
            && journal_metadata.using_journal_table_v2
        {
            // We don't support migrating from journal v2 to journal v1!
            shortcircuit!(Err(InvokerError::DeploymentDeprecated(
                self.invocation_target.service_name().to_string(),
                deployment.id
            )));
        }

        // Resolve the effective eager state size limit:
        // Per-handler/service override takes precedence over server-level config.
        if let Some(limit) = invocation_attempt_options.eager_state_size_limit {
            self.eager_state_size_limit = Some(limit);
        }

        // Determine if we need to read state (Some(0) means lazy state / no eager state)
        let keyed_service_id = if self.invocation_target.as_keyed_service_id().is_some()
            && self.eager_state_size_limit != Some(0)
        {
            self.invocation_target.as_keyed_service_id()
        } else {
            None
        };

        self.send_invoker_tx(InvocationTaskOutputInner::PinnedDeployment(
            PinnedDeployment::new(deployment.id, chosen_service_protocol_version),
            deployment_changed,
        ));

        if chosen_service_protocol_version <= ServiceProtocolVersion::V3 {
            // Protocol runner for service protocol <= v3
            let service_protocol_runner =
                ServiceProtocolRunner::new(self, chosen_service_protocol_version);
            service_protocol_runner
                .run(
                    txn,
                    journal_metadata,
                    keyed_service_id,
                    cached_journal_items,
                    deployment,
                )
                .await
        } else {
            // Protocol runner for service protocol v4+
            let service_protocol_runner = service_protocol_runner_v4::ServiceProtocolRunner::new(
                self,
                chosen_service_protocol_version,
            );
            service_protocol_runner
                .run(
                    txn,
                    journal_metadata,
                    keyed_service_id,
                    cached_journal_items,
                    deployment,
                )
                .await
        }
    }
}

impl<EE, Schemas> InvocationTask<EE, Schemas> {
    pub(crate) fn send_invoker_tx(&self, invocation_task_output_inner: InvocationTaskOutputInner) {
        let _ = self.invoker_tx.send(InvocationTaskOutput {
            partition: self.partition,
            invocation_id: self.invocation_id,
            inner: invocation_task_output_inner,
        });
    }
}

fn service_protocol_version_to_header_value(
    service_protocol_version: ServiceProtocolVersion,
) -> HeaderValue {
    match service_protocol_version {
        ServiceProtocolVersion::Unspecified => {
            unreachable!("unknown protocol version should never be chosen")
        }
        ServiceProtocolVersion::V1 => SERVICE_PROTOCOL_VERSION_V1,
        ServiceProtocolVersion::V2 => SERVICE_PROTOCOL_VERSION_V2,
        ServiceProtocolVersion::V3 => SERVICE_PROTOCOL_VERSION_V3,
        ServiceProtocolVersion::V4 => SERVICE_PROTOCOL_VERSION_V4,
        ServiceProtocolVersion::V5 => SERVICE_PROTOCOL_VERSION_V5,
        ServiceProtocolVersion::V6 => SERVICE_PROTOCOL_VERSION_V6,
    }
}

fn invocation_id_to_header_value(invocation_id: &InvocationId) -> HeaderValue {
    let value = invocation_id.to_string();

    HeaderValue::try_from(value)
        .unwrap_or_else(|_| unreachable!("invocation id should be always valid"))
}

enum ResponseChunk {
    Parts(ResponseParts),
    Data(Bytes),
}

pin_project_lite::pin_project! {
    #[project = ResponseStreamProj]
    enum ResponseStream {
        WaitingHeaders {
            join_handle: AbortOnDropHandle<Result<Response<ResponseBody>, ServiceClientError>>,
        },
        ReadingBody {
            #[pin]
            body: ResponseBody,
        },
        Terminated,
    }
}

impl ResponseStream {
    fn initialize(client: &ServiceClient, req: Request<InvokerBodyStream>) -> Self {
        // Because the body sender blocks on waiting for the request body buffer to be available,
        // we need to spawn the request initiation separately, otherwise the loop below
        // will deadlock on the journal entry write.
        // This task::spawn won't be required by hyper 1.0, as the connection will be driven by a task
        // spawned somewhere else (perhaps in the connection pool).
        // See: https://github.com/restatedev/restate/issues/96 and https://github.com/restatedev/restate/issues/76
        Self::WaitingHeaders {
            join_handle: AbortOnDropHandle::new(tokio::task::spawn(client.call(req))),
        }
    }
}

impl Stream for ResponseStream {
    type Item = Result<ResponseChunk, InvokerError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.as_mut().project();
        match this {
            ResponseStreamProj::WaitingHeaders { join_handle } => {
                let http_response = match ready!(join_handle.poll_unpin(cx)) {
                    Ok(Ok(res)) => res,
                    Ok(Err(hyper_err)) => {
                        *self = ResponseStream::Terminated;
                        return Poll::Ready(Some(Err(InvokerError::Client(Box::new(hyper_err)))));
                    }
                    Err(join_err) => {
                        *self = ResponseStream::Terminated;
                        return Poll::Ready(Some(Err(InvokerError::UnexpectedJoinError(join_err))));
                    }
                };

                // Convert to response parts
                let (http_response_header, body) = http_response.into_parts();

                // Transition to reading body
                *self = ResponseStream::ReadingBody { body };
                Poll::Ready(Some(Ok(ResponseChunk::Parts(http_response_header))))
            }
            ResponseStreamProj::ReadingBody { body } => {
                let next_element = ready!(body.poll_frame(cx));
                match next_element.transpose() {
                    Ok(Some(frame)) if frame.is_data() => {
                        Poll::Ready(Some(Ok(ResponseChunk::Data(frame.into_data().unwrap()))))
                    }
                    Ok(_) => {
                        *self = ResponseStream::Terminated;
                        Poll::Ready(None)
                    }
                    Err(err) => {
                        *self = ResponseStream::Terminated;
                        Poll::Ready(Some(Err(InvokerError::ClientBody(err))))
                    }
                }
            }
            ResponseStreamProj::Terminated => Poll::Ready(None),
        }
    }
}
