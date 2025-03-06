// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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

use crate::error::InvokerError;
use crate::invocation_task::service_protocol_runner::ServiceProtocolRunner;
use crate::metric_definitions::INVOKER_TASK_DURATION;
use bytes::Bytes;
use futures::{FutureExt, future, stream};
use http::response::Parts as ResponseParts;
use http::{HeaderName, HeaderValue, Response};
use http_body::{Body, Frame};
use metrics::histogram;
use restate_invoker_api::{
    EagerState, EntryEnricher, InvokeInputJournal, JournalReader, StateReader,
};
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
use restate_types::schema::service::ServiceMetadataResolver;
use restate_types::service_protocol::ServiceProtocolVersion;
use std::collections::HashSet;
use std::convert::Infallible;
use std::future::Future;
use std::iter;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{instrument, warn};

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
const X_RESTATE_SERVER: HeaderName = HeaderName::from_static("x-restate-server");

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
        entry: EnrichedRawEntry,
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
pub(super) struct InvocationTask<SR, JR, EE, DMR> {
    // Shared client
    client: ServiceClient,

    // Connection params
    partition: PartitionLeaderEpoch,
    invocation_id: InvocationId,
    invocation_target: InvocationTarget,
    inactivity_timeout: Duration,
    abort_timeout: Duration,
    disable_eager_state: bool,
    message_size_warning: usize,
    message_size_limit: Option<usize>,
    retry_count_since_last_stored_entry: u32,

    // Invoker tx/rx
    state_reader: SR,
    journal_reader: JR,
    entry_enricher: EE,
    schemas: Live<DMR>,
    invoker_tx: mpsc::UnboundedSender<InvocationTaskOutput>,
    invoker_rx: mpsc::UnboundedReceiver<Notification>,
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

impl<SR, JR, EE, Schemas> InvocationTask<SR, JR, EE, Schemas>
where
    SR: StateReader + StateReader + Clone + Send + Sync + 'static,
    JR: JournalReader + Clone + Send + Sync + 'static,
    <JR as JournalReader>::JournalStream: Unpin + Send + 'static,
    <SR as StateReader>::StateIter: Send,
    EE: EntryEnricher,
    Schemas: DeploymentResolver + ServiceMetadataResolver + InvocationTargetResolver,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        client: ServiceClient,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        invocation_target: InvocationTarget,
        default_inactivity_timeout: Duration,
        default_abort_timeout: Duration,
        disable_eager_state: bool,
        message_size_warning: usize,
        message_size_limit: Option<usize>,
        retry_count_since_last_stored_entry: u32,
        state_reader: SR,
        journal_reader: JR,
        entry_enricher: EE,
        deployment_metadata_resolver: Live<Schemas>,
        invoker_tx: mpsc::UnboundedSender<InvocationTaskOutput>,
        invoker_rx: mpsc::UnboundedReceiver<Notification>,
    ) -> Self {
        Self {
            client,
            partition,
            invocation_id,
            invocation_target,
            inactivity_timeout: default_inactivity_timeout,
            abort_timeout: default_abort_timeout,
            disable_eager_state,
            state_reader,
            journal_reader,
            entry_enricher,
            schemas: deployment_metadata_resolver,
            invoker_tx,
            invoker_rx,
            message_size_limit,
            message_size_warning,
            retry_count_since_last_stored_entry,
        }
    }

    /// Loop opening the request to deployment and consuming the stream
    #[instrument(
        level = "debug",
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
    pub async fn run(mut self, input_journal: InvokeInputJournal) {
        let start = Instant::now();
        // Execute the task
        let terminal_state = self.select_protocol_version_and_run(input_journal).await;

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
        histogram!(INVOKER_TASK_DURATION).record(start.elapsed());
    }

    async fn select_protocol_version_and_run(
        &mut self,
        input_journal: InvokeInputJournal,
    ) -> TerminalLoopState<()> {
        // Resolve journal and its metadata
        let read_journal_future = async {
            Ok(match input_journal {
                InvokeInputJournal::NoCachedJournal => {
                    let (journal_meta, journal_stream) = self
                        .journal_reader
                        .read_journal(&self.invocation_id)
                        .await
                        .map_err(|e| InvokerError::JournalReader(e.into()))?;
                    (journal_meta, future::Either::Left(journal_stream))
                }
                InvokeInputJournal::CachedJournal(journal_meta, journal_items) => (
                    journal_meta,
                    future::Either::Right(stream::iter(journal_items)),
                ),
            })
        };
        // Read eager state
        let read_state_future = async {
            let keyed_service_id = self.invocation_target.as_keyed_service_id();
            if self.disable_eager_state || keyed_service_id.is_none() {
                Ok(EagerState::<iter::Empty<_>>::default().map(itertools::Either::Right))
            } else {
                self.state_reader
                    .read_state(&keyed_service_id.unwrap())
                    .await
                    .map_err(|e| InvokerError::StateReader(e.into()))
                    .map(|r| r.map(itertools::Either::Left))
            }
        };

        // We execute those concurrently
        let ((journal_metadata, journal_stream), state_iter) =
            shortcircuit!(tokio::try_join!(read_journal_future, read_state_future));

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
                if !pinned_deployment.service_protocol_version.is_supported() {
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
                    ServiceProtocolVersion::choose_max_supported_version(
                        &deployment.metadata.supported_protocol_versions,
                    )
                    .ok_or_else(|| {
                        InvokerError::IncompatibleServiceEndpoint(
                            deployment.id,
                            deployment.metadata.supported_protocol_versions.clone(),
                        )
                    })
                );

                (
                    deployment,
                    chosen_service_protocol_version,
                    /* has_changed= */ true,
                )
            };
        if let Some(service_metadata) =
            schemas.resolve_latest_service(self.invocation_target.service_name())
        {
            // Override the inactivity timeout and abort timeout, if available
            if let Some(inactivity_timeout) = service_metadata.inactivity_timeout {
                self.inactivity_timeout = inactivity_timeout.into();
            }
            if let Some(abort_timeout) = service_metadata.abort_timeout {
                self.abort_timeout = abort_timeout.into();
            }
        } else {
            warn!("Unexpected service not found, after resolving correctly the deployment.");
        }

        self.send_invoker_tx(InvocationTaskOutputInner::PinnedDeployment(
            PinnedDeployment::new(deployment.id, chosen_service_protocol_version),
            deployment_changed,
        ));

        if chosen_service_protocol_version <= ServiceProtocolVersion::V3 {
            // Protocol runner for service protocol <= v3
            let service_protocol_runner =
                ServiceProtocolRunner::new(self, chosen_service_protocol_version);
            service_protocol_runner
                .run(journal_metadata, deployment, journal_stream, state_iter)
                .await
        } else {
            // Protocol runner for service protocol v4+
            let service_protocol_runner = service_protocol_runner_v4::ServiceProtocolRunner::new(
                self,
                chosen_service_protocol_version,
            );
            service_protocol_runner
                .run(journal_metadata, deployment, journal_stream, state_iter)
                .await
        }
    }
}

impl<SR, JR, EE, DMR> InvocationTask<SR, JR, EE, DMR> {
    fn send_invoker_tx(&self, invocation_task_output_inner: InvocationTaskOutputInner) {
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
    End,
}

enum ResponseStreamState {
    WaitingHeaders(AbortOnDrop<Result<Response<ResponseBody>, ServiceClientError>>),
    ReadingBody(Option<ResponseParts>, ResponseBody),
}

impl ResponseStreamState {
    fn initialize(client: &ServiceClient, req: Request<InvokerBodyStream>) -> Self {
        // Because the body sender blocks on waiting for the request body buffer to be available,
        // we need to spawn the request initiation separately, otherwise the loop below
        // will deadlock on the journal entry write.
        // This task::spawn won't be required by hyper 1.0, as the connection will be driven by a task
        // spawned somewhere else (perhaps in the connection pool).
        // See: https://github.com/restatedev/restate/issues/96 and https://github.com/restatedev/restate/issues/76
        Self::WaitingHeaders(AbortOnDrop(tokio::task::spawn(client.call(req))))
    }

    // Could be replaced by a Future implementation
    fn poll_only_headers(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<ResponseParts, InvokerError>> {
        match self {
            ResponseStreamState::WaitingHeaders(join_handle) => {
                let http_response = match ready!(join_handle.poll_unpin(cx)) {
                    Ok(Ok(res)) => res,
                    Ok(Err(hyper_err)) => return Poll::Ready(Err(InvokerError::Client(hyper_err))),
                    Err(join_err) => {
                        return Poll::Ready(Err(InvokerError::UnexpectedJoinError(join_err)));
                    }
                };

                // Convert to response parts
                let (http_response_header, body) = http_response.into_parts();

                // Transition to reading body
                *self = ResponseStreamState::ReadingBody(None, body);

                Poll::Ready(Ok(http_response_header))
            }
            ResponseStreamState::ReadingBody { .. } => {
                panic!("Unexpected poll after the headers have been resolved already")
            }
        }
    }

    // Could be replaced by a Stream implementation
    fn poll_next_chunk(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<ResponseChunk, InvokerError>> {
        // Could be replaced by a Stream implementation
        loop {
            match self {
                ResponseStreamState::WaitingHeaders(join_handle) => {
                    let http_response = match ready!(join_handle.poll_unpin(cx)) {
                        Ok(Ok(res)) => res,
                        Ok(Err(hyper_err)) => {
                            return Poll::Ready(Err(InvokerError::Client(hyper_err)));
                        }
                        Err(join_err) => {
                            return Poll::Ready(Err(InvokerError::UnexpectedJoinError(join_err)));
                        }
                    };

                    // Convert to response parts
                    let (http_response_header, body) = http_response.into_parts();

                    // Transition to reading body
                    *self = ResponseStreamState::ReadingBody(Some(http_response_header), body);
                }
                ResponseStreamState::ReadingBody(headers, b) => {
                    // If headers are present, take them
                    if let Some(parts) = headers.take() {
                        return Poll::Ready(Ok(ResponseChunk::Parts(parts)));
                    };

                    let mut pinned_body = std::pin::pin!(b);
                    let next_element = ready!(pinned_body.as_mut().poll_frame(cx));
                    return Poll::Ready(match next_element.transpose() {
                        Ok(Some(frame)) if frame.is_data() => {
                            Ok(ResponseChunk::Data(frame.into_data().unwrap()))
                        }
                        Ok(_) => Ok(ResponseChunk::End),
                        Err(err) => Err(InvokerError::ClientBody(err)),
                    });
                }
            }
        }
    }
}

/// This wrapper makes sure we abort the task when the JoinHandle is dropped,
/// but it doesn't wait for the task to complete, because we simply don't have async drops!
/// For more: https://github.com/tokio-rs/tokio/issues/2596
/// Inspired by: https://github.com/cyb0124/abort-on-drop
#[derive(Debug)]
struct AbortOnDrop<T>(JoinHandle<T>);

impl<T> Future for AbortOnDrop<T> {
    type Output = <JoinHandle<T> as Future>::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx)
    }
}

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort()
    }
}
