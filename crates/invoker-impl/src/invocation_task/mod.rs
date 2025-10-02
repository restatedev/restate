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

use std::collections::HashSet;
use std::convert::Infallible;
use std::iter::Empty;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::{FutureExt, Stream, future, stream};
use http::response::Parts as ResponseParts;
use http::{HeaderName, HeaderValue, Response};
use http_body::{Body, Frame};
use metrics::histogram;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::task::AbortOnDropHandle;
use tracing::instrument;

use restate_invoker_api::invocation_reader::{
    EagerState, InvocationReader, InvocationReaderTransaction,
};
use restate_invoker_api::{EntryEnricher, InvokeInputJournal};
use restate_service_client::{Request, ResponseBody, ServiceClient, ServiceClientError};
use restate_types::deployment::PinnedDeployment;
use restate_types::identifiers::{InvocationId, PartitionLeaderEpoch};
use restate_types::invocation::{InvocationEpoch, InvocationTarget};
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
use crate::metric_definitions::INVOKER_TASK_DURATION;

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

pub(super) struct InvocationTaskOutput {
    pub(super) partition: PartitionLeaderEpoch,
    pub(super) invocation_id: InvocationId,
    pub(super) invocation_epoch: InvocationEpoch,
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
pub(super) struct InvocationTask<IR, EE, DMR> {
    // Shared client
    client: ServiceClient,

    // Connection params
    partition: PartitionLeaderEpoch,
    invocation_id: InvocationId,
    invocation_epoch: InvocationEpoch,
    invocation_target: InvocationTarget,
    inactivity_timeout: Duration,
    abort_timeout: Duration,
    disable_eager_state: bool,
    message_size_warning: usize,
    message_size_limit: Option<usize>,
    retry_count_since_last_stored_entry: u32,

    // Invoker tx/rx
    invocation_reader: IR,
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

impl<IR, EE, Schemas> InvocationTask<IR, EE, Schemas>
where
    IR: InvocationReader + Clone + Send + Sync + 'static,
    EE: EntryEnricher,
    Schemas: DeploymentResolver + InvocationTargetResolver,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        client: ServiceClient,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        invocation_epoch: InvocationEpoch,
        invocation_target: InvocationTarget,
        default_inactivity_timeout: Duration,
        default_abort_timeout: Duration,
        disable_eager_state: bool,
        message_size_warning: usize,
        message_size_limit: Option<usize>,
        retry_count_since_last_stored_entry: u32,
        invocation_reader: IR,
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
            invocation_epoch,
            invocation_target,
            inactivity_timeout: default_inactivity_timeout,
            abort_timeout: default_abort_timeout,
            disable_eager_state,
            invocation_reader,
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
        let mut txn = self.invocation_reader.transaction();
        // Resolve journal and its metadata
        let (journal_metadata, journal_stream) = match input_journal {
            InvokeInputJournal::NoCachedJournal => {
                let (journal_meta, journal_stream) = shortcircuit!(
                    txn.read_journal(&self.invocation_id)
                        .await
                        .map_err(|e| InvokerError::JournalReader(e.into()))
                        .and_then(|opt| opt.ok_or_else(|| InvokerError::NotInvoked))
                );
                (journal_meta, future::Either::Left(journal_stream))
            }
            InvokeInputJournal::CachedJournal(journal_meta, journal_items) => (
                journal_meta,
                future::Either::Right(stream::iter(journal_items)),
            ),
        };

        if self.invocation_epoch != journal_metadata.invocation_epoch {
            shortcircuit!(Err(InvokerError::StaleJournalRead {
                actual: journal_metadata.invocation_epoch,
                expected: self.invocation_epoch
            }));
        }

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
                    ServiceProtocolVersion::pick(&deployment.metadata.supported_protocol_versions,)
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

        // Read eager state, if needed
        let state_iter = if let Some(keyed_service_id) =
            self.invocation_target.as_keyed_service_id()
            && invocation_attempt_options.enable_lazy_state != Some(true)
            && !self.disable_eager_state
        {
            // only for keyed service
            shortcircuit!(
                txn.read_state(&keyed_service_id)
                    .await
                    .map_err(|e| InvokerError::StateReader(e.into()))
                    .map(|r| r.map(itertools::Either::Left))
            )
        } else {
            EagerState::<Empty<_>>::default().map(itertools::Either::Right)
        };

        // No need to read from Rocksdb anymore
        drop(txn);

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

impl<IR, EE, DMR> InvocationTask<IR, EE, DMR> {
    fn send_invoker_tx(&self, invocation_task_output_inner: InvocationTaskOutputInner) {
        let _ = self.invoker_tx.send(InvocationTaskOutput {
            partition: self.partition,
            invocation_id: self.invocation_id,
            invocation_epoch: self.invocation_epoch,
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
