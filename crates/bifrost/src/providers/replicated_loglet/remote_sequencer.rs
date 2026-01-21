// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{
    ops::Deref,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore, mpsc};

use restate_core::{
    ShutdownError, TaskCenter, TaskKind,
    network::{
        ConnectError, Connection, ConnectionClosed, DiscoveryError, DrainReason, NetworkError,
        NetworkSender as _, Networking, ReplyRx, Swimlane, TransportConnect,
    },
};
use restate_types::{
    config::Configuration,
    errors::MaybeRetryableError,
    logs::{LogId, Record, TailOffsetWatch, metadata::SegmentIndex},
    net::replicated_loglet::{Append, Appended, CommonRequestHeader, SequencerStatus},
    replicated_loglet::ReplicatedLogletParams,
};
use tracing::{instrument, trace};

use crate::loglet::{AppendError, LogletCommit, LogletCommitResolver, OperationError};

pub struct RemoteSequencer<T> {
    log_id: LogId,
    segment_index: SegmentIndex,
    params: ReplicatedLogletParams,
    networking: Networking<T>,
    max_inflight_records_in_config: AtomicUsize,
    record_permits: Arc<Semaphore>,
    known_global_tail: TailOffsetWatch,
    connection: Arc<Mutex<Option<RemoteSequencerConnection>>>,
    maybe_sealed: AtomicBool,
}

impl<T> RemoteSequencer<T> {
    pub fn mark_as_maybe_sealed(&self) {
        self.maybe_sealed.store(true, Ordering::Relaxed)
    }
    pub fn maybe_sealed(&self) -> bool {
        self.maybe_sealed.load(Ordering::Relaxed)
    }
}

impl<T> RemoteSequencer<T>
where
    T: TransportConnect,
{
    /// Creates a new instance of RemoteSequencer
    pub fn new(
        log_id: LogId,
        segment_index: SegmentIndex,
        params: ReplicatedLogletParams,
        networking: Networking<T>,
        known_global_tail: TailOffsetWatch,
    ) -> Self {
        let max_inflight_records_in_config: usize = Configuration::pinned()
            .bifrost
            .replicated_loglet
            .maximum_inflight_records
            .into();

        let record_permits = Arc::new(Semaphore::new(max_inflight_records_in_config));

        Self {
            log_id,
            segment_index,
            params,
            networking,
            max_inflight_records_in_config: AtomicUsize::new(max_inflight_records_in_config),
            record_permits,
            known_global_tail,
            connection: Arc::default(),
            maybe_sealed: AtomicBool::new(false),
        }
    }

    pub fn ensure_enough_permits(&self, required: usize) {
        let mut available = self.max_inflight_records_in_config.load(Ordering::Relaxed);
        while available < required {
            let delta = required - available;
            match self.max_inflight_records_in_config.compare_exchange(
                available,
                required,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    self.record_permits.add_permits(delta);
                }
                Err(current) => {
                    available = current;
                }
            }
        }
    }

    #[instrument(
        level="trace",
        skip_all,
        fields(
            loglet_id = %self.params.loglet_id,
            otel.name = "replicated_loglet::remote_sequencer: append",
        )
    )]
    pub async fn append(&self, payloads: Arc<[Record]>) -> Result<LogletCommit, OperationError> {
        if self.known_global_tail.is_sealed() {
            return Ok(LogletCommit::sealed());
        }

        self.ensure_enough_permits(payloads.len());

        let len = u32::try_from(payloads.len()).expect("batch sizes fit in u32");

        let permits = self
            .record_permits
            .clone()
            .acquire_many_owned(len)
            .await
            .unwrap();

        let mut connection = match self.get_connection().await {
            Ok(connection) => connection,
            Err(err) => return self.on_handle_network_error(err),
        };

        let loglet_id = self.params.loglet_id;

        let permit = loop {
            match connection.inner.reserve_owned().await {
                Some(permit) => {
                    break permit;
                }
                None => {
                    // re-connect
                    connection = match self.renew_connection(connection).await {
                        Ok(connection) => connection,
                        Err(err) => return self.on_handle_network_error(err),
                    };
                    continue;
                }
            };
        };

        let msg = Append {
            header: CommonRequestHeader {
                log_id: self.log_id,
                loglet_id,
                segment_index: self.segment_index,
            },
            payloads: payloads.into(),
        };

        trace!(
            "Send append message to remote sequencer at {}",
            connection.inner.peer()
        );

        let Ok(reply_rx) = permit.send_rpc(msg, Some(loglet_id.into())) else {
            // sending rpc can fail if message could not be encoded. It's very unlikely that any
            // retries on this connection will resolve it.
            self.drop_connection(connection).await;
            return Err(OperationError::retryable(AppendError::retryable(
                NetworkError::ConnectionClosed(ConnectionClosed),
            )));
        };
        let (commit_token, commit_resolver) = LogletCommit::deferred();

        connection.resolve_on_appended(permits, reply_rx, commit_resolver);

        Ok(commit_token)
    }

    fn on_handle_network_error(&self, err: ConnectError) -> Result<LogletCommit, OperationError> {
        match err {
            err @ ConnectError::Discovery(DiscoveryError::NodeIsGone(_)) => {
                // means that the sequencer is gone, we need reconfiguration.
                Ok(LogletCommit::reconfiguration_needed(format!(
                    "sequencer is gone; {err}"
                )))
            }
            // probably retryable
            err => Err(OperationError::retryable(err)),
        }
    }

    /// Gets or starts a new remote sequencer connection
    async fn get_connection(&self) -> Result<RemoteSequencerConnection, ConnectError> {
        let mut guard = self.connection.lock().await;
        if let Some(connection) = guard.deref() {
            return Ok(connection.clone());
        }

        let connection = self
            .networking
            .get_connection(self.params.sequencer, Swimlane::BifrostData)
            .await?;
        let connection =
            RemoteSequencerConnection::start(self.known_global_tail.clone(), connection)?;

        *guard = Some(connection.clone());

        Ok(connection)
    }

    async fn drop_connection(&self, old: RemoteSequencerConnection) {
        let mut guard = self.connection.lock().await;
        let current = guard.as_ref().expect("connection has been initialized");
        // stream has already been renewed
        if old.inner != current.inner {
            return;
        }
        old.close().await;
        *guard = None;
    }

    /// Renew a connection to a remote sequencer. This guarantees that only a single connection
    /// to the sequencer is available.
    async fn renew_connection(
        &self,
        old: RemoteSequencerConnection,
    ) -> Result<RemoteSequencerConnection, ConnectError> {
        let mut guard = self.connection.lock().await;
        let current = guard.as_ref().expect("connection has been initialized");

        // stream has already been renewed
        if old.inner != current.inner {
            return Ok(current.clone());
        }

        let connection = self
            .networking
            .get_connection(self.params.sequencer, Swimlane::BifrostData)
            .await?;

        let connection =
            RemoteSequencerConnection::start(self.known_global_tail.clone(), connection)?;

        *guard = Some(connection.clone());

        Ok(connection)
    }
}

/// RemoteSequencerConnection represents a single open connection
/// to a remote leader sequencer.
///
/// This connection handles all [`Appended`] responses from the remote
/// sequencer.
///
/// If the connection was lost or if any of the commits failed
/// with a terminal error (like [`SequencerStatus::Sealed`]) all pending commits
/// are resolved with an error.
#[derive(Clone)]
struct RemoteSequencerConnection {
    inner: Connection,
    tx: mpsc::UnboundedSender<RemoteInflightAppend>,
}

impl RemoteSequencerConnection {
    fn start(
        known_global_tail: TailOffsetWatch,
        connection: Connection,
    ) -> Result<Self, ShutdownError> {
        let (tx, rx) = mpsc::unbounded_channel();

        TaskCenter::spawn(
            TaskKind::NetworkMessageHandler,
            "remote-sequencer-connection",
            Self::handle_appended_responses(known_global_tail, connection.clone(), rx),
        )?;

        Ok(Self {
            inner: connection,
            tx,
        })
    }

    pub async fn close(self) {
        // close the connection
        let _ = self.inner.drain(DrainReason::ConnectionDrain).await;
    }

    pub fn resolve_on_appended(
        &self,
        permit: OwnedSemaphorePermit,
        reply_rx: ReplyRx<Appended>,
        commit_resolver: LogletCommitResolver,
    ) {
        let inflight_append = RemoteInflightAppend {
            reply_rx,
            commit_resolver,
            permit,
        };

        if let Err(err) = self.tx.send(inflight_append) {
            // if we failed to push this to be processed by the connection reactor task
            // then we need to notify the caller
            err.0
                .commit_resolver
                .error(AppendError::retryable(NetworkError::ConnectionClosed(
                    ConnectionClosed,
                )));
        }
    }

    /// Handle all [`Appended`] responses
    ///
    /// This task will run until the [`AppendStream`] is dropped. Once dropped
    /// all pending commits will be resolved with an error. it's up to the enqueuer
    /// to retry if needed.
    async fn handle_appended_responses(
        known_global_tail: TailOffsetWatch,
        connection: Connection,
        mut rx: mpsc::UnboundedReceiver<RemoteInflightAppend>,
    ) -> anyhow::Result<()> {
        // handle all rpc tokens in an infinite loop
        // this loop only breaks when it encounters a terminal
        // AppendError.
        // When this happens, the receiver channel is closed
        // and drained. The same error is then used to resolve
        // all pending tokens
        let err = loop {
            let Some(inflight) = rx.recv().await else {
                // connection was dropped.
                break AppendError::retryable(NetworkError::ConnectionClosed(ConnectionClosed));
            };

            let RemoteInflightAppend {
                reply_rx,
                commit_resolver,
                permit: _permit,
            } = inflight;

            // A failure in one of the appends should result in rewinding the retry
            // stream back. Therefore, we mark this as a bad connection (by closing the channel) to
            // signal to the writer that all appends from this point onwards should be retried.
            let appended = reply_rx.await.map_err(|_| {
                AppendError::retryable(NetworkError::ConnectionClosed(ConnectionClosed))
            });

            let appended = match appended {
                Ok(appended) => appended,
                Err(err) => {
                    // this can only be a terminal error (either shutdown or connection is closing)
                    commit_resolver.error(err.clone());
                    break err;
                }
            };

            // good chance to update known global tail
            if let Some(offset) = appended.known_global_tail {
                known_global_tail.notify_offset_update(offset);
            }

            // handle status of the response.
            match appended.header.status {
                None => {
                    commit_resolver.offset(appended.last_offset);
                }

                Some(SequencerStatus::Sealed) => {
                    // A sealed status returns a terminal error since we can immediately cancel
                    // all inflight append jobs.
                    commit_resolver.sealed();
                    break AppendError::Sealed;
                }
                Some(SequencerStatus::Gone | SequencerStatus::Shutdown) => {
                    // this sequencer is not coming back
                    commit_resolver.error(AppendError::ReconfigurationNeeded(
                        format!("sequencer at {} is terminating", connection.peer()).into(),
                    ));
                }
                Some(
                    SequencerStatus::UnknownLogId
                    | SequencerStatus::UnknownSegmentIndex
                    | SequencerStatus::LogletIdMismatch
                    | SequencerStatus::NotSequencer
                    | SequencerStatus::Error { .. }
                    | SequencerStatus::Unknown,
                ) => {
                    let err = RemoteSequencerError::try_from(appended.header.status).unwrap();
                    // While the UnknownLoglet status is non-terminal for the connection
                    // (since only one request is bad),
                    // the AppendError for the caller is terminal
                    commit_resolver.error(AppendError::other(err));
                }
            }
        };

        // close channel to stop any further appends calls on the same connection
        rx.close();

        // Drain and resolve ALL pending appends on this connection.
        //
        // todo(azmy): The order of the RemoteInflightAppend's on the channel
        // does not necessary matches the actual append calls. This is
        // since sending on the connection and pushing on the rx channel is not an atomic
        // operation. Which means that, it's possible when we are draining
        // the pending requests here that we also end up cancelling some inflight appends
        // that has already received a positive response from the sequencer.
        //
        // For now this should not be a problem since they can (possibly) retry
        // to do the write again later.
        while let Some(inflight) = rx.recv().await {
            inflight.commit_resolver.error(err.clone());
        }

        Ok(())
    }
}

pub(crate) struct RemoteInflightAppend {
    reply_rx: ReplyRx<Appended>,
    commit_resolver: LogletCommitResolver,
    permit: OwnedSemaphorePermit,
}

#[derive(thiserror::Error, Debug, Clone)]
pub enum RemoteSequencerError {
    #[error("Unknown log-id")]
    UnknownLogId,
    #[error("Unknown segment index")]
    UnknownSegmentIndex,
    #[error("LogletID mismatch")]
    LogletIdMismatch,
    #[error("Remote node is not a sequencer")]
    NotSequencer,
    #[error("Unknown remote error: {message}")]
    Error { retryable: bool, message: String },
}

impl MaybeRetryableError for RemoteSequencerError {
    fn retryable(&self) -> bool {
        match self {
            Self::UnknownLogId => false,
            Self::UnknownSegmentIndex => false,
            Self::LogletIdMismatch => false,
            Self::NotSequencer => false,
            Self::Error { retryable, .. } => *retryable,
        }
    }
}

impl TryFrom<Option<SequencerStatus>> for RemoteSequencerError {
    type Error = &'static str;
    fn try_from(value: Option<SequencerStatus>) -> Result<Self, &'static str> {
        let value = match value {
            Some(SequencerStatus::UnknownLogId) => RemoteSequencerError::UnknownLogId,
            Some(SequencerStatus::UnknownSegmentIndex) => RemoteSequencerError::UnknownSegmentIndex,
            Some(SequencerStatus::LogletIdMismatch) => RemoteSequencerError::LogletIdMismatch,
            Some(SequencerStatus::NotSequencer) => RemoteSequencerError::NotSequencer,
            Some(SequencerStatus::Error { retryable, message }) => {
                RemoteSequencerError::Error { retryable, message }
            }
            Some(SequencerStatus::Unknown) => RemoteSequencerError::Error {
                retryable: false,
                message: "unknown error".to_owned(),
            },
            None
            | Some(SequencerStatus::Sealed | SequencerStatus::Shutdown | SequencerStatus::Gone) => {
                return Err("not a permanent failure status");
            }
        };

        Ok(value)
    }
}

#[cfg(test)]
mod test {
    use std::{
        sync::atomic::{AtomicU32, Ordering},
        time::Duration,
    };

    use rand::Rng;

    use restate_core::{
        TestCoreEnv, TestCoreEnvBuilder,
        network::{FailingConnector, Handler, Incoming, RawSvcRpc, Verdict},
    };
    use restate_types::{
        GenerationalNodeId,
        logs::{LogId, LogletOffset, Record, SequenceNumber, TailOffsetWatch, TailState},
        net::{
            RpcRequest,
            replicated_loglet::{
                Append, Appended, CommonResponseHeader, SequencerDataService, SequencerStatus,
            },
        },
        replicated_loglet::ReplicatedLogletParams,
        replication::{NodeSet, ReplicationProperty},
    };

    use super::RemoteSequencer;
    use crate::loglet::AppendError;

    struct SequencerMockHandler {
        offset: AtomicU32,
        reply_status: Option<SequencerStatus>,
    }

    impl SequencerMockHandler {
        fn with_reply_status(reply_status: Option<SequencerStatus>) -> Self {
            Self {
                reply_status,
                ..Default::default()
            }
        }
    }

    impl Default for SequencerMockHandler {
        fn default() -> Self {
            Self {
                offset: AtomicU32::new(LogletOffset::OLDEST.into()),
                reply_status: None,
            }
        }
    }

    impl Handler for SequencerMockHandler {
        type Service = SequencerDataService;

        async fn on_rpc(&mut self, message: Incoming<RawSvcRpc<Self::Service>>) {
            if message.msg_type() == Append::TYPE {
                let msg = message.into_typed::<Append>();
                let (reciprocal, msg) = msg.split();
                let last_offset = self
                    .offset
                    .fetch_add(msg.payloads.len() as u32, Ordering::Relaxed);

                let delay = rand::rng().random_range(50..350);
                tokio::time::sleep(Duration::from_millis(delay)).await;
                reciprocal.send(Appended {
                    last_offset: LogletOffset::from(last_offset),
                    header: CommonResponseHeader {
                        known_global_tail: None,
                        sealed: Some(false),
                        status: self.reply_status.clone(),
                    },
                });
            } else {
                message.fail(Verdict::MessageUnrecognized);
            }
        }
    }

    async fn create_test_env(
        sequencer_handler: SequencerMockHandler,
    ) -> TestCoreEnv<FailingConnector> {
        let builder = TestCoreEnvBuilder::with_incoming_only_connector()
            .add_mock_nodes_config()
            .register_buffered_service(
                10,
                restate_core::network::BackPressureMode::PushBack,
                sequencer_handler,
            );

        builder.build().await
    }

    #[restate_core::test]
    async fn test_remote_stream_ok() {
        let handler = SequencerMockHandler::default();
        let test_env = create_test_env(handler).await;

        let params = ReplicatedLogletParams {
            loglet_id: 1.into(),
            nodeset: NodeSet::default(),
            replication: ReplicationProperty::new(1.try_into().unwrap()),
            sequencer: GenerationalNodeId::new(1, 1),
        };
        let known_global_tail = TailOffsetWatch::new(TailState::Open(LogletOffset::OLDEST));

        let remote_sequencer = RemoteSequencer::new(
            LogId::new(1),
            1.into(),
            params,
            test_env.networking.clone(),
            known_global_tail,
        );

        let records: Vec<Record> = vec!["record 1".into(), "record 2".into(), "record 3".into()];

        let commit_1 = remote_sequencer
            .append(records.clone().into())
            .await
            .unwrap();

        let commit_2 = remote_sequencer
            .append(records.clone().into())
            .await
            .unwrap();

        let first_offset_1 = commit_1.await.unwrap();
        assert_eq!(first_offset_1, 1.into());
        let first_offset_2 = commit_2.await.unwrap();
        assert_eq!(first_offset_2, 4.into());
    }

    #[restate_core::test]
    async fn test_remote_stream_sealed() {
        let handler = SequencerMockHandler::with_reply_status(Some(SequencerStatus::Sealed));
        let test_env = create_test_env(handler).await;

        let params = ReplicatedLogletParams {
            loglet_id: 1.into(),
            nodeset: NodeSet::default(),
            replication: ReplicationProperty::new(1.try_into().unwrap()),
            sequencer: GenerationalNodeId::new(1, 1),
        };
        let known_global_tail = TailOffsetWatch::new(TailState::Open(LogletOffset::OLDEST));

        let remote_sequencer = RemoteSequencer::new(
            LogId::new(1),
            1.into(),
            params,
            test_env.networking.clone(),
            known_global_tail,
        );

        let records: Vec<Record> = vec!["record 1".into(), "record 2".into(), "record 3".into()];

        let commit_1 = remote_sequencer
            .append(records.clone().into())
            .await
            .unwrap();

        let commit_2 = remote_sequencer
            .append(records.clone().into())
            .await
            .unwrap();

        let first_offset_1 = commit_1.await;
        assert!(matches!(first_offset_1, Err(AppendError::Sealed)));
        let first_offset_2 = commit_2.await;
        assert!(matches!(first_offset_2, Err(AppendError::Sealed)));
    }
}
