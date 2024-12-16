// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use tokio::sync::{mpsc, Mutex, OwnedSemaphorePermit, Semaphore};

use restate_core::{
    network::{
        rpc_router::{RpcRouter, RpcToken},
        NetworkError, NetworkSendError, Networking, Outgoing, TransportConnect, WeakConnection,
    },
    ShutdownError, TaskCenter, TaskKind,
};
use restate_types::{
    config::Configuration,
    errors::MaybeRetryableError,
    logs::{metadata::SegmentIndex, LogId, Record},
    net::replicated_loglet::{Append, Appended, CommonRequestHeader, SequencerStatus},
    replicated_loglet::ReplicatedLogletParams,
    GenerationalNodeId,
};
use tracing::instrument;

use super::rpc_routers::SequencersRpc;
use crate::loglet::{
    util::TailOffsetWatch, AppendError, LogletCommit, LogletCommitResolver, OperationError,
};

pub struct RemoteSequencer<T> {
    log_id: LogId,
    segment_index: SegmentIndex,
    params: ReplicatedLogletParams,
    networking: Networking<T>,
    max_inflight_records_in_config: AtomicUsize,
    record_permits: Arc<Semaphore>,
    sequencers_rpc: SequencersRpc,
    known_global_tail: TailOffsetWatch,
    connection: Arc<Mutex<Option<RemoteSequencerConnection>>>,
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
        sequencers_rpc: SequencersRpc,
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
            sequencers_rpc,
            known_global_tail,
            connection: Arc::default(),
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

        let mut connection = self.get_connection().await?;

        let mut msg = Append {
            header: CommonRequestHeader {
                log_id: self.log_id,
                loglet_id: self.params.loglet_id,
                segment_index: self.segment_index,
            },
            payloads,
        };

        let rpc_token = loop {
            match connection
                .send(&self.sequencers_rpc.append, self.params.sequencer, msg)
                .await
            {
                Ok(token) => break token,
                Err(err) => {
                    match err.source {
                        NetworkError::ConnectError(_)
                        | NetworkError::ConnectionClosed(_)
                        | NetworkError::Timeout(_) => {
                            // we retry to re-connect one time
                            connection = self.renew_connection(connection).await?;

                            msg = err.original;
                            continue;
                        }
                        err => return Err(err.into()),
                    }
                }
            };
        };
        let (commit_token, commit_resolver) = LogletCommit::deferred();

        connection.resolve_on_appended(permits, rpc_token, commit_resolver);

        Ok(commit_token)
    }

    /// Gets or starts a new remote sequencer connection
    async fn get_connection(&self) -> Result<RemoteSequencerConnection, NetworkError> {
        let mut guard = self.connection.lock().await;
        if let Some(connection) = guard.deref() {
            return Ok(connection.clone());
        }

        let connection = self
            .networking
            .node_connection(self.params.sequencer)
            .await?;
        let connection =
            RemoteSequencerConnection::start(self.known_global_tail.clone(), connection)?;

        *guard = Some(connection.clone());

        Ok(connection)
    }

    /// Renew a connection to a remote sequencer. This guarantees that only a single connection
    /// to the sequencer is available.
    async fn renew_connection(
        &self,
        old: RemoteSequencerConnection,
    ) -> Result<RemoteSequencerConnection, NetworkError> {
        let mut guard = self.connection.lock().await;
        let current = guard.as_ref().expect("connection has been initialized");

        // stream has already been renewed
        if old.inner != current.inner {
            return Ok(current.clone());
        }

        let connection = self
            .networking
            .node_connection(self.params.sequencer)
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
    inner: WeakConnection,
    tx: mpsc::UnboundedSender<RemoteInflightAppend>,
}

impl RemoteSequencerConnection {
    fn start(
        known_global_tail: TailOffsetWatch,
        connection: WeakConnection,
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

    /// Send append message to remote sequencer.
    ///
    /// It's up to the caller to retry on [`NetworkError`]
    pub async fn send(
        &self,
        rpc_router: &RpcRouter<Append>,
        sequencer: GenerationalNodeId,
        msg: Append,
    ) -> Result<RpcToken<Appended>, NetworkSendError<Append>> {
        let outgoing = Outgoing::new(sequencer, msg).assign_connection(self.inner.clone());

        rpc_router
            .send_on_connection(outgoing)
            .await
            .map_err(|err| NetworkSendError::new(err.original.into_body(), err.source))
    }

    pub fn resolve_on_appended(
        &self,
        permit: OwnedSemaphorePermit,
        rpc_token: RpcToken<Appended>,
        commit_resolver: LogletCommitResolver,
    ) {
        let inflight_append = RemoteInflightAppend {
            rpc_token,
            commit_resolver,
            permit,
        };

        if let Err(err) = self.tx.send(inflight_append) {
            // if we failed to push this to be processed by the connection reactor task
            // then we need to notify the caller
            err.0
                .commit_resolver
                .error(AppendError::retryable(NetworkError::ConnectionClosed(
                    self.inner.peer(),
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
        connection: WeakConnection,
        mut rx: mpsc::UnboundedReceiver<RemoteInflightAppend>,
    ) -> anyhow::Result<()> {
        let mut closed = std::pin::pin!(connection.closed());

        // handle all rpc tokens in an infinite loop
        // this loop only breaks when it encounters a terminal
        // AppendError.
        // When this happens, the receiver channel is closed
        // and drained. The same error is then used to resolve
        // all pending tokens
        let err = loop {
            let inflight = tokio::select! {
                inflight = rx.recv() => {
                    inflight
                }
                _ = &mut closed => {
                    break AppendError::retryable(NetworkError::ConnectionClosed(connection.peer()));
                }
            };

            let Some(inflight) = inflight else {
                // connection was dropped.
                break AppendError::retryable(NetworkError::ConnectionClosed(connection.peer()));
            };

            let RemoteInflightAppend {
                rpc_token,
                commit_resolver,
                permit: _permit,
            } = inflight;

            let appended = tokio::select! {
                incoming = rpc_token.recv() => {
                    incoming.map_err(AppendError::Shutdown)
                },
                _ = &mut closed => {
                    Err(AppendError::retryable(NetworkError::ConnectionClosed(connection.peer())))
                }
            };

            let appended = match appended {
                Ok(appended) => appended.into_body(),
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
                SequencerStatus::Ok => {
                    commit_resolver.offset(appended.last_offset);
                }

                SequencerStatus::Sealed => {
                    // A sealed status returns a terminal error since we can immediately cancel
                    // all inflight append jobs.
                    commit_resolver.sealed();
                    break AppendError::Sealed;
                }
                SequencerStatus::UnknownLogId
                | SequencerStatus::UnknownSegmentIndex
                | SequencerStatus::LogletIdMismatch
                | SequencerStatus::NotSequencer
                | SequencerStatus::Shutdown
                | SequencerStatus::Error { .. } => {
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
    rpc_token: RpcToken<Appended>,
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
    #[error("Sequencer shutdown")]
    Shutdown,
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
            Self::Shutdown => false,
            Self::Error { retryable, .. } => *retryable,
        }
    }
}

impl TryFrom<SequencerStatus> for RemoteSequencerError {
    type Error = &'static str;
    fn try_from(value: SequencerStatus) -> Result<Self, &'static str> {
        let value = match value {
            SequencerStatus::UnknownLogId => RemoteSequencerError::UnknownLogId,
            SequencerStatus::UnknownSegmentIndex => RemoteSequencerError::UnknownSegmentIndex,
            SequencerStatus::LogletIdMismatch => RemoteSequencerError::LogletIdMismatch,
            SequencerStatus::NotSequencer => RemoteSequencerError::NotSequencer,
            SequencerStatus::Shutdown => RemoteSequencerError::Shutdown,
            SequencerStatus::Error { retryable, message } => {
                RemoteSequencerError::Error { retryable, message }
            }
            SequencerStatus::Ok | SequencerStatus::Sealed => {
                return Err("not a failure status");
            }
        };

        Ok(value)
    }
}

#[cfg(test)]
mod test {
    use std::{
        future::Future,
        sync::{
            atomic::{AtomicU32, Ordering},
            Arc,
        },
        time::Duration,
    };

    use rand::Rng;

    use restate_core::{
        network::{Incoming, MessageHandler, MockConnector},
        TestCoreEnvBuilder,
    };
    use restate_types::{
        logs::{LogId, LogletOffset, Record, SequenceNumber, TailState},
        net::replicated_loglet::{Append, Appended, CommonResponseHeader, SequencerStatus},
        replicated_loglet::{NodeSet, ReplicatedLogletParams, ReplicationProperty},
        GenerationalNodeId,
    };

    use super::RemoteSequencer;
    use crate::{
        loglet::{util::TailOffsetWatch, AppendError},
        providers::replicated_loglet::rpc_routers::SequencersRpc,
    };

    struct SequencerMockHandler {
        offset: AtomicU32,
        reply_status: SequencerStatus,
    }

    impl SequencerMockHandler {
        fn with_reply_status(reply_status: SequencerStatus) -> Self {
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
                reply_status: SequencerStatus::Ok,
            }
        }
    }

    impl MessageHandler for SequencerMockHandler {
        type MessageType = Append;
        async fn on_message(&self, msg: Incoming<Self::MessageType>) {
            let last_offset = self
                .offset
                .fetch_add(msg.body().payloads.len() as u32, Ordering::Relaxed);

            let outgoing = msg.into_outgoing(Appended {
                last_offset: LogletOffset::from(last_offset),
                header: CommonResponseHeader {
                    known_global_tail: None,
                    sealed: Some(false),
                    status: self.reply_status.clone(),
                },
            });
            let delay = rand::thread_rng().gen_range(50..350);
            tokio::time::sleep(Duration::from_millis(delay)).await;
            outgoing.send().await.unwrap();
        }
    }

    async fn setup<F, O>(sequencer: SequencerMockHandler, test: F)
    where
        O: Future<Output = ()>,
        F: FnOnce(RemoteSequencer<MockConnector>) -> O,
    {
        let (connector, _receiver) = MockConnector::new(100);
        let connector = Arc::new(connector);

        let mut builder = TestCoreEnvBuilder::with_transport_connector(Arc::clone(&connector))
            .add_mock_nodes_config()
            .add_message_handler(sequencer);

        let sequencer_rpc = SequencersRpc::new(&mut builder.router_builder);

        let params = ReplicatedLogletParams {
            loglet_id: 1.into(),
            nodeset: NodeSet::empty(),
            replication: ReplicationProperty::new(1.try_into().unwrap()),
            sequencer: GenerationalNodeId::new(1, 1),
        };
        let known_global_tail = TailOffsetWatch::new(TailState::Open(LogletOffset::OLDEST));
        let remote_sequencer = RemoteSequencer::new(
            LogId::new(1),
            1.into(),
            params,
            builder.networking.clone(),
            known_global_tail,
            sequencer_rpc,
        );

        let _env = builder.build().await;
        test(remote_sequencer).await;
    }

    #[restate_core::test]
    async fn test_remote_stream_ok() {
        let handler = SequencerMockHandler::default();

        setup(handler, |remote_sequencer| async move {
            let records: Vec<Record> =
                vec!["record 1".into(), "record 2".into(), "record 3".into()];

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
        })
        .await;
    }

    #[restate_core::test]
    async fn test_remote_stream_sealed() {
        let handler = SequencerMockHandler::with_reply_status(SequencerStatus::Sealed);

        setup(handler, |remote_sequencer| async move {
            let records: Vec<Record> =
                vec!["record 1".into(), "record 2".into(), "record 3".into()];

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
        })
        .await;
    }
}
