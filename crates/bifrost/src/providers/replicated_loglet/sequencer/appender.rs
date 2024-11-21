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
    cmp::Ordering,
    sync::Arc,
    time::{Duration, Instant},
};

use futures::{stream::FuturesUnordered, StreamExt};
use tokio::{sync::OwnedSemaphorePermit, time::timeout};

use restate_core::{
    cancellation_token,
    network::{
        rpc_router::{RpcError, RpcRouter},
        Incoming, NetworkError, Networking, Outgoing, TransportConnect,
    },
    ShutdownError,
};
use restate_types::{
    config::Configuration,
    live::Live,
    logs::{LogletOffset, Record, SequenceNumber, TailState},
    net::log_server::{LogServerRequestHeader, Status, Store, StoreFlags, Stored},
    replicated_loglet::NodeSet,
    time::MillisSinceEpoch,
};
use tracing::{instrument, trace};

use super::{RecordsExt, SequencerSharedState};
use crate::{
    loglet::{AppendError, LogletCommitResolver},
    providers::replicated_loglet::{
        log_server_manager::{RemoteLogServer, RemoteLogServerManager},
        metric_definitions::{
            BIFROST_SEQ_RECORDS_COMMITTED_BYTES, BIFROST_SEQ_RECORDS_COMMITTED_TOTAL,
            BIFROST_SEQ_STORE_DURATION,
        },
        replication::NodeSetChecker,
    },
};

const DEFAULT_BACKOFF_TIME: Duration = Duration::from_millis(1000);

enum SequencerAppenderState {
    Wave {
        // nodes that should be avoided by the spread selector
        graylist: NodeSet,
    },
    Backoff,
    Done,
    Sealed,
    Cancelled,
}

/// Appender makes sure a batch of records will run to completion
pub(crate) struct SequencerAppender<T> {
    sequencer_shared_state: Arc<SequencerSharedState>,
    log_server_manager: RemoteLogServerManager,
    store_router: RpcRouter<Store>,
    networking: Networking<T>,
    first_offset: LogletOffset,
    records: Arc<[Record]>,
    // permit is held during the entire live
    // of the batch to limit the number of
    // inflight batches
    permit: Option<OwnedSemaphorePermit>,
    commit_resolver: Option<LogletCommitResolver>,
    configuration: Live<Configuration>,
}

impl<T: TransportConnect> SequencerAppender<T> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        sequencer_shared_state: Arc<SequencerSharedState>,
        log_server_manager: RemoteLogServerManager,
        store_router: RpcRouter<Store>,
        networking: Networking<T>,
        first_offset: LogletOffset,
        records: Arc<[Record]>,
        permit: OwnedSemaphorePermit,
        commit_resolver: LogletCommitResolver,
    ) -> Self {
        Self {
            sequencer_shared_state,
            log_server_manager,
            store_router,
            networking,
            first_offset,
            records,
            permit: Some(permit),
            commit_resolver: Some(commit_resolver),
            configuration: Configuration::updateable(),
        }
    }

    #[tracing::instrument(
        level="error",
        skip(self),
        fields(
            loglet_id=%self.sequencer_shared_state.loglet_id(),
            first_offset=%self.first_offset,
            to_offset=%self.records.last_offset(self.first_offset).unwrap(),
            length=%self.records.len(),
            otel.name="replicated_loglet::sequencer::appender: run"
        )
    )]
    pub async fn run(mut self) -> anyhow::Result<()> {
        let mut wave = 0;
        // initial wave has 0 replicated and 0 gray listed node
        let mut state = SequencerAppenderState::Wave {
            graylist: NodeSet::empty(),
        };

        let cancellation = cancellation_token();

        let mut cancelled = std::pin::pin!(cancellation.cancelled());
        let retry_policy = self
            .configuration
            .live_load()
            .bifrost
            .replicated_loglet
            .sequencer_backoff_strategy
            .clone();

        let mut retry = retry_policy.iter();

        // this loop retries forever or until the task is cancelled
        let final_state = loop {
            state = match state {
                // termination conditions
                SequencerAppenderState::Done
                | SequencerAppenderState::Cancelled
                | SequencerAppenderState::Sealed => break state,
                SequencerAppenderState::Wave { graylist } => {
                    wave += 1;
                    tokio::select! {
                        next_state = self.wave(graylist, wave) => {next_state},
                        _ = &mut cancelled => {
                            break SequencerAppenderState::Cancelled;
                        }
                    }
                }
                SequencerAppenderState::Backoff => {
                    // since backoff can be None, or run out of iterations,
                    // but appender should never give up we fall back to fixed backoff
                    let delay = retry.next().unwrap_or(DEFAULT_BACKOFF_TIME);
                    tracing::info!(
                        delay = ?delay,
                        %wave,
                        "Append wave failed, retrying with a new wave after delay"
                    );

                    tokio::select! {
                        _ = tokio::time::sleep(delay) => {},
                        _ = &mut cancelled => {
                            break SequencerAppenderState::Cancelled;
                        }
                    };

                    SequencerAppenderState::Wave {
                        graylist: NodeSet::empty(),
                    }
                }
            }
        };

        match final_state {
            SequencerAppenderState::Done => {
                assert!(self.commit_resolver.is_none());

                metrics::counter!(BIFROST_SEQ_RECORDS_COMMITTED_TOTAL)
                    .increment(self.records.len() as u64);
                metrics::counter!(BIFROST_SEQ_RECORDS_COMMITTED_BYTES)
                    .increment(self.records.estimated_encode_size() as u64);

                tracing::trace!("SequencerAppender task completed");
            }
            SequencerAppenderState::Cancelled => {
                tracing::trace!("SequencerAppender task cancelled");
                if let Some(commit_resolver) = self.commit_resolver.take() {
                    commit_resolver.error(AppendError::Shutdown(ShutdownError));
                }
            }
            SequencerAppenderState::Sealed => {
                tracing::debug!("SequencerAppender ended because of sealing");
                if let Some(commit_resolver) = self.commit_resolver.take() {
                    commit_resolver.sealed();
                }
            }
            SequencerAppenderState::Backoff | SequencerAppenderState::Wave { .. } => {
                unreachable!()
            }
        }
        Ok(())
    }

    async fn wave(&mut self, mut graylist: NodeSet, wave: usize) -> SequencerAppenderState {
        // select the spread
        let spread = match self.sequencer_shared_state.selector.select(
            &mut rand::thread_rng(),
            &self.networking.metadata().nodes_config_ref(),
            &graylist,
        ) {
            Ok(spread) => spread,
            Err(_) => {
                return SequencerAppenderState::Backoff;
            }
        };

        tracing::trace!(%graylist, %spread, %wave, "Sending store wave");

        let mut checker = NodeSetChecker::new(
            self.sequencer_shared_state.selector.nodeset(),
            &self.networking.metadata().nodes_config_ref(),
            self.sequencer_shared_state.selector.replication_property(),
        );

        let mut gray = false;
        let mut servers = Vec::with_capacity(spread.len());
        for id in &spread {
            // at this stage, if we fail to get connection to this server it must be
            // a first time use. We can safely assume this has to be graylisted
            let server = match self.log_server_manager.get(*id, &self.networking).await {
                Ok(server) => server,
                Err(err) => {
                    tracing::debug!(
                        peer=%id,
                        error=%err,
                        %wave,
                        %graylist,
                        %spread,
                        "Failed to connect to node. Graylisting this node in the next wave"
                    );
                    gray = true;
                    graylist.insert(*id);
                    continue;
                }
            };

            checker.set_attribute(*id, true);
            servers.push(server);
        }

        if gray && !checker.check_write_quorum(|attr| *attr) {
            // the remaining nodes in the spread cannot achieve a write quorum
            // hence we try again with the updated new graylist
            return SequencerAppenderState::Wave { graylist };
        }

        // otherwise, we try to send the wave.
        self.send_wave(servers, wave).await
    }

    async fn send_wave(
        &mut self,
        spread_servers: Vec<RemoteLogServer>,
        wave: usize,
    ) -> SequencerAppenderState {
        let last_offset = self.records.last_offset(self.first_offset).unwrap();

        let mut checker = NodeSetChecker::<NodeAttributes>::new(
            self.sequencer_shared_state.selector.nodeset(),
            &self.networking.metadata().nodes_config_ref(),
            self.sequencer_shared_state.selector.replication_property(),
        );

        let store_timeout = self
            .configuration
            .live_load()
            .bifrost
            .replicated_loglet
            .log_server_rpc_timeout;

        let timeout_at = MillisSinceEpoch::after(store_timeout);
        // track the in flight server ids
        let mut pending_servers = NodeSet::empty();
        let mut store_tasks = FuturesUnordered::new();

        let spread: Vec<_> = spread_servers.iter().map(|s| s.node_id()).collect();
        for server in spread_servers {
            pending_servers.insert(server.node_id());

            let task = LogServerStoreTask {
                sequencer_shared_state: &self.sequencer_shared_state,
                server_manager: &self.log_server_manager,
                server,
                networking: &self.networking,
                first_offset: self.first_offset,
                records: &self.records,
                rpc_router: &self.store_router,
                timeout_at,
            };

            store_tasks.push(task.run());
        }

        loop {
            let store_result = match timeout(store_timeout, store_tasks.next()).await {
                Ok(Some(result)) => result,
                Ok(None) => break, // no more tasks
                Err(elapsed) => {
                    // if we have already acknowledged this append, it's okay to retire.
                    if self.commit_resolver.is_none() {
                        tracing::debug!(%pending_servers, %wave, ?spread, ?elapsed, responses=?checker, "Some servers didn't store this batch, but append was committed, giving up");
                        return SequencerAppenderState::Done;
                    }

                    if self
                        .sequencer_shared_state
                        .global_committed_tail()
                        .is_sealed()
                    {
                        tracing::debug!(%pending_servers, %wave, ?spread, ?elapsed, responses=?checker, "Some servers didn't store this batch, but this loglet was sealed, giving up");
                        return SequencerAppenderState::Sealed;
                    }
                    // timed out!
                    // none of the pending tasks has finished in time! we will assume all pending server
                    // are graylisted and try again
                    tracing::debug!(%pending_servers, %wave, ?spread, ?elapsed, responses=?checker, "Timeout waiting on store response");
                    return SequencerAppenderState::Wave {
                        graylist: pending_servers,
                    };
                }
            };

            let LogServerStoreTaskResult { server, status } = store_result;

            let node_id = server.node_id();
            let response = match status {
                StoreTaskStatus::Error(NetworkError::Shutdown(_)) => {
                    return SequencerAppenderState::Cancelled;
                }
                StoreTaskStatus::Error(err) => {
                    // couldn't send store command to remote server
                    tracing::debug!(node_id=%server.node_id(), error=%err, "Failed to send batch to node");
                    continue;
                }
                StoreTaskStatus::Sealed(_) => {
                    tracing::debug!(node_id=%server.node_id(), "Store task cancelled, the node is sealed");
                    checker.set_attribute(node_id, NodeAttributes::sealed());
                    continue;
                }
                StoreTaskStatus::Stored(stored) => {
                    tracing::trace!(node_id=%server.node_id(), "Store task completed");
                    stored
                }
            };

            // we had a response from this node and there is still a lot we can do
            match response.status {
                Status::Ok => {
                    // only if status is okay that we remove this node
                    // from the gray list, and move to replicated list
                    checker.set_attribute(node_id, NodeAttributes::committed());
                    pending_servers.remove(&node_id);
                }
                Status::Sealed | Status::Sealing => {
                    checker.set_attribute(node_id, NodeAttributes::sealed());
                }
                Status::Disabled
                | Status::Dropped
                | Status::SequencerMismatch
                | Status::Malformed
                | Status::OutOfBounds => {
                    // just leave this log server in graylist (pending)
                    tracing::trace!(node_id=%server.node_id(), status=?response.status, "Store task returned an error status");
                }
            }

            if self.commit_resolver.is_some() && checker.check_write_quorum(|attr| attr.committed) {
                // resolve the commit if not resolved yet
                if let Some(resolver) = self.commit_resolver.take() {
                    self.sequencer_shared_state.record_cache.extend(
                        *self.sequencer_shared_state.loglet_id(),
                        self.first_offset,
                        &self.records,
                    );
                    self.sequencer_shared_state
                        .global_committed_tail()
                        .notify_offset_update(last_offset.next());
                    resolver.offset(last_offset);
                }

                // drop the permit
                self.permit.take();
            }
        }

        if checker.check_write_quorum(|attr| attr.committed) {
            SequencerAppenderState::Done
        } else if checker.check_fmajority(|attr| attr.sealed).passed() {
            SequencerAppenderState::Sealed
        } else {
            SequencerAppenderState::Wave {
                graylist: pending_servers,
            }
        }
    }
}

#[derive(Default, Debug)]
struct NodeAttributes {
    committed: bool,
    sealed: bool,
}

impl NodeAttributes {
    fn committed() -> Self {
        NodeAttributes {
            committed: true,
            sealed: false,
        }
    }

    fn sealed() -> Self {
        NodeAttributes {
            committed: false,
            sealed: true,
        }
    }
}

#[derive(Debug)]
enum StoreTaskStatus {
    Sealed(LogletOffset),
    Stored(Stored),
    Error(NetworkError),
}

impl From<Result<StoreTaskStatus, NetworkError>> for StoreTaskStatus {
    fn from(value: Result<StoreTaskStatus, NetworkError>) -> Self {
        match value {
            Ok(result) => result,
            Err(err) => Self::Error(err),
        }
    }
}

struct LogServerStoreTaskResult {
    pub server: RemoteLogServer,
    pub status: StoreTaskStatus,
}

/// The LogServerStoreTask takes care of running a [`Store`] to end.
///
/// The task will retry to connect to the remote server is connection
/// was lost.
struct LogServerStoreTask<'a, T> {
    sequencer_shared_state: &'a Arc<SequencerSharedState>,
    server_manager: &'a RemoteLogServerManager,
    networking: &'a Networking<T>,
    server: RemoteLogServer,
    first_offset: LogletOffset,
    records: &'a Arc<[Record]>,
    rpc_router: &'a RpcRouter<Store>,
    timeout_at: MillisSinceEpoch,
}

impl<'a, T: TransportConnect> LogServerStoreTask<'a, T> {
    #[instrument(
        skip_all,
        fields(
            otel.name = "log_server: store",
            first_offset=%self.first_offset,
            log_server_id=%self.server.node_id(),
            loglet_id=%self.server.loglet_id(),
        )
    )]
    async fn run(mut self) -> LogServerStoreTaskResult {
        let result = self.send().await;
        match &result {
            Ok(status) => {
                tracing::trace!(
                    loglet_id = %self.sequencer_shared_state.loglet_id(),
                    node_id = %self.server.node_id(),
                    result = ?status,
                    first_offset = %self.first_offset,
                    last_offset = %self.records.last_offset(self.first_offset).unwrap(),
                    "Got store result from log server"
                );
            }
            Err(err) => {
                tracing::trace!(
                    loglet_id = %self.sequencer_shared_state.loglet_id(),
                    node_id = %self.server.node_id(),
                    error = %err,
                    first_offset = %self.first_offset,
                    last_offset = %self.records.last_offset(self.first_offset).unwrap(),
                    "Failed to send store to log server"
                )
            }
        }

        LogServerStoreTaskResult {
            server: self.server,
            status: result.into(),
        }
    }

    async fn send(&mut self) -> Result<StoreTaskStatus, NetworkError> {
        let server_local_tail = self
            .server
            .local_tail()
            .wait_for_offset_or_seal(self.first_offset);

        let global_tail = self
            .sequencer_shared_state
            .committed_tail
            .wait_for_offset_or_seal(self.first_offset);

        let tail_state = tokio::select! {
            local_state = server_local_tail => {
                local_state?
            }
            global_state = global_tail => {
                global_state?
            }
        };

        match tail_state {
            TailState::Sealed(offset) => return Ok(StoreTaskStatus::Sealed(offset)),
            TailState::Open(offset) => {
                match offset.cmp(&self.first_offset) {
                    Ordering::Equal | Ordering::Greater => {
                        // we ready to send our write
                    }
                    Ordering::Less => {
                        // this should never happen since we waiting
                        // for local tail!
                        unreachable!()
                    }
                };
            }
        }

        let incoming = match self.try_send().await {
            Ok(incoming) => incoming,
            Err(err) => {
                return Err(err);
            }
        };

        self.server
            .local_tail()
            .notify_offset_update(incoming.body().local_tail);

        match incoming.body().status {
            Status::Sealing | Status::Sealed => {
                self.server.local_tail().notify_seal();
                return Ok(StoreTaskStatus::Sealed(incoming.body().header.local_tail));
            }
            _ => {
                // all other status types are handled by the caller
            }
        }

        Ok(StoreTaskStatus::Stored(incoming.into_body()))
    }

    async fn try_send(&mut self) -> Result<Incoming<Stored>, NetworkError> {
        let store = Store {
            header: LogServerRequestHeader::new(
                self.server.loglet_id(),
                self.sequencer_shared_state.committed_tail.latest_offset(),
            ),
            first_offset: self.first_offset,
            flags: StoreFlags::empty(),
            known_archived: LogletOffset::INVALID,
            payloads: Arc::clone(self.records),
            sequencer: self.sequencer_shared_state.my_node_id,
            timeout_at: Some(self.timeout_at),
        };

        let mut msg = Outgoing::new(self.server.node_id(), store);

        let mut attempt = 0;
        loop {
            attempt += 1;
            let with_connection = msg.assign_connection(self.server.connection().clone());
            let store_start_time = Instant::now();

            let result = match self.rpc_router.call_on_connection(with_connection).await {
                Ok(incoming) => Ok(incoming),
                Err(RpcError::Shutdown(shutdown)) => Err(NetworkError::Shutdown(shutdown)),
                Err(RpcError::SendError(err)) => {
                    msg = err.original.forget_connection();

                    match err.source {
                        NetworkError::ConnectionClosed(_)
                        | NetworkError::ConnectError(_)
                        | NetworkError::Timeout(_) => {
                            trace!(
                                loglet_id = %self.server.loglet_id(),
                                node_id = %self.server.node_id(),
                                first_offset = %self.first_offset,
                                last_offset = %self.records.last_offset(self.first_offset).unwrap(),
                                %attempt,
                                "Failed to send store to log server, trying to create a new connection"
                            );
                            self.server_manager
                                .renew(&mut self.server, self.networking)
                                .await?;
                            trace!(
                                loglet_id = %self.server.loglet_id(),
                                node_id = %self.server.node_id(),
                                first_offset = %self.first_offset,
                                last_offset = %self.records.last_offset(self.first_offset).unwrap(),
                                %attempt,
                                "Reconnected to log-server, retrying the store"
                            );
                            // try again
                            continue;
                        }
                        _ => Err(err.source),
                    }
                }
            };

            metrics::histogram!(BIFROST_SEQ_STORE_DURATION, "node_id" => self.server.node_id().to_string())
                .record(store_start_time.elapsed());

            return result;
        }
    }
}
