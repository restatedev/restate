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
use tracing::{instrument, trace};

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
    replicated_loglet::{NodeSet, Spread},
    time::MillisSinceEpoch,
    PlainNodeId,
};

use super::{RecordsExt, SequencerSharedState};
use crate::{
    loglet::{AppendError, LogletCommitResolver},
    providers::replicated_loglet::{
        log_server_manager::RemoteLogServer,
        metric_definitions::{
            BIFROST_SEQ_RECORDS_COMMITTED_BYTES, BIFROST_SEQ_RECORDS_COMMITTED_TOTAL,
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
        store_router: RpcRouter<Store>,
        networking: Networking<T>,
        first_offset: LogletOffset,
        records: Arc<[Record]>,
        permit: OwnedSemaphorePermit,
        commit_resolver: LogletCommitResolver,
    ) -> Self {
        Self {
            sequencer_shared_state,
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
        level="debug",
        skip(self),
        fields(
            loglet_id=%self.sequencer_shared_state.loglet_id(),
            first_offset=%self.first_offset,
            to_offset=%self.records.last_offset(self.first_offset).unwrap(),
            length=%self.records.len(),
            otel.name="replicated_loglet::sequencer::appender: run"
        )
    )]
    pub async fn run(mut self) {
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
            .sequencer_retry_policy
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
                graylist.clear();
                return SequencerAppenderState::Backoff;
            }
        };

        tracing::trace!(%graylist, %spread, %wave, "Sending store wave");

        // let mut gray = false;
        // for id in &spread {
        //     // todo:
        //     // if a node is consistently down? we should have:
        //     //   1) cool-off period between connection attempts
        //     //   2) remember generation number to reset the cool-off period
        //     //   3) Attempt to connect concurrently
        //     //
        //     // at this stage, if we fail to get connection to this server it must be
        //     // a first time use. We can safely assume this has to be graylisted
        //     let server = match self
        //         .sequencer_shared_state
        //         .log_server_manager
        //         .get(*id, &self.networking)
        //         .await
        //     {
        //         Ok(server) => server,
        //         Err(err) => {
        //             tracing::debug!(
        //                 peer=%id,
        //                 error=%err,
        //                 %wave,
        //                 %graylist,
        //                 %spread,
        //                 "Failed to connect to node. Graylisting this node in the next wave"
        //             );
        //             gray = true;
        //             graylist.insert(*id);
        //             continue;
        //         }
        //     };
        //
        //     checker.set_attribute(*id, true);
        //     servers.push(server);
        // }

        // todo: we generated a spread but we couldn't connect to enough nodes. That's an
        // optimization that wouldn't make sense if:
        // 1) we maintain a global logserver-health map
        // 2) we connect async.
        //
        // - sequencer creates the appender
        // - appender checks if can attempt a wave:
        //   - generate a spread while taking into account the log-server health
        //   - if we can't create a spread. can we generate a spread by ignoring log-server health?
        //   (no graylist)
        //   - if we can, we generate a spread and attempt to send the wave
        //   - send the wave (concurrent)
        //   - continously collecting responses
        //   - on errors of connection we report back to health - consider dedicated connection
        //   (connection role)
        //   - a wave timeout and a single store timeout???
        // if gray && !checker.check_write_quorum(|attr| *attr) {
        // TODO: WE SHOULD ACCOUNT FOR THOSE SERVERS THAT HAS ALREADY RESPONDED IN PREVIOUS
        // ITERATIONS YA NEGM..... AS IN, set_attribute() THEM to TRUE. because we can still
        // achieve quorum.
        // the remaining nodes in the spread cannot achieve a write quorum
        // hence we try again with the updated new graylist??
        // return SequencerAppenderState::Wave { graylist };
        // }

        // otherwise, we try to send the wave.
        self.send_wave(spread, wave).await
    }

    async fn send_wave(&mut self, spread: Spread, wave: usize) -> SequencerAppenderState {
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

        for node_id in &spread {
            pending_servers.insert(*node_id);

            store_tasks.push(
                LogServerStoreTask {
                    node_id: *node_id,
                    sequencer_shared_state: &self.sequencer_shared_state,
                    networking: &self.networking,
                    first_offset: self.first_offset,
                    records: &self.records,
                    rpc_router: &self.store_router,
                    timeout_at,
                }
                .run(),
            );
        }

        loop {
            let store_result = match timeout(store_timeout, store_tasks.next()).await {
                Ok(Some(result)) => result,
                Ok(None) => break, // no more tasks
                Err(_) => {
                    // if we have already acknowledged this append, it's okay to retire.
                    if self.commit_resolver.is_none() {
                        tracing::debug!(%pending_servers, %wave, %spread, responses=?checker, "Some servers didn't store this batch, but append was committed, giving up");
                        return SequencerAppenderState::Done;
                    }

                    if self.sequencer_shared_state.known_global_tail.is_sealed() {
                        tracing::debug!(%pending_servers, %wave, %spread, responses=?checker, "Some servers didn't store this batch, but this loglet was sealed, giving up");
                        return SequencerAppenderState::Sealed;
                    }
                    // timed out!
                    // none of the pending tasks has finished in time! we will assume all pending server
                    // are graylisted and try again
                    tracing::debug!(%pending_servers, %wave, %spread, responses=?checker, "Timeout waiting on store response");
                    return SequencerAppenderState::Wave {
                        graylist: pending_servers,
                    };
                }
            };

            let LogServerStoreTaskResult {
                node_id: peer,
                status,
            } = store_result;

            let response = match status {
                StoreTaskStatus::Error(NetworkError::Shutdown(_)) => {
                    return SequencerAppenderState::Cancelled;
                }
                StoreTaskStatus::Error(err) => {
                    // couldn't send store command to remote server
                    tracing::debug!(%peer, error=%err, "Failed to send batch to node");
                    continue;
                }
                StoreTaskStatus::Sealed(_) => {
                    tracing::debug!(%peer, "Store task cancelled, the node is sealed");
                    checker.set_attribute(peer, NodeAttributes::sealed());
                    continue;
                }
                StoreTaskStatus::Stored(stored) => {
                    tracing::trace!(%peer, "Store task completed");
                    stored
                }
            };

            // we had a response from this node and there is still a lot we can do
            match response.status {
                Status::Ok => {
                    // only if status is okay that we remove this node
                    // from the gray list, and move to replicated list
                    checker.set_attribute(peer, NodeAttributes::committed());
                    pending_servers.remove(&peer);
                }
                Status::Sealed | Status::Sealing => {
                    checker.set_attribute(peer, NodeAttributes::sealed());
                }
                Status::Disabled
                | Status::Dropped
                | Status::SequencerMismatch
                | Status::Malformed
                | Status::OutOfBounds => {
                    // just leave this log server in graylist (pending)
                    tracing::debug!(%peer, status=?response.status, "Store task returned an error status");
                }
            }

            if self.commit_resolver.is_some() && checker.check_write_quorum(|attr| attr.committed) {
                // resolve the commit if not resolved yet
                if let Some(resolver) = self.commit_resolver.take() {
                    self.sequencer_shared_state
                        .known_global_tail
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
    pub node_id: PlainNodeId,
    pub status: StoreTaskStatus,
}

/// The task will retry to connect to the remote server if connection
/// was lost.
struct LogServerStoreTask<'a, T> {
    node_id: PlainNodeId,
    sequencer_shared_state: &'a Arc<SequencerSharedState>,
    networking: &'a Networking<T>,
    first_offset: LogletOffset,
    records: &'a Arc<[Record]>,
    rpc_router: &'a RpcRouter<Store>,
    timeout_at: MillisSinceEpoch,
}

impl<'a, T: TransportConnect> LogServerStoreTask<'a, T> {
    #[instrument(
        skip_all,
        fields(
            otel.name = "sequencer: store_task",
            loglet_id = %self.sequencer_shared_state.loglet_id(),
            first_offset=%self.first_offset,
            last_offset = %self.records.last_offset(self.first_offset).unwrap(),
            peer=%self.node_id,
        )
    )]
    async fn run(mut self) -> LogServerStoreTaskResult {
        let result = self.send().await;
        match &result {
            Ok(status) => {
                tracing::trace!(
                    result = ?status,
                    "Got store result from log server"
                );
            }
            Err(err) => {
                tracing::trace!(
                    error = %err,
                    "Failed to send store to log server"
                )
            }
        }

        LogServerStoreTaskResult {
            node_id: self.node_id,
            status: result.into(),
        }
    }

    async fn send(&mut self) -> Result<StoreTaskStatus, NetworkError> {
        let mut server = self
            .sequencer_shared_state
            .log_server_manager
            .get(self.node_id, self.networking)
            .await?;
        let server_local_tail = server
            .local_tail()
            .wait_for_offset_or_seal(self.first_offset);

        let global_tail = self
            .sequencer_shared_state
            .known_global_tail
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

        let incoming = match self.try_send(&mut server).await {
            Ok(incoming) => incoming,
            Err(err) => {
                return Err(err);
            }
        };

        server
            .local_tail()
            .notify_offset_update(incoming.body().local_tail);

        match incoming.body().status {
            Status::Sealing | Status::Sealed => {
                server.local_tail().notify_seal();
                return Ok(StoreTaskStatus::Sealed(incoming.body().header.local_tail));
            }
            _ => {
                // all other status types are handled by the caller
            }
        }

        Ok(StoreTaskStatus::Stored(incoming.into_body()))
    }

    async fn try_send(
        &mut self,
        server: &mut RemoteLogServer,
    ) -> Result<Incoming<Stored>, NetworkError> {
        let store = Store {
            header: LogServerRequestHeader::new(
                *self.sequencer_shared_state.loglet_id(),
                self.sequencer_shared_state
                    .known_global_tail
                    .latest_offset(),
            ),
            first_offset: self.first_offset,
            flags: StoreFlags::empty(),
            known_archived: LogletOffset::INVALID,
            payloads: Arc::clone(self.records),
            sequencer: *self.sequencer_shared_state.sequencer(),
            timeout_at: Some(self.timeout_at),
        };

        let mut msg = Outgoing::new(self.node_id, store);

        let mut attempt = 0;
        loop {
            attempt += 1;

            let with_connection = msg.assign_connection(server.connection().clone());
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
                                %attempt,
                                "Failed to send store to log server, trying to create a new connection"
                            );
                            self.sequencer_shared_state
                                .log_server_manager
                                .renew(server, self.networking)
                                .await?;
                            trace!(
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

            server.store_latency().record(store_start_time.elapsed());

            return result;
        }
    }
}
