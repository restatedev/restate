// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{cmp::Ordering, fmt::Display, sync::Arc, time::Duration};

use tokio::time::Instant;
use tokio::{sync::OwnedSemaphorePermit, task::JoinSet};
use tracing::{debug, instrument, trace, warn};

use restate_core::{
    cancellation_token,
    network::{rpc_router::RpcRouter, Incoming, NetworkError, Networking, TransportConnect},
    ShutdownError, TaskCenterFutureExt,
};
use restate_types::{
    config::Configuration,
    live::Live,
    logs::{LogletOffset, Record, SequenceNumber, TailState},
    net::log_server::{LogServerRequestHeader, Status, Store, StoreFlags, Stored},
    replication::{DecoratedNodeSet, NodeSet},
    time::MillisSinceEpoch,
    Merge, PlainNodeId,
};

use super::{RecordsExt, SequencerSharedState};
use crate::providers::replicated_loglet::metric_definitions::BIFROST_SEQ_APPEND_DURATION;
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

enum State {
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
    checker: NodeSetChecker<NodeAttributes>,
    nodeset_status: DecoratedNodeSet<PerNodeStatus>,
    current_wave: usize,
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
        // todo: in the future, we should update the checker's view over nodes configuration before
        // each wave. At the moment this is not required as nodes will not change their storage
        // state after the nodeset has been created until the loglet is sealed.
        let checker = NodeSetChecker::<NodeAttributes>::new(
            sequencer_shared_state.selector.nodeset(),
            &networking.metadata().nodes_config_ref(),
            sequencer_shared_state.selector.replication_property(),
        );

        let nodeset_status =
            DecoratedNodeSet::from(sequencer_shared_state.selector.nodeset().clone());

        Self {
            sequencer_shared_state,
            store_router,
            networking,
            checker,
            nodeset_status,
            current_wave: 0,
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
        let start = Instant::now();
        // initial wave has 0 replicated and 0 gray listed node
        let mut state = State::Wave {
            graylist: NodeSet::default(),
        };

        let cancellation = cancellation_token();

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
                State::Done | State::Cancelled | State::Sealed => break state,
                State::Wave { graylist } => {
                    self.current_wave += 1;
                    let Some(next_state) = cancellation
                        .run_until_cancelled(self.send_wave(graylist))
                        .await
                    else {
                        break State::Cancelled;
                    };
                    next_state
                }
                State::Backoff => {
                    // since backoff can be None, or run out of iterations,
                    // but appender should never give up we fall back to fixed backoff
                    let delay = retry.next().unwrap_or(DEFAULT_BACKOFF_TIME);
                    if self.current_wave > 5 {
                        warn!(
                            wave = %self.current_wave,
                            "Append wave failed, retrying with a new wave after {:?}. Status is {}", delay, self.nodeset_status
                        );
                    } else {
                        debug!(
                            wave = %self.current_wave,
                            "Append wave failed, retrying with a new wave after {:?}. Status is {}", delay, self.nodeset_status
                        );
                    }

                    if cancellation
                        .run_until_cancelled(tokio::time::sleep(delay))
                        .await
                        .is_none()
                    {
                        break State::Cancelled;
                    };

                    State::Wave {
                        graylist: NodeSet::default(),
                    }
                }
            }
        };

        match final_state {
            State::Done => {
                assert!(self.commit_resolver.is_none());

                metrics::counter!(BIFROST_SEQ_RECORDS_COMMITTED_TOTAL)
                    .increment(self.records.len() as u64);
                metrics::counter!(BIFROST_SEQ_RECORDS_COMMITTED_BYTES)
                    .increment(self.records.estimated_encode_size() as u64);
                metrics::histogram!(BIFROST_SEQ_APPEND_DURATION).record(start.elapsed());

                trace!(
                    wave = %self.current_wave,
                    "Append succeeded in {:?}, status {}",
                    start.elapsed(),
                    self.nodeset_status
                );
            }
            State::Cancelled => {
                trace!("Append cancelled");
                if let Some(commit_resolver) = self.commit_resolver.take() {
                    commit_resolver.error(AppendError::Shutdown(ShutdownError));
                }
            }
            State::Sealed => {
                trace!("Append ended because of sealing");
                if let Some(commit_resolver) = self.commit_resolver.take() {
                    commit_resolver.sealed();
                }
            }
            State::Backoff | State::Wave { .. } => {
                unreachable!()
            }
        }
    }

    #[instrument(skip_all, fields(wave = %self.current_wave))]
    async fn send_wave(&mut self, mut graylist: NodeSet) -> State {
        // select the spread
        let spread = match self.sequencer_shared_state.selector.select(
            &mut rand::rng(),
            &self.networking.metadata().nodes_config_ref(),
            &graylist,
        ) {
            Ok(spread) => spread,
            Err(_) => {
                graylist.clear();
                trace!(
                    %graylist,
                    "Cannot select a spread, perhaps too many nodes are graylisted, will clear the list and try again"
                );
                return State::Backoff;
            }
        };

        trace!(%graylist, %spread, "Sending append wave");
        let last_offset = self.records.last_offset(self.first_offset).unwrap();

        // todo: should be exponential backoff
        let store_timeout = *self
            .configuration
            .live_load()
            .bifrost
            .replicated_loglet
            .log_server_rpc_timeout;

        // track the in flight server ids
        let mut pending_servers = NodeSet::from_iter(spread.iter().copied());
        let mut store_tasks = JoinSet::new();

        for node_id in spread.iter().copied() {
            // do not attempt on nodes that we know they're committed || sealed
            if let Some(status) = self.checker.get_attribute(&node_id) {
                if status.committed || status.sealed {
                    pending_servers.remove(node_id);
                    continue;
                }
            }
            store_tasks.spawn({
                let store_task = LogServerStoreTask {
                    node_id,
                    sequencer_shared_state: self.sequencer_shared_state.clone(),
                    networking: self.networking.clone(),
                    first_offset: self.first_offset,
                    records: self.records.clone(),
                    rpc_router: self.store_router.clone(),
                    store_timeout,
                };
                async move { (node_id, store_task.run().await) }.in_current_tc()
            });
        }

        while let Some(store_result) = store_tasks.join_next().await {
            // unlikely to happen, but it's there for completeness
            if self.sequencer_shared_state.known_global_tail.is_sealed() {
                trace!(%pending_servers, %spread, "Loglet was sealed, stopping this sequencer appender");
                return State::Sealed;
            }
            let Ok((node_id, store_result)) = store_result else {
                // task panicked, ignore
                continue;
            };

            let stored = match store_result {
                StoreTaskStatus::Error(NetworkError::Shutdown(_)) => {
                    return State::Cancelled;
                }
                StoreTaskStatus::Error(NetworkError::Timeout(_)) => {
                    trace!(peer = %node_id, "Timeout waiting for node {} to commit a batch", node_id);
                    self.nodeset_status.merge(node_id, PerNodeStatus::timeout());
                    graylist.insert(node_id);
                    continue;
                }
                StoreTaskStatus::Error(err) => {
                    // couldn't send store command to remote server
                    // todo: only log if number of attempts is high.
                    debug!(peer = %node_id, %err, "Failed to send batch to node");
                    self.nodeset_status.merge(node_id, PerNodeStatus::failed());
                    graylist.insert(node_id);
                    continue;
                }
                StoreTaskStatus::Sealed => {
                    debug!(peer = %node_id, "Store task cancelled, the node is sealed");
                    self.checker
                        .set_attribute(node_id, NodeAttributes::sealed());
                    self.nodeset_status.merge(node_id, PerNodeStatus::Sealed);
                    continue;
                }
                StoreTaskStatus::Stored(stored) => {
                    trace!(peer = %node_id, "Store task completed");
                    stored
                }
            };

            // we had a response from this node and there is still a lot we can do
            match stored.status {
                Status::Ok => {
                    // only if status is okay that we remove this node
                    // from the gray list, and move to replicated list
                    self.checker
                        .set_attribute(node_id, NodeAttributes::committed());
                    self.nodeset_status.merge(node_id, PerNodeStatus::Committed);
                    pending_servers.remove(node_id);
                }
                Status::Sealed | Status::Sealing => {
                    self.checker
                        .set_attribute(node_id, NodeAttributes::sealed());
                    graylist.insert(node_id);
                }
                Status::Dropped => {
                    // overloaded, or request expired
                    debug!(peer = %node_id, status=?stored.status, "Store failed on peer. Peer is load shedding");
                    graylist.insert(node_id);
                }
                Status::Disabled => {
                    // overloaded, or request expired
                    debug!(peer = %node_id, status=?stored.status, "Store failed on peer. Peer's log-store is disabled");
                    graylist.insert(node_id);
                }
                Status::SequencerMismatch | Status::Malformed | Status::OutOfBounds => {
                    warn!(peer = %node_id, status=?stored.status, "Store failed on peer due to unexpected error, please check logs of the peer to investigate");
                    graylist.insert(node_id);
                }
            }

            if self.checker.check_write_quorum(|attr| attr.committed) {
                // resolve the commit if not resolved yet
                if let Some(resolver) = self.commit_resolver.take() {
                    self.sequencer_shared_state
                        .known_global_tail
                        .notify_offset_update(last_offset.next());
                    resolver.offset(last_offset);
                }
                // drop the permit
                self.permit.take();
                return State::Done;
            }
        }

        if self.checker.check_fmajority(|attr| attr.sealed).passed() {
            State::Sealed
        } else {
            // We couldn't achieve write quorum with this wave. We will try again, as fast as
            // possible until the graylist eats up enough nodes such that we won't be able to
            // generate node nodesets. Only then we backoff.
            State::Wave { graylist }
        }
    }
}

#[derive(Default, Debug, PartialEq, Clone, Copy)]
enum PerNodeStatus {
    #[default]
    NotAttempted,
    Failed {
        attempts: usize,
    },
    Timeout {
        attempts: usize,
    },
    Committed,
    Sealed,
}

impl Display for PerNodeStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PerNodeStatus::NotAttempted => write!(f, ""),
            PerNodeStatus::Failed { attempts } => write!(f, "E({})", attempts),
            PerNodeStatus::Committed => write!(f, "X"),
            PerNodeStatus::Timeout { attempts } => write!(f, "TIMEDOUT({})", attempts),
            PerNodeStatus::Sealed => write!(f, "SEALED"),
        }
    }
}

impl PerNodeStatus {
    fn timeout() -> Self {
        Self::Timeout { attempts: 1 }
    }
    fn failed() -> Self {
        Self::Failed { attempts: 1 }
    }
}

impl Merge for PerNodeStatus {
    fn merge(&mut self, other: Self) -> bool {
        use PerNodeStatus::*;
        match (*self, other) {
            (NotAttempted, NotAttempted) => false,
            (Committed, Committed) => false,
            (NotAttempted, e) => {
                *self = e;
                true
            }
            // we will not transition from committed to seal because
            // committed is more important for showing where did we write. Not that this is likely
            // to ever happen though.
            (Committed, _) => false,
            (Failed { attempts }, Failed { .. }) => {
                *self = Failed {
                    attempts: attempts + 1,
                };
                true
            }
            (Failed { attempts }, Timeout { .. }) => {
                *self = Timeout {
                    attempts: attempts + 1,
                };
                true
            }
            (Timeout { attempts }, Failed { .. }) => {
                *self = Failed {
                    attempts: attempts + 1,
                };
                true
            }
            (Timeout { attempts }, Timeout { .. }) => {
                *self = Timeout {
                    attempts: attempts + 1,
                };
                true
            }
            (_, Committed) => {
                *self = Committed;
                true
            }
            (Sealed, Sealed) => false,
            (_, Sealed) => {
                *self = Sealed;
                true
            }
            (Sealed, _) => false,
            _ => false,
        }
    }
}

#[derive(Default, Debug, Eq, PartialEq, Hash, Clone)]
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

impl Display for NodeAttributes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (self.committed, self.sealed) {
            // legend X = committed to be consistent with restatectl digest output
            (true, true) => write!(f, "X(S)"),
            (true, false) => write!(f, "X"),
            (false, true) => write!(f, "-(S)"),
            (false, false) => write!(f, ""),
        }
    }
}

#[derive(Debug)]
enum StoreTaskStatus {
    Sealed,
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

/// The task will retry to connect to the remote server if connection
/// was lost.
struct LogServerStoreTask<T> {
    node_id: PlainNodeId,
    sequencer_shared_state: Arc<SequencerSharedState>,
    networking: Networking<T>,
    first_offset: LogletOffset,
    records: Arc<[Record]>,
    rpc_router: RpcRouter<Store>,
    store_timeout: Duration,
}

impl<T: TransportConnect> LogServerStoreTask<T> {
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
    async fn run(mut self) -> StoreTaskStatus {
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

        result.into()
    }

    async fn send(&mut self) -> Result<StoreTaskStatus, NetworkError> {
        let server = self
            .sequencer_shared_state
            .log_server_manager
            .get(self.node_id);
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
            TailState::Sealed(_) => return Ok(StoreTaskStatus::Sealed),
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

        let incoming = match self.try_send(server).await {
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
                self.sequencer_shared_state.mark_as_maybe_sealed();
                return Ok(StoreTaskStatus::Sealed);
            }
            _ => {
                // all other status types are handled by the caller
            }
        }

        Ok(StoreTaskStatus::Stored(incoming.into_body()))
    }

    async fn try_send(&self, server: &RemoteLogServer) -> Result<Incoming<Stored>, NetworkError> {
        // let the log-server keep the store message a little longer than our own timeout, since
        // our timeout includes the connection time, the server should still try and store it even
        // if we timeout locally.
        let timeout_at = MillisSinceEpoch::after(self.store_timeout * 2);
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
            payloads: Arc::clone(&self.records),
            sequencer: *self.sequencer_shared_state.sequencer(),
            timeout_at: Some(timeout_at),
        };

        let store_start_time = Instant::now();

        match self
            .rpc_router
            .call_timeout(&self.networking, self.node_id, store, self.store_timeout)
            .await
        {
            Ok(incoming) => {
                server.store_latency().record(store_start_time.elapsed());
                Ok(incoming)
            }
            Err(NetworkError::Shutdown(shutdown)) => Err(NetworkError::Shutdown(shutdown)),
            Err(err) => Err(err),
        }
    }
}
