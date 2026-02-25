// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::Deref;
use std::sync::LazyLock;
use std::{cmp::Ordering, fmt::Display, sync::Arc, time::Duration};

use adaptive_timeout::{AdaptiveTimeout, BackoffInterval, TimeoutConfig};
use adaptive_timeout::{LatencyTracker, TrackerConfig};
use parking_lot::Mutex;
use tokio::time::Instant;
use tokio::{sync::OwnedSemaphorePermit, task::JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::{debug, instrument, trace, warn};

use restate_core::TaskCenter;
use restate_core::network::{NetworkSender, RpcError, Swimlane};
use restate_core::{
    Metadata, TaskCenterFutureExt,
    network::{Networking, TransportConnect},
};
use restate_serde_util::ByteCount;
use restate_time_util::DurationExt;
use restate_types::replicated_loglet::Spread;
use restate_types::retries::with_jitter;
use restate_types::{
    Merge, PlainNodeId,
    config::Configuration,
    live::Live,
    logs::{LogletOffset, Record, SequenceNumber, TailState},
    net::log_server::{LogServerRequestHeader, Status, Store, StoreFlags, Stored},
    replication::{DecoratedNodeSet, NodeSet, NodeSetChecker},
    time::MillisSinceEpoch,
};

use super::{RecordsExt, SequencerSharedState};
use crate::loglet::{AppendError, LogletCommitResolver};
use crate::providers::replicated_loglet::replication::spread_selector::SpreadSelectorError;

static STORE_LATENCY_TRACKER: LazyLock<Mutex<LatencyTracker<PlainNodeId, Instant>>> =
    LazyLock::new(|| {
        Mutex::new(LatencyTracker::<PlainNodeId, Instant>::new(
            TrackerConfig::default(),
        ))
    });

const TONE_ESCALATION_THRESHOLD: u32 = 5;

enum State {
    Wave,
    Backoff,
    Done,
    Sealed,
    Cancelled,
    WriteUnavailable,
}

/// Appender makes sure a batch of records will run to completion
pub(crate) struct SequencerAppender<T> {
    sequencer_shared_state: Arc<SequencerSharedState>,
    networking: Networking<T>,
    first_offset: LogletOffset,
    records: Arc<[Record]>,
    payload_size: u32,
    checker: NodeSetChecker<NodeAttributes>,
    nodeset_status: DecoratedNodeSet<PerNodeStatus>,
    current_wave: u32,
    // permit is held during the entire live
    // of the batch to limit the number of
    // inflight batches
    permit: Option<OwnedSemaphorePermit>,
    commit_resolver: Option<LogletCommitResolver>,
    configuration: Live<Configuration>,
    // nodes that should be avoided by the spread selector
    graylist: NodeSet,
}

impl<T: TransportConnect> SequencerAppender<T> {
    pub fn new(
        sequencer_shared_state: Arc<SequencerSharedState>,
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
            &Metadata::with_current(|m| m.nodes_config_ref()),
            sequencer_shared_state.selector.replication_property(),
        );

        let nodeset_status =
            DecoratedNodeSet::from(sequencer_shared_state.selector.nodeset().clone());

        let mut payload_size: u32 = 0;
        for record in records.iter() {
            // it's safe to cast since a record would be never be larger than 4GiB.
            let record_size = record.estimated_encode_size() as u32;
            sequencer_shared_state.stats.record_size(record_size);
            payload_size += record_size;
        }

        Self {
            sequencer_shared_state,
            networking,
            first_offset,
            records,
            payload_size,
            checker,
            nodeset_status,
            current_wave: 0,
            permit: Some(permit),
            commit_resolver: Some(commit_resolver),
            configuration: Configuration::live(),
            graylist: NodeSet::default(),
        }
    }

    #[tracing::instrument(
        level="debug",
        skip(self),
        fields(
            log_id=%self.sequencer_shared_state.loglet_id().log_id(),
            loglet_id=%self.sequencer_shared_state.loglet_id(),
            first_offset=%self.first_offset,
            to_offset=%self.records.last_offset(self.first_offset).unwrap(),
            length=%self.records.len(),
        )
    )]
    pub async fn run(mut self, cancellation_token: CancellationToken) {
        let start = Instant::now();
        // initial wave has 0 replicated and 0 gray listed node
        let mut state = State::Wave;

        // this loop retries forever or until the task is cancelled
        let final_state = loop {
            let store_backoff = self
                .configuration
                .live_load()
                .bifrost
                .replicated_loglet
                .rpc_timeout;

            let delay_config = AdaptiveTimeout::new(TimeoutConfig {
                backoff: store_backoff,
                quantile: 0.90,     // base timeout on P90
                safety_factor: 1.0, // do not overshoot the delay between waves
            });
            state = match state {
                // termination conditions
                State::Done | State::Cancelled | State::Sealed | State::WriteUnavailable => {
                    break state;
                }
                State::Wave => {
                    self.current_wave = self.current_wave.wrapping_add(1);
                    // # Why is this cancellation safe?
                    // Because we don't await any futures inside the join_next() loop, so we are
                    // confident that have cancelled before resolving the commit token.
                    // We want to make sure we don't cancel _after_ updating the global offset, *then* reporting Cancelled.
                    // This is because we don't want appenders after our offset to make progress,
                    // therefore (potentially) dropping records in the writer prefix. Even if a store was
                    // fully replicated and we cancelled before updating the tail, that's an acceptable
                    // and safe result because we didn't acknowledge the append to the writer and from
                    // their perspective it has failed, and because the global tail was not moved, all
                    // appends after this one cannot move the global tail as well.
                    let wave_start = Instant::now();
                    let Some(next_state) = cancellation_token
                        .run_until_cancelled(self.send_wave(store_backoff))
                        .await
                    else {
                        break State::Cancelled;
                    };
                    self.sequencer_shared_state
                        .stats
                        .record_wave_latency(wave_start.elapsed());
                    next_state
                }
                State::Backoff => {
                    // we delay the next wave by the same amount as store timeout of the wave.
                    let delay = delay_config.select_timeout(
                        &mut STORE_LATENCY_TRACKER.lock(),
                        self.sequencer_shared_state.nodeset().iter(),
                        self.current_wave,
                        Instant::now(),
                    );

                    let delay = with_jitter(delay, 0.3);
                    if self.current_wave >= TONE_ESCALATION_THRESHOLD {
                        warn!(
                            wave = %self.current_wave,
                            log_id = %self.sequencer_shared_state.loglet_id().log_id(),
                            loglet_id = %self.sequencer_shared_state.loglet_id(),
                            first_offset = %self.first_offset,
                            to_offset = %self.records.last_offset(self.first_offset).unwrap(),
                            length = %self.records.len(),
                            size = %ByteCount::from(self.payload_size),
                            "Append wave failed, retrying with a new wave after {}. Status is {}", delay.friendly(), self.nodeset_status
                        );
                    } else {
                        debug!(
                            wave = %self.current_wave,
                            "Append wave failed, retrying with a new wave after {}. Status is {}", delay.friendly(), self.nodeset_status
                        );
                    }

                    if cancellation_token
                        .run_until_cancelled(tokio::time::sleep(delay))
                        .await
                        .is_none()
                    {
                        break State::Cancelled;
                    };

                    State::Wave
                }
            }
        };

        match final_state {
            State::Done => {
                assert!(self.commit_resolver.is_none());
                self.sequencer_shared_state
                    .stats
                    .record_waves_per_append(self.current_wave);

                self.sequencer_shared_state
                    .stats
                    .increment_committed(self.records.len() as u64);

                self.sequencer_shared_state
                    .stats
                    .increment_bytes_committed(self.records.estimated_encode_size() as u64);

                self.sequencer_shared_state
                    .stats
                    .record_append_latency(start.elapsed());

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
                    commit_resolver.error(AppendError::ReconfigurationNeeded(
                        "sequencer is draining".into(),
                    ));
                }
            }
            State::WriteUnavailable => {
                trace!("Loglet has not enough writable nodes");
                if let Some(commit_resolver) = self.commit_resolver.take() {
                    commit_resolver.error(AppendError::ReconfigurationNeeded(
                        "loglet is write unavailable".into(),
                    ));
                }
            }
            State::Sealed => {
                trace!("Append ended because of sealing");
                if let Some(commit_resolver) = self.commit_resolver.take() {
                    commit_resolver.sealed();
                }
            }
            State::Backoff | State::Wave => {
                unreachable!()
            }
        }
    }

    fn reset_graylist(&mut self) {
        self.graylist.clear();
        // add back the sealed nodes to the gray list, those will never be writeable again.
        self.graylist.extend(
            self.checker
                .filter(|attr| attr.sealed)
                .map(|(node_id, _)| *node_id),
        );
    }

    fn generate_spread(&mut self) -> Result<Spread, SpreadSelectorError> {
        let rng = &mut rand::rng();
        let nodes_config = Metadata::with_current(|m| m.nodes_config_ref());
        TaskCenter::with_current(|tc| {
            let cs = tc.cluster_state();
            for node_id in self.sequencer_shared_state.nodeset().iter() {
                if !cs.is_alive(node_id.into()) {
                    self.graylist.insert(*node_id);
                }
            }
        });

        match self
            .sequencer_shared_state
            .selector
            .select(rng, &nodes_config, &self.graylist)
        {
            Ok(spread) => Ok(spread),
            Err(err) => {
                trace!(
                    nodeset_status = %self.nodeset_status,
                    graylist = %self.graylist,
                    %err,
                    "Cannot select a spread, perhaps too many nodes are graylisted, will clear the list and try again"
                );
                // In this case, we will ignore the cluster state and try all nodes.
                self.reset_graylist();
                self.sequencer_shared_state
                    .selector
                    .select(rng, &nodes_config, &self.graylist)
            }
        }
    }

    #[instrument(skip_all, fields(wave = %self.current_wave))]
    async fn send_wave(&mut self, store_backoff: BackoffInterval) -> State {
        let timeout_config = AdaptiveTimeout::new(TimeoutConfig {
            backoff: store_backoff,
            quantile: 0.9999,   // base timeout on P99.99
            safety_factor: 2.0, // 2x headroom over the observed quantile
        });

        // select the spread
        let spread = match self.generate_spread() {
            Ok(spread) => spread,
            Err(err @ SpreadSelectorError::InsufficientWriteableNodes) => {
                trace!(
                    nodeset_status = %self.nodeset_status,
                    "Cannot select a spread: {err}"
                );
                return State::WriteUnavailable;
            }
        };

        trace!(graylist = %self.graylist, %spread, wave = %self.current_wave, nodeset_status = %self.nodeset_status, "Sending append wave");
        let last_offset = self.records.last_offset(self.first_offset).unwrap();

        // track the in flight server ids
        let mut pending_servers: NodeSet = spread
            .iter()
            .filter(|node_id| {
                // do not attempt on nodes that we know they're committed || sealed
                !self
                    .checker
                    .get_attribute(node_id)
                    .is_some_and(|status| status.committed || status.sealed)
            })
            .copied()
            .collect();
        let mut store_tasks = JoinSet::new();

        let store_timeout = timeout_config.select_timeout(
            &mut STORE_LATENCY_TRACKER.lock(),
            pending_servers.iter(),
            self.current_wave,
            Instant::now(),
        );

        for node_id in pending_servers.iter().copied() {
            store_tasks
                .build_task()
                .name(&format!("store-to-{node_id}"))
                .spawn({
                    let store_task = LogServerStoreTask {
                        state: StoreState::default(),
                        node_id,
                        sequencer_shared_state: self.sequencer_shared_state.clone(),
                        networking: self.networking.clone(),
                        first_offset: self.first_offset,
                        records: self.records.clone(),
                        store_timeout,
                    };
                    async move { (node_id, store_task.run().await) }.in_current_tc()
                })
                .unwrap();
        }

        // NOTE: It's very important to keep this loop cancellation safe. If the appender future
        // was cancelled, we don't want to move the global commit offset.
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
                StoreTaskStatus::Shutdown => {
                    return State::Cancelled;
                }
                StoreTaskStatus::Error(err) => {
                    // couldn't send store command to remote server
                    if self.current_wave >= TONE_ESCALATION_THRESHOLD {
                        debug!(peer = %node_id, %err, "Failed to send batch to node");
                    } else {
                        trace!(peer = %node_id, %err, "Failed to send batch to node");
                    }
                    self.nodeset_status
                        .merge(node_id, PerNodeStatus::failed(err));
                    self.graylist.insert(node_id);
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

            // We had a response from this node and there is still a lot we can do
            match stored.status {
                Status::Unknown => {
                    warn!(peer = %node_id, "Store failed on peer. Unknown error!");
                    self.graylist.insert(node_id);
                }
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
                    self.graylist.insert(node_id);
                }
                Status::Dropped => {
                    // Overloaded, or request expired
                    debug!(peer = %node_id, status=?stored.status, "Store failed on peer. Peer is load shedding");
                    self.graylist.insert(node_id);
                }
                Status::Disabled => {
                    debug!(peer = %node_id, status=?stored.status, "Store failed on peer. Peer's log-store is disabled");
                    self.graylist.insert(node_id);
                }
                Status::SequencerMismatch | Status::Malformed | Status::OutOfBounds => {
                    warn!(peer = %node_id, status=?stored.status, "Store failed on peer due to unexpected error, please check logs of the peer to investigate");
                    self.graylist.insert(node_id);
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
            State::Backoff
        }
    }
}

#[derive(Default, Debug)]
enum PerNodeStatus {
    #[default]
    NotAttempted,
    Failed {
        attempts: usize,
        last_err: RpcError,
    },
    Committed,
    Sealed,
}

impl Display for PerNodeStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PerNodeStatus::NotAttempted => write!(f, ""),
            PerNodeStatus::Failed { attempts, last_err } => {
                write!(f, "ERROR(attempts={attempts}, last_err='{last_err}')")
            }
            PerNodeStatus::Committed => write!(f, "COMMITTED"),
            PerNodeStatus::Sealed => write!(f, "SEALED"),
        }
    }
}

impl PerNodeStatus {
    fn failed(err: RpcError) -> Self {
        Self::Failed {
            attempts: 1,
            last_err: err,
        }
    }
}

impl Merge for PerNodeStatus {
    fn merge(&mut self, other: Self) -> bool {
        use PerNodeStatus::*;
        match (&self, other) {
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
            (
                Failed { attempts: a1, .. },
                Failed {
                    attempts: a2,
                    last_err,
                },
            ) => {
                *self = Failed {
                    attempts: *a1 + a2,
                    last_err,
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
    Error(RpcError),
    Shutdown,
}

impl From<Result<StoreTaskStatus, RpcError>> for StoreTaskStatus {
    fn from(value: Result<StoreTaskStatus, RpcError>) -> Self {
        match value {
            Ok(result) => result,
            Err(err) => Self::Error(err),
        }
    }
}

#[derive(Default)]
enum StoreState {
    #[default]
    New,
    WaitingTail,
    Sending {
        started_at: Instant,
    },
    Failed,
    StoredReceived,
}

/// The task will retry to connect to the remote server if connection
/// was lost.
struct LogServerStoreTask<T> {
    state: StoreState,
    node_id: PlainNodeId,
    sequencer_shared_state: Arc<SequencerSharedState>,
    networking: Networking<T>,
    first_offset: LogletOffset,
    records: Arc<[Record]>,
    store_timeout: Duration,
}

impl<T> Drop for LogServerStoreTask<T> {
    fn drop(&mut self) {
        if let StoreState::Sending { started_at } = self.state {
            trace!(
                log_id = %self.sequencer_shared_state.loglet_id().log_id(),
                loglet_id = %self.sequencer_shared_state.loglet_id(),
                first_offset=%self.first_offset,
                "Sequencer: store task for peer {} dropped, age={}",
                self.node_id,
                started_at.elapsed().friendly()
            );
            STORE_LATENCY_TRACKER.lock().record_latency_from(
                &self.node_id,
                started_at,
                Instant::now(),
            );
        }
    }
}

impl<T: TransportConnect> LogServerStoreTask<T> {
    #[instrument(
        skip_all,
        fields(
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
                trace!(
                    result = ?status,
                    "Got store result from log server"
                );
            }
            Err(err) => {
                self.state = StoreState::Failed;
                debug!(
                    log_id = %self.sequencer_shared_state.loglet_id().log_id(),
                    loglet_id = %self.sequencer_shared_state.loglet_id(),
                    error = %err,
                    "Failed to send store to log server node {}", self.node_id
                );
            }
        }

        result.into()
    }

    async fn send(&mut self) -> Result<StoreTaskStatus, RpcError> {
        self.state = StoreState::WaitingTail;
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

        trace!(global_tail = ?self.sequencer_shared_state.known_global_tail.get().deref(), "Find tail state");
        let tail_state = tokio::select! {
            Ok(l) = server_local_tail => l,
            Ok(g) = global_tail => g,
            else => return Ok(StoreTaskStatus::Shutdown),
        };
        trace!(?tail_state, global_tail = ?self.sequencer_shared_state.known_global_tail.get().deref(), "Found tail state");

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

        let started_at = Instant::now();
        self.state = StoreState::Sending { started_at };
        let stored = self.try_send().await?;

        let latency = STORE_LATENCY_TRACKER.lock().record_latency_from(
            &self.node_id,
            started_at,
            Instant::now(),
        );

        server.store_latency().record(latency);
        self.state = StoreState::StoredReceived;
        server.local_tail().notify_offset_update(stored.local_tail);

        match stored.status {
            Status::Sealing | Status::Sealed => {
                server.local_tail().notify_seal();
                self.sequencer_shared_state.mark_as_maybe_sealed();
                return Ok(StoreTaskStatus::Sealed);
            }
            _ => {
                // all other status types are handled by the caller
            }
        }

        Ok(StoreTaskStatus::Stored(stored))
    }

    async fn try_send(&self) -> Result<Stored, RpcError> {
        let timeout_at = MillisSinceEpoch::after(self.store_timeout);
        let loglet_id = *self.sequencer_shared_state.loglet_id();
        let store = Store {
            header: LogServerRequestHeader::new(
                loglet_id,
                self.sequencer_shared_state
                    .known_global_tail
                    .latest_offset(),
            ),
            first_offset: self.first_offset,
            flags: StoreFlags::empty(),
            known_archived: LogletOffset::INVALID,
            payloads: Arc::clone(&self.records).into(),
            sequencer: *self.sequencer_shared_state.sequencer(),
            timeout_at: Some(timeout_at),
        };

        // note: we are over-indexing on the fact that currently the sequencer will send one
        // message at a time per log-server. My argument to make us not sticking to a single
        // connection is that the complexity with the previous design didn't add any value. When we
        // support pipelined writes, it's unlikely that we'll also be doing the coordination through
        // the offset watch as we are currently doing (due to its lock-contention downside). It'll be a different design altogether.
        let stored = self
            .networking
            .call_rpc(
                self.node_id,
                Swimlane::BifrostData,
                store,
                Some(loglet_id.into()),
                Some(self.store_timeout),
            )
            .await
            .inspect_err(|err| {
                if let RpcError::Timeout(spent) = err {
                    // record the time spent
                    STORE_LATENCY_TRACKER.lock().record_latency(
                        &self.node_id,
                        *spent,
                        Instant::now(),
                    );
                }
            })?;

        Ok(stored)
    }
}
