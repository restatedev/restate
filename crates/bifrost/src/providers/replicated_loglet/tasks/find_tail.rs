// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use tokio::task::JoinSet;
use tracing::{debug, error, info, instrument, trace, warn};

use restate_core::network::rpc_router::{RpcError, RpcRouter};
use restate_core::network::{Networking, Outgoing, TransportConnect};
use restate_core::TaskCenter;
use restate_types::config::Configuration;
use restate_types::logs::metadata::SegmentIndex;
use restate_types::logs::{LogId, LogletOffset, RecordCache, SequenceNumber};
use restate_types::net::log_server::{GetLogletInfo, LogServerRequestHeader, Status, WaitForTail};
use restate_types::net::replicated_loglet::{CommonRequestHeader, GetSequencerState};
use restate_types::replicated_loglet::{
    EffectiveNodeSet, ReplicatedLogletId, ReplicatedLogletParams,
};
use restate_types::PlainNodeId;

use super::{NodeTailStatus, RepairTail, RepairTailResult, SealTask};
use crate::loglet::util::TailOffsetWatch;
use crate::providers::replicated_loglet::replication::NodeSetChecker;
use crate::providers::replicated_loglet::rpc_routers::{LogServersRpc, SequencersRpc};

/// Represents a task to determine and repair the tail of the loglet by consulting f-majority
/// nodes in the nodeset assuming we are not the sequencer node.
///
/// This will communicate directly with log-servers to determine the status of the tail. If none of
/// the log-servers are sealed, we don't assume that the sequencer is alive and that we can contact
/// it to get the tail but this can be used as a side channel optimization.
///
/// If the loglet is being sealed partially, this will create a new seal task to assist (in case
/// the previous seal process crashed). Additionally, we will start a RepairTail task to ensure consistent
/// state of the records between known_global_tail and the max(local_tail) observed from f-majority
/// of sealed log-servers.
///
/// Any response from any log-server can update our view of the `known-global-tail` if that
/// server has observed a newer global tail than us but the calculated global tail will not set it.
pub struct FindTailTask<T> {
    log_id: LogId,
    segment_index: SegmentIndex,
    my_params: ReplicatedLogletParams,
    task_center: TaskCenter,
    networking: Networking<T>,
    logservers_rpc: LogServersRpc,
    sequencers_rpc: SequencersRpc,
    known_global_tail: TailOffsetWatch,
    record_cache: RecordCache,
}

pub enum FindTailResult {
    Open {
        global_tail: LogletOffset,
    },
    /// F-majority of nodes have responded at which _all_ have been sealed.
    /// Tail is consistent for all known-to-be sealed nodes (f-majority or more)
    Sealed {
        global_tail: LogletOffset,
    },
    Error(String),
}

impl<T: TransportConnect> FindTailTask<T> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        task_center: TaskCenter,
        log_id: LogId,
        segment_index: SegmentIndex,
        my_params: ReplicatedLogletParams,
        networking: Networking<T>,
        logservers_rpc: LogServersRpc,
        sequencers_rpc: SequencersRpc,
        known_global_tail: TailOffsetWatch,
        record_cache: RecordCache,
    ) -> Self {
        Self {
            task_center,
            log_id,
            segment_index,
            networking,
            my_params,
            logservers_rpc,
            sequencers_rpc,
            known_global_tail,
            record_cache,
        }
    }

    #[instrument(skip_all)]
    pub async fn run(self) -> FindTailResult {
        // Special case:
        // If all nodes in the nodeset is in "provisioning", we can confidently short-circuit
        // the result to LogletOffset::Oldest and the loglet is definitely unsealed.
        if self
            .my_params
            .nodeset
            .all_provisioning(&self.networking.metadata().nodes_config_ref())
        {
            return FindTailResult::Open {
                global_tail: LogletOffset::OLDEST,
            };
        }

        // Is the sequencer a potential candidate?
        //
        // If the sequencer is dead, let's not wait for too long on its response. But if
        // it's alive (or a newer generation is running on this node) then this check fails and
        // we won't spend time here.
        if let Ok(connection) = self
            .networking
            .node_connection(self.my_params.sequencer)
            .await
        {
            // todo: use cluster-state information when this becomes node-level available to avoid
            // the sequencer node if it's known to be dead.
            let get_seq_state = Outgoing::new(
                self.my_params.sequencer,
                GetSequencerState {
                    header: CommonRequestHeader {
                        log_id: self.log_id,
                        segment_index: self.segment_index,
                        loglet_id: self.my_params.loglet_id,
                    },
                },
            )
            .assign_connection(connection);
            // todo: configure timeout?
            if let Ok(seq_state) = self
                .sequencers_rpc
                .get_seq_state
                .call_outgoing_timeout(get_seq_state, Duration::from_millis(500))
                .await
            {
                let seq_state = seq_state.into_body();
                if seq_state.header.status.is_ok() {
                    let global_tail = seq_state
                        .header
                        .known_global_tail
                        .expect("global tail must be known by sequencer");
                    return if seq_state
                        .header
                        .sealed
                        .expect("sequencer must set sealed if status=ok")
                    {
                        FindTailResult::Sealed { global_tail }
                    } else {
                        FindTailResult::Open { global_tail }
                    };
                }
            }
        }
        // After this point we don't try the sequencer.

        // Be warned, this is a complex state machine.
        //
        // We need two pieces of information
        // 1) Global tail location
        // 2) Seal status
        //
        // How to determine tail?
        // 1- Use NodeSetChecker to find all possible nodes that can form an f-majority
        // 2- Send GetLogletInfo for all nodes in effective nodeset
        // 3- Keep checking responses as they arrive until f-majority of nodes return their tail+seal
        //
        // How to handle errors and what errors should we expect?
        // - ConnectionErrors or generation updates. Retry with backoff
        // - Sealing. Wait, and retry.
        // - Dropped (timeout). Retry.
        // - Disabled. Node cannot respond for some reason. Retry.
        //
        // We also need to update known_global_tail when we receive it from any response.
        // Requests to individual log-servers are sent as scatter-gather. We keep retrying in
        // sub-tasks until we exhaust the retry policy.
        'find_tail: loop {
            let effective_nodeset = EffectiveNodeSet::new(
                &self.my_params.nodeset,
                &self.networking.metadata().nodes_config_ref(),
            );

            let mut inflight_info_requests = JoinSet::new();
            for node in effective_nodeset.iter() {
                inflight_info_requests.spawn({
                    let tc = self.task_center.clone();
                    let networking = self.networking.clone();
                    let get_loglet_info_rpc = self.logservers_rpc.get_loglet_info.clone();
                    let known_global_tail = self.known_global_tail.clone();
                    let node = *node;
                    async move {
                        let task = FindTailOnNode {
                            node_id: node,
                            loglet_id: self.my_params.loglet_id,
                            get_loglet_info_rpc: &get_loglet_info_rpc,
                            known_global_tail: &known_global_tail,
                        };
                        tc.run_in_scope("find-tail-on-node", None, task.run(&networking))
                            .await
                    }
                });
            }

            // We'll only refresh our view of the effective nodeset if we retry the find-tail
            // procedure.
            let mut nodeset_checker = NodeSetChecker::<'_, NodeTailStatus>::new(
                &effective_nodeset,
                &self.networking.metadata().nodes_config_ref(),
                &self.my_params.replication,
            );

            while let Some(task_result) = inflight_info_requests.join_next().await {
                let Ok((node_id, info)) = task_result else {
                    // task panicked or runtime is shutting down.
                    continue;
                };
                nodeset_checker.merge_attribute(node_id, info);
                // Optimization: do we have other tasks that might have finished?
                // let's get as much as we can before making decisions.
                while let Some(Ok((node_id, info))) = inflight_info_requests.try_join_next() {
                    nodeset_checker.merge_attribute(node_id, info);
                }

                'check_nodeset: loop {
                    // Do we have f-majority responses yet?
                    if !nodeset_checker
                        .check_fmajority(NodeTailStatus::is_known)
                        .passed()
                    {
                        // no f-majority yet, keep waiting for info messages.
                        break 'check_nodeset;
                    }
                    // Three outcomes possible:
                    // Sealed. Good
                    // NoneSealed
                    // SomeSealed. Create a seal task and wait for seal on f-majority of nodes complete before retrying the find-tail.

                    // # Sealed
                    // F-majority sealed?
                    if nodeset_checker
                        .check_fmajority(NodeTailStatus::is_known_sealed)
                        .passed()
                    {
                        // todos:
                        //   1- Do we have some nodes that are unsealed? let's run a seal task in the
                        //      background, but we can safely return the result, iff local-tail is
                        //      consistently max-local-tail.
                        //
                        //   2- Repair the tail if max-tail != known_global_tail
                        //
                        // determine max-local tail
                        let max_local_tail: LogletOffset = nodeset_checker
                            .filter(NodeTailStatus::is_known_sealed)
                            .map(|(_, status)| status.local_tail().unwrap())
                            .max()
                            .expect("at least one node is known and sealed");
                        let current_known_global = self.known_global_tail.latest_offset();
                        if max_local_tail < current_known_global {
                            // what? Something went seriously wrong.
                            panic!(
                                "max_local_tail={} is less than known_global_tail={}",
                                max_local_tail, current_known_global
                            );
                        } else if max_local_tail == current_known_global ||
                        // max_local_tail > known_global_tail
                        // Does a write-quorum check pass for this tail? In other words, do we have
                        // all necessary copies replicated for the `max_local_tail`, or do we need to
                        // repair [known_global_tail..max-tail]?
                        nodeset_checker.check_write_quorum(|attribute| match attribute {
                            // We have all copies of the max-local-tail replicated. It's safe to
                            // consider this offset as committed.
                            NodeTailStatus::Known { local_tail, sealed } => {
                                *sealed && (*local_tail >= max_local_tail)
                            }
                            _ => false,
                        }) {
                            // Great. All nodes sealed and we have a stable value to return.
                            //
                            // todo: If some nodes have lower global-tail than max-local-tail, then
                            // broadcast a release to max-tail to avoid unnecessary repair if
                            // underreplication happened after this point.
                            inflight_info_requests.abort_all();
                            // Note: We don't set the known_global_tail watch to this value nor the
                            // seal bit. The caller can decide whether to do that or not based on the
                            // use-case.
                            return FindTailResult::Sealed {
                                global_tail: max_local_tail,
                            };
                        }
                        // F-majority sealed, but tail needs repair in range
                        // [current_known_global..max_local_tail]
                        //
                        // Although we have f-majority, it's not always guaranteed that we are
                        // able to form a write-quorum within this set of nodes. For instance, if
                        // replication-factor is 4 in a nodeset of 5 nodes, F-majority is 2 nodes which
                        // isn't enough to form write-quorum. In this case, we need to wait for more
                        // nodes to chime in. But we _only_ need this if max_local_tail is higher than
                        // current_known_global. Once we have enough sealed node that match
                        // write-quorum **and** f-majority, then we can repair the tail.
                        if nodeset_checker.check_write_quorum(NodeTailStatus::is_known) {
                            // We can start repair.
                            match RepairTail::new(
                                self.my_params.clone(),
                                self.task_center.clone(),
                                self.networking.clone(),
                                self.logservers_rpc.clone(),
                                self.record_cache.clone(),
                                self.known_global_tail.clone(),
                                current_known_global,
                                max_local_tail,
                            )
                            .run()
                            .await
                            {
                                RepairTailResult::Completed => {
                                    return FindTailResult::Sealed {
                                        global_tail: max_local_tail,
                                    }
                                }
                                RepairTailResult::DigestFailed
                                | RepairTailResult::ReplicationFailed { .. } => {
                                    // retry the whole find-tail procedure.
                                    info!(
                                        "Tail repair failed. Restarting FindTail task for loglet_id={}",
                                        self.my_params.loglet_id
                                    );
                                    // todo: configuration
                                    tokio::time::sleep(Duration::from_secs(2)).await;
                                    continue 'find_tail;
                                }
                                RepairTailResult::Shutdown(e) => {
                                    return FindTailResult::Error(e.to_string());
                                }
                            }
                        } else {
                            // wait for more nodes
                            break 'check_nodeset;
                        }
                    };

                    // # Some Sealed
                    // Not f-majority sealed, but is there any node that's sealed?
                    // Run a `SealTask` to assist in sealing.
                    if nodeset_checker.any(NodeTailStatus::is_known_sealed) {
                        // run seal task then retry the find-tail check.
                        let seal_task = SealTask::new(
                            self.task_center.clone(),
                            self.my_params.clone(),
                            self.logservers_rpc.seal.clone(),
                            self.known_global_tail.clone(),
                        );
                        debug!("Detected unsealed nodes. Running seal task to assist in sealing loglet_id={}", self.my_params.loglet_id);
                        // This returns when we have f-majority sealed.
                        if let Err(e) = seal_task.run(self.networking.clone()).await {
                            return FindTailResult::Error(format!(
                                "Failed to seal loglet_id={}: {:?}",
                                self.my_params.loglet_id, e
                            ));
                        }
                        inflight_info_requests.abort_all();
                        // retry the whole find-tail procedure.
                        trace!(
                            "Restarting FindTail task after sealing f-majority for loglet_id={}",
                            self.my_params.loglet_id
                        );
                        continue 'find_tail;
                    };

                    // # None Sealed
                    // F-majority and no one is sealed. Let's get the max local tail.
                    //
                    // Paths:
                    // - todo(asoli): [optimization] If sequencer is reachable. Let's try and get the known-global-tail from it.
                    // - Wait until any node reports that global_known_tail has reached the max-local-tail OR if any node is sealed, we will switch into SomeSealed state.
                    let max_local_tail: LogletOffset = nodeset_checker
                        .filter(NodeTailStatus::is_known)
                        .map(|(_, status)| status.local_tail().unwrap())
                        .max()
                        .expect("at least one node is known and sealed");

                    // Maybe we are already there?
                    if self.known_global_tail.latest_offset() >= max_local_tail {
                        return FindTailResult::Open {
                            global_tail: self.known_global_tail.latest_offset(),
                        };
                    }

                    // Is there a full write-quorum for the requested tail?
                    if nodeset_checker.check_write_quorum(|attribute| match attribute {
                        // We have all copies of the max-local-tail replicated. It's safe to
                        // consider this offset as committed.
                        NodeTailStatus::Known { local_tail, sealed } => {
                            // not sealed, and reached its local tail reached the max we
                            // are looking for.
                            !sealed && (*local_tail >= max_local_tail)
                        }
                        _ => false,
                    }) {
                        return FindTailResult::Open {
                            global_tail: max_local_tail,
                        };
                    }

                    // Broadcast to all nodes in effective nodeset `WaitForTail` to watch until
                    // known global tail reaches the max-local-tail found on f-majority of
                    // nodes.
                    let mut inflight_tail_update_watches = JoinSet::new();
                    for node in effective_nodeset.iter() {
                        let task = WaitForTailOnNode {
                            node_id: *node,
                            loglet_id: self.my_params.loglet_id,
                            wait_for_tail_rpc: self.logservers_rpc.wait_for_tail.clone(),
                            known_global_tail: self.known_global_tail.clone(),
                        };
                        inflight_tail_update_watches.spawn({
                            let tc = self.task_center.clone();
                            let networking = self.networking.clone();
                            async move {
                                tc.run_in_scope(
                                    "wait-for-tail-on-node",
                                    None,
                                    task.run(max_local_tail, networking),
                                )
                                .await
                            }
                        });
                    }
                    loop {
                        // Re-evaluation conditition is any of:
                        // - todo(asoli): [optimization] Sequencer is reachable and has moved past max_local_tail
                        // - Any node with global_known_tail that reached the max_local_tail we are looking for (due to RELEASE message)
                        // - Any node gets sealed. Back to drawing board.
                        // - todo(asoli): If write-quorum can be formed from unsealed nodes. Risk here is that if
                        //   sequencer is alive, we might move the global commit offset before it does.
                        // - More nodes chimed in (inflight-info-requests)
                        // - Timeout?
                        let updated = tokio::select! {
                            Some(Ok((node_id, tail_update))) = inflight_tail_update_watches.join_next() => {
                                // maybe sealed, maybe global is updated. Who knows.
                                nodeset_checker.merge_attribute(node_id, tail_update);
                                true
                            }
                            Some(Ok((node_id, info))) = inflight_info_requests.join_next() => {
                                nodeset_checker.merge_attribute(node_id, info);
                                true
                            }
                            else =>  false,
                        };

                        if nodeset_checker.any(NodeTailStatus::is_known_sealed) {
                            // A node got sealed. Let's re-evaluate to finish up this seal before
                            // we respond. We expect to switching to `SomeSealed`
                            inflight_tail_update_watches.abort_all();
                            continue 'check_nodeset;
                        }

                        if self.known_global_tail.latest_offset() >= max_local_tail {
                            return FindTailResult::Open {
                                global_tail: self.known_global_tail.latest_offset(),
                            };
                        }
                        // Is there a full write-quorum for the requested tail?
                        if nodeset_checker.check_write_quorum(|attribute| match attribute {
                            // We have all copies of the max-local-tail replicated. It's safe to
                            // consider this offset as committed.
                            NodeTailStatus::Known { local_tail, sealed } => {
                                // not sealed, and reached its local tail reached the max we
                                // are looking for.
                                !sealed && (*local_tail >= max_local_tail)
                            }
                            _ => false,
                        }) {
                            return FindTailResult::Open {
                                global_tail: max_local_tail,
                            };
                        }
                        if !updated {
                            // Nothing left to wait on. The tail didn't reach expected
                            // target and no more nodes are expected to send us responses.
                            return FindTailResult::Error(format!(
                                    "Could not determine a safe tail offset for loglet_id={}, perhaps too many nodes down? status={}",
                                    self.my_params.loglet_id, nodeset_checker));
                        }
                    }
                }
            }

            // We exhausted all retries on all nodes before finding the tail. We have no option but to
            // give up and return an error.
            return FindTailResult::Error(format!(
            "Insufficient nodes responded to GetLogletInfo requests, we cannot determine tail status of loglet_id={}, status={}",
            self.my_params.loglet_id, nodeset_checker,
        ));
        }
    }
}

pub(super) struct FindTailOnNode<'a> {
    pub(super) node_id: PlainNodeId,
    pub(super) loglet_id: ReplicatedLogletId,
    pub(super) get_loglet_info_rpc: &'a RpcRouter<GetLogletInfo>,
    pub(super) known_global_tail: &'a TailOffsetWatch,
}

impl<'a> FindTailOnNode<'a> {
    pub async fn run<T: TransportConnect>(
        self,
        networking: &'a Networking<T>,
    ) -> (PlainNodeId, NodeTailStatus) {
        let request_timeout = Configuration::pinned()
            .bifrost
            .replicated_loglet
            .log_server_rpc_timeout;

        let retry_policy = Configuration::pinned()
            .bifrost
            .replicated_loglet
            .log_server_retry_policy
            .clone();

        let mut retry_iter = retry_policy.into_iter();
        let mut attempt = 0;
        loop {
            attempt += 1;
            let request = GetLogletInfo {
                header: LogServerRequestHeader::new(
                    self.loglet_id,
                    self.known_global_tail.latest_offset(),
                ),
            };
            // loop and retry until this task is aborted.
            let maybe_info = tokio::time::timeout(
                request_timeout,
                self.get_loglet_info_rpc
                    .call(networking, self.node_id, request),
            )
            .await;

            match maybe_info {
                Ok(Ok(msg)) => {
                    self.known_global_tail
                        .notify_offset_update(msg.body().header.known_global_tail);
                    // We retry on the following errors.
                    match msg.body().status {
                        Status::Ok | Status::Sealed => {
                            return (
                                self.node_id,
                                NodeTailStatus::Known {
                                    local_tail: msg.body().header.local_tail,
                                    sealed: msg.body().header.sealed,
                                },
                            );
                        }
                        // retyrable errors
                        Status::Sealing | Status::Disabled | Status::Dropped => {
                            // fall-through for retries
                        }
                        // unexpected statuses
                        Status::SequencerMismatch | Status::OutOfBounds | Status::Malformed => {
                            warn!(
                                %attempt,
                                loglet_id = %self.loglet_id,
                                peer = %self.node_id,
                                "Unexpected status from log-server when calling GetLogletInfo: {:?}",
                                msg.body().status
                            );
                            return (self.node_id, NodeTailStatus::Unknown);
                        }
                    }
                }
                Ok(Err(RpcError::SendError(e))) => {
                    trace!(
                        %attempt,
                        "Failed to get loglet info from node_id={} for loglet_id={}: {:?}",
                        self.node_id,
                        self.loglet_id,
                        e.original
                    );
                }
                Ok(Err(RpcError::Shutdown(_))) => {
                    // RPC router has shutdown, terminating.
                    return (self.node_id, NodeTailStatus::Unknown);
                }
                Err(_timeout_error) => {
                    trace!(
                        %attempt,
                        "Timeout when getting loglet info from node_id={} for loglet_id={}. Configured timeout={:?} ",
                        self.node_id, self.loglet_id, request_timeout
                    );
                }
            }

            // Should we retry?
            if let Some(pause) = retry_iter.next() {
                trace!(
                    %attempt,
                    "Retrying to get loglet info from node_id={} and loglet_id={} after {:?}",
                    self.node_id,
                    self.loglet_id,
                    pause
                );
                tokio::time::sleep(pause).await;
            } else {
                trace!(
                    %attempt,
                    "Exhausted retries while attempting to get loglet-info from node_id={} and loglet_id={}",
                    self.node_id,
                    self.loglet_id,
                );
                return (self.node_id, NodeTailStatus::Unknown);
            }
        }
    }
}

struct WaitForTailOnNode {
    node_id: PlainNodeId,
    loglet_id: ReplicatedLogletId,
    wait_for_tail_rpc: RpcRouter<WaitForTail>,
    known_global_tail: TailOffsetWatch,
}

impl WaitForTailOnNode {
    pub async fn run<T: TransportConnect>(
        self,
        requested_tail: LogletOffset,
        networking: Networking<T>,
    ) -> (PlainNodeId, NodeTailStatus) {
        let request_timeout = Configuration::pinned()
            .bifrost
            .replicated_loglet
            .log_server_rpc_timeout;

        let retry_policy = Configuration::pinned()
            .bifrost
            .replicated_loglet
            .log_server_retry_policy
            .clone();

        let mut retry_iter = retry_policy.into_iter();
        loop {
            let request = WaitForTail {
                header: LogServerRequestHeader::new(
                    self.loglet_id,
                    self.known_global_tail.latest_offset(),
                ),
                query: restate_types::net::log_server::TailUpdateQuery::LocalOrGlobal(
                    requested_tail,
                ),
            };
            // loop and retry until this task is aborted.
            let maybe_updated = tokio::time::timeout(
                request_timeout,
                self.wait_for_tail_rpc
                    .call(&networking, self.node_id, request),
            )
            .await;

            match maybe_updated {
                Ok(Ok(msg)) => {
                    self.known_global_tail
                        .notify_offset_update(msg.body().header.known_global_tail);
                    // We retry on the following errors.
                    match msg.body().status {
                        Status::Ok | Status::Sealed => {
                            return (
                                self.node_id,
                                NodeTailStatus::Known {
                                    local_tail: msg.body().header.local_tail,
                                    sealed: msg.body().header.sealed,
                                },
                            );
                        }
                        // retyrable errors
                        Status::Sealing | Status::Disabled | Status::Dropped => {
                            // fall-through for retries
                        }
                        // unexpected statuses
                        Status::SequencerMismatch | Status::OutOfBounds | Status::Malformed => {
                            error!("Unexpected status from log-server node_id={} when waiting for tail update for loglet_id={}: {:?}", self.node_id, self.loglet_id, msg.body().status);
                            return (self.node_id, NodeTailStatus::Unknown);
                        }
                    }
                }
                Ok(Err(RpcError::SendError(e))) => {
                    trace!(
                        "Failed to watch loglet tail updates from node_id={} for loglet_id={}: {:?}",
                        self.node_id, self.loglet_id, e.original
                    );
                }
                Ok(Err(RpcError::Shutdown(_))) => {
                    // RPC router has shutdown, terminating.
                    return (self.node_id, NodeTailStatus::Unknown);
                }
                Err(_timeout_error) => {
                    trace!(
                        "Timeout when attempting to watch loglet tail updates from node_id={} for loglet_id={}. Configured timeout={:?} ",
                        self.node_id, self.loglet_id, request_timeout
                    );
                }
            }

            // Should we retry?
            if let Some(pause) = retry_iter.next() {
                trace!(
                    "Retrying to watch loglet tail update from node_id={} and loglet_id={} after {:?}",
                    self.node_id,
                    self.loglet_id,
                    pause
                );
                tokio::time::sleep(pause).await;
            } else {
                trace!("Exhausted retries while attempting to watch loglet tail update from node_id={} and loglet_id={}", self.node_id, self.loglet_id);
                return (self.node_id, NodeTailStatus::Unknown);
            }
        }
    }
}
