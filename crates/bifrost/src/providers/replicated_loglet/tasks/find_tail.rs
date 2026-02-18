// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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
use tokio::time::Instant;
use tracing::{Instrument, Span, debug, error, info, instrument, trace, warn};

use restate_core::network::{NetworkSender, Networking, RpcError, Swimlane, TransportConnect};
use restate_core::{Metadata, TaskCenter, TaskCenterFutureExt};
use restate_types::cluster_state::ClusterState;
use restate_types::config::Configuration;
use restate_types::logs::metadata::SegmentIndex;
use restate_types::logs::{
    LogId, LogletId, LogletOffset, RecordCache, SequenceNumber, TailOffsetWatch,
};
use restate_types::net::log_server::{GetLogletInfo, LogServerRequestHeader, Status, WaitForTail};
use restate_types::net::replicated_loglet::{CommonRequestHeader, GetSequencerState};
use restate_types::nodes_config::{NodesConfigError, NodesConfiguration};
use restate_types::replicated_loglet::{LogNodeSetExt, ReplicatedLogletParams};
use restate_types::replication::NodeSetChecker;
use restate_types::{GenerationalNodeId, PlainNodeId};

use super::{NodeTailStatus, RepairTail, RepairTailResult, SealTask};
use crate::providers::replicated_loglet::loglet::FindTailFlags;

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
    networking: Networking<T>,
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
        log_id: LogId,
        segment_index: SegmentIndex,
        my_params: ReplicatedLogletParams,
        networking: Networking<T>,
        known_global_tail: TailOffsetWatch,
        record_cache: RecordCache,
    ) -> Self {
        Self {
            log_id,
            segment_index,
            networking,
            my_params,
            known_global_tail,
            record_cache,
        }
    }

    #[instrument(skip_all)]
    pub async fn run(self, opts: FindTailFlags) -> FindTailResult {
        let metadata = Metadata::current();
        let mut nodes_config = metadata.updateable_nodes_config();
        // Special case:
        // If all nodes in the nodeset is in "provisioning", we can confidently short-circuit
        // the result to LogletOffset::Oldest and the loglet is definitely unsealed.
        if self
            .my_params
            .nodeset
            .all_provisioning(nodes_config.live_load())
        {
            return FindTailResult::Open {
                global_tail: LogletOffset::OLDEST,
            };
        }

        let my_node = metadata.my_node_id();

        // If the sequencer is dead, let's not wait for too long on its response. But if
        // it's alive (or a newer generation is running on this node) then this check fails and
        // we won't spend time here.
        let cs = TaskCenter::with_current(|tc| tc.cluster_state().clone());

        let get_seq_state = GetSequencerState {
            header: CommonRequestHeader {
                log_id: self.log_id,
                segment_index: self.segment_index,
                loglet_id: self.my_params.loglet_id,
            },
            force_seal_check: opts == FindTailFlags::ForceSealCheck,
        };

        if is_sequencer_alive(&cs, self.my_params.sequencer, my_node) {
            trace!(
                loglet_id = %self.my_params.loglet_id,
                sequencer = %self.my_params.sequencer,
               "Asking the sequencer about the tail of the loglet"
            );
            if let Ok(seq_state) = self
                .networking
                .call_rpc(
                    self.my_params.sequencer,
                    Swimlane::General,
                    get_seq_state,
                    Some(self.my_params.loglet_id.into()),
                    // todo: configure timeout?
                    Some(Duration::from_millis(500)),
                )
                .await
                && seq_state.header.status.is_none()
            {
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
        // After this point we don't try the sequencer.

        // Be warned, this is a complex state machine.
        //
        // We need to determine two pieces of information:
        // 1) Seal status
        // 2) Global tail offset (global committed)
        //
        // # 1. Determining the seal status:
        // - SEALED: if f-majority of nodes are SEALED
        // - OPEN: if write-quorum of nodes are OPEN
        // - UNKNOWN: We cannot establish any of the above due to insufficient responses, if we can
        // determine a safe tail value, we should return it. Therefore, in this routine, we
        // sometimes return `Open` when we are not entirely sure that we are sealed as long as we
        // uphold the invariants described below.
        //
        // # 2. How to determine the global tail offset?
        // The global commit offset (tail) is the highest offset that was fully committed
        // (write-quorum) and that was likely acknowledged to writers as a committed offset. The
        // correctness of the consensus protocol rely on us upholding the following invariants:
        //   a) If we report that an offset is globally committed, no future find-tail call under
        //   any failure scenario will report a lower value.
        //   b) If a loglet is sealed at offset N, the loglet *must* remain sealed for all higher offset.
        //   c) It's acceptable for a global offset to be observed one time as open then later as
        //   sealed. The opposite _might_ happen without impacting correctness as long as it's
        //   exactly the same offset.
        //   d) A client that received commit acknowledgement for a record *must* observe this
        //   record in future reads of the loglet. This means that if we decided that tail is N and
        //   the loglet is SEALED, no writer will ever receive an acknowledgment for any records
        //   after offset N, even if such records have eventually been written to the loglet.
        //
        //   This is to uphold the invariant of bifrost; if this state machine returned a tail
        //   offset and the SEALED status, bifrost can use this value as the tail offset of the
        //   segment when sealing/updating the chain, even if future calls to this function
        //   returned higher offsets.
        //
        //   In order to achieve this we require:
        //   1- Responses from f-majority of nodes. This guarantees that we observe the maximum
        //   offset that was potentially committed partially or fully.
        //   2- We piggyback on `known_global_tail` that's communicated through network headers to
        //   speed up finding the previously agreed upon tail.
        //
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
            let effective_nodeset = self
                .my_params
                .nodeset
                .to_effective(nodes_config.live_load());

            let mut inflight_info_requests = JoinSet::new();
            for node in effective_nodeset.iter() {
                inflight_info_requests
                    .build_task()
                    .name("find-tail")
                    .spawn({
                        let networking = self.networking.clone();
                        let known_global_tail = self.known_global_tail.clone();
                        let node = *node;
                        async move {
                            let task = FindTailOnNode {
                                node_id: node,
                                loglet_id: self.my_params.loglet_id,
                                known_global_tail: &known_global_tail,
                            };
                            task.run(&networking).await
                        }
                        .in_current_tc()
                        .instrument(Span::current())
                    })
                    .expect("to spawn find tail on node");
            }

            // We'll only refresh our view of the effective nodeset if we retry the find-tail
            // procedure.
            let mut nodeset_checker = NodeSetChecker::<NodeTailStatus>::new(
                &effective_nodeset,
                nodes_config.live_load(),
                &self.my_params.replication,
            );

            // To determine seal status. We need f-majority sealed or write-quorum of open. Whichever
            // comes first determines the seal status.
            //
            // To determine the tail, we need to f-majority responses. The maximum local offset of
            // that that set becomes the convergence target.
            //
            // - f-majority of nodes to respond with their tail+seal status
            // - write-quorum of nodes
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
                            // what? Something went seriously wrong. This indicates that we somehow
                            // have previously determined that global tail is higher than the max
                            // observed local tail on f-majority of nodes.
                            panic!(
                                "max_local_tail={max_local_tail} is less than known_global_tail={current_known_global}"
                            );
                        } else if max_local_tail == current_known_global ||
                        // max_local_tail > known_global_tail
                        // Does a write-quorum check pass for this tail? In other words, do we have
                        // all necessary copies replicated for the `max_local_tail`, or do we need to
                        // repair [known_global_tail..max-tail]?
                        nodeset_checker.check_write_quorum(|attribute| match attribute {
                            // We have all copies of the max-local-tail replicated. It's safe to
                            // consider this offset as committed.
                            NodeTailStatus::Known { local_tail, .. } => {
                                *local_tail >= max_local_tail
                            }
                            _ => false,
                        }) {
                            // Great. All nodes sealed and we have a stable value to return.
                            //
                            // todo: If some nodes have lower global-tail than max-local-tail, then
                            // broadcast a release to max-tail to avoid unnecessary repair if
                            // underreplication happened after this point.
                            //
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
                                self.networking.clone(),
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
                                    };
                                }
                                RepairTailResult::DigestFailed
                                | RepairTailResult::ReplicationFailed => {
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
                        debug!(
                            "Detected unsealed nodes. Running seal task to assist in sealing loglet_id={}",
                            self.my_params.loglet_id
                        );
                        // run seal task then retry the find-tail check.
                        // This returns when we have f-majority sealed.
                        if let Err(e) = SealTask::run(
                            &self.my_params,
                            &self.known_global_tail,
                            &self.networking,
                        )
                        .await
                        {
                            return FindTailResult::Error(format!(
                                "Failed to seal loglet_id={}: {:?}",
                                self.my_params.loglet_id, e
                            ));
                        }
                        // Just to make our intention clear that we want to drain the set and abort
                        // all tasks. This happens automatically on the next iteration of the loop
                        // but it's left here for visibility.
                        drop(inflight_info_requests);
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

                    trace!(
                        loglet_id = %self.my_params.loglet_id,
                        "Max local tail was determined to be {}", max_local_tail,
                    );
                    let mut last_updated = Instant::now();

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

                    // Maybe we are already there?
                    // NOTE: This is a hack-ish optimization that treats the `Open` status as
                    // more like "probably open" because we _might_ actually be sealed but we just
                    // don't know it yet.
                    if self.known_global_tail.latest_offset() >= max_local_tail {
                        // todo: add an explicit ProbablyOpen/Unknown response variant
                        return FindTailResult::Open {
                            global_tail: self.known_global_tail.latest_offset(),
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
                            known_global_tail: self.known_global_tail.clone(),
                        };
                        inflight_tail_update_watches
                            .build_task()
                            .name(&format!("wait-for-tail-on-{node}"))
                            .spawn({
                                let networking = self.networking.clone();
                                task.run(max_local_tail, networking).in_current_tc()
                            })
                            .expect("to spawn wait for tail on node task");
                    }
                    trace!(
                        loglet_id = %self.my_params.loglet_id,
                        %max_local_tail,
                        "Waiting for write-quorum of nodes to move to the max tail determined earlier",
                    );

                    loop {
                        // Re-evaluation condition is any of:
                        // - todo(asoli): [optimization] Sequencer is reachable and has moved past max_local_tail
                        // - Any node with global_known_tail that reached the max_local_tail we are looking for (due to RELEASE message)
                        // - Any node gets sealed. Back to drawing board.
                        // - todo(asoli): If write-quorum can be formed from unsealed nodes. Risk here is that if
                        //   sequencer is alive, we might move the global commit offset before it does.
                        // - More nodes chimed in (inflight-info-requests)
                        // - Timeout?
                        tokio::select! {
                            Some(Ok((node_id, tail_update))) = inflight_tail_update_watches.join_next() => {
                                // maybe sealed, maybe global is updated. Who knows.
                                nodeset_checker.merge_attribute(node_id, tail_update);
                                last_updated = Instant::now();
                            }
                            Some(Ok((node_id, info))) = inflight_info_requests.join_next() => {
                                nodeset_checker.merge_attribute(node_id, info);
                                last_updated = Instant::now();
                            }
                            _ = tokio::time::sleep(Duration::from_secs(1)), if nodeset_checker.check_write_quorum(NodeTailStatus::is_known) => {}
                            else => {}
                        };

                        if nodeset_checker.any(NodeTailStatus::is_known_sealed) {
                            // A node got sealed. Let's re-evaluate to finish up this seal before
                            // we respond. We expect to switching to `SomeSealed`
                            //
                            // the drop+abort happens automatically but this is left for visibility
                            // of intent.
                            drop(inflight_tail_update_watches);
                            continue 'check_nodeset;
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

                        // NOTE: This is a hack-ish optimization that that treats the `Open` status as
                        // more like "probably open" because we _might_ actually be sealed but we just
                        // don't know it yet.
                        if self.known_global_tail.latest_offset() >= max_local_tail {
                            // todo: add an explicit ProbablyOpen/Unknown response variant
                            return FindTailResult::Open {
                                global_tail: self.known_global_tail.latest_offset(),
                            };
                        }

                        // if the sequencer is "gone", we can immediately go ahead and seal the
                        // loglet.
                        //
                        // sequencer is dead AND:
                        // - We have f-majority responses already
                        // - We have write-quorum responses (i.e. tail is repairable)
                        //
                        // This means that waiting for nodes to move their local tail is:
                        // A) unlikely to happen (because sequencer is observed dead)
                        // B) and if we sealed, we'll be able to repair this tail since we can
                        // reach to write-quorum.
                        //
                        // Our goal is to detect when we should just go ahead and seal instead of
                        // waiting for the tail to move.
                        //
                        // This **must** take into account the sequencer status. If the sequencer
                        // node is believed to be alive, we should not step over it. Instead, we continue
                        // waiting because it could be struggling to replicate the tail still. However,
                        // if the sequencer is believed to be dead, we should seal and repair the tail and
                        // restart the find-tail procedure if it has been over 5 seconds since the
                        // last update from any log-server (quiescent state).
                        //
                        // This is an attempt to reduce over-reacting solely based on observing a "dead"
                        // sequencer. A dead sequencer can be observed in benign situations, such
                        // as:
                        // 1. On startup, we might not have a good view of the cluster state. We don't
                        //    want to prematurely preempt the sequencer unless we are confident that
                        //    it's gone for good. Gone is a state defined by observing a higher
                        //    generation of the same plain node id. If the sequencer is _gone_, it's
                        //    okay to be eager to seal.
                        // 2. We don't attempt to seal unless we have heard from the union of the write-quorum
                        //    and the f-majority, this protects us against being partitioned with the
                        //    minority of log-servers in the nodeset.
                        // 3. If the sequencer is not gone (but dead), we react after being quiescent for 5 seconds.
                        if (is_sequencer_gone(
                            &cs,
                            self.my_params.sequencer,
                            nodes_config.live_load(),
                        ) || is_sequencer_likely_gone(
                            &cs,
                            self.my_params.sequencer,
                            last_updated,
                        )) && nodeset_checker.check_write_quorum(NodeTailStatus::is_known)
                        {
                            // SEAL AND REPAIR
                            debug!(
                                loglet_id = %self.my_params.loglet_id,
                                %max_local_tail,
                                "Detected gone (or likely gone) sequencer and inconsistent tail. Sealing the loglet to repair the tail; status={}",
                                nodeset_checker,
                            );
                            // run seal task then retry the find-tail check.
                            // This returns when we have f-majority sealed.
                            let sealed_tail = match SealTask::run(
                                &self.my_params,
                                &self.known_global_tail,
                                &self.networking,
                            )
                            .await
                            {
                                Ok(sealed_tail) => sealed_tail,
                                Err(e) => {
                                    return FindTailResult::Error(format!(
                                        "Failed to seal the loglet during find-tail procedure for loglet_id={}: {:?}",
                                        self.my_params.loglet_id, e
                                    ));
                                }
                            };
                            // Just to make our intention clear that we want to drain the set and abort
                            // all tasks. This happens automatically on the next iteration of the loop
                            // but it's left here for visibility.
                            drop(inflight_info_requests);
                            // retry the whole find-tail procedure.
                            trace!(
                                %sealed_tail,
                                %max_local_tail,
                                "Restarting FindTail task after sealing f-majority for loglet_id={}",
                                self.my_params.loglet_id
                            );
                            continue 'find_tail;
                        }

                        // nothing left to wait on, the tail didn't reach the expected target and
                        // we decided not to seal the loglet (either we cannot reach write quorum
                        // of responses or the sequencer is still alive)
                        if inflight_info_requests.is_empty()
                            && inflight_tail_update_watches.is_empty()
                        {
                            // Nothing left to wait on. The tail didn't reach expected
                            // target and no more nodes are expected to send us responses.
                            return FindTailResult::Error(format!(
                                "Could not determine a safe tail offset for loglet_id={}, perhaps too many nodes down? status={}",
                                self.my_params.loglet_id, nodeset_checker
                            ));
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
    pub(super) loglet_id: LogletId,
    pub(super) known_global_tail: &'a TailOffsetWatch,
}

impl<'a> FindTailOnNode<'a> {
    pub async fn run<T: TransportConnect>(
        self,
        networking: &'a Networking<T>,
    ) -> (PlainNodeId, NodeTailStatus) {
        let request_timeout = *Configuration::pinned()
            .bifrost
            .replicated_loglet
            .log_server_rpc_timeout;

        let request = GetLogletInfo {
            header: LogServerRequestHeader::new(
                self.loglet_id,
                self.known_global_tail.latest_offset(),
            ),
        };

        trace!(
            loglet_id = %self.loglet_id,
            peer = %self.node_id,
            known_global_tail = %self.known_global_tail,
           "Request tail info from log-server"
        );
        let maybe_info = networking
            .call_rpc(
                self.node_id,
                Swimlane::default(),
                request,
                Some(self.loglet_id.into()),
                Some(request_timeout),
            )
            .await;
        trace!(
            loglet_id = %self.loglet_id,
            peer = %self.node_id,
            known_global_tail = %self.known_global_tail,
           "Received tail info from log-server: {:?}", maybe_info
        );

        match maybe_info {
            Ok(msg) => {
                self.known_global_tail
                    .notify_offset_update(msg.header.known_global_tail);
                // We retry on the following errors.
                match msg.status {
                    Status::Ok | Status::Sealed => {
                        return (
                            self.node_id,
                            NodeTailStatus::Known {
                                local_tail: msg.header.local_tail,
                                sealed: msg.header.sealed,
                            },
                        );
                    }
                    // retryable errors
                    Status::Sealing | Status::Disabled | Status::Dropped => {
                        // fall-through for retries
                    }
                    // unexpected statuses
                    Status::SequencerMismatch
                    | Status::OutOfBounds
                    | Status::Malformed
                    | Status::Unknown => {
                        warn!(
                            loglet_id = %self.loglet_id,
                            peer = %self.node_id,
                            "Unexpected status from log-server when calling GetLogletInfo: {:?}",
                            msg.status
                        );
                    }
                }
            }
            Err(RpcError::Timeout(spent)) => {
                trace!(
                    "Timeout when getting loglet info from node_id={} for loglet_id={}. Configured timeout={:?}, spent={:?}",
                    self.node_id, self.loglet_id, request_timeout, spent,
                );
            }
            Err(err) => {
                trace!(
                    "Failed to get loglet info from node_id={} for loglet_id={}: {}",
                    self.node_id, self.loglet_id, err
                );
            }
        }

        (self.node_id, NodeTailStatus::Unknown)
    }
}

struct WaitForTailOnNode {
    node_id: PlainNodeId,
    loglet_id: LogletId,
    known_global_tail: TailOffsetWatch,
}

impl WaitForTailOnNode {
    pub async fn run<T: TransportConnect>(
        self,
        requested_tail: LogletOffset,
        networking: Networking<T>,
    ) -> (PlainNodeId, NodeTailStatus) {
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
            let maybe_updated = networking
                .call_rpc(
                    self.node_id,
                    Swimlane::default(),
                    request,
                    Some(self.loglet_id.into()),
                    None,
                )
                .await;

            match maybe_updated {
                Ok(msg) => {
                    self.known_global_tail
                        .notify_offset_update(msg.header.known_global_tail);
                    match msg.status {
                        Status::Ok | Status::Sealed => {
                            return (
                                self.node_id,
                                NodeTailStatus::Known {
                                    local_tail: msg.header.local_tail,
                                    sealed: msg.header.sealed,
                                },
                            );
                        }
                        // retryable errors
                        Status::Sealing | Status::Disabled | Status::Dropped => {
                            // fall-through for retries
                        }
                        // unexpected statuses
                        Status::SequencerMismatch
                        | Status::OutOfBounds
                        | Status::Malformed
                        | Status::Unknown => {
                            error!(
                                "Unexpected status from log-server node_id={} when waiting for tail update for loglet_id={}: {:?}",
                                self.node_id, self.loglet_id, msg.status
                            );
                            return (self.node_id, NodeTailStatus::Unknown);
                        }
                    }
                }
                Err(e) => {
                    trace!(
                        "Failed to watch loglet tail updates from node_id={} for loglet_id={}: {:?}",
                        self.node_id, self.loglet_id, e
                    );
                }
            }

            // Should we retry?
            if let Some(pause) = retry_iter.next() {
                trace!(
                    "Retrying to watch loglet tail update from node_id={} and loglet_id={} after {:?}",
                    self.node_id, self.loglet_id, pause
                );
                tokio::time::sleep(pause).await;
            } else {
                trace!(
                    "Exhausted retries while attempting to watch loglet tail update from node_id={} and loglet_id={}",
                    self.node_id, self.loglet_id
                );
                return (self.node_id, NodeTailStatus::Unknown);
            }
        }
    }
}

fn is_sequencer_alive(
    cs: &ClusterState,
    sequencer: GenerationalNodeId,
    my_node_id: GenerationalNodeId,
) -> bool {
    // if our own node is not seen to be alive yet, then we should still try to connect to
    // the sequencer.
    cs.is_alive(sequencer.into()) || !cs.is_alive(my_node_id.into())
}

fn is_sequencer_gone(
    cs: &ClusterState,
    sequencer: GenerationalNodeId,
    nodes_config: &NodesConfiguration,
) -> bool {
    if cs.is_alive(sequencer.into()) {
        // it's alive, so we assume it's not gone
        false
    } else {
        // node is dead, but is it gone?
        match nodes_config.find_node_by_id(sequencer) {
            // it's just dead, not necessarily gone
            Ok(_) => false,
            Err(NodesConfigError::UnknownNodeId(_)) => true,
            Err(NodesConfigError::Deleted(_)) => true,
            Err(NodesConfigError::GenerationMismatch { found, .. }) => {
                found.is_newer_than(sequencer)
            }
        }
    }
}

fn is_sequencer_likely_gone(
    cs: &ClusterState,
    sequencer: GenerationalNodeId,
    last_updated: Instant,
) -> bool {
    // 5 seconds since last tail update/movement **and** sequencer is dead is a good indicator that
    // we should attempt to seal the loglet to repair the inconsistent tail.
    !cs.is_alive(sequencer.into()) && last_updated.elapsed() > Duration::from_secs(5)
}
