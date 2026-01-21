// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use tokio::task::JoinSet;
use tracing::{Instrument, debug, instrument, trace};

use restate_core::network::{Networking, TransportConnect};
use restate_core::{Metadata, ShutdownError, TaskCenterFutureExt};
use restate_types::PlainNodeId;
use restate_types::logs::TailOffsetWatch;
use restate_types::net::log_server::{
    GetLogletInfo, LogServerRequestHeader, LogServerResponseHeader, LogletInfo, Status,
};
use restate_types::replicated_loglet::{LogNodeSetExt, ReplicatedLogletParams};
use restate_types::replication::NodeSetChecker;
use restate_types::retries::RetryPolicy;

use super::util::Disposition;
use crate::providers::replicated_loglet::log_server_manager::RemoteLogServerManager;
use crate::providers::replicated_loglet::tasks::util::RunOnSingleNode;

/// Attempts to detect if the loglet has been sealed or if there is a seal in progress by
/// consulting nodes until it reaches f-majority. If it cannot achieve f-majority, it'll still
/// let us know if any node is in sealing state so we can continue the original seal operation.
///
/// This allows PeriodicFindTail to detect seal that was triggered externally to unblock read
/// streams running locally
///
/// Note: This is designed to be used with the local-sequencer but in the future it may be
/// extended to be more general purpose.
pub struct CheckSealTask;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum CheckSealOutcome {
    // We have F-majority of sealed nodes
    Sealed,
    // We have write-quorum nodes that are open. `is_partially_sealed` is set to `true` if any node
    // in the nodeset was observed to be sealed.
    Open { is_partially_sealed: bool },
    // We can't determine if we are fully sealed or if we are fully open. No sufficient responses
    // were received to determine the seal status. However, `is_partially_sealed` is set to `true`
    // if any node was observed to be sealed.
    Unknown { is_partially_sealed: bool },
}

impl CheckSealTask {
    #[instrument(skip_all)]
    pub async fn run<T: TransportConnect>(
        my_params: &ReplicatedLogletParams,
        log_servers: &RemoteLogServerManager,
        known_global_tail: &TailOffsetWatch,
        networking: &Networking<T>,
    ) -> Result<CheckSealOutcome, ShutdownError> {
        let metadata = Metadata::current();
        if known_global_tail.is_sealed() {
            return Ok(CheckSealOutcome::Sealed);
        }
        debug!(
            loglet_id = %my_params.loglet_id,
            "Checking seal status for loglet"
        );
        // If all nodes in the nodeset is in "provisioning", we can confidently short-circuit
        // the result to LogletOffset::Oldest and the loglet is definitely unsealed since such
        // nodes can transition into read-write at a later stage.
        if my_params
            .nodeset
            .all_provisioning(&metadata.nodes_config_ref())
        {
            return Ok(CheckSealOutcome::Open {
                is_partially_sealed: false,
            });
        }

        let effective_nodeset = my_params.nodeset.to_effective(&metadata.nodes_config_ref());

        let mut nodeset_checker = NodeSetChecker::<bool>::new(
            &effective_nodeset,
            &metadata.nodes_config_ref(),
            &my_params.replication,
        );

        let local_tails: BTreeMap<PlainNodeId, TailOffsetWatch> = effective_nodeset
            .iter()
            .map(|node_id| (*node_id, log_servers.get_tail_offset(*node_id).clone()))
            .collect();

        // If some of the nodes are already sealed, we know our answer and we don't need to go through
        // the rest of the nodes.
        for (node_id, local_tail) in local_tails.iter() {
            // do not use the unsealed signal as authoritative source here.
            if local_tail.is_sealed() {
                nodeset_checker.set_attribute(*node_id, true);
            }
        }

        // You might be wondering, why don't we just check if any of the nodes are sealed and
        // return here? Because we want to dispatch background tasks to update the seal/local-tail
        // status for the rest of the nodeset. This aids with future runs of this task, and helps
        // the sequencer get a fresh view if it didn't communicate with those nodes for some time.

        let mut inflight_requests = JoinSet::new();
        for node_id in effective_nodeset.iter().copied() {
            // only create tasks for nodes that we think they are open
            if nodeset_checker
                .get_attribute(&node_id)
                .copied()
                .unwrap_or(false)
            {
                // This node is known to be sealed, don't run a task for it.
                continue;
            }
            let request = GetLogletInfo {
                header: LogServerRequestHeader::new(
                    my_params.loglet_id,
                    known_global_tail.latest_offset(),
                ),
            };

            inflight_requests
                .build_task()
                .name("check-seal")
                .spawn({
                    let networking = networking.clone();
                    let known_global_tail = known_global_tail.clone();
                    let local_tail = local_tails.get(&node_id).cloned();

                    async move {
                        let task = RunOnSingleNode::new(
                            node_id,
                            request,
                            &known_global_tail,
                            // do not retry
                            RetryPolicy::None,
                        );

                        (
                            node_id,
                            task.run(on_info_response(local_tail), &networking).await,
                        )
                    }
                    .in_current_tc()
                    .in_current_span()
                })
                .expect("to spawn check seal tasks");
        }

        // Waiting for GetLogletInfo responses
        loop {
            // are we fully sealed? We use BestEffort here because we'd still want to consider a
            // loglet sealed even if some nodes lost data. The check-seal wouldn't determine the
            // known_global_tail, only whether the loglet is sealed or not and it's safe to do so.
            // non-authoritative nodes cannot accept normal writes, only repair writes.
            if nodeset_checker.check_fmajority(|attr| *attr).passed() {
                // no need to detach, we don't need responses from the rest of the nodes
                return Ok(CheckSealOutcome::Sealed);
            }

            // We have sufficient results to say we are Open or Sealing
            if nodeset_checker.check_write_quorum(|attr| !(*attr)) {
                let is_partially_sealed = nodeset_checker.any(|attr| *attr);
                return Ok(CheckSealOutcome::Open {
                    is_partially_sealed,
                });
            }

            // keep grabbing results
            let Some(response) = inflight_requests.join_next().await else {
                // no more results, since we didn't return earlier, we know that we didn't get
                // enough responses to determine the result authoritatively
                break;
            };
            let Ok((node_id, response)) = response else {
                // task panicked or runtime is shutting down.
                continue;
            };
            let Ok(response) = response else {
                // GetLogletInfo task failed/aborted on this node. The inner task will log the error in this case.
                continue;
            };

            nodeset_checker.set_attribute(node_id, response.sealed);
        }

        // are we partially sealed?
        let is_partially_sealed = nodeset_checker.any(|attr| *attr);

        debug!(
            loglet_id = %my_params.loglet_id,
            status = %nodeset_checker,
            effective_nodeset = %effective_nodeset,
            replication = %my_params.replication,
            "Insufficient nodes responded to GetLogletInfo requests, we cannot determine seal status, we'll assume it's unsealed for now",
        );
        return Ok(CheckSealOutcome::Unknown {
            is_partially_sealed,
        });
    }
}

fn on_info_response(
    server_local_tail: Option<TailOffsetWatch>,
) -> impl Fn(PlainNodeId, LogletInfo) -> Disposition<LogServerResponseHeader> {
    move |peer: PlainNodeId, msg: LogletInfo| -> Disposition<LogServerResponseHeader> {
        if let Status::Ok = msg.header.status {
            if let Some(server_local_tail) = &server_local_tail {
                server_local_tail.notify_offset_update(msg.local_tail);
                if msg.header.sealed {
                    server_local_tail.notify_seal();
                }
            }
            Disposition::Return(msg.header)
        } else {
            trace!(
                "GetLogletInfo request failed on node {}, status is {:?}",
                peer, msg.header.status
            );
            Disposition::Abort
        }
    }
}
