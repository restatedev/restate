// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use restate_types::retries::RetryPolicy;
use restate_types::PlainNodeId;
use tokio::task::JoinSet;
use tracing::{debug, instrument, trace, Instrument, Span};

use restate_core::network::rpc_router::RpcRouter;
use restate_core::network::{Incoming, Networking, TransportConnect};
use restate_core::{ShutdownError, TaskCenterFutureExt};
use restate_types::net::log_server::{
    GetLogletInfo, LogServerRequestHeader, LogServerResponseHeader, LogletInfo, Status,
};
use restate_types::replicated_loglet::{LogNodeSetExt, ReplicatedLogletParams};

use super::util::Disposition;
use crate::loglet::util::TailOffsetWatch;
use crate::providers::replicated_loglet::log_server_manager::RemoteLogServerManager;
use crate::providers::replicated_loglet::replication::{FMajorityResult, NodeSetChecker};
use crate::providers::replicated_loglet::tasks::util::RunOnSingleNode;

/// Attempts to detect if the loglet has been sealed or if there is a seal in progress by
/// consulting nodes until it reaches f-majority, and it stops at the first sealed response
/// from any log-server since this is a sufficient signal that a seal is on-going.
///
/// This allows PeriodicFindTail to detect seal that was triggered externally to unblock read
/// streams running locally
///
/// Note that this task can return Open if it cannot reach out to any node, so we should not use it
/// for operations that rely on absolute correctness of the tail. For those, use FindTailTask
/// instead.
pub struct CheckSealTask {}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum CheckSealOutcome {
    FullySealed,
    Sealing,
    ProbablyOpen,
    Open,
}

impl CheckSealTask {
    #[instrument(skip_all)]
    pub async fn run<T: TransportConnect>(
        my_params: &ReplicatedLogletParams,
        get_loglet_info_rpc: &RpcRouter<GetLogletInfo>,
        log_servers: &RemoteLogServerManager,
        known_global_tail: &TailOffsetWatch,
        networking: &Networking<T>,
    ) -> Result<CheckSealOutcome, ShutdownError> {
        if known_global_tail.is_sealed() {
            return Ok(CheckSealOutcome::FullySealed);
        }
        debug!(
            loglet_id = %my_params.loglet_id,
            "Checking seal status for loglet"
        );
        // If all nodes in the nodeset is in "provisioning", we can confidently short-circuit
        // the result to LogletOffset::Oldest and the loglet is definitely unsealed.
        if my_params
            .nodeset
            .all_provisioning(&networking.metadata().nodes_config_ref())
        {
            return Ok(CheckSealOutcome::Open);
        }
        // todo: If effective nodeset is empty, should we consider that the loglet is implicitly
        // sealed?

        let effective_nodeset = my_params
            .nodeset
            .to_effective(&networking.metadata().nodes_config_ref());

        let mut nodeset_checker = NodeSetChecker::<bool>::new(
            &effective_nodeset,
            &networking.metadata().nodes_config_ref(),
            &my_params.replication,
        );

        let local_tails: BTreeMap<PlainNodeId, TailOffsetWatch> = effective_nodeset
            .iter()
            .filter_map(|node_id| {
                log_servers
                    .try_get_tail_offset(*node_id)
                    .and_then(|w| Some((*node_id, w)))
            })
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
                .map(|attr| *attr)
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

            inflight_requests.spawn({
                let networking = networking.clone();
                let rpc_router = get_loglet_info_rpc.clone();
                let known_global_tail = known_global_tail.clone();
                let local_tail = local_tails.get(&node_id).cloned();

                async move {
                    let task = RunOnSingleNode::new(
                        node_id,
                        request,
                        &rpc_router,
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
                .instrument(Span::current())
            });
        }

        // Waiting for trim responses
        loop {
            // are we fully sealed?
            if nodeset_checker.check_fmajority(|attr| *attr) >= FMajorityResult::BestEffort {
                // no need to detach, we don't need responses from the rest of the nodes
                return Ok(CheckSealOutcome::FullySealed);
            }

            // are we partially sealed?
            if nodeset_checker.any(|attr| *attr) {
                // detach to let them continue in the background.
                inflight_requests.detach_all();
                return Ok(CheckSealOutcome::Sealing);
            }

            if nodeset_checker.check_fmajority(|attr| *attr == false) >= FMajorityResult::BestEffort
            {
                return Ok(CheckSealOutcome::Open);
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

        debug!(
            loglet_id = %my_params.loglet_id,
            status = %nodeset_checker,
            effective_nodeset = %effective_nodeset,
            replication = %my_params.replication,
            "Insufficient nodes responded to GetLogletInfo requests, we cannot determine seal status, we'll assume it's unsealed for now",
        );
        return Ok(CheckSealOutcome::ProbablyOpen);
    }
}

fn on_info_response(
    server_local_tail: Option<TailOffsetWatch>,
) -> impl Fn(Incoming<LogletInfo>) -> Disposition<LogServerResponseHeader> {
    move |msg: Incoming<LogletInfo>| -> Disposition<LogServerResponseHeader> {
        if let Status::Ok = msg.body().header.status {
            if let Some(server_local_tail) = &server_local_tail {
                server_local_tail.notify_offset_update(msg.body().local_tail);
                if msg.body().header.sealed {
                    server_local_tail.notify_seal();
                }
            }
            Disposition::Return(msg.into_body().header)
        } else {
            trace!(
                "GetLogletInfo request failed on node {}, status is {:?}",
                msg.peer(),
                msg.body().header.status
            );
            Disposition::Abort
        }
    }
}
