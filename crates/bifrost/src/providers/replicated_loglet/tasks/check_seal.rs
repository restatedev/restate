// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use tracing::{info, instrument, trace};

use restate_core::network::rpc_router::RpcRouter;
use restate_core::network::{Networking, TransportConnect};
use restate_core::{cancellation_watcher, ShutdownError};
use restate_types::net::log_server::GetLogletInfo;
use restate_types::replicated_loglet::{EffectiveNodeSet, ReplicatedLogletParams};

use super::{FindTailOnNode, NodeTailStatus};
use crate::loglet::util::TailOffsetWatch;
use crate::providers::replicated_loglet::replication::NodeSetChecker;

/// Attempts to detect if the loglet has been sealed or if there is a seal in progress by
/// consulting nodes until it reaches f-majority, and it stops at the first sealed response
/// from any log-server since this is a sufficient signal that a seal is on-going.
///
/// the goal of this operation to get a signal on sequencer node that a seal has happened (or
/// ongoing) if we have not been receiving appends for some time.
///
/// This allows PeriodicFindTail to detect seal that was triggered externally to unblock read
/// streams running locally that rely on the sequencer's view of known_global_tail.
///
///
/// Note that this task can return Open if it cannot reach out to any node, so we should not use it
/// for operations that rely on absolute correctness of the tail. For those, use FindTailTask
/// instead.
pub struct CheckSealTask {}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum CheckSealOutcome {
    Sealing,
    ProbablyOpen,
}

impl CheckSealTask {
    #[instrument(skip_all)]
    pub async fn run<T: TransportConnect>(
        my_params: &ReplicatedLogletParams,
        get_loglet_info_rpc: &RpcRouter<GetLogletInfo>,
        known_global_tail: &TailOffsetWatch,
        networking: &Networking<T>,
    ) -> Result<CheckSealOutcome, ShutdownError> {
        // If all nodes in the nodeset is in "provisioning", we can confidently short-circuit
        // the result to LogletOffset::Oldest and the loglet is definitely unsealed.
        if my_params
            .nodeset
            .all_provisioning(&networking.metadata().nodes_config_ref())
        {
            return Ok(CheckSealOutcome::ProbablyOpen);
        }
        // todo: If effective nodeset is empty, should we consider that the loglet is implicitly
        // sealed?

        let effective_nodeset = EffectiveNodeSet::new(
            &my_params.nodeset,
            &networking.metadata().nodes_config_ref(),
        );

        let mut nodeset_checker = NodeSetChecker::<'_, NodeTailStatus>::new(
            &effective_nodeset,
            &networking.metadata().nodes_config_ref(),
            &my_params.replication,
        );

        let mut nodes = effective_nodeset.shuffle_for_reads(networking.metadata().my_node_id());

        let mut cancel = std::pin::pin!(cancellation_watcher());
        trace!(
            loglet_id = %my_params.loglet_id,
            effective_nodeset = %effective_nodeset,
            "Checking seal status for loglet",
        );
        loop {
            if nodeset_checker
                .check_fmajority(NodeTailStatus::is_known_unsealed)
                .passed()
            {
                // once we reach f-majority of unsealed, we stop.
                return Ok(CheckSealOutcome::ProbablyOpen);
            }

            let Some(next_node) = nodes.pop() else {
                info!(
                    loglet_id = %my_params.loglet_id,
                    status = %nodeset_checker,
                    effective_nodeset = %effective_nodeset,
                    "Insufficient nodes responded to GetLogletInfo requests, we cannot determine seal status, we'll assume it's unsealed for now",
                );
                return Ok(CheckSealOutcome::ProbablyOpen);
            };

            let task = FindTailOnNode {
                node_id: next_node,
                loglet_id: my_params.loglet_id,
                get_loglet_info_rpc,
                known_global_tail,
            };
            let tail_status = tokio::select! {
                _ = &mut cancel => {
                    return Err(ShutdownError);
                }
                (_, tail_status) = task.run(networking) => { tail_status },
            };
            if tail_status.is_known_sealed() {
                // we only need to see a single node sealed to declare that we are probably sealing (or
                // sealed)
                return Ok(CheckSealOutcome::Sealing);
            }
            nodeset_checker.merge_attribute(next_node, tail_status);
        }
    }
}
