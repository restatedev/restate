// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use tokio::task::JoinSet;
use tokio::time::Instant;
use tracing::{Instrument, debug, info, instrument, trace, warn};

use restate_core::network::{Networking, TransportConnect};
use restate_core::{Metadata, TaskCenterFutureExt};
use restate_types::PlainNodeId;
use restate_types::config::Configuration;
use restate_types::logs::{LogletOffset, SequenceNumber, TailOffsetWatch};
use restate_types::net::log_server::{LogServerRequestHeader, Seal, Sealed, Status};
use restate_types::replicated_loglet::{LogNodeSetExt, ReplicatedLogletParams};
use restate_types::replication::{DecoratedNodeSet, NodeSetChecker};

use crate::providers::replicated_loglet::error::{NodeSealStatus, ReplicatedLogletError};
use crate::providers::replicated_loglet::tasks::util::RunOnSingleNode;

use super::util::Disposition;

/// Sends a seal request to as many log-servers in the nodeset
///
/// We broadcast the seal to all nodes that we can, but only wait for f-majority
/// responses before acknowledging the seal. The rest of the seal requests are dropped if they
/// weren't completed by that time.
///
/// The seal operation is idempotent. It's safe to seal a loglet if it's already partially or fully
/// sealed.
///
/// Note 1: The seal task will ignore the "seal" state in the input `known_global_tail` watch.
/// Note 2: The seal task will *not* set the seal flag on `known_global_tail` even if seal was
/// successful. This is the responsibility of the caller.
pub struct SealTask;

impl SealTask {
    #[instrument(skip_all)]
    pub async fn run<T: TransportConnect>(
        my_params: &ReplicatedLogletParams,
        known_global_tail: &TailOffsetWatch,
        networking: &Networking<T>,
    ) -> Result<LogletOffset, ReplicatedLogletError> {
        let start = Instant::now();
        let metadata = Metadata::current();
        debug!(
            loglet_id = %my_params.loglet_id,
            is_sealed = known_global_tail.is_sealed(),
            "Starting a seal task for loglet"
        );
        // If all nodes in the nodeset is in "provisioning", we can cannot seal.
        if my_params
            .nodeset
            .all_provisioning(&metadata.nodes_config_ref())
        {
            warn!(
                loglet_id = %my_params.loglet_id,
                is_sealed = known_global_tail.is_sealed(),
                "Cannot seal the loglet as all nodeset members are in `Provisioning` storage state"
            );
            return Err(ReplicatedLogletError::SealFailed(Default::default()));
        }
        // Use the entire nodeset except for StorageState::Disabled.
        let effective_nodeset = my_params.nodeset.to_effective(&metadata.nodes_config_ref());

        let mut nodeset_checker = NodeSetChecker::<NodeSealStatus>::new(
            &effective_nodeset,
            &metadata.nodes_config_ref(),
            &my_params.replication,
        );

        let retry_policy = Configuration::pinned()
            .bifrost
            .replicated_loglet
            .log_server_retry_policy
            .clone();

        let mut inflight_requests = JoinSet::new();

        for node_id in effective_nodeset.iter().copied() {
            let request = Seal {
                header: LogServerRequestHeader::new(
                    my_params.loglet_id,
                    known_global_tail.latest_offset(),
                ),
                sequencer: my_params.sequencer,
            };
            inflight_requests
                .build_task()
                .name("seal")
                .spawn({
                    let networking = networking.clone();
                    let known_global_tail = known_global_tail.clone();
                    let retry_policy = retry_policy.clone();
                    async move {
                        let task = RunOnSingleNode::new(
                            node_id,
                            request,
                            &known_global_tail,
                            retry_policy,
                        );

                        (node_id, task.run(on_seal_response, &networking).await)
                    }
                    .in_current_tc()
                    .in_current_span()
                })
                .expect("to spawn seal task");
        }

        // Max observed local-tail from sealed nodes
        let mut max_local_tail = LogletOffset::OLDEST;
        while let Some(response) = inflight_requests.join_next().await {
            let Ok((node_id, response)) = response else {
                // task panicked or runtime is shutting down.
                continue;
            };
            let Ok(response) = response else {
                // Seal failed/aborted on this node.
                nodeset_checker.set_attribute(node_id, NodeSealStatus::Error);
                continue;
            };

            max_local_tail = max_local_tail.max(response.header.local_tail);
            known_global_tail.notify_offset_update(response.header.known_global_tail);
            nodeset_checker.set_attribute(node_id, NodeSealStatus::Sealed);

            if nodeset_checker
                .check_fmajority(|attr| *attr == NodeSealStatus::Sealed)
                .passed()
            {
                let mut nodeset_status = DecoratedNodeSet::from_iter(effective_nodeset);
                nodeset_status.extend(&nodeset_checker);
                info!(
                    loglet_id = %my_params.loglet_id,
                    replication = %my_params.replication,
                    %max_local_tail,
                    global_tail = %known_global_tail.latest_offset(),
                    "Seal task completed on f-majority of nodes in {:?}. Nodeset status {}",
                    start.elapsed(),
                    nodeset_status,
                );
                // note that we drop the rest of the seal requests after return
                return Ok(max_local_tail);
            }
        }

        let mut nodeset_status = DecoratedNodeSet::from_iter(effective_nodeset);
        nodeset_status.extend(&nodeset_checker);
        // no more tasks left. This means that we failed to seal
        Err(ReplicatedLogletError::SealFailed(nodeset_status))
    }
}

fn on_seal_response(peer: PlainNodeId, msg: Sealed) -> Disposition<Sealed> {
    if msg.header.status == Status::Ok || msg.sealed {
        return Disposition::Return(msg);
    }
    trace!(
        "Seal request failed on node {}, status is {:?}",
        peer, msg.header.status
    );
    Disposition::Abort
}
