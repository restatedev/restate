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
use tracing::{Instrument, Span, debug, instrument, trace, warn};

use restate_core::network::{Networking, TransportConnect};
use restate_core::{Metadata, TaskCenterFutureExt};
use restate_types::PlainNodeId;
use restate_types::config::Configuration;
use restate_types::logs::{LogletOffset, SequenceNumber, TailOffsetWatch};
use restate_types::net::log_server::{LogServerRequestHeader, Status, Trim, Trimmed};
use restate_types::replicated_loglet::{LogNodeSetExt, ReplicatedLogletParams};
use restate_types::replication::{NodeSet, NodeSetChecker};

use crate::loglet::OperationError;
use crate::providers::replicated_loglet::tasks::util::RunOnSingleNode;

use super::util::Disposition;

#[derive(Debug, thiserror::Error)]
#[error("trim loglet operation failed")]
struct TrimError;

/// Sends a trim request to as many log-servers in the nodeset.
///
/// We broadcast the trim to all nodes that we can, but only wait for write-quorum
/// responses before acknowledging the trim.
///
/// The trim operation is idempotent. It's safe to trim a loglet if it's already partially or fully
/// trimmed and if it's sealed.
///
/// Note that the trim task will clip the input trim-point to the last known global-tail of the
/// loglet, it'll not attempt to perform a FindTail internally, the actual trim point
/// might be higher if a node in the nodeset has higher trim point and it wasn't visited during
/// this trim write-quorum check.
///
/// In other words, the trim operation confirms that at least write-quorum nodes in the nodeset
/// are trimmed to the requested (possibly clamped) trim point or higher.
///
/// Calls to `get_trim_point()` that happen after this task should return the clamped trim point
/// or higher (best effort).
pub struct TrimTask<'a> {
    my_params: &'a ReplicatedLogletParams,
    known_global_tail: TailOffsetWatch,
}

impl<'a> TrimTask<'a> {
    pub fn new(my_params: &'a ReplicatedLogletParams, known_global_tail: TailOffsetWatch) -> Self {
        Self {
            my_params,
            known_global_tail,
        }
    }

    #[instrument(level = "error", skip_all, fields(%trim_point))]
    pub async fn run<T: TransportConnect>(
        self,
        trim_point: LogletOffset,
        networking: Networking<T>,
    ) -> Result<(), OperationError> {
        let metadata = Metadata::current();
        // Use the entire nodeset except for StorageState::Disabled.
        let effective_nodeset = self
            .my_params
            .nodeset
            .to_effective(&metadata.nodes_config_ref());
        // caller might have already done this, but it's here for resilience against user error.
        let trim_point = trim_point.min(self.known_global_tail.latest_offset().prev_unchecked());
        if trim_point == LogletOffset::INVALID {
            // nothing to trim, really.
            debug!(
                loglet_id = %self.my_params.loglet_id,
                requested_trim_point = %trim_point,
                known_global_tail = %self.known_global_tail,
                "Will not send trim messages to log-servers since the effective trim_point requested is 0"
            );
            return Ok(());
        }

        let mut nodeset_checker = NodeSetChecker::<bool>::new(
            &effective_nodeset,
            &metadata.nodes_config_ref(),
            &self.my_params.replication,
        );

        trace!(
            loglet_id = %self.my_params.loglet_id,
            trim_point = %trim_point,
            known_global_tail = %self.known_global_tail,
            effective_nodeset = %effective_nodeset,
            "Trimming loglet"
        );

        let mut inflight_requests = JoinSet::new();
        for node_id in effective_nodeset.iter().copied() {
            let request = Trim {
                header: LogServerRequestHeader::new(
                    self.my_params.loglet_id,
                    self.known_global_tail.latest_offset(),
                ),
                trim_point,
            };

            inflight_requests
                .build_task()
                .name("trim")
                .spawn({
                    let networking = networking.clone();
                    let known_global_tail = self.known_global_tail.clone();

                    async move {
                        let task = RunOnSingleNode::new(
                            node_id,
                            request,
                            &known_global_tail,
                            Configuration::pinned()
                                .bifrost
                                .replicated_loglet
                                .log_server_retry_policy
                                .clone(),
                        );

                        (node_id, task.run(on_trim_response, &networking).await)
                    }
                    .in_current_tc()
                    .instrument(Span::current())
                })
                .expect("to spawn trim task");
        }

        // Waiting for trim responses
        while let Some(res) = inflight_requests.join_next().await {
            let Ok((node_id, res)) = res else {
                // task panicked or runtime is shutting down.
                continue;
            };
            if res.is_err() {
                // Trim task failed/aborted on this node. The inner task will log the error in this case.
                continue;
            };

            nodeset_checker.set_attribute(node_id, true);
            if nodeset_checker.check_write_quorum(|s| *s) {
                // We have enough nodes with a trim point at or higher than what we requested.
                // Let's keep the rest of the trim requests running in the background.
                debug!(
                    loglet_id = %self.my_params.loglet_id,
                     trim_point = %trim_point,
                    known_global_tail = %self.known_global_tail,
                    "Loglet has been trimmed"
                );
                // continue to run the rest of trim requests in the background
                inflight_requests.detach_all();
                return Ok(());
            }
        }

        let confirmed_nodes: NodeSet = nodeset_checker
            .filter(|p| *p)
            .map(|(node_id, _)| *node_id)
            .collect();

        // Not enough nodes have successful responses
        warn!(
            loglet_id = %self.my_params.loglet_id,
            trim_point = %trim_point,
            known_global_tail = %self.known_global_tail,
            effective_nodeset = %effective_nodeset,
            "Could not trim the loglet, since we could not confirm the new trim point with write-quorum nodes. Nodes that have confirmed are {}",
            confirmed_nodes,
        );

        Err(OperationError::retryable(TrimError))
    }
}

fn on_trim_response(peer: PlainNodeId, msg: Trimmed) -> Disposition<()> {
    if let Status::Ok = msg.header.status {
        Disposition::Return(())
    } else {
        trace!(
            "Trim request failed on node {}, status is {:?}",
            peer, msg.header.status
        );
        Disposition::Abort
    }
}
