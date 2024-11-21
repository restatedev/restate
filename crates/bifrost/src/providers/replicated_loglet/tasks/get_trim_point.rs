// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use tokio::task::JoinSet;
use tracing::trace;

use restate_core::network::{Incoming, Networking, TransportConnect};
use restate_core::task_center;
use restate_types::config::Configuration;
use restate_types::logs::{LogletOffset, SequenceNumber};
use restate_types::net::log_server::{GetLogletInfo, LogServerRequestHeader, LogletInfo, Status};
use restate_types::replicated_loglet::{EffectiveNodeSet, ReplicatedLogletParams};

use crate::loglet::util::TailOffsetWatch;
use crate::loglet::OperationError;
use crate::providers::replicated_loglet::replication::NodeSetChecker;
use crate::providers::replicated_loglet::rpc_routers::LogServersRpc;
use crate::providers::replicated_loglet::tasks::util::{Disposition, RunOnSingleNode};

#[derive(Debug, thiserror::Error)]
#[error("could not determine the trim point of the loglet")]
struct GetTrimPointError;

/// Find the trim point for a loglet
///
/// The trim point doesn't require a quorum-check, the maximum observed trim-point on
/// log-servers can be used, but we wait for write-quorum (or) f-majority whichever happens first
/// before we respond to increase the chances of getting a reliable trim point.
///
/// We don't provide a guarantee that `get_trim_point` return an always increasing offset,
/// and it should not be used in gap-detection in read streams. In read-streams, the
/// observed trim_point in record header is what's used to perform the correct
/// gap-detection.
pub struct GetTrimPointTask<'a> {
    my_params: &'a ReplicatedLogletParams,
    logservers_rpc: LogServersRpc,
    known_global_tail: TailOffsetWatch,
}

impl<'a> GetTrimPointTask<'a> {
    pub fn new(
        my_params: &'a ReplicatedLogletParams,
        logservers_rpc: LogServersRpc,
        known_global_tail: TailOffsetWatch,
    ) -> Self {
        Self {
            my_params,
            logservers_rpc,
            known_global_tail,
        }
    }

    pub async fn run<T: TransportConnect>(
        self,
        networking: Networking<T>,
    ) -> Result<Option<LogletOffset>, OperationError> {
        // Special case:
        // If all nodes in the nodeset are in "provisioning", we can confidently short-circuit
        // the result to None assuming that this loglet has never been trimmed.
        if self
            .my_params
            .nodeset
            .all_provisioning(&networking.metadata().nodes_config_ref())
        {
            return Ok(None);
        }
        // Use the entire nodeset except for StorageState::Disabled.
        let effective_nodeset = EffectiveNodeSet::new(
            &self.my_params.nodeset,
            &networking.metadata().nodes_config_ref(),
        );

        trace!(
            loglet_id = %self.my_params.loglet_id,
            known_global_tail = %self.known_global_tail,
            effective_nodeset = %effective_nodeset,
            "Finding trim point for loglet"
        );

        let mut inflight_requests = JoinSet::new();
        for node_id in effective_nodeset.iter().copied() {
            let request = GetLogletInfo {
                header: LogServerRequestHeader::new(
                    self.my_params.loglet_id,
                    self.known_global_tail.latest_offset(),
                ),
            };

            inflight_requests.spawn({
                let networking = networking.clone();
                let trim_rpc_router = self.logservers_rpc.get_loglet_info.clone();
                let known_global_tail = self.known_global_tail.clone();
                let tc = task_center();

                async move {
                    let task = RunOnSingleNode::new(
                        node_id,
                        request,
                        &trim_rpc_router,
                        &known_global_tail,
                        Configuration::pinned()
                            .bifrost
                            .replicated_loglet
                            .log_server_retry_policy
                            .clone(),
                    );
                    (
                        node_id,
                        tc.run_in_scope(
                            "find-trimpoint-on-node",
                            None,
                            task.run(on_info_response, &networking),
                        )
                        .await,
                    )
                }
            });
        }

        let mut nodeset_checker = NodeSetChecker::<'_, Option<LogletOffset>>::new(
            &effective_nodeset,
            &networking.metadata().nodes_config_ref(),
            &self.my_params.replication,
        );

        let predicate = |o: &Option<LogletOffset>| o.is_some();
        // Waiting for trim responses
        while let Some(res) = inflight_requests.join_next().await {
            let Ok((node_id, res)) = res else {
                // task panicked or runtime is shutting down.
                continue;
            };
            let Ok(res) = res else {
                // GetLogletInfo task failed/aborted on this node. The inner task will log the error in this case.
                continue;
            };

            // either f-majority or write-quorum is enough
            nodeset_checker.set_attribute(node_id, Some(res));
            if nodeset_checker.check_write_quorum(predicate)
                || nodeset_checker.check_fmajority(predicate).passed()
            {
                break;
            }
        }

        let results_from_nodes: Vec<_> = nodeset_checker.filter(predicate).collect();
        if results_from_nodes.is_empty() {
            // We didn't get _any_ responses!
            return Err(OperationError::retryable(GetTrimPointError));
        }

        let max_trim_point = nodeset_checker
            .filter(predicate)
            .map(|(_, o)| o.unwrap()) // we filter Some already
            .max()
            .expect("at least one node returned trim point");
        trace!(
            loglet_id = %self.my_params.loglet_id,
            known_global_tail = %self.known_global_tail,
            effective_nodeset = %effective_nodeset,
            "Trim point was determined to be {} calculated from this result set {:?}",
            max_trim_point,
            results_from_nodes,
        );
        // todo: assist in converging trim point of nodes are not aligned.
        if max_trim_point == LogletOffset::INVALID {
            Ok(None)
        } else {
            Ok(Some(max_trim_point))
        }
    }
}

fn on_info_response(msg: Incoming<LogletInfo>) -> Disposition<LogletOffset> {
    if let Status::Ok = msg.body().header.status {
        Disposition::Return(msg.body().trim_point)
    } else {
        trace!(
            "GetLogletInfo request failed on node {}, status is {:?}",
            msg.peer(),
            msg.body().header.status
        );
        Disposition::Abort
    }
}
