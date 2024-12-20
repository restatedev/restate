// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use tokio::sync::mpsc;
use tracing::{debug, trace};

use restate_core::network::rpc_router::{RpcError, RpcRouter};
use restate_core::network::{Incoming, Networking, TransportConnect};
use restate_core::{TaskCenter, TaskKind};
use restate_types::config::Configuration;
use restate_types::logs::{LogletId, LogletOffset, SequenceNumber};
use restate_types::net::log_server::{LogServerRequestHeader, Seal, Sealed, Status};
use restate_types::replicated_loglet::{EffectiveNodeSet, NodeSet, ReplicatedLogletParams};
use restate_types::retries::RetryPolicy;
use restate_types::{GenerationalNodeId, PlainNodeId};

use crate::loglet::util::TailOffsetWatch;
use crate::providers::replicated_loglet::error::ReplicatedLogletError;
use crate::providers::replicated_loglet::replication::NodeSetChecker;

/// Sends a seal request to as many log-servers in the nodeset
///
/// We broadcast the seal to all nodes that we can, but only wait for f-majority
/// responses before acknowleding the seal.
///
/// The seal operation is idempotent. It's safe to seal a loglet if it's already partially or fully
/// sealed. Note that the seal task ignores the "seal" state in the input known_global_tail watch.
pub struct SealTask {
    my_params: ReplicatedLogletParams,
    seal_router: RpcRouter<Seal>,
    known_global_tail: TailOffsetWatch,
}

impl SealTask {
    pub fn new(
        my_params: ReplicatedLogletParams,
        seal_router: RpcRouter<Seal>,
        known_global_tail: TailOffsetWatch,
    ) -> Self {
        Self {
            my_params,
            seal_router,
            known_global_tail,
        }
    }

    pub async fn run<T: TransportConnect>(
        self,
        networking: Networking<T>,
    ) -> Result<LogletOffset, ReplicatedLogletError> {
        // Use the entire nodeset except for StorageState::Disabled.
        let effective_nodeset = EffectiveNodeSet::new(
            &self.my_params.nodeset,
            &networking.metadata().nodes_config_ref(),
        );

        let (tx, mut rx) = mpsc::unbounded_channel();

        let mut nodeset_checker = NodeSetChecker::<'_, bool>::new(
            &effective_nodeset,
            &networking.metadata().nodes_config_ref(),
            &self.my_params.replication,
        );

        let retry_policy = Configuration::pinned()
            .bifrost
            .replicated_loglet
            .log_server_retry_policy
            .clone();

        for node in effective_nodeset.iter() {
            let task = SealSingleNode {
                node_id: *node,
                loglet_id: self.my_params.loglet_id,
                sequencer: self.my_params.sequencer,
                seal_router: self.seal_router.clone(),
                networking: networking.clone(),
                known_global_tail: self.known_global_tail.clone(),
            };
            TaskCenter::spawn_child(TaskKind::Disposable, "send-seal-request", {
                let retry_policy = retry_policy.clone();
                let tx = tx.clone();
                async move {
                    if let Err(e) = task.run(tx, retry_policy).await {
                        // We only want to trace-log if an individual seal request fails.
                        // If we leave the task to fail, task-center will log a scary error-level log
                        // which can be misleading to users.
                        trace!("Seal: {e}");
                    }
                    Ok(())
                }
            })?;
        }
        drop(tx);

        // Max observed local-tail from sealed nodes
        let mut max_tail = LogletOffset::OLDEST;
        while let Some((node_id, local_tail)) = rx.recv().await {
            max_tail = std::cmp::max(max_tail, local_tail);
            nodeset_checker.set_attribute(node_id, true);

            // Do we have f-majority responses?
            if nodeset_checker.check_fmajority(|sealed| *sealed).passed() {
                let sealed_nodes: NodeSet = nodeset_checker
                    .filter(|sealed| *sealed)
                    .map(|(n, _)| *n)
                    .collect();

                debug!(loglet_id = %self.my_params.loglet_id,
                    max_tail = %max_tail,
                    "Seal task completed on f-majority of nodes. Sealed log-servers '{}'",
                    sealed_nodes,
                );
                // note that the rest of seal requests will continue in the background
                return Ok(max_tail);
            }
        }

        // no more tasks left. We this means that we failed to seal
        Err(ReplicatedLogletError::SealFailed(self.my_params.loglet_id))
    }
}

struct SealSingleNode<T> {
    node_id: PlainNodeId,
    loglet_id: LogletId,
    sequencer: GenerationalNodeId,
    seal_router: RpcRouter<Seal>,
    networking: Networking<T>,
    known_global_tail: TailOffsetWatch,
}

impl<T: TransportConnect> SealSingleNode<T> {
    /// Returns local-tail. Note that this will _only_ return if seal was successful, otherwise,
    /// it'll continue to retry.
    pub async fn run(
        self,
        tx: mpsc::UnboundedSender<(PlainNodeId, LogletOffset)>,
        retry_policy: RetryPolicy,
    ) -> anyhow::Result<()> {
        let mut retry_iter = retry_policy.into_iter();
        loop {
            match self.do_seal().await {
                Ok(res) if res.body().sealed || res.body().status == Status::Ok => {
                    let _ = tx.send((self.node_id, res.body().local_tail));
                    self.known_global_tail
                        .notify_offset_update(res.body().header.known_global_tail);
                    return Ok(());
                }
                // not sealed, or seal has failed
                Ok(res) => {
                    // Sent, but sealing not successful
                    trace!(loglet_id = %self.loglet_id, "Seal failed on node {} with status {:?}", self.node_id, res.body().status);
                }
                Err(_) => {
                    trace!(loglet_id = %self.loglet_id, "Failed to send seal message to node {}", self.node_id);
                }
            }
            if let Some(pause) = retry_iter.next() {
                tokio::time::sleep(pause).await;
            } else {
                return Err(anyhow::anyhow!(format!(
                    "Exhausted retries while attempting to seal the loglet {} on node {}",
                    self.loglet_id, self.node_id
                )));
            }
        }
    }

    async fn do_seal(&self) -> Result<Incoming<Sealed>, RpcError<Seal>> {
        let request = Seal {
            header: LogServerRequestHeader::new(
                self.loglet_id,
                self.known_global_tail.latest_offset(),
            ),
            sequencer: self.sequencer,
        };
        trace!(loglet_id = %self.loglet_id, "Sending seal message to node {}", self.node_id);
        self.seal_router
            .call(&self.networking, self.node_id, request)
            .await
    }
}
