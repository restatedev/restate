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

use restate_core::network::{Networking, TransportConnect};
use restate_core::{ShutdownError, TaskCenter};
use restate_types::logs::{KeyFilter, LogletOffset, RecordCache, SequenceNumber};
use restate_types::net::log_server::{GetDigest, LogServerRequestHeader};
use restate_types::replicated_loglet::{EffectiveNodeSet, ReplicatedLogletParams};
use tokio::task::JoinSet;
use tracing::{trace, warn};

use crate::loglet::util::TailOffsetWatch;
use crate::providers::replicated_loglet::read_path::ReadStreamTask;
use crate::providers::replicated_loglet::rpc_routers::LogServersRpc;

use super::digests::Digests;

/// # Overview
///
/// The primary reason for the need to repair the tail is that the sequencer node will mark the record as
/// released _before_ it sends out a Release message to log-servers. We also don't
/// require a quorum of nodes to acknowledge the Release message. This is an important optimization
/// for write latency as the sequencer needs 1-RTT(store wave) to acknowledge a write instead of 2-RTT
/// (store+release).
///
/// The tradeoff is the speed at which we can determine the last global committed tail if the
/// sequencer is not available. There is a number of optimizations that can be added to improve the
/// efficiency of this operation, those include but are not limited to:
/// - Log-servers persisting the last known_global_tail periodically and resetting their local-tail
///   to this value on startup.
/// - Sequencer-driven seal. If the sequencer is alive, it can send a special value with the seal
///   message to indicate what is the actual known-global-tail that nodes should try and repair to
///   instead of relying on the max-tail.
/// - Limit `start_offset` to repair from to max(min(local_tails), max(known_global_tails), known_archived, trim_point)
/// - Archiving records to the external object-store instead of re-replication.
///
/// The process to repair the tail of a loglet so we have confidence in the max-tail value.
/// The goal of repair is to ensure
///
/// The repair process must be reentrant, fault-tolerant, and yields correct immutable state of the
/// tail even with concurrent runs.
///
///
/// # Details
///
/// ## Digest Phase (where are my records?)
/// Given the input range [max(known_global_tails)..max-local-tail], determine the biggest offset
/// at which we can find evidence of a write-quorum of a record, including unauthoritative nodes
/// (`StorageState::DataLoss`). Call this `max_known_durable_offset`. Update the repair range to
/// [max-known-durable-offset..max-local-tail]. if len()=0, we have no work to do. max-local-tail is fully replicated.
///
/// Ask nodes to send a digest first before reading the records? Who has what.
/// - For a digest request [start_offset, max-tail-requested]
/// - Node sends a list that looks like this:
///   (inclusive_from, inclusive_to)
///   [start_offset..t1] -> A. Good to know.
///   [t2..t10] -> X (Spread1) - len = t10-t1 = 9 records. how much does the spread info make
///   a difference here? it just tells me which nodes to read from, but we don't care. we don't
///   need single copy-delivery.
///   [t11..t11] -> X - 1 record
///   this implies [t12..max-tail-requested-1) has no records.
///
///
/// ## Replication Phase (restore replication)
///
/// Starting from the beginning of the range, send a request to read record from nodes according to digest results,
/// This depends on the repair strategy:
///   A) Replicate while ignoring the seal flag to achieve write-quorum. Use original spread information if available and add new nodes
///      if insufficient nodes are writeable.
///   B) Batch the set of records and upload to object-store. Send an update to write-quorum to at-least a write-quorum of nodes to move
///      the known-archived pointer. The write-quorum is not strictly required in this case iff the
///      object-store format allows a cheap query to ask if an offset is archived or not.
/// or every repaired record, update the known_global_tail to allow nodes to move their
/// local-tail during repair. This also allows them to persist this value.
///
/// ## todo: Completion Phase (try to avoid same-range repair in future runs)
/// Once max-tail is reached. Send a Release message with special flag (repair) to update
/// log-servers with the newly agreed-upon known_global_tail. This is a best-effort phase
/// and it should not block the completion of the repair task.
pub struct RepairTail<T> {
    my_params: ReplicatedLogletParams,
    task_center: TaskCenter,
    networking: Networking<T>,
    logservers_rpc: LogServersRpc,
    record_cache: RecordCache,
    known_global_tail: TailOffsetWatch,
    digests: Digests,
}

pub enum RepairTailResult {
    Completed,
    DigestFailed,
    ReplicationFailed,
    // Due to system shutdown
    Shutdown(ShutdownError),
}

impl<T: TransportConnect> RepairTail<T> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        my_params: ReplicatedLogletParams,
        task_center: TaskCenter,
        networking: Networking<T>,
        logservers_rpc: LogServersRpc,
        record_cache: RecordCache,
        known_global_tail: TailOffsetWatch,
        start_offset: LogletOffset,
        target_tail: LogletOffset,
    ) -> Self {
        let digests = Digests::new(&my_params, start_offset, target_tail);
        RepairTail {
            my_params,
            task_center,
            networking,
            logservers_rpc,
            record_cache,
            known_global_tail,
            digests,
        }
    }

    pub async fn run(mut self) -> RepairTailResult {
        if self.digests.is_finished() {
            return RepairTailResult::Completed;
        }
        let mut get_digest_requests = JoinSet::new();
        let effective_nodeset = EffectiveNodeSet::new(
            &self.my_params.nodeset,
            &self.networking.metadata().nodes_config_ref(),
        );

        // Dispatch GetDigest to all readable nodes
        for node in effective_nodeset.iter() {
            let msg = GetDigest {
                header: LogServerRequestHeader::new(
                    self.my_params.loglet_id,
                    self.known_global_tail.latest_offset(),
                ),
                from_offset: self.digests.start_offset(),
                to_offset: self.digests.target_tail().prev(),
            };
            get_digest_requests.spawn({
                let tc = self.task_center.clone();
                let networking = self.networking.clone();
                let logservers_rpc = self.logservers_rpc.clone();
                let peer = *node;
                async move {
                    tc.run_in_scope("get-digest-from-node", None, async move {
                        loop {
                            // todo: handle retries with exponential backoff...
                            let Ok(incoming) = logservers_rpc
                                .get_digest
                                .call(&networking, peer, msg.clone())
                                .await
                            else {
                                tokio::time::sleep(Duration::from_secs(1)).await;
                                continue;
                            };
                            return incoming;
                        }
                    })
                    .await
                }
            });
        }

        // # Digest Phase
        while let Some(Ok(digest_message)) = get_digest_requests.join_next().await {
            let peer_node = digest_message.peer().as_plain();
            self.digests.on_digest_message(
                peer_node,
                digest_message.into_body(),
                &self.known_global_tail,
            );
            if self
                .digests
                .advance(&self.networking.metadata().nodes_config_ref())
            {
                trace!(
                    loglet_id = %self.my_params.loglet_id,
                    node_id = %peer_node,
                    "Digest phase completed."
                );
                break;
            }
            // can we start repair, but continue to accept digest responses as repair is on-going.
        }

        if self.digests.is_finished() {
            return RepairTailResult::Completed;
        }

        // No enough nodes responded to be able to repair
        if !self
            .digests
            .can_repair(&self.networking.metadata().nodes_config_ref())
        {
            return RepairTailResult::DigestFailed;
        }

        // Keep reading responses from digest requests since it can assist moving the start_offset
        // during repair. In this case, we'll fast-forward and ignore replication for the updated
        // range, this might lead to some over-replication and that's fine.
        // For every record between start_offset->target_tail.prev() we need to replicate enough
        // copies to satisfy the write-quorum.
        //
        // Replication goes in the direction of "start_offset" towards the tail, one record at a
        // a time.
        let Ok((mut rx, read_stream_task)) = ReadStreamTask::start(
            self.my_params.clone(),
            self.networking.clone(),
            self.logservers_rpc.clone(),
            KeyFilter::Any,
            self.digests.start_offset(),
            Some(self.digests.target_tail().prev()),
            self.known_global_tail.clone(),
            self.record_cache.clone(),
            /* move-beyond-global-tail = */ true,
        )
        .await
        else {
            return RepairTailResult::Shutdown(ShutdownError);
        };

        'replication_phase: loop {
            if self.digests.is_finished() {
                break;
            }
            tokio::select! {
                // we fail the readstream during shutdown only, in that case, there is not much
                // we can do but to stop.
                Some(Ok(entry)) = rx.recv()  => {
                    // we received a record. Should we replicate it?
                    if let Err(e) = self.digests.replicate_record_and_advance(
                        entry,
                        self.my_params.sequencer,
                        &self.networking,
                        &self.logservers_rpc.store,
                    ).await {
                        warn!(error=%e, "Failed to replicate record while repairing the tail");
                        break 'replication_phase;
                    }
                }
                Some(Ok(digest_message)) = get_digest_requests.join_next() => {
                    let peer_node = digest_message.peer().as_plain();
                    self.digests.on_digest_message(
                        peer_node,
                        digest_message.into_body(),
                        &self.known_global_tail,
                    );
                    self.digests.advance(&self.networking.metadata().nodes_config_ref());

                }
                    // we have no more work to do. We'll likely fail.
                else => {}
            }
        }

        read_stream_task.abort();
        get_digest_requests.abort_all();

        // Are we complete?
        if self.digests.is_finished() {
            return RepairTailResult::Completed;
        }

        warn!(
            loglet_id = %self.my_params.loglet_id,
            "Failed to repair the tail. The unrepaired region is from {} to {}",
            self.digests.start_offset(),
            self.digests.target_tail().prev()
        );
        RepairTailResult::ReplicationFailed
    }
}
