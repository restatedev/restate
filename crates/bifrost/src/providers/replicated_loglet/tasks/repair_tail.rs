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
use tracing::{Instrument, debug, error, info, warn};

use restate_core::network::{NetworkSender, Networking, Swimlane, TransportConnect};
use restate_core::{Metadata, ShutdownError, TaskCenterFutureExt};
use restate_types::logs::{KeyFilter, LogletOffset, RecordCache, SequenceNumber, TailOffsetWatch};
use restate_types::net::log_server::{GetDigest, LogServerRequestHeader};
use restate_types::replicated_loglet::{LogNodeSetExt, ReplicatedLogletParams};

use crate::providers::replicated_loglet::read_path::ReadStreamTask;

use super::digests::Digests;

/// # Overview
///
/// The primary reason for why we need such a procedure is because the sequencer node marks the record as
/// released _before_ it sends out a Release message to log-servers. In other words, we don't require a
/// quorum of nodes to acknowledge the `Release` message to consider a record committed. This is an important
/// design optimization to reduce latency of the write path. The sequencer needs 1-RTT (store wave) to commit
/// a record rather than 2-RTT (store+release).
///
/// The tradeoff is the speed at which we can determine the last global committed tail if the sequencer is not
/// available. There is a number of optimizations that can be added to improve the efficiency of this operation,
/// those include but are not limited to:
/// 1. Log-servers persisting the last known_global_tail periodically/async and using this value as known_global_tail on startup.
/// 2. Sequencer-driven seal. If the sequencer is alive, it can send a special value with the seal message to
///    indicate what is the ultimate known-global-tail that nodes should repair to instead of relying on the observed max-tail.
/// 3. Limit `from_offset` to repair from to max(min(local_tails), max(known_global_tails), known_archived, trim_point)
/// 4. Archiving records to the external object-store instead of re-replication, although, this
///    doesn't improve the efficiency of the repair operation itself.
///
/// The repair process must be reentrant, fault-tolerant, and yields correct immutable state of the
/// tail even with concurrent runs.
///
/// # Details
///
/// ## Digest Phase (where are my records?)
/// Given the input range [max(known_global_tails)..max-local-tail], determine the biggest offset at which we can
/// find evidence of a write-quorum of a record, including unauthoritative nodes (`StorageState::DataLoss`).
/// Let's call this `max_known_durable_offset`. We narrow down the repair range to [max_known_durable_offset+1..max-local-tail).
/// If len()=0, we have no work to do. max-local-tail is fully replicated.
///
/// Ask nodes to send a digest first before reading the records? Who has what.
/// - For a digest request [start_offset, max-tail-requested]
/// - Node sends a list that looks like this:
///   FROM           TO  -> STATUS
///   [start_offset..t1] -> A (Archived)                      Good to know. (A = Archived)
///   [t2..t10]          -> X (Exists)                        len = t10-t1 = 9 records exists on this node
///   [t11..t11]         -> X
///   this implies [t12..max-tail-requested-1) has no records.
///
/// ## Replication Phase (restore replication)
///
/// Starting from the beginning of the range, we create a special read-stream to read records up to
/// the target tail.
/// Then we send a store wave to fixup the replication of this record while ignoring the seal flag to achieve write-quorum.
/// We use knowledge obtained from digests to decide which nodes to replicate to, if insufficient nodes are writeable.
/// [Future] Batch the set of records and upload to object-store. Update the archived pointer a write-quorum of nodes after upload.
/// The write-quorum is not strictly required in the case where the object-store allows a cheap query to ask if an offset is archived or not.
///
/// Update the known_global_tail to allow nodes to move their local-tail during repair. This also allows them to persist this value.
///
/// ## todo: Completion Phase (try to avoid same-range repair in future runs)
/// Once max-tail is reached. Send a Release message with special flag (repair) to update log-servers with the newly agreed-upon
/// known_global_tail. This is a best-effort phase and it should not block the completion of the repair task.
pub struct RepairTail<T> {
    my_params: ReplicatedLogletParams,
    networking: Networking<T>,
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
        networking: Networking<T>,
        record_cache: RecordCache,
        known_global_tail: TailOffsetWatch,
        start_offset: LogletOffset,
        target_tail: LogletOffset,
    ) -> Self {
        let digests = Digests::new(&my_params, start_offset, target_tail);
        RepairTail {
            my_params,
            networking,
            record_cache,
            known_global_tail,
            digests,
        }
    }

    pub async fn run(mut self) -> RepairTailResult {
        let metadata = Metadata::current();
        if self.digests.is_finished() {
            debug!(
                loglet_id = %self.my_params.loglet_id,
                known_global_tail = %self.known_global_tail.latest_offset(),
                target_tail = %self.digests.target_tail(),
                "Repair task completed, no records required repairing"
            );
            return RepairTailResult::Completed;
        }
        let start = Instant::now();
        info!(
            loglet_id = %self.my_params.loglet_id,
            start_offset = %self.digests.start_offset(),
            target_tail = %self.digests.target_tail(),
            "Tail records are under-replicated, starting a repair tail task",
        );
        let mut get_digest_requests = JoinSet::new();
        let effective_nodeset = self
            .my_params
            .nodeset
            .to_effective(&metadata.nodes_config_ref());

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
            get_digest_requests
                .build_task()
                .name("get-digest")
                .spawn({
                    let networking = self.networking.clone();
                    let peer = *node;
                    async move {
                        networking
                            .call_rpc(
                                peer,
                                Swimlane::default(),
                                msg.clone(),
                                Some(self.my_params.loglet_id.into()),
                                None,
                            )
                            .await
                            .map(|reply| (peer, reply))
                    }
                    .in_current_tc()
                    .in_current_span()
                })
                .expect("to spawn get digest task");
        }

        // # Digest Phase
        while let Some(Ok(response)) = get_digest_requests.join_next().await {
            let Ok((peer_node, digest_message)) = response else {
                // ignore nodes that we can't get digest from
                continue;
            };
            self.digests
                .on_digest_message(peer_node, digest_message, &self.known_global_tail);
            debug!(loglet_id = %self.my_params.loglet_id, "Received digest from {}", peer_node);
            if self.digests.advance(&metadata.nodes_config_ref()) {
                break;
                // can we start repair, but continue to accept digest responses as repair is on-going.
            }
            debug!(
                loglet_id = %self.my_params.loglet_id,
                "Still need more nodes to chime in for offsets between {} and {}. We already heard from nodes {}",
                self.digests.start_offset(),
                self.digests.target_tail(),
                self.digests.known_nodes()
            );
        }
        debug!(
            loglet_id = %self.my_params.loglet_id,
            start_offset = %self.digests.start_offset(),
            target_tail = %self.digests.target_tail(),
            elapsed = ?start.elapsed(),
            "Digest phase completed."
        );

        if self.digests.is_finished() {
            debug!(
                loglet_id = %self.my_params.loglet_id,
                known_global_tail = %self.known_global_tail.latest_offset(),
                target_tail = %self.digests.target_tail(),
                elapsed = ?start.elapsed(),
                "Repair task completed, no records required repairing"
            );
            return RepairTailResult::Completed;
        }

        // No enough nodes responded to be able to repair
        if !self.digests.can_repair(&metadata.nodes_config_ref()) {
            error!(
                loglet_id = %self.my_params.loglet_id,
                start_offset = %self.digests.start_offset(),
                target_tail = %self.digests.target_tail(),
                nodeset = %self.my_params.nodeset,
                nodes_responded = %self.digests.known_nodes(),
                replication = %self.my_params.replication,
                elapsed = ?start.elapsed(),
                "Couldn't repair the tail! We have records to repair but not enough writeable nodes \
                have responded to our digest request. We'll not be able to re-replicate the missing records until \
                they are online and responsive",
            );
            return RepairTailResult::DigestFailed;
        }

        // we have enough nodes to form write quorum, but we cannot repair because no nodes have
        // reported any copies for the oldest record within the repair range.
        // couldn't find any node with copies
        if !self.digests.advance(&metadata.nodes_config_ref()) {
            error!(
                loglet_id = %self.my_params.loglet_id,
                start_offset = %self.digests.start_offset(),
                target_tail = %self.digests.target_tail(),
                nodeset = %self.my_params.nodeset,
                nodes_responded = %self.digests.known_nodes(),
                replication = %self.my_params.replication,
                elapsed = ?start.elapsed(),
                "Couldn't repair the tail! We have records to repair **and** enough nodes to repair, but \
                 we couldn't find any node with copies for the oldest record within the repair range. \
                 We'll not be able to re-replicate the missing records until we can read back this \
                 record",
            );
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
                Some(Ok(entry)) = rx.recv() => {
                    // we received a record. Should we replicate it?
                    if let Err(e) = self.digests.replicate_record_and_advance(
                        entry,
                        self.my_params.sequencer,
                        &self.networking,
                    ).await {
                        warn!(error=%e, "Failed to replicate record while repairing the tail");
                        break 'replication_phase;
                    }
                }
                Some(Ok(Ok((peer_node, digest_message)))) = get_digest_requests.join_next() => {
                    self.digests.on_digest_message(
                        peer_node,
                        digest_message,
                        &self.known_global_tail,
                    );
                    self.digests.advance(&metadata.nodes_config_ref());

                }
                    // we have no more work to do. We'll likely fail.
                else => {}
            }
        }

        read_stream_task.abort();
        get_digest_requests.abort_all();

        // Are we complete?
        if self.digests.is_finished() {
            info!(
                loglet_id = %self.my_params.loglet_id,
                known_global_tail = %self.known_global_tail.latest_offset(),
                elapsed = ?start.elapsed(),
                "Repair task completed, {} record(s) have been repaired",
                self.digests.num_fixups(),
            );
            return RepairTailResult::Completed;
        }

        warn!(
            loglet_id = %self.my_params.loglet_id,
            nodeset = %self.my_params.nodeset,
            nodes_responded = %self.digests.known_nodes(),
            replication = %self.my_params.replication,
            elapsed = ?start.elapsed(),
            "Failed to repair the tail. The under-replicated region is from {} to {} [inclusive]. \
             {} records have been repaired during the process",
            self.digests.start_offset(),
            self.digests.target_tail().prev(),
            self.digests.num_fixups(),
        );
        RepairTailResult::ReplicationFailed
    }
}
