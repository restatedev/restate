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

use rand::rng;
use tokio::task::JoinSet;
use tracing::{debug, trace, warn};

use restate_core::network::{NetworkSender, Networking, Swimlane, TransportConnect};
use restate_core::{Metadata, ShutdownError, TaskCenterFutureExt, cancellation_watcher};
use restate_types::logs::{LogletId, LogletOffset, SequenceNumber, TailOffsetWatch};
use restate_types::net::log_server::{
    Digest, LogServerRequestHeader, RecordStatus, Status, Store, StoreFlags,
};
use restate_types::nodes_config::NodesConfiguration;
use restate_types::replicated_loglet::ReplicatedLogletParams;
use restate_types::replication::{NodeSet, NodeSetChecker};
use restate_types::{GenerationalNodeId, PlainNodeId};

use crate::LogEntry;
use crate::loglet::OperationError;
use crate::providers::replicated_loglet::replication::spread_selector::{
    SelectorStrategy, SpreadSelector,
};

#[derive(Debug, thiserror::Error)]
#[error("could not replicate record, exhausted all store attempts")]
struct ReplicationFailed;

/// Tracks digest responses and record repairs to achieve a consistent and durable
/// state of the loglet tail.
pub struct Digests {
    loglet_id: LogletId,
    /// inclusive. The first record we need to repair.
    start_offset: LogletOffset,
    /// exclusive (this should be the durable global_tail after finishing)
    target_tail: LogletOffset,
    /// all offsets `[start_offset..target_tail)`
    offsets_under_repair: BTreeMap<LogletOffset, NodeSet>,
    known_nodes: NodeSet,
    spread_selector: SpreadSelector,
    /// number of records we have re-replicated
    num_fixups: usize,
}

impl Digests {
    pub fn new(
        my_params: &ReplicatedLogletParams,
        start_offset: LogletOffset,
        target_tail: LogletOffset,
    ) -> Self {
        let offsets = if start_offset >= target_tail {
            // already finished
            Default::default()
        } else {
            BTreeMap::from_iter(
                (*start_offset..*target_tail).map(|o| (LogletOffset::new(o), Default::default())),
            )
        };

        let spread_selector = SpreadSelector::new(
            my_params.nodeset.clone(),
            // todo: should be from the same configuration source as what the sequencer uses.
            SelectorStrategy::Flood,
            my_params.replication.clone(),
        );

        Digests {
            loglet_id: my_params.loglet_id,
            start_offset,
            target_tail,
            known_nodes: Default::default(),
            offsets_under_repair: offsets,
            spread_selector,
            num_fixups: 0,
        }
    }

    /// The start of the range currently under repair
    pub fn start_offset(&self) -> LogletOffset {
        self.start_offset
    }

    /// The target tail we are repairing to
    pub fn target_tail(&self) -> LogletOffset {
        self.target_tail
    }

    /// Number of records that have been replicated
    pub fn num_fixups(&self) -> usize {
        self.num_fixups
    }

    /// The nodes that chimed in during digest
    pub fn known_nodes(&self) -> &NodeSet {
        &self.known_nodes
    }

    /// If true, no repairs are needed
    pub fn is_finished(&self) -> bool {
        let res = self.start_offset >= self.target_tail;
        if res {
            debug_assert!(
                self.offsets_under_repair.is_empty(),
                "no records should be under repair if digest said we are finished"
            );
        }
        res
    }

    /// Processes an incoming digest message from a node. Entries outside the current range under
    /// repair are ignored.
    ///
    /// This will also update the known_global_tail from digest message headers as expected
    pub fn on_digest_message(
        &mut self,
        peer_node: PlainNodeId,
        msg: Digest,
        known_global_tail: &TailOffsetWatch,
    ) {
        known_global_tail.notify_offset_update(msg.header.known_global_tail);
        self.update_start_offset(known_global_tail.latest_offset());

        if msg.header.status != Status::Ok {
            return;
        }

        if !self.known_nodes.insert(peer_node) {
            warn!(
                loglet_id = %self.loglet_id,
                node_id = %peer_node,
                "We have received a successful digest from this node already!"
            );
            return;
        }

        if self.is_finished() {
            return;
        }

        for entry in msg.entries {
            // todo: consider handling Archived and Trimmed responses.
            if entry.status == RecordStatus::Exists {
                for (_, copies) in self
                    .offsets_under_repair
                    .range_mut(entry.from_offset..=entry.to_offset)
                {
                    copies.insert(peer_node);
                }
            }
        }
    }

    /// Attempts to copy the given entry to satisfy its replication property if needed.
    ///
    /// This returns when the record is sufficiently replicated.
    pub async fn replicate_record_and_advance<T: TransportConnect>(
        &mut self,
        entry: LogEntry<LogletOffset>,
        sequencer: GenerationalNodeId,
        networking: &Networking<T>,
    ) -> Result<(), OperationError> {
        let metadata = Metadata::current();
        let offset = entry.sequence_number();
        if offset < self.start_offset {
            // Ignore this record. We have already moved past this offset.
            return Ok(());
        }
        // If we see a trim gap, we always assume that the trim-gap is up to a previously known
        // global tail value so we fast forward until the end of the gap.
        if entry.is_trim_gap() {
            debug!(
                loglet_id = %self.loglet_id,
                "Observed a trim gap from offset {} to {} while repairing the tail of the loglet, \
                 those offsets will be considered repaired",
                offset,
                entry.trim_gap_to_sequence_number().unwrap(),
            );
            self.update_start_offset(entry.next_sequence_number());
            return Ok(());
        }

        let known_copies = self
            .offsets_under_repair
            .get(&offset)
            .expect("repairing an offset that we have not truncated");

        // See how many copies do we need to do to achieve write quorum.
        let fixup_nodes = self
            .spread_selector
            .select_fixups(
                known_copies,
                &mut rng(),
                &metadata.nodes_config_ref(),
                &NodeSet::default(),
            )
            // what do we do if we can't generate a spread? nodes are in data-loss or readonly, or
            // whatever state.
            // We definitely cannot proceed but should we retry? let's only do that when we need to,
            // for now, we bail.
            .map_err(OperationError::retryable)?;

        trace!(
            loglet_id = %self.loglet_id,
            %offset,
            "Repairing record, existing copies on {} and fixup nodes are {}",
            known_copies,
            fixup_nodes,
        );

        // We handled trim gaps before this point.
        let record = entry.into_record().expect("must be a data record");

        let mut replication_checker = NodeSetChecker::new(
            self.spread_selector.nodeset(),
            &metadata.nodes_config_ref(),
            self.spread_selector.replication_property(),
        );
        // record is already replicated on those nodes
        replication_checker.set_attribute_on_each(known_copies.iter().copied(), true);

        let payloads = vec![record].into();

        let msg = Store {
            // As we send store messages, we consider the start_offset a reliable source of
            // `global_known_tail`. This allows log-servers to accept those writes if they were still
            // behind.
            header: LogServerRequestHeader::new(self.loglet_id, self.start_offset),
            timeout_at: None,
            // Must be set to bypass the seal
            flags: StoreFlags::IgnoreSeal,
            first_offset: offset,
            sequencer,
            known_archived: LogletOffset::INVALID,
            payloads,
        };
        // We run stores as tasks because we'll wait only for the necessary write-quorum but the
        // rest of the stores can continue in the background as best-effort replication (if the
        // spread selector strategy picked extra nodes)
        let mut inflight_stores = JoinSet::new();
        for node in fixup_nodes {
            inflight_stores
                .build_task()
                .name("replicate-records")
                .spawn({
                    let networking = networking.clone();
                    let msg = msg.clone();
                    let loglet_id = self.loglet_id;

                    async move {
                        (
                            node,
                            networking
                                .call_rpc(
                                    node,
                                    Swimlane::BifrostData,
                                    msg,
                                    Some(loglet_id.into()),
                                    None,
                                )
                                .await,
                        )
                    }
                    .in_current_tc()
                })
                .expect("to spawn store task for replicating records");
        }
        let mut cancel = std::pin::pin!(cancellation_watcher());

        loop {
            if replication_checker.check_write_quorum(|attr| *attr) {
                trace!(
                    loglet_id = %self.loglet_id,
                    %offset,
                    "Record has been repaired"
                );
                self.num_fixups += 1;
                // record has been fully replicated.
                self.update_start_offset(offset.next());
                inflight_stores.detach_all();
                return Ok(());
            }

            if inflight_stores.is_empty() {
                // No more store attempts left. We couldn't replicate this record.
                return Err(OperationError::retryable(ReplicationFailed));
            }

            let stored_on_peer = tokio::select! {
                _ = &mut cancel => {
                    return Err(OperationError::Shutdown(ShutdownError));
                }
                Some(Ok((peer, maybe_stored))) = inflight_stores.join_next() => {
                    // maybe_stored is err if we can't send the store (or shutdown)
                    match maybe_stored {
                        Ok(stored) if stored.header.status == Status::Ok =>  {
                            Some(peer)
                        }
                        Ok(stored) => {
                            // Store failed with some non-ok status
                            debug!(
                                loglet_id = %self.loglet_id,
                                %peer,
                                %offset,
                                "Could not store record on node as part of the tail repair procedure. Log server responded with status={:?}",
                                stored.header.status
                            );
                            None
                        }
                        // give up on this store.
                        Err(e) => {
                            debug!(
                                loglet_id = %self.loglet_id,
                                %peer,
                                %offset,
                                %e,
                                "Could not store record on node as part of the tail repair procedure. Network error",
                            );
                            None
                        }
                    }
                }
            };

            if let Some(stored_on_peer) = stored_on_peer {
                replication_checker.set_attribute(stored_on_peer, true);
            }
        }
    }

    pub fn can_repair(&self, nodes_config: &NodesConfiguration) -> bool {
        // only do that if known_nodes can satisfy write-quorum.
        let mut checker = NodeSetChecker::new(
            self.spread_selector.nodeset(),
            nodes_config,
            self.spread_selector.replication_property(),
        );
        checker.set_attribute_on_each(self.known_nodes.iter().copied(), true);
        checker.check_write_quorum(|known| *known)
    }

    // returns true if we can advance to repair
    pub fn advance(&mut self, nodes_config: &NodesConfiguration) -> bool {
        if !self.can_repair(nodes_config) {
            // we don't have write-quorum of nodes yet, we can't advance start_offset.
            return false;
        }
        let mut range = self.offsets_under_repair.range(..);
        let mut checker = NodeSetChecker::new(
            self.spread_selector.nodeset(),
            nodes_config,
            self.spread_selector.replication_property(),
        );
        // walk backwards
        while let Some((offset, nodes)) = range.next_back() {
            checker.fill_with_default();
            checker.set_attribute_on_each(nodes.iter().copied(), true);
            if checker.check_write_quorum(|known| *known) {
                // this offset is good, advance to the next one
                self.update_start_offset(offset.next());
                return true;
            }
        }
        // nothing to repair.
        if self.is_finished() {
            return true;
        }
        // when can we advance to repair from digest?
        // - When we have enough digests to find at least a single copy of the oldest record in the
        // repair range.
        // - we have write-quorum of "writeable" nodes that responded to our digest requests (even if they
        // don't have the records)
        !self
            .offsets_under_repair
            .first_key_value()
            .expect("must have at least one if we are not finished")
            .1
            .is_empty()
    }

    fn update_start_offset(&mut self, new_start_offset: LogletOffset) {
        let original = self.start_offset;
        self.start_offset = self.start_offset.max(new_start_offset);
        if self.start_offset != original {
            self.truncate_range();
        }
    }

    fn truncate_range(&mut self) {
        if self.start_offset >= self.target_tail {
            self.offsets_under_repair.clear();
        } else {
            self.offsets_under_repair = self.offsets_under_repair.split_off(&self.start_offset);
        }
    }
}
