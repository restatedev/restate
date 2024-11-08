// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, HashMap};

use restate_bifrost::providers::replicated_loglet::replication::NodeSetChecker;
use restate_types::nodes_config::NodesConfiguration;
use tracing::warn;

use restate_bifrost::loglet::util::TailOffsetWatch;
use restate_bifrost::providers::replicated_loglet::replication::spread_selector::{
    SelectorStrategy, SpreadSelector,
};
use restate_types::logs::{LogletOffset, SequenceNumber, TailState};
use restate_types::net::log_server::{Digest, LogServerResponseHeader, RecordStatus, Status};
use restate_types::replicated_loglet::{NodeSet, ReplicatedLogletId, ReplicatedLogletParams};
use restate_types::PlainNodeId;

/// Tracks digest responses and record statuses
pub struct DigestsHelper {
    loglet_id: ReplicatedLogletId,
    // all offsets `[start_offset..target_tail)`
    offsets: BTreeMap<LogletOffset, HashMap<PlainNodeId, RecordStatus>>,
    known_nodes: HashMap<PlainNodeId, LogServerResponseHeader>,
    max_local_tail: LogletOffset,
    max_trim_point: LogletOffset,
    spread_selector: SpreadSelector,
}

impl DigestsHelper {
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

        Self {
            loglet_id: my_params.loglet_id,
            known_nodes: Default::default(),
            offsets,
            max_trim_point: LogletOffset::INVALID,
            max_local_tail: LogletOffset::INVALID,
            spread_selector,
        }
    }

    /// Processes an incoming digest message from a node. Entries outside the current range are ignored.
    /// This will also update the known_global_tail from digest message headers as expected
    pub fn add_digest_message(
        &mut self,
        peer_node: PlainNodeId,
        msg: Digest,
        known_global_tail: &TailOffsetWatch,
    ) {
        known_global_tail.notify(msg.header.sealed, msg.header.known_global_tail);
        self.max_local_tail = self.max_local_tail.max(msg.header.local_tail);

        if msg.header.status != Status::Ok {
            return;
        }

        if self.known_nodes.contains_key(&peer_node) {
            warn!(
                loglet_id = %self.loglet_id,
                node_id = %peer_node,
                "We have received a successful digest from this node already!"
            );
            return;
        }

        self.known_nodes.insert(peer_node, msg.header.clone());

        for entry in msg.entries {
            if let RecordStatus::Trimmed = entry.status {
                self.max_trim_point = self.max_trim_point.max(entry.to_offset);
            }
            for (_, copies) in self.offsets.range_mut(entry.from_offset..=entry.to_offset) {
                copies.insert(peer_node, entry.status.clone());
            }
        }
    }

    pub fn is_sealed(&self, node_id: &PlainNodeId) -> Option<bool> {
        if let Some(header) = self.known_nodes.get(node_id) {
            Some(header.sealed)
        } else {
            None
        }
    }

    pub fn is_known(&self, node_id: &PlainNodeId) -> bool {
        self.known_nodes.contains_key(node_id)
    }

    pub fn max_trim_point(&self) -> LogletOffset {
        self.max_trim_point
    }

    pub fn max_local_tail(&self) -> LogletOffset {
        self.max_local_tail
    }

    pub fn iter(
        &self,
    ) -> impl Iterator<Item = (&LogletOffset, &HashMap<PlainNodeId, RecordStatus>)> {
        self.offsets.range(..)
    }
}
