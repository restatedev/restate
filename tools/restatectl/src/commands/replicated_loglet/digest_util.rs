// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, HashMap};

use tracing::warn;

use restate_types::PlainNodeId;
use restate_types::logs::TailOffsetWatch;
use restate_types::logs::{LogletId, LogletOffset, SequenceNumber};
use restate_types::net::log_server::{Digest, LogServerResponseHeader, RecordStatus, Status};
use restate_types::replicated_loglet::ReplicatedLogletParams;

/// Tracks digest responses and record statuses
pub struct DigestsHelper {
    loglet_id: LogletId,
    // all offsets `[start_offset..target_tail)`
    offsets: BTreeMap<LogletOffset, HashMap<PlainNodeId, RecordStatus>>,
    known_nodes: HashMap<PlainNodeId, LogServerResponseHeader>,
    max_local_tail: LogletOffset,
    max_trim_point: LogletOffset,
}

impl DigestsHelper {
    pub fn new(
        my_params: &ReplicatedLogletParams,
        start_offset: LogletOffset,
        target_tail: LogletOffset,
    ) -> Self {
        let offsets = if start_offset >= target_tail {
            Default::default()
        } else {
            BTreeMap::from_iter(
                (*start_offset..*target_tail).map(|o| (LogletOffset::new(o), Default::default())),
            )
        };

        Self {
            loglet_id: my_params.loglet_id,
            known_nodes: Default::default(),
            offsets,
            max_trim_point: LogletOffset::INVALID,
            max_local_tail: LogletOffset::INVALID,
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
        self.known_nodes.get(node_id).map(|header| header.sealed)
    }

    pub fn is_known(&self, node_id: &PlainNodeId) -> bool {
        self.known_nodes.contains_key(node_id)
    }

    pub fn max_local_tail(&self) -> LogletOffset {
        self.max_local_tail
    }

    pub fn get_response_header(&self, node_id: &PlainNodeId) -> Option<LogServerResponseHeader> {
        self.known_nodes.get(node_id).cloned()
    }

    pub fn iter(
        &self,
    ) -> impl Iterator<Item = (&LogletOffset, &HashMap<PlainNodeId, RecordStatus>)> {
        self.offsets.range(..)
    }
}
