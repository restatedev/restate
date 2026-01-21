// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::network::RpcReplyTx;

type Tag = u64;
type DashMap<K, V> = dashmap::DashMap<K, V, ahash::RandomState>;

/// A tracker for responses but can be used to track responses for requests that were dispatched
/// via other mechanisms (e.g. ingress flow)
#[derive(Default)]
pub struct ReplyTracker {
    rpcs: DashMap<Tag, RpcReplyTx>,
}

impl ReplyTracker {
    /// Returns None if an in-flight request holds the same msg_id.
    pub fn register_rpc(&self, id: Tag, sender: RpcReplyTx) {
        self.rpcs.insert(id, sender);
    }

    pub fn pop_rpc_sender(&self, id: &Tag) -> Option<RpcReplyTx> {
        self.rpcs.remove(id).map(|(_, v)| v)
    }
}
