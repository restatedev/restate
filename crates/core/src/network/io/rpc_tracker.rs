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

/// Tracks in-flight RPC requests awaiting responses.
///
/// The tracker provides a garbage collection mechanism to remove entries where the caller
/// has dropped their receiver (i.e., given up waiting for the response). This prevents
/// memory leaks from accumulating entries for RPCs where replies never arrive as those entries
/// could be holding the wakers for the receiver end of the reply channel. Not removing these
/// entries would cause tokio's RawTask to leak.
#[derive(Default)]
pub struct ReplyTracker {
    rpcs: DashMap<Tag, RpcReplyTx>,
}

impl ReplyTracker {
    /// Registers an RPC request for tracking.
    pub fn register_rpc(&self, id: Tag, sender: RpcReplyTx) {
        self.rpcs.insert(id, sender);
    }

    /// Removes and returns the reply sender for the given RPC id.
    ///
    /// Returns `None` if no RPC with the given id is being tracked (either it was never
    /// registered, already completed, or was garbage collected).
    pub fn pop_rpc_sender(&self, id: &Tag) -> Option<RpcReplyTx> {
        self.rpcs.remove(id).map(|(_, v)| v)
    }

    /// Returns the number of RPCs currently being tracked.
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.rpcs.len()
    }

    /// Returns true if no RPCs are being tracked.
    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.rpcs.is_empty()
    }

    /// Removes entries where the receiver has been dropped (caller gave up waiting).
    pub fn gc(&self) {
        self.rpcs.retain(|_, tx| !tx.is_closed());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tokio::sync::oneshot;

    use crate::network::RawRpcReply;

    #[tokio::test]
    async fn register_and_pop_rpc() {
        let tracker = ReplyTracker::default();
        let (tx, _rx) = oneshot::channel();

        tracker.register_rpc(1, tx);
        assert_eq!(tracker.len(), 1);

        let popped = tracker.pop_rpc_sender(&1);
        assert!(popped.is_some());
        assert!(tracker.is_empty());

        // Popping again should return None
        assert!(tracker.pop_rpc_sender(&1).is_none());
    }

    #[tokio::test]
    async fn gc_removes_closed_senders() {
        let tracker = ReplyTracker::default();
        let (tx1, rx1) = oneshot::channel::<RawRpcReply>();
        let (tx2, _rx2) = oneshot::channel::<RawRpcReply>();

        tracker.register_rpc(1, tx1);
        tracker.register_rpc(2, tx2);
        assert_eq!(tracker.len(), 2);

        // Drop the first receiver - its sender should be detected as closed
        drop(rx1);

        // GC should remove the entry with the closed sender
        tracker.gc();
        assert_eq!(tracker.len(), 1);

        // The second entry should still be there
        assert!(tracker.pop_rpc_sender(&2).is_some());
    }

    #[tokio::test]
    async fn gc_with_no_closed_senders() {
        let tracker = ReplyTracker::default();
        let (tx1, _rx1) = oneshot::channel::<RawRpcReply>();
        let (tx2, _rx2) = oneshot::channel::<RawRpcReply>();

        tracker.register_rpc(1, tx1);
        tracker.register_rpc(2, tx2);

        // No receivers dropped, GC should remove nothing
        tracker.gc();
        assert_eq!(tracker.len(), 2);
    }

    #[tokio::test]
    async fn normal_completion_removes_entry() {
        let tracker = ReplyTracker::default();
        let (tx, rx) = oneshot::channel();

        tracker.register_rpc(42, tx);

        // Simulate normal completion by popping the sender
        let sender = tracker.pop_rpc_sender(&42).unwrap();
        sender
            .send(RawRpcReply::Success((
                restate_types::net::CURRENT_PROTOCOL_VERSION,
                bytes::Bytes::new(),
            )))
            .ok();

        // Receiver should get the success response
        let result = rx.await;
        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), RawRpcReply::Success(_)));

        // Tracker should be empty
        assert!(tracker.is_empty());
    }
}
