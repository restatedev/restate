// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_types::GenerationalNodeId;

use super::Connection;

pub trait PeerRouting {
    /// Registering a connection that can be used by others to send requests to this peer
    fn register(&self, connection: &Connection);
    /// Remove this connection from the pool of connections that we can use to send requests.
    fn deregister(&self, connection: &Connection);
}

/// A trait for tracking connection lifecycle
pub trait ConnectionTracking {
    // a connection has been created
    fn connection_created(&self, conn: &Connection);
    // a node has told us that it's shutting down
    fn notify_peer_shutdown(&self, node_id: GenerationalNodeId);
    // connection is has dropped
    fn connection_dropped(&self, conn: &Connection);
}

/// Null tracker
pub struct NoopTracker;
/// Null router
pub struct NoopRouter;

impl ConnectionTracking for NoopTracker {
    fn connection_created(&self, _conn: &Connection) {}
    fn notify_peer_shutdown(&self, _node_id: GenerationalNodeId) {}
    fn connection_dropped(&self, _conn: &Connection) {}
}

impl PeerRouting for NoopRouter {
    fn register(&self, _conn: &Connection) {}
    fn deregister(&self, _conn: &Connection) {}
}
