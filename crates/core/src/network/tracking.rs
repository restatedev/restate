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

/// A trait for tracking connection lifecycle
pub trait ConnectionTracking {
    // a connection started a drain
    fn connection_draining(&self, conn: &Connection);
    // a connection has been created
    fn connection_created(&self, conn: &Connection, is_dedicated: bool);
    // a node has told us that it's shutting down
    fn notify_peer_shutdown(&self, node_id: GenerationalNodeId);
    // connection is has dropped
    fn connection_dropped(&self, conn: &Connection);
}

/// Null tracker
#[cfg(feature = "test-util")]
#[derive(Clone, Copy, Default)]
pub struct NoopTracker;

#[cfg(feature = "test-util")]
impl ConnectionTracking for NoopTracker {
    fn connection_draining(&self, _conn: &Connection) {}
    fn connection_created(&self, _conn: &Connection, _is_dedicated: bool) {}
    fn notify_peer_shutdown(&self, _node_id: GenerationalNodeId) {}
    fn connection_dropped(&self, _conn: &Connection) {}
}
