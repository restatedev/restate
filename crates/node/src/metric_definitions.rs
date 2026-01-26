// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use metrics::{Unit, describe_counter, describe_gauge};

pub const STATE_ALIVE: &str = "alive";
pub const STATE_SUSPECT: &str = "suspect";
pub const STATE_DEAD: &str = "dead";
pub const STATE_FAILING_OVER: &str = "failing_over";
// Failure Detector Metrics
pub const GOSSIP_RECEIVED: &str = "restate.failure_detector.gossip_received.total";
pub const GOSSIP_SENT: &str = "restate.failure_detector.gossip_sent.total";

pub const GOSSIP_INSTANCE: &str = "restate.failure_detector.instance";
pub const GOSSIP_LONELY: &str = "restate.failure_detector.lonely";
/// dimensioned by "state" (STATE_*)
pub const GOSSIP_NODES: &str = "restate.failure_detector.nodes.total";

// Jemalloc memory statistics
pub const JEMALLOC_ALLOCATED: &str = "restate.jemalloc.allocated.bytes";
pub const JEMALLOC_ACTIVE: &str = "restate.jemalloc.active.bytes";
pub const JEMALLOC_METADATA: &str = "restate.jemalloc.metadata.bytes";
pub const JEMALLOC_MAPPED: &str = "restate.jemalloc.mapped.bytes";
pub const JEMALLOC_RETAINED: &str = "restate.jemalloc.retained.bytes";
pub const JEMALLOC_RESIDENT: &str = "restate.jemalloc.resident.bytes";

pub fn describe_metrics() {
    describe_counter!(
        GOSSIP_RECEIVED,
        Unit::Count,
        "Number of gossip messages received"
    );

    describe_counter!(GOSSIP_SENT, Unit::Count, "Number of gossip messages sent");
    describe_gauge!(GOSSIP_INSTANCE, "Gossip Instance TS/ID for this node");
    describe_gauge!(
        GOSSIP_LONELY,
        "Node didn't receive gossip messages for too long"
    );
    // dimensioned by "state" (STATE_*)
    describe_gauge!(
        GOSSIP_NODES,
        "Number of nodes per node state, dimensioned by state"
    );

    // jemalloc memory statistics
    describe_gauge!(
        JEMALLOC_ALLOCATED,
        Unit::Bytes,
        "Total bytes allocated by the application"
    );
    describe_gauge!(
        JEMALLOC_ACTIVE,
        Unit::Bytes,
        "Total bytes in active pages allocated by the application"
    );
    describe_gauge!(
        JEMALLOC_METADATA,
        Unit::Bytes,
        "Total bytes dedicated to jemalloc metadata"
    );
    describe_gauge!(
        JEMALLOC_MAPPED,
        Unit::Bytes,
        "Total bytes in chunks mapped on behalf of the application"
    );
    describe_gauge!(
        JEMALLOC_RETAINED,
        Unit::Bytes,
        "Total bytes in virtual memory mappings retained rather than returned to the OS"
    );
    describe_gauge!(
        JEMALLOC_RESIDENT,
        Unit::Bytes,
        "Total bytes in physically resident data pages mapped by the allocator"
    );
}
