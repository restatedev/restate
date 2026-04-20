// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use metrics::{
    Unit, counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram,
};

use super::Swimlane;
use crate::metric_definitions::LazyIntern;

// -- Pre-existing metrics (names unchanged for dashboard compatibility) --
pub const NETWORK_CONNECTION_CREATED: &str = "restate.network.connection_created.total";
pub const NETWORK_CONNECTION_DROPPED: &str = "restate.network.connection_dropped.total";
pub const NETWORK_SERVICE_ACCEPTED_REQUEST_BYTES: &str =
    "restate.network.service.accepted_request_bytes.total";
pub const NETWORK_SERVICE_REJECTED_REQUEST_BYTES: &str =
    "restate.network.service.rejected_request_bytes.total";
pub const NETWORK_MESSAGE_PROCESSING_DURATION: &str =
    "restate.network.message_processing_duration.seconds";

// -- New metrics --
const NETWORK_CONNECTION_OPENED: &str = "restate.network.connection.opened.total";
const NETWORK_CONNECTION_CLOSED: &str = "restate.network.connection.closed.total";
const NETWORK_POOL_CONNECTIONS_ACTIVE: &str = "restate.network.pool.connections.active";
const NETWORK_CONNECTION_HANDSHAKE_DURATION: &str =
    "restate.network.connection.handshake.duration.seconds";
const NETWORK_PERMIT_ACQUISITION_DURATION: &str =
    "restate.network.permit_acquisition.duration.seconds";
const NETWORK_CONNECTIONS_PENDING: &str = "restate.network.connection.pending";
const NETWORK_CONNECTION_LIFETIME: &str = "restate.network.connection.duration.seconds";
const NETWORK_CONNECTION_ACQUISITION_DURATION: &str =
    "restate.network.connection.acquisition.duration.seconds";
const NETWORK_RPC_DURATION: &str = "restate.network.rpc.duration.seconds";

pub fn describe_metrics() {
    describe_counter!(
        NETWORK_CONNECTION_CREATED,
        Unit::Count,
        "Number of connections created"
    );
    describe_counter!(
        NETWORK_CONNECTION_DROPPED,
        Unit::Count,
        "Number of connections dropped"
    );
    describe_counter!(
        NETWORK_SERVICE_ACCEPTED_REQUEST_BYTES,
        Unit::Bytes,
        "Number of bytes accepted by service name"
    );
    describe_counter!(
        NETWORK_SERVICE_REJECTED_REQUEST_BYTES,
        Unit::Bytes,
        "Number of bytes received and dropped/rejected by service name"
    );
    describe_histogram!(
        NETWORK_MESSAGE_PROCESSING_DURATION,
        Unit::Seconds,
        "Latency of deserializing and processing incoming messages"
    );
    describe_counter!(
        NETWORK_CONNECTION_OPENED,
        Unit::Count,
        "Number of connections opened (with direction and peer labels)"
    );
    describe_counter!(
        NETWORK_CONNECTION_CLOSED,
        Unit::Count,
        "Number of connections closed (with direction and peer labels)"
    );
    describe_gauge!(
        NETWORK_POOL_CONNECTIONS_ACTIVE,
        Unit::Count,
        "Current number of active connections in the pool"
    );
    describe_histogram!(
        NETWORK_CONNECTION_HANDSHAKE_DURATION,
        Unit::Seconds,
        "Time to establish a new connection including transport and handshake"
    );
    describe_histogram!(
        NETWORK_PERMIT_ACQUISITION_DURATION,
        Unit::Seconds,
        "Time waiting for a send permit on a connection"
    );
    describe_gauge!(
        NETWORK_CONNECTIONS_PENDING,
        Unit::Count,
        "Current number of connections pending (in-flight handshakes)"
    );
    describe_histogram!(
        NETWORK_CONNECTION_LIFETIME,
        Unit::Seconds,
        "Duration a connection was alive before being closed"
    );
    describe_histogram!(
        NETWORK_CONNECTION_ACQUISITION_DURATION,
        Unit::Seconds,
        "Time to acquire or establish a connection from the pool"
    );
    describe_histogram!(
        NETWORK_RPC_DURATION,
        Unit::Seconds,
        "End-to-end RPC call duration including connection, permit, send, and reply"
    );
}

static PEER_LOOKUP: LazyIntern<restate_types::GenerationalNodeId> = LazyIntern::new();

/// Pool-level metrics scoped to a swimlane. One instance per swimlane variant,
/// accessed via [`NetworkMetrics::from`]. Creates [`ConnectionMetrics`] via
/// [`Self::connection`] when a peer and direction are known.
#[derive(Clone, Copy, Debug)]
pub struct NetworkMetrics {
    swimlane: &'static str,
}

impl NetworkMetrics {
    pub fn new(swimlane: Swimlane) -> Self {
        Self {
            swimlane: swimlane.as_str_name(),
        }
    }

    /// Narrow the scope to a specific connection.
    pub fn connection(
        &self,
        peer: restate_types::GenerationalNodeId,
        direction: &'static str,
    ) -> ConnectionMetrics {
        ConnectionMetrics {
            swimlane: self.swimlane,
            peer: PEER_LOOKUP.get(&peer),
            direction,
        }
    }

    pub fn pool_connections_active(&self) -> metrics::Gauge {
        gauge!(NETWORK_POOL_CONNECTIONS_ACTIVE, "swimlane" => self.swimlane)
    }

    pub fn connections_pending(&self) -> metrics::Gauge {
        gauge!(NETWORK_CONNECTIONS_PENDING, "swimlane" => self.swimlane)
    }

    pub fn connection_acquisition_duration(
        &self,
        cache: &'static str,
        success: bool,
    ) -> metrics::Histogram {
        let result = if success { "success" } else { "error" };
        histogram!(NETWORK_CONNECTION_ACQUISITION_DURATION, "swimlane" => self.swimlane, "cache" => cache, "result" => result)
    }

    pub fn handshake_duration(&self, success: bool) -> metrics::Histogram {
        let result = if success { "success" } else { "error" };
        histogram!(NETWORK_CONNECTION_HANDSHAKE_DURATION, "swimlane" => self.swimlane, "result" => result)
    }

    pub fn rpc_duration(&self, success: bool) -> metrics::Histogram {
        let result = if success { "success" } else { "error" };
        histogram!(NETWORK_RPC_DURATION, "swimlane" => self.swimlane, "result" => result)
    }
}

/// Per-connection metrics. Created from [`NetworkMetrics::connection`], inheriting
/// the swimlane + peer labels and adding direction.
///
/// All fields are `&'static str` so the struct is `Copy` and metric emission
/// never allocates.
#[derive(Clone, Copy, Debug)]
pub struct ConnectionMetrics {
    swimlane: &'static str,
    peer: &'static str,
    direction: &'static str,
}

impl ConnectionMetrics {
    /// Increments both the legacy `connection_created` counter and the new
    /// `connection.opened` counter.
    pub fn record_opened(&self) {
        // Legacy metric (original labels + new peer_name)
        counter!(
            NETWORK_CONNECTION_CREATED,
            "direction" => self.direction,
            "swimlane" => self.swimlane,
            "peer_name" => self.peer,
        )
        .increment(1);
        // New metric
        counter!(
            NETWORK_CONNECTION_OPENED,
            "direction" => self.direction,
            "swimlane" => self.swimlane,
            "peer_name" => self.peer,
        )
        .increment(1);
    }

    /// Increments both the legacy `connection_dropped` counter and the new
    /// `connection.closed` counter.
    pub fn record_closed(&self) {
        // Legacy metric (originally had no labels, now enriched)
        counter!(
            NETWORK_CONNECTION_DROPPED,
            "direction" => self.direction,
            "swimlane" => self.swimlane,
            "peer_name" => self.peer,
        )
        .increment(1);
        // New metric
        counter!(
            NETWORK_CONNECTION_CLOSED,
            "direction" => self.direction,
            "swimlane" => self.swimlane,
            "peer_name" => self.peer,
        )
        .increment(1);
    }

    pub fn permit_acquisition_duration(&self) -> metrics::Histogram {
        histogram!(
            NETWORK_PERMIT_ACQUISITION_DURATION,
            "direction" => self.direction,
            "swimlane" => self.swimlane,
            "peer_name" => self.peer,
        )
    }

    pub fn connection_lifetime(&self) -> metrics::Histogram {
        histogram!(
            NETWORK_CONNECTION_LIFETIME,
            "direction" => self.direction,
            "swimlane" => self.swimlane,
            "peer_name" => self.peer,
        )
    }
}
