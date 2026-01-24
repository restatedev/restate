// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use ahash::HashMap;
use metrics::Histogram;

use restate_types::logs::TailOffsetWatch;
use restate_types::{
    PlainNodeId,
    logs::{LogletOffset, SequenceNumber, TailState},
    replication::NodeSet,
};

use super::metric_definitions::BIFROST_SEQ_STORE_DURATION;

/// LogServer instance
#[derive(Clone)]
pub struct RemoteLogServer {
    tail: TailOffsetWatch,
    store_latency: Histogram,
}

impl RemoteLogServer {
    fn new(node_id: PlainNodeId) -> Self {
        Self {
            tail: TailOffsetWatch::new(TailState::Open(LogletOffset::OLDEST)),
            store_latency: metrics::histogram!(BIFROST_SEQ_STORE_DURATION, "node_id" => node_id.to_string()),
        }
    }

    pub fn local_tail(&self) -> &TailOffsetWatch {
        &self.tail
    }

    pub fn store_latency(&self) -> &Histogram {
        &self.store_latency
    }
}

/// LogServerManager maintains a set of [`RemoteLogServer`]s that provided via the
/// [`NodeSet`].
#[derive(Clone)]
pub struct RemoteLogServerManager {
    servers: Arc<HashMap<PlainNodeId, RemoteLogServer>>,
}

impl RemoteLogServerManager {
    /// creates the node set and start the appenders
    pub fn new(nodeset: &NodeSet) -> Self {
        let servers = nodeset
            .iter()
            .copied()
            .map(|node_id| (node_id, RemoteLogServer::new(node_id)))
            .collect();
        let servers = Arc::new(servers);

        Self { servers }
    }

    pub fn get_tail_offset(&self, id: PlainNodeId) -> &TailOffsetWatch {
        let server = self.servers.get(&id).expect("node is in nodeset");
        server.local_tail()
    }

    pub fn get(&self, id: PlainNodeId) -> &RemoteLogServer {
        self.servers.get(&id).expect("node is in nodeset")
    }
}
