// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// todo(asoli): remove once this is used
#![allow(dead_code)]

use restate_core::network::rpc_router::RpcRouter;
use restate_core::network::MessageRouterBuilder;
use restate_types::net::log_server::{
    GetDigest, GetLogletInfo, GetRecords, Release, Seal, Store, Trim, WaitForTail,
};
use restate_types::net::replicated_loglet::Append;

/// Used by replicated loglets to send requests and receive responses from log-servers
/// Cloning this is cheap and all clones will share the same internal trackers.
#[derive(Clone)]
pub struct LogServersRpc {
    pub store: RpcRouter<Store>,
    pub release: RpcRouter<Release>,
    pub trim: RpcRouter<Trim>,
    pub seal: RpcRouter<Seal>,
    pub get_loglet_info: RpcRouter<GetLogletInfo>,
    pub get_records: RpcRouter<GetRecords>,
    pub get_digest: RpcRouter<GetDigest>,
    pub wait_for_tail: RpcRouter<WaitForTail>,
}

impl LogServersRpc {
    /// Registers all routers into the supplied message router. This ensures that
    /// responses are routed correctly.
    pub fn new(router_builder: &mut MessageRouterBuilder) -> Self {
        let store = RpcRouter::new(router_builder);
        let release = RpcRouter::new(router_builder);
        let trim = RpcRouter::new(router_builder);
        let seal = RpcRouter::new(router_builder);
        let get_loglet_info = RpcRouter::new(router_builder);
        let get_records = RpcRouter::new(router_builder);
        let get_digest = RpcRouter::new(router_builder);
        let wait_for_tail = RpcRouter::new(router_builder);

        Self {
            store,
            release,
            trim,
            seal,
            get_loglet_info,
            get_records,
            get_digest,
            wait_for_tail,
        }
    }
}

/// Used by replicated loglets to send requests and receive responses from sequencers (other nodes
/// running replicated loglets)
/// Cloning this is cheap and all clones will share the same internal trackers.
#[derive(Clone)]
pub struct SequencersRpc {
    pub append: RpcRouter<Append>,
}

impl SequencersRpc {
    /// Registers all routers into the supplied message router. This ensures that
    /// responses are routed correctly.
    pub fn new(router_builder: &mut MessageRouterBuilder) -> Self {
        let append = RpcRouter::new(router_builder);

        Self { append }
    }
}
