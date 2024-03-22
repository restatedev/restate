// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::net::SocketAddr;
use std::str::FromStr;

use serde_with::serde_as;

use restate_network::ConnectionManager;
use restate_types::net::AdvertisedAddress;

use crate::network_server::service::{AdminDependencies, NetworkServer, WorkerDependencies};

/// # Node server options
#[serde_as]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "options_schema", schemars(rename = "NodeServerOptions"))]
#[cfg_attr(feature = "options_schema", schemars(default))]
pub struct Options {
    /// Address to bind for the Node server.
    pub bind_address: SocketAddr,

    /// Address that other nodes will use to connect to this node. Defaults to use bind_address if
    /// unset.
    #[cfg_attr(feature = "options_schema", schemars(with = "String"))]
    pub advertise_address: AdvertisedAddress,

    /// Timeout for idle histograms.
    ///
    /// The duration after which a histogram is considered idle and will be removed from
    /// metric responses to save memory. Unsetting means that histograms will never be removed.
    #[serde(with = "serde_with::As::<Option<serde_with::DisplayFromStr>>")]
    #[cfg_attr(feature = "options_schema", schemars(with = "Option<String>"))]
    pub histogram_inactivity_timeout: Option<humantime::Duration>,

    /// Disable prometheus metric recording and reporting. Default is `false`.
    pub disable_prometheus: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0:5122".parse().unwrap(),
            advertise_address: AdvertisedAddress::from_str("http://127.0.0.1:5122/").unwrap(),
            histogram_inactivity_timeout: None,
            disable_prometheus: false,
        }
    }
}

impl Options {
    pub fn build(
        self,
        connection_manager: ConnectionManager,
        node_deps: Option<WorkerDependencies>,
        admin_deps: Option<AdminDependencies>,
    ) -> NetworkServer {
        NetworkServer::new(self, connection_manager, node_deps, admin_deps)
    }
}
