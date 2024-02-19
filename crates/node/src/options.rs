// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::server;
use enumset::EnumSet;
use restate_types::nodes_config::{AdvertisedAddress, Role};
use restate_types::PlainNodeId;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

#[serde_as]
#[derive(Clone, Debug, Serialize, Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "options_schema", schemars(default))]
#[builder(default)]
pub struct Options {
    pub node_name: String,
    pub node_id: Option<PlainNodeId>,
    /// A unique identifier for the cluster. All nodes in the same cluster should
    /// have the same.
    pub cluster_name: String,
    /// If true, then a new cluster is bootstrapped. This node *must* be has an admin
    /// role and a new nodes configuration will be created that includes this node.
    pub bootstrap_cluster: bool,

    pub meta: restate_meta::Options,
    pub worker: restate_worker::Options,
    pub server: server::Options,
    pub admin: restate_admin::Options,
    pub bifrost: restate_bifrost::Options,
    pub cluster_controller: restate_cluster_controller::Options,

    /// Defines the roles which this Restate node should run
    #[cfg_attr(feature = "options_schema", schemars(with = "Vec<String>"))]
    pub roles: EnumSet<Role>,

    /// Configures the admin address. If it is not specified, then this
    /// node needs to run the admin role
    #[serde_as(as = "serde_with::NoneAsEmptyString")]
    #[cfg_attr(feature = "options_schema", schemars(with = "String"))]
    pub admin_address: Option<AdvertisedAddress>,
}

impl Default for Options {
    fn default() -> Self {
        let node_name = hostname::get()
            .map(|name| name.to_string_lossy().to_string())
            .unwrap_or_else(|_| "localhost".to_string());

        Options {
            node_name,
            cluster_name: "Local-Cluster".to_owned(),
            // boot strap the cluster by default. This is very likely to change in the future to be
            // false by default. For now, this is true to make the converged deployment backward
            // compatible and easy for users.
            bootstrap_cluster: true,
            node_id: None,
            meta: Default::default(),
            worker: Default::default(),
            server: Default::default(),
            admin: Default::default(),
            bifrost: Default::default(),
            cluster_controller: Default::default(),
            roles: Role::Worker | Role::Admin,
            admin_address: None,
        }
    }
}
