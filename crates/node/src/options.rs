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
use restate_types::nodes_config::{NetworkAddress, Role};
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

    pub meta: restate_meta::Options,
    pub worker: restate_worker::Options,
    pub server: server::Options,
    pub admin: restate_admin::Options,
    pub bifrost: restate_bifrost::Options,
    pub cluster_controller: restate_cluster_controller::Options,

    /// Defines the roles which this Restate node should run
    #[cfg_attr(feature = "options_schema", schemars(with = "Vec<String>"))]
    pub roles: EnumSet<Role>,

    /// Configures the cluster controller address. If it is not specified, then this
    /// node needs to run the cluster controller
    #[serde_as(as = "serde_with::NoneAsEmptyString")]
    #[cfg_attr(feature = "options_schema", schemars(with = "String"))]
    pub cluster_controller_address: Option<NetworkAddress>,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            node_name: "LocalNode".to_owned(),
            node_id: None,
            meta: Default::default(),
            worker: Default::default(),
            server: Default::default(),
            admin: Default::default(),
            bifrost: Default::default(),
            cluster_controller: Default::default(),
            roles: Role::Worker | Role::ClusterController,
            cluster_controller_address: None,
        }
    }
}
