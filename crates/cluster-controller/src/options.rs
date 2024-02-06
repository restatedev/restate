// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::net::SocketAddr;

/// # Controller service options
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
#[cfg_attr(
    feature = "options_schema",
    schemars(rename = "ClusterControllerOptions")
)]
#[cfg_attr(feature = "options_schema", schemars(default))]
pub struct Options {
    /// Address to bind for the cluster controller service.
    pub bind_address: SocketAddr,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            bind_address: "0.0.0.0:5123"
                .parse()
                .expect("should be valid socket address"),
        }
    }
}
