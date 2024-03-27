// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_types::net::AdvertisedAddress;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

#[serde_as]
#[derive(Clone, Default, Debug, Serialize, Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "options_schema", schemars(default))]
#[builder(default)]
pub struct Options {
    pub meta: restate_meta::Options,
    pub worker: restate_worker::Options,
    pub admin: restate_admin::Options,
    pub bifrost: restate_bifrost::Options,
    pub cluster_controller: restate_cluster_controller::Options,
    pub metadata_store: restate_metadata_store::local::Options,

    /// Configures the admin address. If it is not specified, then this
    /// node needs to run the admin role
    #[serde_as(as = "serde_with::NoneAsEmptyString")]
    #[cfg_attr(feature = "options_schema", schemars(with = "String"))]
    pub admin_address: Option<AdvertisedAddress>,
}
