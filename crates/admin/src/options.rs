// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_bifrost::Bifrost;
use restate_meta::MetaHandle;
use restate_schema_impl::Schemas;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::net::SocketAddr;

use crate::service::AdminService;

/// # Admin server options
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "options_schema", schemars(rename = "AdminOptions", default))]
#[builder(default)]
pub struct Options {
    /// # Endpoint address
    ///
    /// Address to bind for the Admin APIs.
    pub bind_address: SocketAddr,

    /// # Concurrency limit
    ///
    /// Concurrency limit for the Admin APIs.
    pub concurrency_limit: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0:9070".parse().unwrap(),
            concurrency_limit: 1000,
        }
    }
}

impl Options {
    pub fn build(
        self,
        schemas: Schemas,
        meta_handle: MetaHandle,
        bifrost: Bifrost,
    ) -> AdminService {
        AdminService::new(self, schemas, meta_handle, bifrost)
    }
}
