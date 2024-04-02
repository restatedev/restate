// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::loglets::local_loglet;
use crate::service::BifrostService;
use restate_types::logs::metadata::ProviderKind;

/// # Bifrost options
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
#[cfg_attr(
    feature = "options_schema",
    schemars(rename = "BifrostOptions", default)
)]
#[builder(default)]
pub struct Options {
    /// # The default kind of loglet to be used
    #[cfg_attr(feature = "options_schema", schemars(with = "String"))]
    pub default_provider: ProviderKind,
    #[cfg(any(test, feature = "local_loglet"))]
    #[cfg_attr(feature = "options_schema", schemars(with = "String"))]
    /// Configuration of local loglet provider
    pub local: local_loglet::Options,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            default_provider: ProviderKind::Local,
            #[cfg(any(test, feature = "local_loglet"))]
            local: local_loglet::Options::default(),
        }
    }
}

impl Options {
    pub fn build(self) -> BifrostService {
        BifrostService::new(self)
    }

    #[cfg(any(test, feature = "memory_loglet"))]
    pub fn memory() -> Self {
        Self {
            default_provider: ProviderKind::InMemory,
            ..Default::default()
        }
    }
}
