// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytestring::ByteString;

use crate::metadata::LogletKind;
use crate::service::BifrostService;

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
    pub default_loglet: LogletKind,
    #[cfg_attr(feature = "options_schema", schemars(with = "String"))]
    pub default_loglet_params: ByteString,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            default_loglet: LogletKind::File,
            default_loglet_params: "{\"path\": \"target/logs/\"}".into(),
        }
    }
}

impl Options {
    pub fn build(self, num_partitions: u64) -> BifrostService {
        BifrostService::new(self, num_partitions)
    }
}
