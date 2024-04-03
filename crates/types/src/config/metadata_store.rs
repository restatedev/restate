// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use super::NODE_BASE_DIR;
use crate::net::BindAddress;

#[derive(Debug, Clone, Serialize, Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(
    feature = "schemars",
    schemars(rename = "LocalMetadataStoreOptions", default)
)]
#[serde(rename_all = "kebab-case")]
#[builder(default)]
pub struct MetadataStoreOptions {
    /// Address to which the metadata store will bind to.
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub bind_address: BindAddress,
    /// Number of in-flight metadata store requests.
    pub request_queue_length: usize,
}

impl MetadataStoreOptions {
    pub fn data_dir(&self) -> PathBuf {
        NODE_BASE_DIR
            .get()
            .expect("base_dir is initialized")
            .join("local-metadata-store")
    }
}

impl Default for MetadataStoreOptions {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0:5123".parse().expect("valid bind address"),
            request_queue_length: 32,
        }
    }
}
