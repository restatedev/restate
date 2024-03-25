// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::local::store::{BuildError, LocalMetadataStore};
use crate::local::LocalMetadataStoreService;
use restate_types::net::BindAddress;
use restate_types::DEFAULT_STORAGE_DIRECTORY;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
#[cfg_attr(
    feature = "options_schema",
    schemars(rename = "LocalMetadataStoreOptions", default)
)]
#[builder(default)]
pub struct Options {
    /// Address to which the metadata store will bind to.
    #[cfg_attr(feature = "options_schema", schemars(with = "String"))]
    bind_address: BindAddress,
    /// Storage path under which the metadata store will store its data.
    path: PathBuf,
    /// Number of in-flight metadata store requests.
    request_queue_length: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0:5123".parse().expect("valid bind address"),
            path: Path::new(DEFAULT_STORAGE_DIRECTORY).join("local_metadata_store"),
            request_queue_length: 32,
        }
    }
}

impl Options {
    pub fn build(self) -> Result<LocalMetadataStoreService, BuildError> {
        let store = LocalMetadataStore::new(self.path, self.request_queue_length)?;
        Ok(LocalMetadataStoreService::new(store, self.bind_address))
    }

    pub fn storage_path(&self) -> &Path {
        self.path.as_path()
    }
}
