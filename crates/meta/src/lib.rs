// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod error;
mod service;
mod storage;

use std::path::{Path, PathBuf};

pub use error::Error;
pub use restate_service_client::{
    Options as ServiceClientOptions, OptionsBuilder as ServiceClientOptionsBuilder,
    OptionsBuilderError as LambdaClientOptionsBuilderError,
};
pub use service::{ApplyMode, Force, MetaHandle, MetaService};
pub use storage::{FileMetaReader, FileMetaStorage, MetaReader, MetaStorage};

use codederror::CodedError;
use restate_types::DEFAULT_STORAGE_DIRECTORY;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

#[derive(Debug, thiserror::Error, CodedError)]
#[error("failed building the meta service: {0}")]
pub enum BuildError {
    Storage(
        #[from]
        #[code]
        storage::BuildError,
    ),
    #[code(unknown)]
    ServiceClient(#[from] restate_service_client::BuildError),
}

/// # Meta options
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "options_schema", schemars(rename = "MetaOptions", default))]
#[builder(default)]
pub struct Options {
    // todo: remove after moving schema to metadata store
    /// # [DEPRECATED] Storage path
    ///
    /// Root path for Schema storage.
    schema_storage_path: PathBuf,

    discovery: ServiceClientOptions,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            schema_storage_path: Path::new(DEFAULT_STORAGE_DIRECTORY).join("meta"),
            discovery: Default::default(),
        }
    }
}

impl Options {
    pub fn storage_path(&self) -> &Path {
        self.schema_storage_path.as_path()
    }
}
