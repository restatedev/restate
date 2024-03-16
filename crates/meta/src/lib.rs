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

use restate_schema_impl::Schemas;
use restate_service_client::AssumeRoleCacheMode;
use restate_types::retries::RetryPolicy;
use std::path::Path;

pub use error::Error;
pub use restate_service_client::{
    Options as ServiceClientOptions, OptionsBuilder as ServiceClientOptionsBuilder,
    OptionsBuilderError as LambdaClientOptionsBuilderError,
};
pub use service::{ApplyMode, Force, MetaHandle, MetaService};
pub use storage::{FileMetaReader, FileMetaStorage, MetaReader, MetaStorage};

use std::time::Duration;

use codederror::CodedError;
use restate_schema_api::subscription::SubscriptionValidator;
use restate_types::DEFAULT_STORAGE_DIRECTORY;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

#[derive(Debug, thiserror::Error, CodedError)]
#[error("failed building the meta service: {0}")]
pub struct BuildError(
    #[from]
    #[code]
    storage::BuildError,
);

/// # Meta options
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "options_schema", schemars(rename = "MetaOptions", default))]
#[builder(default)]
pub struct Options {
    /// # Storage path
    ///
    /// Root path for Meta storage.
    storage_path: String,

    service_client: ServiceClientOptions,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            storage_path: Path::new(DEFAULT_STORAGE_DIRECTORY)
                .join("meta")
                .into_os_string()
                .into_string()
                .expect("valid path"),
            service_client: Default::default(),
        }
    }
}

impl Options {
    pub fn storage_path(&self) -> &str {
        &self.storage_path
    }

    pub fn build<SV: SubscriptionValidator>(
        self,
        subscription_validator: SV,
    ) -> Result<MetaService<FileMetaStorage, SV>, BuildError> {
        let schemas = Schemas::default();
        let client = self.service_client.build(AssumeRoleCacheMode::None);
        Ok(MetaService::new(
            schemas.clone(),
            FileMetaStorage::new(self.storage_path.into())?,
            subscription_validator,
            // Total duration roughly 66 seconds
            RetryPolicy::exponential(
                Duration::from_millis(100),
                2.0,
                10,
                Some(Duration::from_secs(20)),
            ),
            client,
        ))
    }
}
