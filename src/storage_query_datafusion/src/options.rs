// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::context::QueryContext;
use codederror::CodedError;
use datafusion::error::DataFusionError;

use restate_schema_api::key::RestateKeyConverter;
use restate_schema_api::service::ServiceMetadataResolver;
use restate_storage_rocksdb::RocksDBStorage;
use std::fmt::Debug;

/// # Storage query datafusion options
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
#[cfg_attr(
    feature = "options_schema",
    schemars(rename = "StorageQueryDatafusionOptions")
)]
pub struct Options {
    /// # Memory limit
    ///
    /// The total memory in bytes that can be used to preform sql queries
    #[cfg_attr(
        feature = "options_schema",
        schemars(default = "Options::default_memory_limit")
    )]
    pub memory_limit: Option<usize>,

    /// # Temp folder to use for spill
    ///
    /// The path to spill to
    #[cfg_attr(
        feature = "options_schema",
        schemars(default = "Options::default_temp_folder")
    )]
    pub temp_folder: Option<String>,

    /// # Default query parallelism
    ///
    /// The number of parallel partitions to use for a query execution
    #[cfg_attr(
        feature = "options_schema",
        schemars(default = "Options::default_query_parallelism")
    )]
    pub query_parallelism: Option<usize>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            memory_limit: Options::default_memory_limit(),
            temp_folder: Options::default_temp_folder(),
            query_parallelism: Options::default_query_parallelism(),
        }
    }
}

impl Options {
    fn default_memory_limit() -> Option<usize> {
        None
    }

    fn default_temp_folder() -> Option<String> {
        None
    }

    fn default_query_parallelism() -> Option<usize> {
        None
    }

    pub fn build<
        EMR: ServiceMetadataResolver + RestateKeyConverter + Sync + Debug + Clone + Send + 'static,
    >(
        self,
        rocksdb: RocksDBStorage,
        service_endpoint_registry: EMR,
    ) -> Result<QueryContext, BuildError> {
        let Options {
            memory_limit,
            temp_folder,
            query_parallelism,
        } = self;

        let ctx = QueryContext::new(memory_limit, temp_folder, query_parallelism);
        crate::service::register_self(&ctx, service_endpoint_registry.clone())?;
        crate::service::register_udf(&ctx, service_endpoint_registry);
        crate::status::register_self(&ctx, rocksdb.clone())?;
        crate::state::register_self(&ctx, rocksdb)?;

        Ok(ctx)
    }
}

#[derive(Debug, thiserror::Error, CodedError)]
pub enum BuildError {
    #[error(transparent)]
    #[code(unknown)]
    Datafusion(#[from] DataFusionError),
}
