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
use restate_invoker_api::StatusHandle;
use restate_schema_api::key::RestateKeyConverter;
use restate_storage_rocksdb::RocksDBStorage;
use std::fmt::Debug;

/// # Storage query datafusion options
#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
#[cfg_attr(
    feature = "options_schema",
    schemars(rename = "StorageQueryDatafusionOptions")
)]
#[cfg_attr(feature = "options_schema", schemars(default))]
pub struct Options {
    /// # Memory limit
    ///
    /// The total memory in bytes that can be used to preform sql queries
    pub memory_limit: Option<usize>,

    /// # Temp folder to use for spill
    ///
    /// The path to spill to
    pub temp_folder: Option<String>,

    /// # Default query parallelism
    ///
    /// The number of parallel partitions to use for a query execution
    pub query_parallelism: Option<usize>,
}

impl Options {
    pub fn build(
        self,
        rocksdb: RocksDBStorage,
        schema: impl RestateKeyConverter + Sync + Send + Clone + Debug + 'static,
        status: impl StatusHandle + Send + Sync + Debug + Clone + 'static,
    ) -> Result<QueryContext, BuildError> {
        let Options {
            memory_limit,
            temp_folder,
            query_parallelism,
        } = self;

        let ctx = QueryContext::new(memory_limit, temp_folder, query_parallelism);
        crate::status::register_self(&ctx, rocksdb.clone(), schema.clone())?;
        crate::state::register_self(&ctx, rocksdb.clone(), schema.clone())?;
        crate::journal::register_self(&ctx, rocksdb.clone(), schema.clone())?;
        crate::invocation_state::register_self(&ctx, status, schema.clone())?;
        crate::inbox::register_self(&ctx, rocksdb, schema)?;

        Ok(ctx)
    }
}

#[derive(Debug, thiserror::Error, CodedError)]
pub enum BuildError {
    #[error(transparent)]
    #[code(unknown)]
    Datafusion(#[from] DataFusionError),
}
