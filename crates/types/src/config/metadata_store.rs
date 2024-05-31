// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroUsize;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use restate_serde_util::NonZeroByteCount;
use tracing::warn;

use super::{data_dir, CommonOptions, RocksDbOptions, RocksDbOptionsBuilder};
use crate::net::BindAddress;

/// # Metadata store options
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(
    feature = "schemars",
    schemars(rename = "LocalMetadataStoreOptions", default)
)]
#[serde(rename_all = "kebab-case")]
#[builder(default)]
pub struct MetadataStoreOptions {
    /// # Bind address of the metadata store
    ///
    /// Address to which the metadata store will bind to.
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub bind_address: BindAddress,

    /// # Limit number of in-flight requests
    ///
    /// Number of in-flight metadata store requests.
    request_queue_length: NonZeroUsize,

    /// The memory budget for rocksdb memtables in bytes
    ///
    /// If this value is set, it overrides the ratio defined in `rocksdb-memory-ratio`.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde_as(as = "Option<NonZeroByteCount>")]
    #[cfg_attr(feature = "schemars", schemars(with = "Option<NonZeroByteCount>"))]
    rocksdb_memory_budget: Option<NonZeroUsize>,

    /// The memory budget for rocksdb memtables as ratio
    ///
    /// This defines the total memory for rocksdb as a ratio of all memory available to memtables
    /// (See `rocksdb-total-memtables-ratio` in common).
    rocksdb_memory_ratio: f32,

    /// # RocksDB options for metadata store's RocksDB instance
    ///
    /// The RocksDB options which will be used to configure the metadata store's RocksDB instance.
    pub rocksdb: RocksDbOptions,
}

impl MetadataStoreOptions {
    pub fn apply_common(&mut self, common: &CommonOptions) {
        self.rocksdb.apply_common(&common.rocksdb);

        if self.rocksdb_memory_budget.is_none() {
            self.rocksdb_memory_budget = Some(
                // 1MB minimum
                NonZeroUsize::new(
                    (common.rocksdb_safe_total_memtables_size() as f64
                        * self.rocksdb_memory_ratio as f64)
                        .floor()
                        .max(1024.0 * 1024.0) as usize,
                )
                .unwrap(),
            );
        }
    }

    pub fn rocksdb_memory_budget(&self) -> usize {
        self.rocksdb_memory_budget
            .unwrap_or_else(|| {
                warn!("MetadataStore rocksdb_memory_budget is not set, defaulting to 1MB");
                // 1MB minimum
                NonZeroUsize::new(1024 * 1024).unwrap()
            })
            .get()
    }

    pub fn data_dir(&self) -> PathBuf {
        data_dir("local-metadata-store")
    }

    pub fn request_queue_length(&self) -> usize {
        self.request_queue_length.get()
    }
}

impl Default for MetadataStoreOptions {
    fn default() -> Self {
        let rocksdb = RocksDbOptionsBuilder::default()
            .rocksdb_disable_wal(Some(false))
            .build()
            .expect("valid RocksDbOptions");
        Self {
            bind_address: "0.0.0.0:5123".parse().expect("valid bind address"),
            request_queue_length: NonZeroUsize::new(32).unwrap(),
            // set by apply_common in runtime
            rocksdb_memory_budget: None,
            rocksdb_memory_ratio: 0.01,
            rocksdb,
        }
    }
}
