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

use super::{data_dir, RocksDbOptions, RocksDbOptionsBuilder};
use crate::net::BindAddress;

/// # Metadata store options
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

    /// # RocksDB options for metadata store's RocksDB instance
    ///
    /// The RocksDB options which will be used to configure the metadata store's RocksDB instance.
    pub rocksdb: RocksDbOptions,
}

impl MetadataStoreOptions {
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
            rocksdb,
        }
    }
}
