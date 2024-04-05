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
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use crate::logs::metadata::ProviderKind;

use super::{RocksDbOptions, RocksDbOptionsBuilder};

/// # Bifrost options
#[derive(Debug, Clone, Serialize, Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(rename = "BifrostOptions", default))]
#[serde(rename_all = "kebab-case")]
#[builder(default)]
pub struct BifrostOptions {
    /// # The default kind of loglet to be used
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub default_provider: ProviderKind,
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    /// Configuration of local loglet provider
    pub local: LocalLogletOptions,
}

impl Default for BifrostOptions {
    fn default() -> Self {
        Self {
            default_provider: ProviderKind::Local,
            local: LocalLogletOptions::default(),
        }
    }
}

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(rename = "LocalLoglet", default))]
#[serde(rename_all = "kebab-case")]
#[builder(default)]
pub struct LocalLogletOptions {
    #[serde(flatten)]
    pub rocksdb: RocksDbOptions,

    /// Trigger a commit when the batch size exceeds this threshold. Set to 0 or 1 to commit the
    /// write batch on every command.
    ///
    /// Supports hot-reloading.
    pub writer_commit_batch_size_threshold: usize,
    /// Trigger a commit when the time since the last commit exceeds this threshold.
    ///
    /// Supports hot-reloading.
    #[serde_as(as = "serde_with::DisplayFromStr")]
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub writer_commit_time_interval: humantime::Duration,
    /// The maximum number of write commands that can be queued.
    pub writer_queue_len: usize,

    #[cfg(any(test, feature = "test-util"))]
    #[serde(skip, default = "super::default_arc_tmp")]
    data_dir: std::sync::Arc<tempfile::TempDir>,
}

impl LocalLogletOptions {
    #[cfg(not(any(test, feature = "test-util")))]
    pub fn data_dir(&self) -> PathBuf {
        super::data_dir("local-loglet")
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn data_dir(&self) -> PathBuf {
        self.data_dir.path().join("local-loglet")
    }
}

impl Default for LocalLogletOptions {
    fn default() -> Self {
        let rocksdb = RocksDbOptionsBuilder::default()
            .rocksdb_disable_wal(Some(false))
            .build()
            .unwrap();
        Self {
            rocksdb,
            writer_commit_batch_size_threshold: 200,
            writer_commit_time_interval: Duration::from_millis(13).into(),
            writer_queue_len: 200,
            #[cfg(any(test, feature = "test-util"))]
            data_dir: super::default_arc_tmp(),
        }
    }
}
