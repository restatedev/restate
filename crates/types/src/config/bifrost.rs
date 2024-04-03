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

use enum_map::Enum;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use super::{data_dir, RocksDbOptions, RocksDbOptionsBuilder};

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

/// An enum with the list of supported loglet providers.
/// For each variant we must have a corresponding implementation of the
/// [`crate::loglet::Loglet`] trait
#[derive(
    Debug,
    Clone,
    Hash,
    Eq,
    PartialEq,
    Copy,
    serde::Serialize,
    serde::Deserialize,
    Enum,
    strum_macros::EnumIter,
    strum_macros::Display,
)]
#[serde(rename_all = "kebab-case")]
pub enum ProviderKind {
    /// A local rocksdb-backed loglet.
    Local,
    /// An in-memory loglet, primarily for testing.
    InMemory,
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
    pub writer_commit_batch_size_threshold: usize,
    /// Trigger a commit when the time since the last commit exceeds this threshold.
    #[serde_as(as = "serde_with::DisplayFromStr")]
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub writer_commit_time_interval: humantime::Duration,
    /// The maximum number of write commands that can be queued.
    pub writer_queue_len: usize,
    /// If true, rocksdb flushes follow writing record batches, otherwise, we
    /// fallback to rocksdb automatic WAL flushes.
    pub flush_wal_on_commit: bool,
}

impl LocalLogletOptions {
    pub fn data_dir(&self) -> PathBuf {
        data_dir("local-loglet")
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
            flush_wal_on_commit: true,
        }
    }
}
