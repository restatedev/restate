// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroUsize;

use serde::{Deserialize, Deserializer, Serialize};
use serde_with::{DeserializeAs, serde_as};
use tracing::warn;

use restate_serde_util::NonZeroByteCount;
use restate_time_util::NonZeroFriendlyDuration;

use super::{
    CommonOptions, Configuration, RocksDbOptions, RocksDbOptionsBuilder, StructWithDefaults,
};

/// # Metadata store options
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, derive_builder::Builder, PartialEq)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(
    feature = "schemars",
    schemars(rename = "MetadataServerOptions", default)
)]
#[serde(rename_all = "kebab-case")]
#[builder(default)]
pub struct MetadataServerOptions {
    /// Limit number of in-flight requests
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

    /// RocksDB options for metadata store's RocksDB instance
    ///
    /// The RocksDB options which will be used to configure the metadata store's RocksDB instance.
    #[serde(flatten)]
    pub rocksdb: RocksDbOptions,

    #[serde(flatten)]
    pub raft_options: RaftOptions,

    /// Auto join the metadata cluster when being started
    ///
    /// Defines whether this node should auto join the metadata store cluster when being started
    /// for the first time.
    pub auto_join: bool,
}

impl MetadataServerOptions {
    pub fn set_raft_options(&mut self, raft_options: RaftOptions) {
        self.raft_options = raft_options;
    }

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
                warn!("metadata-server rocksdb_memory_budget is not set, defaulting to 1MB");
                // 1MB minimum
                NonZeroUsize::new(1024 * 1024).unwrap()
            })
            .get()
    }

    pub fn request_queue_length(&self) -> usize {
        self.request_queue_length.get()
    }

    fn rocksdb_defaults() -> RocksDbOptionsBuilder {
        let mut builder = RocksDbOptionsBuilder::default();
        builder.rocksdb_disable_wal(Some(false));
        builder
    }
}

impl Default for MetadataServerOptions {
    fn default() -> Self {
        let rocksdb = Self::rocksdb_defaults()
            .build()
            .expect("valid RocksDbOptions");
        Self {
            request_queue_length: NonZeroUsize::new(32).unwrap(),
            // set by apply_common in runtime
            rocksdb_memory_budget: None,
            rocksdb_memory_ratio: 0.01,
            rocksdb,
            raft_options: RaftOptions::default(),
            auto_join: true,
        }
    }
}

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "kebab-case", default)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct RaftOptions {
    /// The number of ticks before triggering an election
    ///
    /// The number of ticks before triggering an election. The value must be larger than
    /// `raft_heartbeat_tick`. It's recommended to set `raft_election_tick = 10 * raft_heartbeat_tick`.
    /// Decrease this value if you want to react faster to failed leaders. Note, decreasing this
    /// value too much can lead to cluster instabilities due to falsely detecting dead leaders.
    pub raft_election_tick: NonZeroUsize,
    /// The number of ticks before sending a heartbeat
    ///
    /// A leader sends heartbeat messages to maintain its leadership every heartbeat ticks.
    /// Decrease this value to send heartbeats more often.
    pub raft_heartbeat_tick: NonZeroUsize,
    pub raft_batch_append: bool,
    /// The raft tick interval
    ///
    /// The interval at which the raft node will tick. Decrease this value in order to let the Raft
    /// node react more quickly to changes. Note, that every tick comes with an overhead. Moreover,
    /// the tick interval directly affects the election timeout. If the election timeout becomes too
    /// small, then this can cause cluster instabilities due to frequent leader changes.
    pub raft_tick_interval: NonZeroFriendlyDuration,
    /// The status update interval
    ///
    /// The interval at which the raft node will update its status. Decrease this value in order to
    /// see more recent status updates.
    pub status_update_interval: NonZeroFriendlyDuration,

    /// # The raft log trim threshold
    ///
    /// The threshold for trimming the raft log. The log will be trimmed if the number of apply entries
    /// exceeds this threshold. The default value is `1000`.
    pub log_trim_threshold: Option<u64>,
}

impl Default for RaftOptions {
    fn default() -> Self {
        RaftOptions {
            raft_election_tick: NonZeroUsize::new(10).expect("be non zero"),
            raft_heartbeat_tick: NonZeroUsize::new(2).expect("be non zero"),
            raft_batch_append: false,
            raft_tick_interval: NonZeroFriendlyDuration::from_millis_unchecked(100),
            status_update_interval: NonZeroFriendlyDuration::from_secs_unchecked(5),
            log_trim_threshold: Some(1000),
        }
    }
}

/// Deserializer for [`MetadataServerOptions`] that uses the [`Configuration`] defaults.
pub struct MetadataServerOptionsWithConfigurationDefaults;

impl<'de> DeserializeAs<'de, MetadataServerOptions>
    for MetadataServerOptionsWithConfigurationDefaults
{
    fn deserialize_as<D>(deserializer: D) -> Result<MetadataServerOptions, D::Error>
    where
        D: Deserializer<'de>,
    {
        let struct_with_defaults =
            StructWithDefaults::new(Configuration::default().metadata_server);
        deserializer.deserialize_map(struct_with_defaults)
    }
}

#[cfg(test)]
mod tests {
    use crate::config::{Configuration, MetadataServerOptions};
    use crate::config_loader::ConfigLoaderBuilder;
    use std::fs;
    use std::num::NonZeroUsize;

    #[test]
    fn metadata_server_precedence() -> googletest::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let config_path_address = temp_dir.path().join("config1.toml");
        let config_file_address = r#"
        [metadata-server]
        request-queue-length = 1337

        [metadata-store]
        type = "replicated"
        "#;

        fs::write(config_path_address.clone(), config_file_address)?;

        let config_loader = ConfigLoaderBuilder::default()
            .path(Some(config_path_address))
            .disable_apply_cascading_values(true)
            .build()?;
        let configuration = config_loader.load_once()?;

        assert_eq!(
            configuration.metadata_server,
            MetadataServerOptions {
                request_queue_length: NonZeroUsize::new(1337).unwrap(),
                ..Configuration::default().metadata_server
            }
        );

        Ok(())
    }

    #[test]
    fn metadata_store_sub_field_does_not_clear_defaults() -> googletest::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let config_path_address = temp_dir.path().join("config1.toml");
        let config_file_address = r#"
        [metadata-server]
        rocksdb-disable-direct-io-for-reads = false
        "#;

        fs::write(config_path_address.clone(), config_file_address)?;

        let config_loader = ConfigLoaderBuilder::default()
            .path(Some(config_path_address))
            .disable_apply_cascading_values(true)
            .build()?;
        let configuration = config_loader.load_once()?;
        let mut rocksdb_defaults = MetadataServerOptions::rocksdb_defaults();
        rocksdb_defaults.rocksdb_disable_direct_io_for_reads(Some(false));
        let rocksdb = rocksdb_defaults.build().expect("should build");

        assert_eq!(
            configuration.metadata_server,
            MetadataServerOptions {
                rocksdb,
                ..Configuration::default().metadata_server
            }
        );

        Ok(())
    }
}
