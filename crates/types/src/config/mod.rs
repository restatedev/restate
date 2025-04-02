// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
mod util;

use enumset::EnumSet;
pub use util::*;
mod admin;
mod aws;
mod bifrost;
#[cfg(feature = "clap")]
mod cli_option_overrides;
mod common;
mod http;
mod ingress;
mod kafka;
mod log_server;
mod metadata_server;
mod networking;
mod object_store;
mod query_engine;
mod rocksdb;
mod worker;

pub use admin::*;
pub use aws::*;
pub use bifrost::*;
#[cfg(feature = "clap")]
pub use cli_option_overrides::*;
pub use common::*;
pub use http::*;
pub use ingress::*;
pub use kafka::*;
pub use log_server::*;
pub use metadata_server::*;
pub use networking::*;
pub use object_store::*;
pub use query_engine::*;
pub use rocksdb::*;
pub use worker::*;

use std::fmt::Display;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock};

use arc_swap::ArcSwap;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use super::live::{LiveLoad, Pinned};
use crate::PlainNodeId;
use crate::errors::GenericError;
use crate::live::Live;
use crate::nodes_config::Role;

/// Overrides production profile
pub static PRODUCTION_PROFILE_DEFAULTS: LazyLock<Configuration> = LazyLock::new(|| {
    let mut default = Configuration::default();

    default.common.auto_provision = false;

    default
});

#[cfg(any(test, feature = "test-util"))]
enum TempOrPath {
    Temp(tempfile::TempDir),
    Path(PathBuf),
}

static CONFIGURATION: LazyLock<Arc<ArcSwap<Configuration>>> = LazyLock::new(Arc::default);
#[cfg(not(any(test, feature = "test-util")))]
static NODE_BASE_DIR: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();

#[cfg(any(test, feature = "test-util"))]
static NODE_BASE_DIR: LazyLock<parking_lot::RwLock<TempOrPath>> =
    LazyLock::new(|| parking_lot::RwLock::new(TempOrPath::Temp(tempfile::TempDir::new().unwrap())));

#[cfg(not(any(test, feature = "test-util")))]
pub fn node_dir() -> PathBuf {
    NODE_BASE_DIR
        .get()
        .expect("base_dir is initialized")
        .clone()
}

#[cfg(not(any(test, feature = "test-util")))]
pub fn data_dir(dir: &str) -> PathBuf {
    node_dir().join(dir)
}

#[cfg(any(test, feature = "test-util"))]
pub fn node_dir() -> PathBuf {
    let guard = NODE_BASE_DIR.read();
    match &*guard {
        TempOrPath::Temp(temp) => temp.path().to_path_buf(),
        TempOrPath::Path(path) => path.clone(),
    }
}

#[cfg(any(test, feature = "test-util"))]
pub fn data_dir(dir: &str) -> PathBuf {
    node_dir().join(dir)
}

pub fn node_filepath(filename: &str) -> PathBuf {
    node_dir().join(filename)
}

#[cfg(any(test, feature = "test-util"))]
pub fn set_base_temp_dir(path: PathBuf) {
    let mut guard = NODE_BASE_DIR.write();
    *guard = TempOrPath::Path(path);
}

#[cfg(any(test, feature = "test-util"))]
pub fn reset_base_temp_dir() -> PathBuf {
    let mut guard = NODE_BASE_DIR.write();
    let new = tempfile::TempDir::new().unwrap();
    let path = PathBuf::from(new.path());
    *guard = TempOrPath::Temp(new);
    path
}

#[cfg(any(test, feature = "test-util"))]
/// Reset the base temp dir and leaves the temporary directory in place after
/// the test is done (no automatic deletion)
pub fn reset_base_temp_dir_and_retain() -> PathBuf {
    let mut guard = NODE_BASE_DIR.write();
    let path = tempfile::TempDir::new().unwrap().into_path();
    *guard = TempOrPath::Path(path.clone());
    path
}

/// Set the current configuration, this is temporary until we have a dedicated configuration loader
/// thread.
pub fn set_current_config(config: Configuration) {
    #[cfg(not(any(test, feature = "test-util")))]
    let proposed_cwd = config.common.base_dir().join(config.node_name());
    #[cfg(any(test, feature = "test-util"))]
    if let Some(base_dir) = config.common.base_dir_opt() {
        // overwrite temp directory if an explicit base dir was configured
        set_base_temp_dir(base_dir.clone().join(config.node_name()));
    }
    // todo: potentially validate the config
    CONFIGURATION.store(Arc::new(config));
    #[cfg(not(any(test, feature = "test-util")))]
    NODE_BASE_DIR.get_or_init(|| proposed_cwd);
    notify_config_update();
}

/// # Restate configuration file
///
/// Configuration for Restate server.
#[serde_as]
#[derive(Debug, Clone, Default, Serialize, Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(default))]
#[builder(default)]
#[serde(rename_all = "kebab-case", from = "ConfigurationShadow")]
pub struct Configuration {
    #[serde(flatten)]
    pub common: CommonOptions,
    pub worker: WorkerOptions,
    pub admin: AdminOptions,
    pub ingress: IngressOptions,
    pub bifrost: BifrostOptions,
    pub metadata_server: MetadataServerOptions,
    pub networking: NetworkingOptions,
    pub log_server: LogServerOptions,
}

impl Configuration {
    /// Potentially fast access to a snapshot, should be used if an Updateable<T>
    /// isn't possible (Updateable trait is not object-safe, and requires mut to load()).
    /// Guard acquired doesn't track config updates. ~10x slower than Updateable's load().
    ///
    /// There’s only limited number of “fast” slots for borrowing from the underlying ArcSwap
    /// for each single thread (currently 8, but this might change). If these run out, the
    /// algorithm falls back to slower path (fallback to `snapshot()`).
    ///
    /// If too many Guards are kept around, the performance might be poor. These are not intended
    /// to be stored in data structures or used across async yield points.
    pub fn pinned() -> Pinned<Configuration> {
        let c: &Arc<ArcSwap<_>> = &CONFIGURATION;
        Pinned::new(c)
    }

    /// The best way to access an updateable when holding a mutable Updateable is
    /// viable.
    ///
    /// ~10% slower than `snapshot()` to create (YMMV), load() is as fast as accessing local objects,
    /// and will always load the latest configuration reference. The downside is that `load()` requires
    /// exclusive reference. This should be the preferred method for accessing the updateable, but
    /// avoid using `to_updateable()` or `snapshot()` in tight loops. Instead, get a new updateable,
    /// and pass it down to the loop by value for very efficient access.
    pub fn updateable() -> Live<Self> {
        Live::from(CONFIGURATION.clone())
    }

    pub fn updateable_common() -> impl LiveLoad<CommonOptions> {
        Self::updateable().map(|c| &c.common)
    }

    pub fn updateable_worker() -> impl LiveLoad<WorkerOptions> {
        Self::updateable().map(|c| &c.worker)
    }

    /// Create an updateable that projects a part of the config
    pub fn mapped_updateable<F, U>(f: F) -> impl LiveLoad<U>
    where
        F: FnMut(&Configuration) -> &U + 'static + Clone,
        U: Clone,
    {
        Configuration::updateable().map(f)
    }

    pub fn watcher() -> ConfigWatch {
        ConfigWatch::new(CONFIG_UPDATE.subscribe())
    }

    pub fn apply_cascading_values(mut self) -> Self {
        self.worker.storage.apply_common(&self.common);
        self.bifrost.apply_common(&self.common);
        self.metadata_server.apply_common(&self.common);
        self.log_server.apply_common(&self.common);
        self
    }

    pub fn roles(&self) -> &EnumSet<Role> {
        &self.common.roles
    }
    pub fn has_role(&self, role: Role) -> bool {
        self.common.roles.contains(role)
    }

    pub fn node_name(&self) -> &str {
        self.common.node_name()
    }

    /// Dumps the configuration to a string
    pub fn dump(&self) -> Result<String, GenericError> {
        Ok(toml::to_string_pretty(self)?)
    }

    /// Checks whether the given configuration is valid. Returns an [`InvalidConfigurationError`]
    /// it if is not valid.
    pub fn validate(&self) -> Result<(), InvalidConfigurationError> {
        if self
            .common
            .force_node_id
            .is_some_and(|force_node_id| force_node_id == PlainNodeId::new(0))
        {
            return Err(InvalidConfigurationError::ForceNodeIdZero);
        }

        Ok(())
    }
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum InvalidConfigurationError {
    #[error(
        "force-node-id can not be 0 since it is a reserved value. Please choose a non-zero value or unset this option. Existing clusters will be auto-migrated"
    )]
    ForceNodeIdZero,
    #[error("could not derive bind address: {0}")]
    DeriveBindAddress(String),
}

/// Used to deserialize the [`Configuration`] in backwards compatible way which allows to specify
/// a `metadata_store` field instead of `metadata_server`. Can be removed once we drop support for
/// `metadata_store`.
#[derive(serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ConfigurationShadow {
    #[serde(flatten)]
    common: CommonOptions,
    worker: WorkerOptions,
    admin: AdminOptions,
    ingress: IngressOptions,
    bifrost: BifrostOptions,
    metadata_server: MetadataServerOptions,
    // previous name of metadata server options; kept for backwards compatibility
    metadata_store: Option<MetadataServerOptions>,
    networking: NetworkingOptions,
    log_server: LogServerOptions,
}

impl From<ConfigurationShadow> for Configuration {
    fn from(value: ConfigurationShadow) -> Self {
        let metadata_server = if value.metadata_server == MetadataServerOptions::default()
            && value.metadata_store.is_some()
        {
            print_warning_deprecated_config_option("metadata-store", Some("metadata-server"));
            value.metadata_store.unwrap()
        } else {
            value.metadata_server
        };

        Configuration {
            common: value.common,
            worker: value.worker,
            admin: value.admin,
            ingress: value.ingress,
            bifrost: value.bifrost,
            metadata_server,
            networking: value.networking,
            log_server: value.log_server,
        }
    }
}

fn print_warning_deprecated_config_option(deprecated: &str, replacement: Option<&str>) {
    // we can't use tracing since config loading happens before tracing is initialized
    if let Some(replacement) = replacement {
        eprintln!(
            "Using the deprecated config option '{deprecated}' instead of '{replacement}'. Please update the config to use '{replacement}' instead."
        );
    } else {
        eprintln!("Using the deprecated config option '{deprecated}'.");
    }
}

fn print_warning_deprecated_value<D>(option: &str, value: &str, default: &D)
where
    D: Display,
{
    eprintln!(
        "Option {option} does no longer support config value {value}. Using default value of {default}"
    );
}
