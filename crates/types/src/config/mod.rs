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
mod gossip;
mod http;
mod ingress;
mod invocation;
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
pub use gossip::*;
pub use http::*;
pub use ingress::*;
pub use invocation::*;
pub use kafka::*;
pub use log_server::*;
pub use metadata_server::*;
pub use networking::*;
pub use object_store::*;
pub use query_engine::*;
pub use rocksdb::*;
pub use worker::*;

use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock};

use arc_swap::ArcSwap;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use super::live::{LiveLoad, Pinned};
use crate::PlainNodeId;
use crate::errors::GenericError;
use crate::live::Live;
use crate::live::LiveLoadExt;
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
    let path = tempfile::TempDir::new().unwrap().keep();
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
#[serde(rename_all = "kebab-case")]
pub struct Configuration {
    #[serde(flatten)]
    pub common: CommonOptions,
    #[serde(flatten)]
    pub invocation: InvocationOptions,
    pub worker: WorkerOptions,
    pub admin: AdminOptions,
    pub ingress: IngressOptions,
    pub bifrost: BifrostOptions,
    pub metadata_server: MetadataServerOptions,
    pub networking: NetworkingOptions,
    pub log_server: LogServerOptions,
}

impl Configuration {
    #[cfg(any(test, feature = "test-util"))]
    pub fn set(c: Configuration) {
        set_current_config(c);
    }
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

    /// The best way to access live when holding a mutable [`LiveLoad`] is
    /// viable.
    ///
    /// ~10% slower than [`Self::current()`] to create (YMMV), [`LiveLoad::live_load()`] is as fast as accessing local objects,
    /// and will always load the latest configuration reference. The downside is that [`LiveLoad::live_load()`] requires
    /// exclusive reference. This should be the preferred method for accessing the live value.
    /// Avoid using [`Self::with_current`] or [`Self::current`] in tight loops. Instead, get a new live value,
    /// and pass it down to the loop by value for very efficient access.
    pub fn live() -> Live<Self> {
        Live::from(CONFIGURATION.clone())
    }

    /// Create an updateable that projects a part of the config
    pub fn map_live<F, U>(f: F) -> impl LiveLoad<Live = U>
    where
        F: FnMut(&Configuration) -> &U + Send + Sync + 'static + Clone,
        U: Clone,
    {
        Configuration::live().map(f)
    }

    pub fn watcher() -> ConfigWatch {
        ConfigWatch::new(CONFIG_UPDATE.subscribe())
    }

    pub fn apply_cascading_values(mut self) -> Self {
        self.admin.print_deprecation_warnings();
        self.worker.storage.print_deprecation_warnings();
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

        if self.common.node_name.is_none() {
            // If the node name is not set, we will fallback to use hostname as the node name.
            // So to avoid changing hostname to make data loss, we must validate the directory's entry.
            let dirs = read_subdirs(&self.common.base_dir());
            match dirs.len().cmp(&1) {
                std::cmp::Ordering::Less => {
                    // If it's an empty directory, it's safe to use default behavior.
                }
                std::cmp::Ordering::Equal => {
                    // If there is only one directory, it must be the node name.
                    // And if it's not equal to hostname, return an error.
                    if self.common.node_name() != dirs[0].to_string_lossy() {
                        return Err(InvalidConfigurationError::RequiredNodeName(format!(
                            "The working directory '{}' contains data from node '{}' but the default node name is '{}'. This would ignore the existing data. To avoid accidental misconfigurations, you have to explicitly specify the node name to tell Restate to resume from the existing data or to start a fresh node.",
                            self.common.base_dir().to_string_lossy(),
                            dirs[0].to_string_lossy(),
                            self.common.node_name()
                        )));
                    }
                }
                std::cmp::Ordering::Greater => {
                    return Err(InvalidConfigurationError::RequiredNodeName(format!(
                        "The working directory '{}' contains data from multiple nodes. Please specify the node name to not accidentally start with the wrong data.",
                        self.common.base_dir().to_string_lossy()
                    )));
                }
            }
        }

        Ok(())
    }
}

#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum InvalidConfigurationError {
    #[error(
        "force-node-id can not be 0 since it is a reserved value. Please choose a non-zero value or unset this option. Existing clusters will be auto-migrated"
    )]
    ForceNodeIdZero,
    #[error("could not derive bind address: {0}")]
    DeriveBindAddress(String),
    #[error("node-name is required: {0}")]
    RequiredNodeName(String),
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

#[allow(unused)]
fn print_warning_deprecated_value(option: &str, value: &str, help_msg: &str) {
    eprintln!("Value '{value}' of config option '{option}' is deprecated: {help_msg}")
}

/// read_subdirs reads the given directory and returns a vector of subdirectories.
/// If the directory does not exist, it returns an empty vector.
fn read_subdirs(dir: &PathBuf) -> Vec<PathBuf> {
    if !dir.exists() {
        return vec![];
    }

    let paths = fs::read_dir(dir).unwrap();
    let dirs: Vec<PathBuf> = paths
        .filter(|path| path.as_ref().unwrap().path().is_dir())
        .map(|path| path.as_ref().unwrap().file_name().into())
        .collect();

    dirs
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_subdirs_did_not_exist() {
        let temp_dir = tempfile::tempdir().unwrap();
        let temp_dir_path = temp_dir.path().to_path_buf();
        assert!(fs::remove_dir(temp_dir).is_ok());
        assert!(read_subdirs(&temp_dir_path).is_empty());
    }

    #[test]
    fn test_read_subdirs_empty() {
        let temp_dir = tempfile::tempdir().unwrap();
        let temp_dir_path = temp_dir.path().to_path_buf();
        assert!(read_subdirs(&temp_dir_path).is_empty());
    }

    #[test]
    fn test_read_subdirs_with_subdirs_and_files() {
        let temp_dir = tempfile::tempdir().unwrap();
        let temp_dir_path = temp_dir.path().to_path_buf();

        fs::create_dir(temp_dir.path().join("dir1")).unwrap();
        fs::create_dir(temp_dir.path().join("dir2")).unwrap();
        fs::File::create_new(temp_dir.path().join("file1")).unwrap();

        let mut sub_dirs = read_subdirs(&temp_dir_path);
        sub_dirs.sort();

        let expect_sub_dirs = [
            std::path::Path::new("dir1").to_path_buf(),
            std::path::Path::new("dir2").to_path_buf(),
        ];
        assert_eq!(sub_dirs, expect_sub_dirs);
    }

    #[test]
    fn test_configuration_validate_empty_base_dir() {
        let mut config = Configuration::default();
        assert!(config.validate().is_ok());

        let temp_dir = tempfile::tempdir().unwrap();
        let temp_dir_path = temp_dir.path().to_path_buf();
        config.common.base_dir = Some(temp_dir_path);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_configuration_validate_base_dir_one_subdir() {
        let mut config = Configuration::default();
        assert!(config.validate().is_ok());

        let temp_dir = tempfile::tempdir().unwrap();
        let temp_dir_path = temp_dir.path().to_path_buf();
        config.common.base_dir = Some(temp_dir_path);

        fs::create_dir(temp_dir.path().join("dir1")).unwrap();
        let valid_result = config.validate();
        assert!(valid_result.is_err());
        match valid_result.unwrap_err() {
            InvalidConfigurationError::RequiredNodeName(_) => {}
            _ => panic!("Shoule be RequiredNodeName error"),
        }
    }

    #[test]
    fn test_configuration_validate_base_dir_multi_subdir() {
        let mut config = Configuration::default();
        assert!(config.validate().is_ok());

        let temp_dir = tempfile::tempdir().unwrap();
        let temp_dir_path = temp_dir.path().to_path_buf();
        config.common.base_dir = Some(temp_dir_path);

        fs::create_dir(temp_dir.path().join("dir1")).unwrap();
        fs::create_dir(temp_dir.path().join("dir2")).unwrap();
        let valid_result = config.validate();
        assert!(valid_result.is_err());
        match valid_result.unwrap_err() {
            InvalidConfigurationError::RequiredNodeName(_) => {}
            _ => panic!("Shoule be RequiredNodeName error"),
        }
    }
}
