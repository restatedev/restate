// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
#[cfg(feature = "clap")]
mod cli_option_overrides;
mod config_loader;
mod util;

use std::path::PathBuf;
use std::sync::{Arc, LazyLock};

use arc_swap::ArcSwap;
use enumset::EnumSet;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use restate_types::PlainNodeId;
use restate_types::config::{
    AdminOptions, BifrostOptions, CommonOptions, IngressOptions, LogServerOptions,
    MetadataServerOptions, NetworkingOptions, WorkerOptions,
    print_warning_deprecated_config_option,
};
use restate_types::errors::GenericError;
use restate_types::live::{Live, LiveLoad, LiveLoadExt, Pinned};
use restate_types::nodes_config::Role;

use crate::TaskCenter;
#[cfg(feature = "clap")]
pub use cli_option_overrides::*;
pub use config_loader::{
    ConfigLoadError, ConfigLoader, ConfigLoaderBuilder, ConfigLoaderBuilderError,
};
pub use util::*;

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

/// Retrieves the node directory for the current node.
///
/// # Important
/// This method needs to be called from within a [`TaskCenter`] task. Otherwise, it panics.
pub fn node_dir() -> PathBuf {
    TaskCenter::with_current(|tc| tc.node_dir())
}

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
pub fn set_global_config(config: Configuration) {
    #[cfg(not(any(test, feature = "test-util")))]
    {
        let proposed_cwd = config.common.base_dir().join(config.node_name());
        NODE_BASE_DIR.get_or_init(|| proposed_cwd);
    }
    #[cfg(any(test, feature = "test-util"))]
    if let Some(base_dir) = config.common.base_dir_opt() {
        // overwrite temp directory if an explicit base dir was configured
        set_base_temp_dir(base_dir.clone().join(config.node_name()));
    }

    // todo: potentially validate the config
    CONFIGURATION.store(Arc::new(config));
    notify_config_update();
}

pub(crate) fn global_node_base_dir() -> PathBuf {
    #[cfg(any(test, feature = "test-util"))]
    {
        let guard = NODE_BASE_DIR.read();
        match &*guard {
            TempOrPath::Temp(temp) => temp.path().to_path_buf(),
            TempOrPath::Path(path) => path.clone(),
        }
    }

    #[cfg(not(any(test, feature = "test-util")))]
    NODE_BASE_DIR
        .get()
        .expect("base_dir is initialized")
        .clone()
}

pub(crate) fn global_configuration() -> Arc<Configuration> {
    CONFIGURATION.load_full()
}

pub(crate) fn global_configuration_pinned() -> Pinned<Configuration> {
    Pinned::new(CONFIGURATION.as_ref())
}

pub(crate) fn global_configuration_live() -> Live<Configuration> {
    Live::from(CONFIGURATION.clone())
}

pub(crate) fn global_configuration_watcher() -> ConfigWatch {
    ConfigWatch::new(CONFIG_UPDATE.subscribe())
}

/// # Restate configuration file
///
/// Configuration for Restate server.
#[serde_as]
#[derive(Debug, Clone, Default, Serialize, Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "options_schema", schemars(default))]
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
    /// Gets the current configuration as an owned value. This might entail a [`ArcSwap::load_full`].
    ///
    /// # Important
    /// This method needs to be called from within a [`TaskCenter`] task. Otherwise, it panics.
    pub fn current() -> Arc<Configuration> {
        TaskCenter::with_current(|tc| tc.configuration())
    }

    /// Potentially fast access to a snapshot, should be used if a [`Live<T>`]
    /// isn't possible ([`Live`] trait is not object-safe, and requires mut to load()).
    /// Guard acquired doesn't track config updates. ~10x slower than Live's load().
    ///
    /// There’s only limited number of “fast” slots for borrowing from the underlying ArcSwap
    /// for each single thread (currently 8, but this might change). If these run out, the
    /// algorithm falls back to slower path (fallback to [`Self::current()`]).
    ///
    /// If too many Guards are kept around, the performance might be poor. These are not intended
    /// to be stored in data structures or used across async yield points.
    ///
    /// # Important
    /// This method needs to be called from within a [`TaskCenter`] task. Otherwise, it panics.
    pub fn with_current<F, R>(f: F) -> R
    where
        F: FnOnce(&Configuration) -> R,
    {
        TaskCenter::with_configuration(|config| f(config))
    }

    /// The best way to access live when holding a mutable [`LiveLoad`] is
    /// viable.
    ///
    /// ~10% slower than [`Self::current()`] to create (YMMV), [`LiveLoad::live_load()`] is as fast as accessing local objects,
    /// and will always load the latest configuration reference. The downside is that [`LiveLoad::live_load()`] requires
    /// exclusive reference. This should be the preferred method for accessing the live value.
    /// Avoid using [`Self::with_current`] or [`Self::current`] in tight loops. Instead, get a new live value,
    /// and pass it down to the loop by value for very efficient access.
    ///
    /// # Important
    /// This method needs to be called from within a [`TaskCenter`] task. Otherwise, it panics.
    pub fn live() -> Live<Self> {
        TaskCenter::with_current(|tc| tc.configuration_live())
    }

    /// Creates a live configuration that projects a part of the config.
    ///
    /// # Important
    /// This method needs to be called from within a [`TaskCenter`] task. Otherwise, it panics.
    pub fn map_live<F, U>(f: F) -> impl LiveLoad<Live = U>
    where
        F: FnMut(&Configuration) -> &U + Send + Sync + 'static + Clone,
        U: Clone,
    {
        Configuration::live().map(f)
    }

    /// Creates a [`ConfigWatch`] watcher that signals whenever the configuration changes.
    ///
    /// # Important
    /// This method needs to be called from within a [`TaskCenter`] task. Otherwise, it panics.
    pub fn watcher() -> ConfigWatch {
        TaskCenter::with_current(|tc| tc.configuration_watcher())
    }

    pub fn apply_cascading_values(mut self) -> Self {
        self.worker.storage.apply_common(&self.common);
        self.bifrost.local.apply_common(&self.common);
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

#[cfg(test)]
mod tests {
    use crate::config::ConfigLoaderBuilder;
    use googletest::prelude::eq;
    use googletest::{assert_that, elements_are, pat};
    use http::Uri;
    use restate_types::config::MetadataClientKind;
    use restate_types::net::AdvertisedAddress;

    #[test]
    fn metadata_store_client_backwards_compatibility() -> googletest::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let config_path_address = temp_dir.path().join("config1.toml");
        let config_file_address = r#"
        [metadata-store-client]
        address = "http://127.0.0.1:15123/"
        "#;

        std::fs::write(config_path_address.clone(), config_file_address)?;

        let config_loader = ConfigLoaderBuilder::default()
            .path(Some(config_path_address))
            .build()?;
        let configuration = config_loader.load_once()?;

        assert_that!(
            configuration.common.metadata_client.kind,
            pat!(MetadataClientKind::Replicated {
                addresses: elements_are![eq(AdvertisedAddress::Http(Uri::from_static(
                    "http://127.0.0.1:15123/"
                )))]
            })
        );

        let config_path_addresses = temp_dir.path().join("config2.toml");
        let config_file_addresses = r#"
        [metadata-store-client]
        addresses = ["http://127.0.0.1:15123/", "http://127.0.0.1:15124/"]
        "#;

        std::fs::write(config_path_addresses.clone(), config_file_addresses)?;

        let config_loader = ConfigLoaderBuilder::default()
            .path(Some(config_path_addresses))
            .build()?;
        let configuration = config_loader.load_once()?;

        assert_that!(
            configuration.common.metadata_client.kind,
            pat!(MetadataClientKind::Replicated {
                addresses: elements_are![
                    eq(AdvertisedAddress::Http(Uri::from_static(
                        "http://127.0.0.1:15123/"
                    ))),
                    eq(AdvertisedAddress::Http(Uri::from_static(
                        "http://127.0.0.1:15124/"
                    )))
                ]
            })
        );

        let config_path_etcd = temp_dir.path().join("config2.toml");
        let config_file_etcd = r#"
        [metadata-store-client]
        type = "etcd"
        addresses = ["http://127.0.0.1:15123/", "http://127.0.0.1:15124/"]
        "#;

        std::fs::write(config_path_etcd.clone(), config_file_etcd)?;

        let config_loader = ConfigLoaderBuilder::default()
            .path(Some(config_path_etcd))
            .build()?;
        let configuration = config_loader.load_once()?;

        assert_that!(
            configuration.common.metadata_client.kind,
            pat!(MetadataClientKind::Etcd {
                addresses: elements_are![
                    eq("http://127.0.0.1:15123/"),
                    eq("http://127.0.0.1:15124/")
                ]
            })
        );

        Ok(())
    }

    #[test]
    fn metadata_client_compatibility() -> googletest::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let config_path_address = temp_dir.path().join("config1.toml");
        let config_file_address = r#"
        [metadata-client]
        address = "http://127.0.0.1:15123/"
        "#;

        std::fs::write(config_path_address.clone(), config_file_address)?;

        let config_loader = ConfigLoaderBuilder::default()
            .path(Some(config_path_address))
            .build()?;
        let configuration = config_loader.load_once()?;

        assert_that!(
            configuration.common.metadata_client.kind,
            pat!(MetadataClientKind::Replicated {
                addresses: elements_are![eq(AdvertisedAddress::Http(Uri::from_static(
                    "http://127.0.0.1:15123/"
                )))]
            })
        );

        let config_path_addresses = temp_dir.path().join("config2.toml");
        let config_file_addresses = r#"
        [metadata-client]
        addresses = ["http://127.0.0.1:15123/", "http://127.0.0.1:15124/"]
        "#;

        std::fs::write(config_path_addresses.clone(), config_file_addresses)?;

        let config_loader = ConfigLoaderBuilder::default()
            .path(Some(config_path_addresses))
            .build()?;
        let configuration = config_loader.load_once()?;

        assert_that!(
            configuration.common.metadata_client.kind,
            pat!(MetadataClientKind::Replicated {
                addresses: elements_are![
                    eq(AdvertisedAddress::Http(Uri::from_static(
                        "http://127.0.0.1:15123/"
                    ))),
                    eq(AdvertisedAddress::Http(Uri::from_static(
                        "http://127.0.0.1:15124/"
                    )))
                ]
            })
        );

        let config_path_etcd = temp_dir.path().join("config2.toml");
        let config_file_etcd = r#"
        [metadata-client]
        type = "etcd"
        addresses = ["http://127.0.0.1:15123/", "http://127.0.0.1:15124/"]
        "#;

        std::fs::write(config_path_etcd.clone(), config_file_etcd)?;

        let config_loader = ConfigLoaderBuilder::default()
            .path(Some(config_path_etcd))
            .build()?;
        let configuration = config_loader.load_once()?;

        assert_that!(
            configuration.common.metadata_client.kind,
            pat!(MetadataClientKind::Etcd {
                addresses: elements_are![
                    eq("http://127.0.0.1:15123/"),
                    eq("http://127.0.0.1:15124/")
                ]
            })
        );

        Ok(())
    }
}
