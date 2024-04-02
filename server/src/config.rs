// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::Div;
use std::path::Path;
use std::sync::Arc;

use arc_swap::ArcSwap;
use derive_getters::Getters;
use figment::providers::{Env, Format, Serialized, Toml, Yaml};
use figment::Figment;
use once_cell::sync::Lazy;
use restate_types::arc_util::{ArcSwapExt, Pinned, Updateable};
use restate_types::config::notify_config_update;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

pub use restate_admin::Options as AdminOptions;
pub use restate_bifrost::Options as BifrostOptions;
use restate_core::options::{CommonOptionCliOverride, CommonOptions};
pub use restate_meta::{
    Options as MetaOptions, OptionsBuilder as MetaOptionsBuilder,
    OptionsBuilderError as MetaOptionsBuilderError,
};
use restate_storage_rocksdb::TableKind;

static CONFIGURATION: Lazy<ArcSwap<Configuration>> = Lazy::new(ArcSwap::default);

pub fn set_config(config: Configuration) {
    CONFIGURATION.store(Arc::new(config));
    notify_config_update();
}

/// # Restate configuration file
///
/// Configuration for the Restate single binary deployment.
///
/// You can specify the configuration file to use through the `--config-file <PATH>` argument or
/// with `RESTATE_CONFIG=<PATH>` environment variable.
///
/// Each configuration entry can be overridden using environment variables,
/// prefixing them with `RESTATE_` and separating nested structs with `__` (double underscore).
///
/// For example, to configure `admin.bind_address`, the corresponding environment variable is `RESTATE_ADMIN__BIND_ADDRESS`.
#[serde_as]
#[derive(Debug, Default, Getters, Serialize, Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "options_schema", schemars(default))]
#[builder(default)]
pub struct Configuration {
    #[serde(flatten)]
    pub common: CommonOptions,
    #[serde(flatten)]
    pub node: restate_node::Options,
}

/// Global memory options. These may only be set by environment variable
#[derive(Serialize, Deserialize)]
pub struct MemoryOptions {
    /// Global memory limit, configured with `MEMORY_LIMIT` environment variable only.
    /// This controls rocksdb cache size defaults
    limit: usize,
}

impl Default for MemoryOptions {
    fn default() -> Self {
        Self {
            limit: 3 * (1 << 30), // 3 GiB
        }
    }
}

impl MemoryOptions {
    fn apply_defaults(self, figment: Figment) -> Figment {
        let table_count = TableKind::all().count();

        let write_buffer_size = self
            .limit
            // target 50% usage
            .div(2)
            // split across all the tables
            .div(table_count)
            // where there's at most 3 column families per table
            .div(3)
            // with 8 MiB min and 256 MiB max
            .clamp(8 * (1 << 20), 256 * (1 << 20));

        let cache_size = self.limit.div(3); // target 33% usage, no min or max

        figment
            .merge(Figment::from(Serialized::default(
                "worker.storage_rocksdb.write_buffer_size",
                write_buffer_size,
            )))
            .merge(Figment::from(Serialized::default(
                "worker.storage_rocksdb.cache_size",
                cache_size,
            )))
    }
}

#[derive(Debug, thiserror::Error, codederror::CodedError)]
#[code(restate_errors::RT0002)]
#[error("configuration error: {0}")]
pub struct Error(#[from] figment::Error);

impl Configuration {
    // Methods to access the configuration

    pub fn current() -> &'static impl ArcSwapExt<Self> {
        &CONFIGURATION
    }

    pub fn pinned() -> Pinned<Self> {
        CONFIGURATION.pinned()
    }

    pub fn updateable() -> impl Updateable<Self> {
        CONFIGURATION.to_updateable()
    }

    pub fn updateable_common() -> impl Updateable<CommonOptions> {
        CONFIGURATION.map_as_updateable(|c| c.common())
    }

    pub fn updateable_worker() -> impl Updateable<restate_worker::Options> {
        CONFIGURATION.map_as_updateable(|c| &c.node().worker)
    }

    /// Load [`Configuration`] from file with overrides from from environment variables and command
    /// line arguments.
    pub fn load(
        config_file: Option<&Path>,
        cli_overrides: CommonOptionCliOverride,
    ) -> Result<Self, Error> {
        Self::load_with_default(Configuration::default(), config_file, cli_overrides)
    }

    pub fn load_default() -> Configuration {
        let default = Self::load_custom_default(Configuration::default()).expect("default config");
        Self::extract(default).expect("default config")
    }

    /// Load [`Configuration`] from an optional file with overrides from environment
    /// variables based on a default configuration.
    pub fn load_with_default(
        default: Configuration,
        config_file: Option<&Path>,
        cli_overrides: CommonOptionCliOverride,
    ) -> Result<Self, Error> {
        let figment = Self::load_custom_default(default)?;
        let figment = Self::merge_with_file(figment, config_file);
        let figment = Self::merge_with_env(figment);
        let figment = Self::merge_with_cli_overrides(figment, cli_overrides);
        Self::extract(figment)
    }

    fn load_custom_default(default: Configuration) -> Result<Figment, Error> {
        let figment = Figment::from(Serialized::defaults(default));
        // get memory options separately, and use them to set certain defaults
        let memory: MemoryOptions = Figment::from(Serialized::defaults(MemoryOptions::default()))
            .merge(Env::prefixed("MEMORY_").split("__"))
            .extract()?;
        Ok(memory.apply_defaults(figment))
    }

    fn merge_with_file(figment: Figment, config_file: Option<&Path>) -> Figment {
        if let Some(config_file) = config_file {
            match config_file.extension() {
                Some(ext) if ext == "yaml" || ext == "yml" => {
                    figment.merge(Yaml::file_exact(config_file))
                }
                Some(ext) if ext == "toml" || ext == "tml" => {
                    figment.merge(Toml::file_exact(config_file))
                }
                // No extension or something else? assume yaml!
                _ => figment.merge(Yaml::file_exact(config_file)),
            }
        } else {
            figment
        }
    }

    fn merge_with_env(figment: Figment) -> Figment {
        figment
            .merge(Env::prefixed("RESTATE_").split("__"))
            // Override tracing.log with RUST_LOG, if present
            .merge(Env::raw().only(&["RUST_LOG"]).map(|_| "log_filter".into()))
            .merge(
                Env::raw()
                    .only(&["HTTP_PROXY"])
                    .map(|_| "worker.invoker.service_client.http.proxy_uri".into()),
            )
            .merge(
                Env::raw()
                    .only(&["HTTP_PROXY"])
                    .map(|_| "meta.service_client.http.proxy_uri".into()),
            )
            .merge(
                Env::raw()
                    .only(&["AWS_EXTERNAL_ID"])
                    .map(|_| "meta.service_client.lambda.assume_role_external_id".into()),
            )
            .merge(
                Env::raw()
                    .only(&["AWS_EXTERNAL_ID"])
                    .map(|_| "worker.invoker.service_client.lambda.assume_role_external_id".into()),
            )
    }

    fn merge_with_cli_overrides(
        figment: Figment,
        cli_overrides: CommonOptionCliOverride,
    ) -> Figment {
        figment.merge(Figment::from(Serialized::defaults(cli_overrides)))
    }

    fn extract(figment: Figment) -> Result<Self, Error> {
        Ok(figment.extract()?)
    }
}
