// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use figment::providers::{Env, Format, Serialized, Yaml};
use figment::Figment;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::path::Path;
use std::time::Duration;

pub use crate::rt::{
    Options as TokioOptions, OptionsBuilder as TokioOptionsBuilder,
    OptionsBuilderError as TokioOptionsBuilderError,
};
pub use restate_meta::{
    Options as MetaOptions, OptionsBuilder as MetaOptionsBuilder,
    OptionsBuilderError as MetaOptionsBuilderError,
};
pub use restate_tracing_instrumentation::{
    LogOptions, LogOptionsBuilder, LogOptionsBuilderError, Options as ObservabilityOptions,
    OptionsBuilder as ObservabilityOptionsBuilder,
    OptionsBuilderError as ObservabilityOptionsBuilderError, TracingOptions, TracingOptionsBuilder,
    TracingOptionsBuilderError,
};
pub use restate_worker::{
    IngressOptions, IngressOptionsBuilder, IngressOptionsBuilderError, InvokerOptions,
    InvokerOptionsBuilder, InvokerOptionsBuilderError, Options as WorkerOptions,
    OptionsBuilder as WorkerOptionsBuilder, OptionsBuilderError as WorkerOptionsBuilderError,
    RocksdbOptions, RocksdbOptionsBuilder, RocksdbOptionsBuilderError, StorageQueryOptions,
    StorageQueryOptionsBuilder, StorageQueryOptionsBuilderError, TimerOptions, TimerOptionsBuilder,
    TimerOptionsBuilderError,
};

/// # Restate configuration file
///
/// Configuration for the Restate single binary deployment.
///
/// You can specify the configuration file to use through the `--config-file <PATH>` argument or
/// with `RESTATE_CONFIG=<PATH>` environment variable.
///
/// Each configuration entry can be overridden using environment variables,
/// prefixing them with `RESTATE_` and separating nested structs with `__` (double underscore).
/// For example, to configure `meta.rest_address`, the corresponding environment variable is `RESTATE_META__REST_ADDRESS`.
#[serde_as]
#[derive(Debug, Serialize, Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
#[builder(default)]
pub struct Configuration {
    /// # Shutdown grace timeout
    ///
    /// This timeout is used when shutting down the various Restate components to drain all the internal queues.
    ///
    /// Can be configured using the [`humantime`](https://docs.rs/humantime/latest/humantime/fn.parse_duration.html) format.
    #[serde_as(as = "serde_with::DisplayFromStr")]
    #[cfg_attr(
        feature = "options_schema",
        schemars(
            with = "String",
            default = "Configuration::default_shutdown_grace_period"
        )
    )]
    pub shutdown_grace_period: humantime::Duration,
    #[cfg_attr(feature = "options_schema", schemars(default))]
    pub observability: restate_tracing_instrumentation::Options,
    #[cfg_attr(feature = "options_schema", schemars(default))]
    pub meta: restate_meta::Options,
    #[cfg_attr(feature = "options_schema", schemars(default))]
    pub worker: WorkerOptions,
    #[cfg_attr(feature = "options_schema", schemars(default))]
    pub tokio_runtime: crate::rt::Options,
}

impl Default for Configuration {
    fn default() -> Self {
        Self {
            shutdown_grace_period: Configuration::default_shutdown_grace_period(),
            observability: Default::default(),
            meta: Default::default(),
            worker: Default::default(),
            tokio_runtime: Default::default(),
        }
    }
}

#[derive(Debug, thiserror::Error, codederror::CodedError)]
#[code(restate_errors::RT0002)]
#[error("configuration error: {0}")]
pub struct Error(#[from] figment::Error);

impl Configuration {
    fn default_shutdown_grace_period() -> humantime::Duration {
        Duration::from_secs(60).into()
    }

    /// Load [`Configuration`] from yaml with overwrites from environment variables.
    pub fn load<P: AsRef<Path>>(config_file: P) -> Result<Self, Error> {
        Self::load_with_default(Configuration::default(), Some(config_file.as_ref()))
    }

    /// Load [`Configuration`] from an optional yaml with overwrites from environment
    /// variables based on a default configuration.
    pub fn load_with_default(
        default_configuration: Configuration,
        config_file: Option<&Path>,
    ) -> Result<Self, Error> {
        let figment = Figment::from(Serialized::defaults(default_configuration));

        let figment = if let Some(config_file) = config_file {
            figment.merge(Yaml::file(config_file))
        } else {
            figment
        };

        let figment = figment
            .merge(Env::prefixed("RESTATE_").split("__"))
            // Override tracing.log with RUST_LOG, if present
            .merge(
                Env::raw()
                    .only(&["RUST_LOG"])
                    .map(|_| "observability.log.filter".into()),
            )
            .merge(
                Env::raw()
                    .only(&["HTTP_PROXY"])
                    .map(|_| "worker.invoker.proxy_uri".into()),
            )
            .merge(
                Env::raw()
                    .only(&["HTTP_PROXY"])
                    .map(|_| "meta.proxy_uri".into()),
            )
            .extract()?;

        Ok(figment)
    }
}
