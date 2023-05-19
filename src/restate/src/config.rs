use figment::providers::{Env, Format, Serialized, Yaml};
use figment::Figment;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::path::Path;
use std::time::Duration;

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
#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
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
    pub worker: restate_worker::Options,
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

    /// Load [`Configuration`] from yaml with overrides from environment variables.
    #[allow(dead_code)]
    pub fn load<P: AsRef<Path>>(config_file: P) -> Result<Self, Error> {
        Ok(
            Figment::from(Serialized::defaults(Configuration::default()))
                .merge(Yaml::file(config_file))
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
                .extract()?,
        )
    }
}
