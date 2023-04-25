use convert_case::{Case, Casing};
use figment::providers::{Env, Format, Serialized, Yaml};
use figment::value::{Dict, Map, Value};
use figment::{Figment, Metadata, Profile, Provider};
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
/// prefixing them with `RESTATE_`, separating nested structs with `__` (double underscore) and
/// converting entry name from `camelCase` to `snake_case`.
/// For example, to configure `meta.restAddress`, the corresponding environment variable is `RESTATE_META__REST_ADDRESS`.
#[serde_as]
#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
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
    pub tracing: restate_tracing_instrumentation::Options,
    #[cfg_attr(feature = "options_schema", schemars(default))]
    pub meta: restate_meta::Options,
    #[cfg_attr(feature = "options_schema", schemars(default))]
    pub worker: restate_worker::Options,
}

impl Default for Configuration {
    fn default() -> Self {
        Self {
            shutdown_grace_period: Configuration::default_shutdown_grace_period(),
            tracing: Default::default(),
            meta: Default::default(),
            worker: Default::default(),
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

    fn load_figment<P: AsRef<Path>>(config_file: P) -> Figment {
        Figment::from(Serialized::defaults(Configuration::default()))
            .merge(Yaml::file(config_file))
            .merge(RestateEnv::parse())
            // Override tracing.log with RUST_LOG, if present
            .merge(Env::raw().only(&["RUST_LOG"]).map(|_| "tracing.log".into()))
    }

    /// Load [`Configuration`] from yaml with overrides from environment variables.
    #[allow(dead_code)]
    pub fn load<P: AsRef<Path>>(config_file: P) -> Result<Self, Error> {
        Ok(Self::load_figment(config_file).extract()?)
    }
}

/// Wrapper around [`Env`] to provide correct casing
/// See https://github.com/SergioBenitez/Figment/issues/36
struct RestateEnv(Env);

impl RestateEnv {
    fn parse() -> Self {
        Self(Env::prefixed("RESTATE_"))
    }
}

impl Provider for RestateEnv {
    fn metadata(&self) -> Metadata {
        self.0.metadata()
    }

    fn data(&self) -> Result<Map<Profile, Dict>, figment::Error> {
        let mut dict = Dict::new();
        for (k, v) in self.0.iter() {
            let key = k
                .as_str()
                .replace("__", ".")
                .from_case(Case::Snake)
                .to_case(Case::Camel);
            let nested_dict = figment::util::nest(&key, v.parse().expect("infallible"))
                .into_dict()
                .expect("key is non-empty: must have dict");

            dict = merge_dict(dict, nested_dict);
        }

        Ok(self.0.profile.collect(dict))
    }
}

fn merge_dict(mut this: Dict, other: Dict) -> Dict {
    fn merge_value(this: Value, other: Value) -> Value {
        match (this, other) {
            (Value::Dict(_, l), Value::Dict(t, r)) => Value::Dict(t, merge_dict(l, r)),
            (_, v) => v,
        }
    }

    for (key, value) in other {
        let old_value = this.remove(&key);
        this.insert(
            key,
            if let Some(old_value) = old_value {
                merge_value(old_value, value)
            } else {
                value
            },
        );
    }

    this
}

#[cfg(test)]
mod tests {
    use super::*;

    use figment::Jail;

    #[test]
    fn check_env_loading() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "restate.yaml",
                r#"
                meta:
                    restAddress: 0.0.0.0:9999
            "#,
            )?;
            jail.set_env("RESTATE_META__REST_ADDRESS", "0.0.0.0:9092");

            let figment = Configuration::load_figment("restate.yaml");

            assert_eq!(
                "0.0.0.0:9092",
                figment.extract_inner::<String>("meta.restAddress").unwrap()
            );

            Ok(())
        });
    }
}
