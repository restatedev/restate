use figment::providers::{Env, Format, Serialized, Yaml};
use figment::Figment;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::path::Path;
use std::time::Duration;

/// This is the entrypoint struct for configuring the Restate single-binary deployment.
#[serde_as]
#[derive(Debug, Serialize, Deserialize)]
pub struct Configuration {
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub shutdown_grace_period: humantime::Duration,
    pub tracing: restate_tracing_instrumentation::Options,
    pub meta: restate_meta::Options,
    pub worker: restate_worker::Options,
}

impl Default for Configuration {
    fn default() -> Self {
        Self {
            shutdown_grace_period: Duration::from_secs(60).into(),
            tracing: Default::default(),
            meta: Default::default(),
            worker: Default::default(),
        }
    }
}

impl Configuration {
    pub fn load<P: AsRef<Path>>(config_file: P) -> Configuration {
        Figment::from(Serialized::defaults(Configuration::default()))
            .merge(Yaml::file(config_file))
            .merge(Env::prefixed("RESTATE_").split("__"))
            .extract()
            .expect("Error when loading configuration")
    }
}
