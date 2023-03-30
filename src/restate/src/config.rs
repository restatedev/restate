use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::time::Duration;

/// This is the entrypoint struct for configuring the Restate single-binary deployment.
#[serde_as]
#[derive(Debug, Serialize, Deserialize)]
pub struct Configuration {
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub shutdown_grace_period: humantime::Duration,
    pub tracing: tracing_instrumentation::Options,
    pub meta: meta::Options,
    pub worker: worker::Options,
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
