use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use tokio::runtime::{Builder, Runtime};

/// # Runtime options
///
/// Configuration for the Tokio runtime used by Restate.
#[serde_as]
#[derive(Debug, Default, Clone, Serialize, Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
#[builder(default)]
pub struct Options {
    /// # Worker threads
    ///
    /// Configure the number of [worker threads](https://docs.rs/tokio/latest/tokio/runtime/struct.Builder.html#method.worker_threads) of the Tokio runtime.
    /// If not set, it uses the Tokio default, where worker_threads is equal to number of cores.
    #[cfg_attr(feature = "options_schema", schemars(default))]
    worker_threads: Option<usize>,
    /// # Max blocking threads
    ///
    /// Configure the number of [max blocking threads](https://docs.rs/tokio/latest/tokio/runtime/struct.Builder.html#method.max_blocking_threads) of the Tokio runtime.
    /// If not set, it uses the Tokio default 512.
    #[cfg_attr(feature = "options_schema", schemars(default))]
    max_blocking_threads: Option<usize>,
}

impl Options {
    #[allow(dead_code)]
    pub fn build(self) -> Result<Runtime, std::io::Error> {
        let mut builder = Builder::new_multi_thread();
        builder.enable_all().thread_name("restate");

        if let Some(worker_threads) = self.worker_threads {
            builder.worker_threads(worker_threads);
        }
        if let Some(max_blocking_threads) = self.max_blocking_threads {
            builder.max_blocking_threads(max_blocking_threads);
        }

        builder.build()
    }
}
