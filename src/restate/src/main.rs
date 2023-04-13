use app::Application;
use clap::Parser;
use config::Configuration;
use std::path::PathBuf;
use tracing::{info, warn};

mod app;
mod config;
mod future_util;
mod rt;
mod signal;

#[derive(Debug, clap::Parser)]
#[command(author, version, about)]
struct RestateArguments {
    /// Set a configuration file to use for Restate.
    /// For more details, check the documentation.
    #[arg(
        short,
        long = "config-file",
        env = "RESTATE_CONFIG",
        default_value = "restate.yaml",
        value_name = "FILE"
    )]
    config_file: PathBuf,
}

fn main() {
    let cli_args = RestateArguments::parse();

    let config = Configuration::load(&cli_args.config_file);

    let runtime = rt::build_runtime().expect("failed to build Tokio runtime!");

    runtime.block_on(async move {
        config
            .tracing
            .init("Restate binary", std::process::id())
            .expect("failed to instrument logging and tracing!");

        info!(?cli_args, ?config, "Running Restate.");

        let app = Application::new(config.meta, config.worker);

        let drain_signal = app.run();

        tokio::select! {
            () = signal::shutdown() => {
                info!("Received shutdown signal.")
            }
        }

        if tokio::time::timeout(config.shutdown_grace_period.into(), drain_signal.drain())
            .await
            .is_err()
        {
            warn!("Could not gracefully shut down Restate, terminating now.");
        } else {
            info!("Restate has been gracefully shut down.");
        }
    });
}
