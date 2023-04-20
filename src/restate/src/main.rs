use app::Application;
use clap::Parser;
use codederror::CodedError;
use restate::Configuration;
use restate_errors::fmt::CodedErrorExt;
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

    let config = match Configuration::load(&cli_args.config_file) {
        Ok(c) => c,
        Err(e) => {
            // We cannot use tracing here as it's not configured yet
            println!("{}", e.decorate());
            e.print_description_as_markdown();
            std::process::exit(1);
        }
    };

    let runtime = rt::build_runtime().expect("failed to build Tokio runtime!");

    runtime.block_on(async move {
        // Apply tracing config globally
        // We need to apply this first to log correctly
        config
            .tracing
            .init("Restate binary", std::process::id())
            .expect("failed to instrument logging and tracing!");

        info!("Starting Restate");
        info!(
            "Loading configuration file from {}",
            cli_args.config_file.display()
        );
        info!(
            "Configuration dump (MAY CONTAIN SENSITIVE DATA!):\n{}",
            serde_yaml::to_string(&config).unwrap()
        );

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
