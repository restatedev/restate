use app::Application;
use clap::Parser;
use codederror::CodedError;
use restate::Configuration;
use restate_errors::fmt::RestateCode;
use std::error::Error;
use std::path::PathBuf;
use tokio::io;
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

    /// Wipes the configured data before starting Restate.
    ///
    /// **WARNING** all the wiped data will be lost permanently!
    #[arg(value_enum, long = "wipe")]
    wipe: Option<WipeMode>,
}

#[derive(Debug, Clone, clap::ValueEnum)]
enum WipeMode {
    /// Wipe all worker state, including all the service instances and their state, all enqueued invocations, all waiting timers.
    Worker,
    /// Wipe all the meta information, including discovered services and their respective schemas.
    Meta,
    /// Wipe all
    All,
}

impl WipeMode {
    async fn wipe(
        mode: Option<&WipeMode>,
        meta_storage_dir: PathBuf,
        worker_storage_dir: PathBuf,
    ) -> io::Result<()> {
        async fn wipe_if_exists(dir: PathBuf) -> io::Result<()> {
            if tokio::fs::try_exists(&dir).await.ok().unwrap_or(false) {
                info!("Wiping directory: {}", dir.display());
                tokio::fs::remove_dir_all(&dir).await?;
            }
            Ok(())
        }

        let (wipe_meta, wipe_worker) = match mode {
            Some(WipeMode::Worker) => (false, true),
            Some(WipeMode::Meta) => (true, false),
            Some(WipeMode::All) => (true, true),
            None => (false, false),
        };

        if wipe_meta {
            wipe_if_exists(meta_storage_dir).await?;
        }
        if wipe_worker {
            wipe_if_exists(worker_storage_dir).await?;
        }
        Ok(())
    }
}

const EXIT_CODE_FAILURE: i32 = 1;

fn main() {
    let cli_args = RestateArguments::parse();

    let config = match Configuration::load(&cli_args.config_file) {
        Ok(c) => c,
        Err(e) => {
            // We cannot use tracing here as it's not configured yet
            println!("{}", e.decorate());
            println!("{:#?}", RestateCode::from(&e));
            std::process::exit(EXIT_CODE_FAILURE);
        }
    };

    let runtime = config
        .tokio_runtime
        .clone()
        .build()
        .expect("failed to build Tokio runtime!");

    runtime.block_on(async move {
        // Apply tracing config globally
        // We need to apply this first to log correctly
        config
            .observability
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

        WipeMode::wipe(
            cli_args.wipe.as_ref(),
            config.meta.storage_path().into(),
            config.worker.storage_path().into()
        ).await.expect("Error when trying to wipe the configured storage path");

        let app = Application::new(config.meta, config.worker);

        if let Err(err) = app {
            handle_error(err);
        }
        let app = app.unwrap();

        let (shutdown_signal, shutdown_watch) = drain::channel();
        let application = app.run(shutdown_watch);
        tokio::pin!(application);

        tokio::select! {
            _ = signal::shutdown() => {
                info!("Received shutdown signal.");

                let shutdown_with_timeout = tokio::time::timeout(config.shutdown_grace_period.into(), shutdown_signal.drain());

                // ignore the result because we are shutting down
                let (shutdown_result, _) = tokio::join!(shutdown_with_timeout, application);

                if  shutdown_result.is_err() {
                    warn!("Could not gracefully shut down Restate, terminating now.");
                } else {
                    info!("Restate has been gracefully shut down.");
                }
            },
            result = &mut application => {
                if let Err(err) = result {
                    handle_error(err);
                } else {
                    panic!("Unexpected termination of restate application. Please contact the Restate developers.");
                }
            }
        }
    });
}

fn handle_error<E: Error + CodedError>(err: E) -> ! {
    restate_errors::error_it!(err, "Restate application failed");
    // We terminate the main here in order to avoid the destruction of the Tokio
    // runtime. If we did this, potentially running Tokio tasks might otherwise cause panics
    // which adds noise.
    std::process::exit(EXIT_CODE_FAILURE);
}
