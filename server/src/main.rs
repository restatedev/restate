// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use clap::Parser;
use codederror::CodedError;
use restate_errors::fmt::RestateCode;
use restate_server::build_info;
use restate_server::Application;
use restate_server::Configuration;
use restate_tracing_instrumentation::TracingGuard;
use std::error::Error;
use std::ops::Div;
use std::path::PathBuf;
use std::time::Duration;
use tokio::io;
use tracing::{info, trace, warn};

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
        let (wipe_meta, wipe_worker) = match mode {
            Some(WipeMode::Worker) => (false, true),
            Some(WipeMode::Meta) => (true, false),
            Some(WipeMode::All) => (true, true),
            None => (false, false),
        };

        if wipe_meta {
            restate_fs_util::remove_dir_all_if_exists(meta_storage_dir).await?;
        }
        if wipe_worker {
            restate_fs_util::remove_dir_all_if_exists(worker_storage_dir).await?;
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
            eprintln!("{}", e.decorate());
            eprintln!("{:#?}", RestateCode::from(&e));
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
        let tracing_guard = config
            .observability
            .init("Restate binary", std::process::id())
            .expect("failed to instrument logging and tracing!");

        // Log panics as tracing errors if possible
        let prev_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |panic_info| {
            tracing_panic::panic_hook(panic_info);
            // run original hook if any.
            prev_hook(panic_info);
        }));

        info!("Starting Restate Server {}", build_info::build_info());
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

        let app = Application::new(
            config.node_ctrl,
            config.meta,
            config.worker,
            config.admin,
        );

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

                let shutdown_tasks = futures_util::future::join(shutdown_signal.drain(), application);
                let shutdown_with_timeout = tokio::time::timeout(config.shutdown_grace_period.into(), shutdown_tasks);

                // ignore the result because we are shutting down
                let shutdown_result = shutdown_with_timeout.await;

                if shutdown_result.is_err() {
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

        shutdown_tracing(config.shutdown_grace_period.div(2), tracing_guard).await;
    });
}

async fn shutdown_tracing(grace_period: Duration, tracing_guard: TracingGuard) {
    trace!("Shutting down tracing to flush pending spans");

    // Make sure that all pending spans are flushed
    let shutdown_tracing_with_timeout =
        tokio::time::timeout(grace_period, tracing_guard.async_shutdown());
    let shutdown_result = shutdown_tracing_with_timeout.await;

    if shutdown_result.is_err() {
        trace!("Failed to fully flush pending spans, terminating now.");
    }
}

fn handle_error<E: Error + CodedError>(err: E) -> ! {
    restate_errors::error_it!(err, "Restate application failed");
    // We terminate the main here in order to avoid the destruction of the Tokio
    // runtime. If we did this, potentially running Tokio tasks might otherwise cause panics
    // which adds noise.
    std::process::exit(EXIT_CODE_FAILURE);
}
