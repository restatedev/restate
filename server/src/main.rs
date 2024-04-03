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
use restate_core::TaskCenterFactory;
use restate_core::TaskKind;
use restate_errors::fmt::RestateCode;
use restate_node::Configuration;
use restate_server::build_info;
use restate_server::config_loader::ConfigLoaderBuilder;
use restate_server::rt::build_tokio;
use restate_tracing_instrumentation::init_tracing_and_logging;
use restate_tracing_instrumentation::TracingGuard;
use restate_types::config::CommonOptionCliOverride;
use std::error::Error;
use std::ops::Div;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::io;
use tracing::error;
use tracing::{info, trace, warn};

mod signal;

use restate_node::Node;
#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[derive(Debug, clap::Parser)]
#[command(author, version, about)]
struct RestateArguments {
    /// Set a configuration file to use for Restate.
    /// For more details, check the documentation.
    #[arg(
        short,
        long = "config-file",
        env = "RESTATE_CONFIG",
        value_name = "FILE"
    )]
    config_file: Option<PathBuf>,

    /// Dumps the loaded configuration (or default if no config-file is set) to stdout and exits.
    /// Defaults will include any values overridden by environment variables.
    #[clap(long)]
    dump_config: bool,

    /// Wipes the configured data before starting Restate.
    ///
    /// **WARNING** all the wiped data will be lost permanently!
    #[arg(value_enum, long = "wipe")]
    wipe: Option<WipeMode>,

    #[clap(flatten)]
    opts_overrides: CommonOptionCliOverride,
}

#[derive(Debug, Clone, clap::ValueEnum)]
enum WipeMode {
    /// Wipe all worker state, including all the service instances and their state, all enqueued invocations, all waiting timers.
    Worker,
    /// Wipe all the meta information, including discovered services and their respective schemas.
    Meta,
    /// Wipe the local rocksdb-based loglet.
    LocalLoglet,
    /// Wipe the local rocksdb-based metadata-store.
    LocalMetadataStore,
    /// Wipe all
    All,
}

impl WipeMode {
    async fn wipe(
        mode: Option<&WipeMode>,
        meta_storage_dir: PathBuf,
        worker_storage_dir: PathBuf,
        local_loglet_storage_dir: &Path,
        local_metadata_store_storage_dir: &Path,
    ) -> io::Result<()> {
        let (wipe_meta, wipe_worker, wipe_local_loglet, wipe_local_metadata_store) = match mode {
            Some(WipeMode::Worker) => (false, true, true, false),
            Some(WipeMode::Meta) => (true, false, false, false),
            Some(WipeMode::LocalLoglet) => (false, false, true, false),
            Some(WipeMode::LocalMetadataStore) => (false, false, false, true),
            Some(WipeMode::All) => (true, true, true, true),
            None => (false, false, false, false),
        };

        if wipe_meta {
            restate_fs_util::remove_dir_all_if_exists(meta_storage_dir).await?;
        }
        if wipe_worker {
            restate_fs_util::remove_dir_all_if_exists(worker_storage_dir).await?;
        }
        if wipe_local_loglet {
            restate_fs_util::remove_dir_all_if_exists(local_loglet_storage_dir).await?;
        }
        if wipe_local_metadata_store {
            restate_fs_util::remove_dir_all_if_exists(local_metadata_store_storage_dir).await?
        }
        Ok(())
    }
}

const EXIT_CODE_FAILURE: i32 = 1;

fn main() {
    let cli_args = RestateArguments::parse();

    // We capture the absolute path of the config file on startup before we change the current
    // working directory (base-dir arg)
    let config_path = cli_args
        .config_file
        .as_ref()
        .map(|p| std::fs::canonicalize(p).expect("config-file path is valid"));

    // Initial configuration loading
    let config_loader = ConfigLoaderBuilder::default()
        .load_env(true)
        .path(config_path.clone())
        .cli_override(cli_args.opts_overrides.clone())
        .build()
        .unwrap();

    let config = match config_loader.load_once() {
        Ok(c) => c,
        Err(e) => {
            // We cannot use tracing here as it's not configured yet
            eprintln!("{}", e.decorate());
            eprintln!("{:#?}", RestateCode::from(&e));
            std::process::exit(EXIT_CODE_FAILURE);
        }
    };
    if cli_args.dump_config {
        println!("{}", config.dump().expect("config is toml serializable"));
        std::process::exit(0);
    }

    let old_config = match Configuration::load(config_path.as_deref(), cli_args.opts_overrides) {
        Ok(c) => c,
        Err(e) => {
            // We cannot use tracing here as it's not configured yet
            eprintln!("{}", e.decorate());
            eprintln!("{:#?}", RestateCode::from(&e));
            std::process::exit(EXIT_CODE_FAILURE);
        }
    };

    restate_node::set_current_config(old_config);
    restate_types::config::set_current_config(config);

    let config = Configuration::pinned();
    let runtime = build_tokio(config.common()).expect("failed to build Tokio runtime!");

    let tc = TaskCenterFactory::create(runtime.handle().clone());

    runtime.block_on({
        let tc = tc.clone();
        async move {
            // Apply tracing config globally
            // We need to apply this first to log correctly
            let tracing_guard =
                init_tracing_and_logging(config.common(), "Restate binary", std::process::id())
                    .expect("failed to configure logging and tracing!");

            // Log panics as tracing errors if possible
            let prev_hook = std::panic::take_hook();
            std::panic::set_hook(Box::new(move |panic_info| {
                tracing_panic::panic_hook(panic_info);
                // run original hook if any.
                prev_hook(panic_info);
            }));

            info!("Starting Restate Server {}", build_info::build_info());
            if cli_args.config_file.is_some() {
                info!(
                    "Loading configuration file from {}",
                    cli_args.config_file.as_ref().unwrap().display()
                );
            } else {
                info!("Loading default built-in configuration");
            }
            info!(
                "Configuration dump (MAY CONTAIN SENSITIVE DATA!):\n{}",
                config.dump().unwrap()
            );

            // start config watcher
            config_loader.start();

            WipeMode::wipe(
                cli_args.wipe.as_ref(),
                config.node.admin.meta.storage_path().into(),
                config.node.worker.storage_path().into(),
                config.node.bifrost.local.path.as_path(),
                config.node.metadata_store.storage_path(),
            )
            .await
            .expect("Error when trying to wipe the configured storage path");

            let task_center_watch = tc.watch_shutdown();
            let node = Node::new(config.common.clone(), config.node.clone());
            if let Err(err) = node {
                handle_error(err);
            }
            // We ignore errors since we will wait for shutdown below anyway.
            // This starts node roles and the rest of the system async under tasks managed by
            // the TaskCenter.
            let _ = tc.spawn(TaskKind::SystemBoot, "init", None, node.unwrap().start());

            tokio::select! {
                signal_name = signal::shutdown() => {
                    info!("Received shutdown signal.");
                    let signal_reason = format!("received signal {}", signal_name);

                    let shutdown_with_timeout = tokio::time::timeout(
                        config.common().shutdown_grace_period(),
                        tc.shutdown_node(&signal_reason, 0)
                    );

                    // ignore the result because we are shutting down
                    let shutdown_result = shutdown_with_timeout.await;

                    if shutdown_result.is_err() {
                        warn!("Could not gracefully shut down Restate, terminating now.");
                    } else {
                        info!("Restate has been gracefully shut down.");
                    }
                },
                _ = signal::sigusr_dump_config() => {},
                _ = task_center_watch => {
                    // Shutdown was requested by task center and it has completed.
                },
            };

            shutdown_tracing(
                config.common().shutdown_grace_period().div(2),
                tracing_guard,
            )
            .await;
        }
    });
    let exit_code = tc.exit_code();
    if exit_code != 0 {
        error!("Restate terminated with exit code {}!", exit_code);
    } else {
        info!("Restate terminated");
    }
    // The process terminates with the task center requested exit code
    std::process::exit(exit_code);
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
