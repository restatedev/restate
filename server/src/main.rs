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
use restate_core::TaskCenterBuilder;
use restate_core::TaskKind;
use restate_errors::fmt::RestateCode;
use restate_rocksdb::RocksDbManager;
use restate_server::build_info;
use restate_server::config_loader::ConfigLoaderBuilder;
use restate_tracing_instrumentation::init_tracing_and_logging;
use restate_tracing_instrumentation::TracingGuard;
use restate_types::config::CommonOptionCliOverride;
use restate_types::config::Configuration;
use std::error::Error;
use std::ops::Div;
use std::path::PathBuf;
use std::time::Duration;
use tokio::io;
use tracing::debug;
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
        worker_storage_dir: PathBuf,
        local_loglet_storage_dir: PathBuf,
        local_metadata_store_storage_dir: PathBuf,
    ) -> io::Result<()> {
        let (wipe_worker, wipe_local_loglet, wipe_local_metadata_store) = match mode {
            Some(WipeMode::Worker) => (true, true, false),
            Some(WipeMode::LocalLoglet) => (false, true, false),
            Some(WipeMode::LocalMetadataStore) => (false, false, true),
            Some(WipeMode::All) => (true, true, true),
            None => (false, false, false),
        };

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

    // Setting initial configuration as global current
    restate_types::config::set_current_config(config);

    let tc = TaskCenterBuilder::default()
        .options(Configuration::pinned().common.clone())
        .build()
        .expect("task_center builds");
    tc.block_on("main", None, {
        let tc = tc.clone();
        async move {
            // Apply tracing config globally
            // We need to apply this first to log correctly
            let tracing_guard =
                init_tracing_and_logging(&Configuration::pinned().common, "Restate binary")
                    .expect("failed to configure logging and tracing!");

            // Log panics as tracing errors if possible
            let prev_hook = std::panic::take_hook();
            std::panic::set_hook(Box::new(move |panic_info| {
                tracing_panic::panic_hook(panic_info);
                // run original hook if any.
                prev_hook(panic_info);
            }));

            info!(
                node_name = Configuration::pinned().node_name(),
                "Starting Restate Server {}",
                build_info::build_info()
            );
            if cli_args.config_file.is_some() {
                info!(
                    config_file = %cli_args.config_file.as_ref().unwrap().display(),
                    "Loaded configuration file",
                );
            } else {
                info!("Loaded default configuration");
            }

            debug!(
                "Configuration dump (MAY CONTAIN SENSITIVE DATA!):\n{}",
                Configuration::pinned().dump().unwrap()
            );

            // Initialize rocksdb manager
            let rocksdb_manager =
                RocksDbManager::init(Configuration::mapped_updateable(|c| &c.common));

            // start config watcher
            config_loader.start();

            {
                let config = Configuration::pinned();

                WipeMode::wipe(
                    cli_args.wipe.as_ref(),
                    config.worker.data_dir(),
                    config.bifrost.local.data_dir(),
                    config.metadata_store.data_dir(),
                )
                .await
                .expect("Error when trying to wipe the configured storage path");
            }

            let node = Node::new(Configuration::current().clone());
            if let Err(err) = node {
                handle_error(err);
            }
            // We ignore errors since we will wait for shutdown below anyway.
            // This starts node roles and the rest of the system async under tasks managed by
            // the TaskCenter.
            let _ = tc.spawn(TaskKind::SystemBoot, "init", None, node.unwrap().start());

            let task_center_watch = tc.shutdown_token();
            tokio::pin!(task_center_watch);

            let config_update_watcher = Configuration::watcher();
            tokio::pin!(config_update_watcher);
            let mut shutdown = false;
            while !shutdown {
                tokio::select! {
                    signal_name = signal::shutdown() => {
                        info!("Received shutdown signal.");
                        shutdown = true;
                        let signal_reason = format!("received signal {}", signal_name);


                        let shutdown_with_timeout = tokio::time::timeout(
                            Configuration::pinned().common.shutdown_grace_period(),
                            async {
                                tc.shutdown_node(&signal_reason, 0).await;
                                rocksdb_manager.shutdown().await;
                            }
                        );

                        // ignore the result because we are shutting down
                        let shutdown_result = shutdown_with_timeout.await;

                        if shutdown_result.is_err() {
                            warn!("Could not gracefully shut down Restate, terminating now.");
                        } else {
                            info!("Restate has been gracefully shut down.");
                        }
                    },
                    _ = config_update_watcher.changed() => {
                        let config = Configuration::pinned();
                        tracing_guard.reload_log_filter(&config.common);
                    }
                    _ = signal::sigusr_dump_config() => {},
                    _ = task_center_watch.cancelled() => {
                        shutdown = true;
                        // Shutdown was requested by task center and it has completed.
                    },
                };
            }

            shutdown_tracing(
                Configuration::pinned()
                    .common
                    .shutdown_grace_period()
                    .div(2),
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
