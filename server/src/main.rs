// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::error::Error;
use std::io::IsTerminal;
use std::io::Write as _;
use std::ops::Div;
use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;
use codederror::CodedError;
use tracing::error;
use tracing::{info, trace, warn};

use restate_core::TaskCenter;
use restate_core::TaskCenterBuilder;
use restate_core::TaskKind;
use restate_errors::fmt::RestateCode;
use restate_rocksdb::RocksDbManager;
use restate_server::build_info;
use restate_tracing_instrumentation::TracingGuard;
use restate_tracing_instrumentation::init_tracing_and_logging;
use restate_tracing_instrumentation::prometheus_metrics::Prometheus;
use restate_types::art::render_restate_logo;
use restate_types::config::CommonOptionCliOverride;
use restate_types::config::{Configuration, PRODUCTION_PROFILE_DEFAULTS};
use restate_types::config_loader::ConfigLoaderBuilder;
use restate_types::nodes_config::Role;

mod signal;
mod telemetry;

use restate_node::Node;
#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

// On linux, run jemalloc with profiling enabled, but inactive (so there is no performance impact)
// If needed, profiling can be activated with :5122/debug/pprof/heap/activate, or by overriding this value with $MALLOC_CONF
#[cfg(target_os = "linux")]
#[unsafe(export_name = "malloc_conf")]
pub static MALLOC_CONF: &[u8] = b"prof:true,prof_active:false,lg_prof_sample:19\0";

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

    /// Use default production configuration profile.
    #[clap(long)]
    production: bool,

    #[clap(flatten)]
    opts_overrides: CommonOptionCliOverride,
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
    let mut config_loader_builder = ConfigLoaderBuilder::default();

    if cli_args.production {
        config_loader_builder.custom_default((*PRODUCTION_PROFILE_DEFAULTS).clone());
    }

    let config_loader = config_loader_builder
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
            eprintln!("{:#?}", RestateCode::from_code(e.code()));
            std::process::exit(EXIT_CODE_FAILURE);
        }
    };
    if cli_args.dump_config {
        println!("{}", config.dump().expect("config is toml serializable"));
        std::process::exit(0);
    }

    // Install the recorder as early as possible
    let mut prometheus = Prometheus::install(&config.common);

    if std::io::stdout().is_terminal() {
        let mut stdout = std::io::stdout().lock();
        let _ = writeln!(
            stdout,
            "{}",
            render_restate_logo(!config.common.log_disable_ansi_codes)
        );
        let _ = writeln!(
            &mut stdout,
            "{:^40}",
            format!("Restate {}", build_info::RESTATE_SERVER_VERSION)
        );
        let _ = writeln!(&mut stdout, "{:^40}", "https://restate.dev/");
        if config.has_role(Role::Admin) {
            let _ = writeln!(
                &mut stdout,
                "{:^40}",
                format!(
                    "Admin: {}",
                    config
                        .admin
                        .advertised_admin_endpoint
                        .as_ref()
                        .expect("is set")
                )
            );
        }

        // todo: this should be changed to HttpIngress
        // once it's fully supported
        if config.has_role(Role::Worker) {
            let _ = writeln!(
                &mut stdout,
                "{:^40}",
                format!(
                    "HTTP Ingress: {}",
                    config
                        .ingress
                        .advertised_ingress_endpoint
                        .as_ref()
                        .expect("is set")
                )
            );
        }

        let _ = writeln!(&mut stdout);
    }

    // Setting initial configuration as global current
    restate_types::config::set_current_config(config);
    if rlimit::increase_nofile_limit(u64::MAX).is_err() {
        warn!("Failed to increase the number of open file descriptors limit.");
    }
    let tc = TaskCenterBuilder::default()
        .options(Configuration::with_current(|config| config.common.clone()))
        .build()
        .expect("task_center builds");
    tc.block_on({
        async move {
            // Apply tracing config globally
            // We need to apply this first to log correctly
            let tracing_guard =
                Configuration::with_current(|config| init_tracing_and_logging(&config.common, "restate-server"))
                    .expect("failed to configure logging and tracing!");
            // Starts prometheus periodic upkeep tasks
            prometheus.start_upkeep_task();

            // Log panics as tracing errors if possible
            let prev_hook = std::panic::take_hook();
            std::panic::set_hook(Box::new(move |panic_info| {
                tracing_panic::panic_hook(panic_info);
                // run original hook if any.
                prev_hook(panic_info);
            }));

            let config_source = if let Some(config_file) = cli_args.config_file {
                config_file.display().to_string()
            } else {
                "[default]".to_owned()
            };
            Configuration::with_current(|config| {
                                            info!(
                node_name = config.node_name(),
                config_source = %config_source,
                base_dir = %restate_types::config::node_filepath("").display(),
                "Starting Restate Server {}",
                build_info::build_info()
            ); });

            // Initialize rocksdb manager
            let rocksdb_manager =
                RocksDbManager::init(Configuration::mapped_updateable(|c| &c.common));

            // start config watcher
            config_loader.start();

            // Initialize telemetry
            let telemetry = Configuration::with_current(|config| telemetry::Telemetry::create(&config.common));
            telemetry.start();

            let node = Node::create(Configuration::updateable(), prometheus).await;
            if let Err(err) = node {
                handle_error(err);
            }
            // We ignore errors since we will wait for shutdown below anyway.
            // This starts node roles and the rest of the system async under tasks managed by
            // the TaskCenter.
            let _ = TaskCenter::spawn(TaskKind::SystemBoot, "init", node.unwrap().start());

            let task_center_watch = TaskCenter::current().shutdown_token();
            tokio::pin!(task_center_watch);

            let config_update_watcher = Configuration::watcher();
            tokio::pin!(config_update_watcher);
            let mut shutdown = false;
            while !shutdown {
                tokio::select! {
                    signal_name = signal::shutdown() => {
                        shutdown = true;
                        let signal_reason = format!("received signal {signal_name}");

                        let shutdown_with_timeout = tokio::time::timeout(
                            Configuration::with_current(|config| config.common.shutdown_grace_period()),
                            async {
                                TaskCenter::shutdown_node(&signal_reason, 0).await;
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
                        tracing_guard.on_config_update();
                    },
                    _ = signal::sigusr1_dump_config() => {},
                    _ = signal::sighusr2_compact() => {},
                    _ = task_center_watch.cancelled() => {
                        shutdown = true;
                        // Shutdown was requested by task center and it has completed.
                    },
                };
            }

            shutdown_tracing(
                Configuration::with_current(|config| config
                    .common
                    .shutdown_grace_period()
                    .div(2)),
                tracing_guard,
            )
            .await;
        }
    });
    let exit_code = tc.exit_code();
    if exit_code != 0 {
        error!("Restate terminated with exit code {}!", exit_code);
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
