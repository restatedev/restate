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
use restate_core::TaskCenterFutureExt;
use rustls::crypto::aws_lc_rs;
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
    // We need to install a crypto provider explicitly because we depend on crates that activate the
    // ring as well aws_lc_rs rustls features. Unfortunately, these features are not additive. See
    // https://github.com/rustls/rustls/issues/1877. We can remove this line of code once all our
    // dependencies activate only one of the features or once rustls allows both features to be
    // activated.
    aws_lc_rs::default_provider()
        .install_default()
        .expect("no other default crypto provider being installed");
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

        if config.has_role(Role::HttpIngress) {
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
        .options(Configuration::pinned().common.clone())
        .build()
        .expect("task_center builds");
    let res = tc.block_on({
        async move {
            // Apply tracing config globally
            // We need to apply this first to log correctly
            let tracing_guard =
                init_tracing_and_logging(&Configuration::pinned().common, "restate-server")
                    .expect("failed to configure logging and tracing");
            // spawn checking latest release
            let _ = TaskCenter::spawn_unmanaged(
                TaskKind::Background,
                "check-latest-release",
                build_info::check_if_latest_version(),
            );
            // Starts prometheus periodic upkeep tasks
            prometheus.start_upkeep_task();

            // Log panics as tracing errors if possible
            let prev_hook = std::panic::take_hook();
            std::panic::set_hook(Box::new(move |panic_info| {
                let thread = std::thread::current();
                let thread_name = thread.name();
                // Make sure we also print on stderr in case tracing is borked!
                eprintln!("\n[{thread_name:?}]  PANIC!!!\n{panic_info}\n");
                tracing_panic::panic_hook(panic_info);
                // run original hook if any.
                prev_hook(panic_info);
            }));

            let config_source = if let Some(config_file) = cli_args.config_file {
                config_file.display().to_string()
            } else {
                "[default]".to_owned()
            };
            info!(
                node_name = Configuration::pinned().node_name(),
                config_source = %config_source,
                base_dir = %restate_types::config::node_filepath("").display(),
                "Starting Restate Server {}",
                build_info::build_info()
            );

            // Initialize rocksdb manager
            let rocksdb_manager = RocksDbManager::init(Configuration::map_live(|c| &c.common));

            // ensures we run rocksdb shutdown after the shutdown_node routine.
            TaskCenter::set_on_shutdown(Box::pin(async {
                rocksdb_manager.shutdown().await;
            }));

            // start config watcher
            config_loader.start();

            // Initialize telemetry
            let telemetry = telemetry::Telemetry::create(&Configuration::pinned().common);
            telemetry.start();

            let node = Node::create(Configuration::live(), prometheus).await;
            if let Err(err) = node {
                handle_error(err);
            }
            // We ignore errors since we will wait for shutdown below anyway.
            // This starts node roles and the rest of the system async under tasks managed by
            // the TaskCenter.
            let _ = TaskCenter::spawn(TaskKind::SystemBoot, "init", node.unwrap().start());

            let tc_cancel_token = TaskCenter::current().shutdown_token();
            let config_update_watcher = Configuration::watcher();
            tokio::pin!(config_update_watcher);
            let mut shutdown = false;
            loop {
                tokio::select! {
                    signal_name = signal::shutdown() => {
                        if shutdown {
                            // user is impatient, terminate immediately.
                            warn!("Received {signal_name} during an ongoing shutdown, exiting now!");
                            break;
                        }

                        shutdown = true;
                        tokio::spawn(
                            async move {
                                let signal_reason = format!("received signal {signal_name}");
                                TaskCenter::shutdown_node(&signal_reason, 0).await;
                            }.in_current_tc()
                        );
                    },
                    _ = config_update_watcher.changed(), if !shutdown => {
                        tracing_guard.on_config_update();
                    },
                    _ = signal::sighup_compact(), if !shutdown => {},
                    _ = signal::sigusr1_dump_config() => {},
                    _ = signal::sigusr2_tokio_dump() => {},
                    _ = tc_cancel_token.cancelled() => {
                        // Shutdown was requested by task center and it has completed.
                        break;
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
    // this is a no-op if rocksdb shutdown was completed already.
    RocksDbManager::get().on_ungraceful_shutdown();
    if let Err(err) = res {
        eprintln!("!!! Restate panicked during shutdown! {err:?}");
    }
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
    RocksDbManager::get().on_ungraceful_shutdown();
    std::process::exit(EXIT_CODE_FAILURE);
}
