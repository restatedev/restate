// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use clap::Parser;
use codederror::CodedError;
use metrics_exporter_prometheus::PrometheusBuilder;
use restate_types::clock::ClockUpkeep;
use tokio::runtime::LocalOptions;
use tracing::info;

use restate_cli_util::{CliContext, c_eprintln, c_println, c_warn};
use restate_core::task_center::TaskCenterFutureExt;
use restate_core::task_center::TaskCenterMonitoring;
use restate_core::{MetadataBuilder, TaskCenter, TaskCenterBuilder};
use restate_errors::fmt::RestateCode;
use restate_tracing_instrumentation::init_tracing_and_logging;
use restate_types::config_loader::ConfigLoaderBuilder;

use pp_bench::{Arguments, BenchCommand, command_gen, extract, metrics_server, workload};

// Configure jemalloc to mimic restate server
#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn main() -> anyhow::Result<()> {
    let mut cli_args = Arguments::parse();

    // `generate`, `inspect`, and `extract` are pure offline commands — no TaskCenter,
    // RocksDB, or tracing needed. Handle them early and exit.
    match cli_args.command {
        BenchCommand::Generate(ref opts) => {
            CliContext::new(cli_args.common_opts.clone()).set_as_global();
            return command_gen::generate_command_file(opts);
        }
        BenchCommand::Inspect(ref opts) => {
            CliContext::new(cli_args.common_opts.clone()).set_as_global();
            return command_gen::inspect_command_file(opts);
        }
        BenchCommand::Extract(ref opts) => {
            CliContext::new(cli_args.common_opts.clone()).set_as_global();
            return extract::extract_snapshot_bundle(opts);
        }
        BenchCommand::Run(_) => {}
    }

    // Generate a unique node name so each run gets its own subdirectory under
    // the base-dir (default: ./restate-data), matching how the real restate-server
    // stores data. Users can override via --node-name.
    if cli_args.opts_overrides.node_name.is_none() {
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        cli_args.opts_overrides.node_name = Some(format!("pp-bench-{ts}"));
    }

    if rlimit::increase_nofile_limit(u64::MAX).is_err() {
        c_warn!("Failed to increase the number of open file descriptors limit.");
    }

    let config_path = cli_args
        .config_file
        .as_ref()
        .map(|p| std::fs::canonicalize(p).expect("config-file path is valid"));

    let config_loader = ConfigLoaderBuilder::default()
        .load_env(true)
        .path(config_path)
        .cli_override(cli_args.opts_overrides.clone())
        .build()
        .unwrap();

    let config = match config_loader.load_once() {
        Ok(c) => c,
        Err(e) => {
            c_eprintln!("{}", e.decorate());
            c_eprintln!("{:#?}", RestateCode::from_code(e.code()));
            std::process::exit(1);
        }
    };

    restate_types::config::set_current_config(config.clone());

    let base_dir = config.common.base_dir();
    let node_name = config.common.node_name().to_owned();
    c_println!("Node name: {node_name}");
    c_println!("Base dir:  {}", base_dir.display());

    let recorder = PrometheusBuilder::new().install_recorder().unwrap();

    let _upkeep = ClockUpkeep::start().expect("clock upkeep starts");

    // Build a single-threaded LocalRuntime. The apply loop, interval reporter,
    // and metrics HTTP server all run on this one runtime, interleaved at
    // .await points. This is simpler than the production setup (which runs the
    // partition processor on a dedicated current_thread runtime per partition,
    // isolated from infrastructure tasks on a separate multi-thread runtime),
    // and intentionally accepts the slight measurement perturbation from
    // co-locating background work with the apply loop.
    //
    // LocalRuntime is used (over plain current_thread + LocalSet) because the
    // workload's apply loop holds !Send types — PartitionStoreTransaction wraps
    // a raw RocksDB pointer. LocalRuntime supports spawn_local natively.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .thread_name("pp-bench")
        .build_local(LocalOptions::default())
        .expect("LocalRuntime builds");

    let tc = TaskCenterBuilder::default()
        .default_runtime_handle(runtime.handle().clone())
        .options(config.common.clone())
        .build()
        .expect("task_center builds")
        .into_handle();

    let metadata = MetadataBuilder::default().to_metadata();
    let args = cli_args.clone();
    let task_center = tc.clone();

    runtime.block_on(
        async move {
            TaskCenter::try_set_global_metadata(metadata);

            let tracing_guard = init_tracing_and_logging(&config.common, "pp-bench")
                .expect("failed to configure logging and tracing!");

            CliContext::new_without_tracing(args.common_opts.clone()).set_as_global();

            // Start metrics HTTP server
            metrics_server::start_metrics_server(args.metrics_port, recorder);

            info!("PP-bench environment ready");

            match args.command {
                BenchCommand::Run(ref opts) => {
                    workload::run(
                        opts,
                        args.report_interval,
                        args.raw_rocksdb_stats,
                        args.json,
                    )
                    .await?;
                }
                BenchCommand::Generate(_) | BenchCommand::Inspect(_) | BenchCommand::Extract(_) => {
                    unreachable!("handled above before TaskCenter setup")
                }
            }

            task_center.submit_metrics();
            task_center.shutdown_node("completed", 0).await;

            let shutdown_tracing_with_timeout =
                tokio::time::timeout(Duration::from_secs(10), tracing_guard.async_shutdown());
            if shutdown_tracing_with_timeout.await.is_err() {
                tracing::trace!("Failed to fully flush pending spans, terminating now.");
            }
            anyhow::Ok(())
        }
        .in_tc(&tc),
    )?;

    Ok(())
}
