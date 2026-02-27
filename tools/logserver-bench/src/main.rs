// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;
use codederror::CodedError;
use metrics_exporter_prometheus::PrometheusBuilder;
use tracing::info;

use restate_cli_util::{CliContext, c_eprintln, c_println, c_tip, c_warn};
use restate_core::task_center::TaskCenterMonitoring;
use restate_core::{MetadataBuilder, TaskCenter, TaskCenterBuilder, TaskKind, task_center};
use restate_errors::fmt::RestateCode;
use restate_log_server::metadata::LogletStateMap;
use restate_log_server::rocksdb_logstore::RocksDbLogStoreBuilder;
use restate_rocksdb::RocksDbManager;
use restate_tracing_instrumentation::init_tracing_and_logging;
use restate_types::config::Configuration;
use restate_types::config_loader::ConfigLoaderBuilder;

use logserver_bench::{Arguments, Command, metrics_server, mixed_workload, write_throughput};

// Configure jemalloc to mimic restate server
#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn main() -> anyhow::Result<()> {
    let mut cli_args = Arguments::parse();

    // `generate-payload` is a pure offline command — no TaskCenter, RocksDB, or
    // tracing needed. Handle it early and exit.
    if let Command::GeneratePayload(ref opts) = cli_args.command {
        CliContext::new(cli_args.common_opts.clone()).set_as_global();
        return logserver_bench::payload::generate_payload_file(opts);
    }

    let user_provided_base_dir = cli_args.opts_overrides.base_dir.is_some();

    // If no explicit --base-dir was provided, create a temporary directory.
    // We hold `_temp_dir_guard` to keep the directory alive until the end of main.
    // When `--retain-test-dir` is set, we `.keep()` the TempDir so it persists after exit.
    let (_temp_dir_guard, base_dir): (Option<tempfile::TempDir>, PathBuf) =
        if let Some(ref base_dir) = cli_args.opts_overrides.base_dir {
            (None, base_dir.clone())
        } else {
            let temp = tempfile::TempDir::new().expect("failed to create temp dir");
            let path = temp.path().to_path_buf();
            cli_args.opts_overrides.base_dir = Some(path.clone());
            if cli_args.retain_test_dir {
                let _ = temp.keep();
                (None, path)
            } else {
                (Some(temp), path)
            }
        };

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

    let recorder = PrometheusBuilder::new().install_recorder().unwrap();
    let (tc, log_store, state_map) = spawn_environment(Configuration::live());
    let task_center = tc.clone();
    let args = cli_args.clone();

    tc.block_on(async move {
        let tracing_guard = init_tracing_and_logging(&config.common, "logserver-bench")
            .expect("failed to configure logging and tracing!");

        // Initialize CLI context without tracing — we already have a tracing subscriber
        // from init_tracing_and_logging above.
        CliContext::new_without_tracing(args.common_opts.clone()).set_as_global();

        // Start metrics HTTP server
        metrics_server::start_metrics_server(args.metrics_port, recorder);

        info!("Log-server benchmark environment ready");

        // Run the benchmark as a spawned task so it executes on the tokio runtime
        // worker pool rather than inline in block_on (which runs on the calling
        // thread). This better reflects real-world scheduling behaviour.
        let bench_handle =
            TaskCenter::spawn_unmanaged(TaskKind::Disposable, "benchmark", async move {
                match args.command {
                    Command::WriteThroughput(ref opts) => {
                        write_throughput::run(
                            opts,
                            log_store.clone(),
                            &state_map,
                            args.report_interval,
                            args.raw_rocksdb_stats,
                        )
                        .await
                    }
                    Command::MixedWorkload(ref opts) => {
                        mixed_workload::run(
                            opts,
                            log_store.clone(),
                            &state_map,
                            args.report_interval,
                            args.raw_rocksdb_stats,
                        )
                        .await
                    }
                    Command::GeneratePayload(_) => {
                        unreachable!("handled above before TaskCenter setup")
                    }
                }
            })?;
        bench_handle.await??;

        // Record tokio runtime metrics
        task_center.submit_metrics();

        task_center.shutdown_node("completed", 0).await;
        RocksDbManager::get().shutdown().await;

        let shutdown_tracing_with_timeout =
            tokio::time::timeout(Duration::from_secs(10), tracing_guard.async_shutdown());
        if shutdown_tracing_with_timeout.await.is_err() {
            tracing::trace!("Failed to fully flush pending spans, terminating now.");
        }
        anyhow::Ok(())
    })?;

    if user_provided_base_dir || cli_args.retain_test_dir {
        c_tip!("Keeping the base_dir in {}", base_dir.display());
    } else {
        c_println!("Removing test dir at {}", base_dir.display());
    }

    Ok(())
}

fn spawn_environment(
    config: restate_types::live::Live<Configuration>,
) -> (
    task_center::Handle,
    restate_log_server::rocksdb_logstore::RocksDbLogStore,
    LogletStateMap,
) {
    let tc = TaskCenterBuilder::default()
        .options(config.pinned().common.clone())
        .build()
        .expect("task_center builds")
        .into_handle();

    let (log_store, state_map) = tc.block_on(async move {
        let metadata_builder = MetadataBuilder::default();
        TaskCenter::try_set_global_metadata(metadata_builder.to_metadata());

        RocksDbManager::init();

        let builder = RocksDbLogStoreBuilder::create()
            .await
            .expect("Failed to create RocksDB log store");
        let log_store = builder
            .start(Default::default())
            .await
            .expect("Failed to start RocksDB log store");

        let state_map = LogletStateMap::load_all(&log_store)
            .await
            .expect("Failed to load loglet state map");

        (log_store, state_map)
    });

    (tc, log_store, state_map)
}
