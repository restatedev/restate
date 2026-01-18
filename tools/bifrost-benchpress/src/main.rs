// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use codederror::CodedError;
use metrics_exporter_prometheus::PrometheusBuilder;
use tracing::trace;

use bifrost_benchpress::util::{print_prometheus_stats, print_rocksdb_stats};
use bifrost_benchpress::{Arguments, Command, append_latency, write_to_read};
use restate_bifrost::{Bifrost, BifrostService};
use restate_core::task_center::TaskCenterMonitoring;
use restate_core::{
    MetadataBuilder, MetadataManager, TaskCenter, TaskCenterBuilder, spawn_metadata_manager,
    task_center,
};
use restate_errors::fmt::RestateCode;
use restate_metadata_server::MetadataStoreClient;
use restate_rocksdb::RocksDbManager;
use restate_tracing_instrumentation::init_tracing_and_logging;
use restate_types::config::{
    Configuration, reset_base_temp_dir, reset_base_temp_dir_and_retain, set_base_temp_dir,
};
use restate_types::config_loader::ConfigLoaderBuilder;
use restate_types::live::{Live, LiveLoadExt};
use restate_types::metadata::Precondition;
use restate_types::metadata_store::keys::BIFROST_CONFIG_KEY;

// Configure jemalloc similar to mimic restate server
#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn main() -> anyhow::Result<()> {
    let cli_args = Arguments::parse();
    // base-dir is set by CLI overrides, let's make sure we use it.
    let base_dir = if let Some(base_dir) = &cli_args.opts_overrides.base_dir {
        set_base_temp_dir(base_dir.clone());
        base_dir.clone()
    } else if cli_args.retain_test_dir {
        reset_base_temp_dir_and_retain()
    } else {
        reset_base_temp_dir()
    };

    #[cfg(unix)]
    if rlimit::increase_nofile_limit(u64::MAX).is_err() {
        eprintln!("WARN: Failed to increase the number of open file descriptors limit.");
    }

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

    let mut config = match config_loader.load_once() {
        Ok(c) => c,
        Err(e) => {
            // We cannot use tracing here as it's not configured yet
            eprintln!("{}", e.decorate());
            eprintln!("{:#?}", RestateCode::from_code(e.code()));
            std::process::exit(1);
        }
    };

    // just in case anything reads directly the base dir and it's not set correctly for any random
    // reason.
    config.common.set_base_dir(base_dir.clone());

    restate_types::config::set_current_config(config.clone());

    let recorder = PrometheusBuilder::new().install_recorder().unwrap();
    let (tc, bifrost) = spawn_environment(Configuration::live(), 1);
    let task_center = tc.clone();
    let args = cli_args.clone();
    tc.block_on(async move {
        let tracing_guard = init_tracing_and_logging(&config.common, "Bifrost benchpress")
            .expect("failed to configure logging and tracing!");

        match args.command {
            Command::WriteToRead(ref opts) => {
                write_to_read::run(&args, opts, bifrost).await?;
            }
            Command::AppendLatency(ref opts) => {
                append_latency::run(&args, opts, bifrost).await?;
            }
        }
        // record tokio's runtime metrics
        task_center.submit_metrics();

        task_center.shutdown_node("completed", 0).await;
        // print prometheus if asked.
        if !args.no_prometheus_stats {
            // submit tokio metris
            print_prometheus_stats(&recorder);
        }

        // print rocksdb stats if asked.
        if !args.no_rocksdb_stats {
            print_rocksdb_stats("local-loglet");
        }

        // We shutdown the database after stats to avoid enclosing the shutdown process in our
        // metrics.
        RocksDbManager::get().shutdown().await;
        // Make sure that all pending spans are flushed
        let shutdown_tracing_with_timeout =
            tokio::time::timeout(Duration::from_secs(10), tracing_guard.async_shutdown());
        let shutdown_result = shutdown_tracing_with_timeout.await;

        if shutdown_result.is_err() {
            trace!("Failed to fully flush pending spans, terminating now.");
        }
        anyhow::Ok(())
    })?;

    if cli_args.opts_overrides.base_dir.is_some() || cli_args.retain_test_dir {
        println!("Keeping the base_dir in {}", base_dir.display());
    } else {
        println!("Removing test dir at {}", base_dir.display());
    }

    Ok(())
}

fn spawn_environment(config: Live<Configuration>, num_logs: u16) -> (task_center::Handle, Bifrost) {
    let tc = TaskCenterBuilder::default()
        .options(config.pinned().common.clone())
        .build()
        .expect("task_center builds")
        .into_handle();

    let bifrost = tc.block_on(async move {
        let metadata_builder = MetadataBuilder::default();
        let metadata_store_client = MetadataStoreClient::new_in_memory();
        let metadata = metadata_builder.to_metadata();
        let metadata_manager =
            MetadataManager::new(metadata_builder, metadata_store_client.clone());

        let metadata_writer = metadata_manager.writer();
        TaskCenter::try_set_global_metadata(metadata.clone());

        RocksDbManager::init(config.clone().map(|c| &c.common));

        let logs = restate_types::logs::metadata::bootstrap_logs_metadata(
            config.pinned().bifrost.default_provider,
            None,
            num_logs,
        );

        metadata_store_client
            .put(BIFROST_CONFIG_KEY.clone(), &logs, Precondition::None)
            .await
            .expect("to store bifrost config in metadata store");
        metadata_writer.submit(Arc::new(logs));
        spawn_metadata_manager(metadata_manager).expect("metadata manager starts");

        let bifrost_svc = BifrostService::new(metadata_writer)
            .enable_in_memory_loglet()
            .enable_local_loglet(config.map(|config| &config.bifrost.local).boxed());
        let bifrost = bifrost_svc.handle();

        // start bifrost service in the background
        bifrost_svc.start().await.expect("bifrost starts");
        bifrost
    });
    (tc, bifrost)
}
