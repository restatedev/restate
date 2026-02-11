// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::max;
use std::future::Future;
use std::path::PathBuf;

use anyhow::{Context, bail};
use cling::prelude::*;
use futures_util::StreamExt;
use tracing::{debug, info, warn};

use restate_bifrost::BifrostService;
use restate_bifrost::loglet::FindTailOptions;
use restate_core::TaskCenterBuilder;
use restate_core::TaskCenterFutureExt;
use restate_core::network::MessageRouterBuilder;

use restate_core::network::NetworkServerBuilder;
use restate_core::{MetadataBuilder, MetadataManager, TaskCenter, TaskKind};
use restate_metadata_server::MetadataServer;
use restate_metadata_store::MetadataStoreClient;
use restate_rocksdb::RocksDbManager;
use restate_types::config::{Configuration, MetadataClientKind};
use restate_types::config_loader::ConfigLoaderBuilder;
use restate_types::health::HealthStatus;
use restate_types::live::Live;
use restate_types::live::LiveLoadExt;
use restate_types::live::Pinned;
use restate_types::logs::{KeyFilter, LogId, Lsn, SequenceNumber};
use restate_types::net::address::AdvertisedAddress;
use restate_types::protobuf::common::NodeRpcStatus;
use restate_wal_protocol::Envelope;

/// Loads configuration, creates a task center, executes the supplied function body in scope of TC, and shuts down.
async fn run_in_task_center<F, O>(config_file: Option<&PathBuf>, fn_body: F) -> O::Output
where
    F: FnOnce(Pinned<Configuration>) -> O,
    O: Future,
{
    let config_path = config_file
        .as_ref()
        .map(|p| std::fs::canonicalize(p).expect("config-file path is valid"));

    let config_loader = ConfigLoaderBuilder::default()
        .load_env(true)
        .path(config_path.clone())
        .build()
        .unwrap();

    let config = match config_loader.load_once() {
        Ok(c) => c,
        Err(e) => {
            // We cannot use tracing here as it's not configured yet
            eprintln!("{e:?}");
            std::process::exit(1);
        }
    };

    restate_types::config::set_current_config(config);
    if rlimit::increase_nofile_limit(u64::MAX).is_err() {
        warn!("Failed to increase the number of open file descriptors limit.");
    }

    let config = Configuration::pinned();

    let task_center = TaskCenterBuilder::default()
        .default_runtime_handle(tokio::runtime::Handle::current())
        .options(config.common.clone())
        .build()
        .expect("task_center builds")
        .into_handle();

    let result = fn_body(config).in_tc(&task_center).await;

    task_center.shutdown_node("finished", 0).await;
    result
}

async fn start_metadata_server(mut config: Configuration) -> anyhow::Result<MetadataStoreClient> {
    let mut server_builder = NetworkServerBuilder::default();

    // right now we only support running a local metadata store
    let uds = tempfile::tempdir()?.keep().join("metadata-rpc-server");
    let bind_address = BindAddress::Uds(uds.clone());
    config.common.metadata_client.kind = MetadataClientKind::Replicated {
        addresses: vec![AdvertisedAddress::Uds(uds)],
    };

    let (service, client) = restate_metadata_server::create_metadata_server_and_client(
        Live::from_value(config),
        HealthStatus::default(),
        &mut server_builder,
    )
    .await?;

    let rpc_server_health = if !server_builder.is_empty() {
        let rpc_server_health_status = HealthStatus::default();
        TaskCenter::spawn(TaskKind::MetadataServer, "metadata-rpc-server", {
            let rpc_server_health_status = rpc_server_health_status.clone();
            async move {
                server_builder
                    .run(rpc_server_health_status, &bind_address)
                    .await
            }
        })?;
        Some(rpc_server_health_status)
    } else {
        None
    };

    TaskCenter::spawn(
        TaskKind::MetadataServer,
        "local-metadata-server",
        async move {
            service.run(None).await?;
            Ok(())
        },
    )?;

    if let Some(rpc_server_health) = rpc_server_health {
        info!("Waiting for local metadata store to startup");
        rpc_server_health.wait_for_value(NodeRpcStatus::Ready).await;
    }

    Ok(client)
}

#[derive(Run, Parser, Collect, Clone, Debug)]
#[clap()]
#[cling(run = "dump_log")]
pub struct DumpLogOpts {
    /// Set a configuration file to use for Restate.
    /// For more details, check the documentation.
    #[arg(
        short,
        long = "config-file",
        env = "RESTATE_CONFIG",
        value_name = "FILE"
    )]
    config_file: Option<PathBuf>,

    /// Specifies the log_id to dump.
    #[arg(short, long)]
    log_id: u32,

    /// Start LSN, if unset it'll read from the oldest record in the log.
    #[arg(long)]
    from_lsn: Option<u64>,
}

#[derive(Debug, serde::Serialize)]
struct DecodedLogRecord {
    log_id: LogId,
    lsn: Lsn,
    envelope: Envelope,
}

async fn dump_log(opts: &DumpLogOpts) -> anyhow::Result<()> {
    run_in_task_center(opts.config_file.as_ref(), |config| async move {
        if !config.bifrost.local.data_dir().exists() {
            bail!(
                "The specified path '{}' does not contain a local-loglet directory.",
                config.bifrost.local.data_dir().display()
            );
        }

        let rocksdb_manager = RocksDbManager::init();
        debug!("RocksDB Initialized");

        let metadata_builder = MetadataBuilder::default();
        let metadata = metadata_builder.to_metadata();
        TaskCenter::try_set_global_metadata(metadata.clone());

        let metadata_store_client = start_metadata_server(config.clone()).await?;
        debug!("Metadata store client created");

        let mut metadata_manager =
            MetadataManager::new(metadata_builder, metadata_store_client.clone());
        let metadata_writer = metadata_manager.writer();
        let default_pool = TaskCenter::with_current(|tc| {
            tc.memory_controller().create_pool(
                "fabric-default",
                config.networking.fabric_memory_limit(),
                |pool| {
                    pool.set_capacity(Configuration::pinned().networking.fabric_memory_limit());
                },
            )
        });
        let mut router_builder = MessageRouterBuilder::with_default_pool(default_pool);
        metadata_manager.register_in_message_router(&mut router_builder);

        TaskCenter::spawn(
            TaskKind::SystemService,
            "metadata-manager",
            metadata_manager.run(),
        )?;

        let bifrost_svc = BifrostService::new(metadata_writer).enable_local_loglet(
            Configuration::live()
                .map(|config| &config.bifrost.local)
                .boxed(),
        );

        let bifrost = bifrost_svc.handle();
        // Ensures bifrost has initial metadata synced up before starting the worker.
        // Need to run start in tc scope to have access to metadata()
        bifrost_svc.start().await?;

        let log_id = LogId::from(opts.log_id);
        debug!("Finding log tail");
        let tail = bifrost
            .find_tail(log_id, FindTailOptions::default())
            .await?;
        debug!("Log tail is {:?}", tail);
        let trim_point = bifrost.get_trim_point(log_id).await?;
        debug!("Trim point is {:?}", trim_point);
        let from_lsn: Lsn = opts
            .from_lsn
            .map(Into::into)
            .unwrap_or_else(|| max(trim_point.next(), Lsn::OLDEST));
        debug!(
            ?log_id,
            ?from_lsn,
            to_lsn = ?tail.offset().prev(),
            "Creating Bifrost log reader",
        );
        let mut reader =
            bifrost.create_reader(log_id, KeyFilter::Any, from_lsn, tail.offset().prev())?;

        while let Some(record) = reader.next().await {
            debug!("Got record: {:?}", record);

            let record = record?;
            if record.is_trim_gap() {
                info!(
                    "Trim gap found, skipping until after {}",
                    record.trim_gap_to_sequence_number().unwrap()
                );
                continue;
            }

            let lsn = record.sequence_number();
            let envelope = record.try_decode::<Envelope>().unwrap().with_context(|| {
                format!("Error decoding record at lsn={lsn} from log_id={log_id}")
            })?;

            let decoded_log_record = DecodedLogRecord {
                log_id,
                lsn,
                envelope,
            };
            println!("{}", serde_json::to_string(&decoded_log_record)?);
        }

        rocksdb_manager.shutdown().await;
        anyhow::Ok(())
    })
    .await?;
    Ok(())
}
