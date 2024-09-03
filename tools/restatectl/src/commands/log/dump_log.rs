// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
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

use anyhow::Context;
use cling::prelude::*;

use futures_util::StreamExt;
use restate_bifrost::{BifrostService, FindTailAttributes};
use restate_core::network::net_util::create_tonic_channel_from_advertised_address;
use restate_core::network::{MessageRouterBuilder, Networking};
use restate_core::{MetadataBuilder, MetadataManager, TaskCenter, TaskCenterBuilder, TaskKind};
use restate_metadata_store::local::LocalMetadataStoreService;
use restate_metadata_store::MetadataStoreClient;
use restate_rocksdb::RocksDbManager;
use restate_server::config_loader::ConfigLoaderBuilder;
use restate_types::config::{
    Configuration, MetadataStoreClientOptions, MetadataStoreOptions, RocksDbOptions,
};
use restate_types::live::{BoxedLiveLoad, Live};
use restate_types::logs::{KeyFilter, LogId, Lsn, SequenceNumber};
use restate_types::retries::RetryPolicy;
use restate_wal_protocol::Envelope;
use tonic_health::pb::health_client::HealthClient;
use tonic_health::pb::HealthCheckRequest;
use tracing::{debug, info, warn};

use crate::build_info;

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
    #[arg(short, long = "log")]
    log_id: u32,

    /// Start LSN, if unset it'll read from oldest record in the log.
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
    let config_path = opts
        .config_file
        .as_ref()
        .map(|p| std::fs::canonicalize(p).expect("config-file path is valid"));
    // Initial configuration loading
    let config_loader = ConfigLoaderBuilder::default()
        .load_env(true)
        .path(config_path.clone())
        .build()
        .unwrap();

    let config = match config_loader.load_once() {
        Ok(c) => c,
        Err(e) => {
            // We cannot use tracing here as it's not configured yet
            eprintln!("{:?}", e);
            std::process::exit(1);
        }
    };

    // Setting initial configuration as global current
    restate_types::config::set_current_config(config);
    if rlimit::increase_nofile_limit(u64::MAX).is_err() {
        warn!("Failed to increase the number of open file descriptors limit.");
    }

    let config = Configuration::pinned();

    if !config.bifrost.local.data_dir().exists() {
        eprintln!(
            "The specified path '{}' does not contain a local-loglet directory.",
            config.bifrost.local.data_dir().display()
        );
        std::process::exit(1);
    }

    let task_center = TaskCenterBuilder::default()
        .default_runtime_handle(tokio::runtime::Handle::current())
        .ingress_runtime_handle(tokio::runtime::Handle::current())
        .options(config.common.clone())
        .build()
        .expect("task_center builds");

    let tc = task_center.clone();
    task_center
        .run_in_scope("main", None, async move {
            let config_source = if let Some(config_file) = &opts.config_file {
                config_file.display().to_string()
            } else {
                "[default]".to_owned()
            };
            info!(
                node_name = Configuration::pinned().node_name(),
                config_source = %config_source,
                base_dir = %restate_types::config::node_filepath("").display(),
                "restatectl {}",
                build_info::build_info()
            );

            // Initialize rocksdb manager
            let rocksdb_manager =
                RocksDbManager::init(Configuration::mapped_updateable(|c| &c.common));

            debug!("Rocksdb Initialized");

            let metadata_builder = MetadataBuilder::default();
            let metadata = metadata_builder.to_metadata();
            tc.try_set_global_metadata(metadata.clone());

            let metadata_store_client = start_metadata_store(
                config.common.metadata_store_client.clone(),
                Live::from_value(config.metadata_store.clone()).boxed(),
                Live::from_value(config.metadata_store.clone())
                    .map(|c| &c.rocksdb)
                    .boxed(),
                &tc,
            )
            .await?;
            debug!("Metadata store client created");

            let networking =
                Networking::new(metadata_builder.to_metadata(), config.networking.clone());
            let metadata_manager = MetadataManager::new(
                metadata_builder,
                networking.clone(),
                metadata_store_client.clone(),
            );
            let mut router_builder = MessageRouterBuilder::default();
            metadata_manager.register_in_message_router(&mut router_builder);

            tc.spawn(
                TaskKind::SystemService,
                "metadata-manager",
                None,
                metadata_manager.run(),
            )?;

            let bifrost_svc = BifrostService::new(tc.clone(), metadata.clone())
                .enable_local_loglet(&Configuration::updateable());

            let bifrost = bifrost_svc.handle();
            // Ensures bifrost has initial metadata synced up before starting the worker.
            // Need to run start in new tc scope to have access to metadata()
            bifrost_svc.start().await?;

            let log_id = LogId::from(opts.log_id);
            debug!("Finding log tail");
            let tail = bifrost
                .find_tail(log_id, FindTailAttributes::default())
                .await?;
            debug!("log tail is {:?}", tail);
            let from_lsn: Lsn = opts.from_lsn.map(Into::into).unwrap_or(Lsn::OLDEST);
            let mut reader =
                bifrost.create_reader(log_id, KeyFilter::Any, from_lsn, tail.offset().prev())?;

            while let Some(record) = reader.next().await {
                let record = record?;
                if record.is_trim_gap() {
                    info!(
                        "Trim gap found, skipping until {}",
                        record.trim_gap_to_sequence_number().unwrap()
                    );
                    continue;
                }

                let lsn = record.sequence_number();
                let envelope = record.try_decode::<Envelope>().unwrap().with_context(|| {
                    format!(
                        "Error decoding record at lsn={} from log_id={}",
                        lsn, log_id
                    )
                })?;

                let decoded_log_record = DecodedLogRecord {
                    log_id,
                    lsn,
                    envelope,
                };
                println!("{}", serde_json::to_string(&decoded_log_record)?);
            }

            tc.shutdown_node("finished", 0).await;
            rocksdb_manager.shutdown().await;
            anyhow::Ok(())
        })
        .await?;
    Ok(())
}

async fn start_metadata_store(
    metadata_store_client_options: MetadataStoreClientOptions,
    opts: BoxedLiveLoad<MetadataStoreOptions>,
    updateables_rocksdb_options: BoxedLiveLoad<RocksDbOptions>,
    task_center: &TaskCenter,
) -> anyhow::Result<MetadataStoreClient> {
    let service = LocalMetadataStoreService::from_options(opts, updateables_rocksdb_options);
    let grpc_service_name = service.grpc_service_name().to_owned();

    task_center.spawn(
        TaskKind::MetadataStore,
        "local-metadata-store",
        None,
        async move {
            service.run().await?;
            Ok(())
        },
    )?;

    let address = match &metadata_store_client_options.metadata_store_client {
        restate_types::config::MetadataStoreClient::Embedded { address } => address.clone(),
        _ => panic!("unsupported metadata store type"),
    };

    let health_client = HealthClient::new(create_tonic_channel_from_advertised_address(
        address.clone(),
    )?);
    let retry_policy = RetryPolicy::exponential(Duration::from_millis(10), 2.0, None, None);

    retry_policy
        .retry(|| async {
            health_client
                .clone()
                .check(HealthCheckRequest {
                    service: grpc_service_name.clone(),
                })
                .await
        })
        .await?;

    let client = restate_metadata_store::local::create_client(metadata_store_client_options)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create metadata store client: {}", e))?;

    Ok(client)
}
