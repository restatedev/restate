// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::max;
use std::path::PathBuf;

use anyhow::{bail, Context};
use cling::prelude::*;
use futures_util::StreamExt;
use tracing::{debug, info};

use restate_bifrost::loglet::FindTailAttr;
use restate_bifrost::BifrostService;
use restate_core::network::MessageRouterBuilder;
use restate_core::{MetadataBuilder, MetadataManager, TaskCenter, TaskKind};
use restate_rocksdb::RocksDbManager;
use restate_types::config::Configuration;
use restate_types::logs::{KeyFilter, LogId, Lsn, SequenceNumber};
use restate_wal_protocol::Envelope;

use crate::environment::metadata_store;
use crate::environment::task_center::run_in_task_center;

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

        let rocksdb_manager = RocksDbManager::init(Configuration::mapped_updateable(|c| &c.common));
        debug!("RocksDB Initialized");

        let metadata_builder = MetadataBuilder::default();
        let metadata = metadata_builder.to_metadata();
        TaskCenter::try_set_global_metadata(metadata.clone());

        let metadata_store_client = metadata_store::start_metadata_server(config.clone()).await?;
        debug!("Metadata store client created");

        let metadata_manager =
            MetadataManager::new(metadata_builder, metadata_store_client.clone());
        let metadata_writer = metadata_manager.writer();
        let mut router_builder = MessageRouterBuilder::default();
        metadata_manager.register_in_message_router(&mut router_builder);

        TaskCenter::spawn(
            TaskKind::SystemService,
            "metadata-manager",
            metadata_manager.run(),
        )?;

        let bifrost_svc =
            BifrostService::new(metadata_writer).enable_local_loglet(&Configuration::updateable());

        let bifrost = bifrost_svc.handle();
        // Ensures bifrost has initial metadata synced up before starting the worker.
        // Need to run start in tc scope to have access to metadata()
        bifrost_svc.start().await?;

        let log_id = LogId::from(opts.log_id);
        debug!("Finding log tail");
        let tail = bifrost.find_tail(log_id, FindTailAttr::default()).await?;
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
