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

use bytestring::ByteString;
use clap::Parser;
use cling::{Collect, Run};
use tracing::debug;

use restate_rocksdb::RocksDbManager;
use restate_types::config::Configuration;
use restate_types::live::Live;

use crate::commands::metadata::GenericMetadataValue;
use crate::environment::metadata_store;
use crate::environment::task_center::run_in_task_center;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[clap()]
#[cling(run = "get_value")]
pub struct GetValueOpts {
    /// Restate configuration file
    #[arg(
        short,
        long = "config-file",
        env = "RESTATE_CONFIG",
        value_name = "FILE"
    )]
    config_file: Option<PathBuf>,

    /// The key to get
    #[arg(short, long)]
    key: String,
}

async fn get_value(opts: &GetValueOpts) -> anyhow::Result<()> {
    let value = run_in_task_center(
        opts.config_file.as_ref(),
        |config, task_center| async move {
            let rocksdb_manager =
                RocksDbManager::init(Configuration::mapped_updateable(|c| &c.common));
            debug!("RocksDB Initialized");

            let metadata_store_client = metadata_store::start_metadata_store(
                config.common.metadata_store_client.clone(),
                Live::from_value(config.metadata_store.clone()).boxed(),
                Live::from_value(config.metadata_store.clone())
                    .map(|c| &c.rocksdb)
                    .boxed(),
                &task_center,
            )
            .await?;
            debug!("Metadata store client created");

            let value: Option<GenericMetadataValue> = metadata_store_client
                .get(ByteString::from(opts.key.as_str()))
                .await
                .map_err(|e| anyhow::anyhow!("Failed to get value: {}", e))?;

            rocksdb_manager.shutdown().await;
            anyhow::Ok(value)
        },
    )
    .await?;

    let value = serde_json::to_string_pretty(&value).map_err(|e| anyhow::anyhow!(e))?;
    println!("{}", value);

    Ok(())
}
