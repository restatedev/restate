// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytestring::ByteString;
use clap::Parser;
use cling::{Collect, Run};
use tracing::debug;

use restate_rocksdb::RocksDbManager;
use restate_types::config::Configuration;
use restate_types::live::Live;

use crate::commands::metadata::{
    create_metadata_store_client, GenericMetadataValue, MetadataAccessMode, MetadataCommonOpts,
};
use crate::connection::ConnectionInfo;
use crate::environment::metadata_store;
use crate::environment::task_center::run_in_task_center;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[clap()]
#[cling(run = "get_value")]
pub struct GetValueOpts {
    #[clap(flatten)]
    metadata: MetadataCommonOpts,

    /// The key to get
    #[arg(short, long)]
    key: String,
}

async fn get_value(connection: &ConnectionInfo, opts: &GetValueOpts) -> anyhow::Result<()> {
    let value = match opts.metadata.access_mode {
        MetadataAccessMode::Remote => get_value_remote(connection, opts).await?,
        MetadataAccessMode::Direct => get_value_direct(opts).await?,
    };

    let value = serde_json::to_string_pretty(&value).map_err(|e| anyhow::anyhow!(e))?;
    println!("{value}");

    Ok(())
}

async fn get_value_remote(
    connection: &ConnectionInfo,
    opts: &GetValueOpts,
) -> anyhow::Result<Option<GenericMetadataValue>> {
    let metadata_store_client = create_metadata_store_client(connection, &opts.metadata).await?;

    metadata_store_client
        .get(ByteString::from(opts.key.as_str()))
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get value: {}", e))
}

async fn get_value_direct(opts: &GetValueOpts) -> anyhow::Result<Option<GenericMetadataValue>> {
    run_in_task_center(opts.metadata.config_file.as_ref(), |config| async move {
        let rocksdb_manager = RocksDbManager::init(Configuration::mapped_updateable(|c| &c.common));
        debug!("RocksDB Initialized");

        let metadata_store_client = metadata_store::start_metadata_server(
            config.common.metadata_store_client.clone(),
            &config.metadata_server,
            Live::from_value(config.metadata_server.clone())
                .map(|c| &c.rocksdb)
                .boxed(),
        )
        .await?;
        debug!("Metadata store client created");

        let value: Option<GenericMetadataValue> = metadata_store_client
            .get(ByteString::from(opts.key.as_str()))
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get value: {}", e))?;

        rocksdb_manager.shutdown().await;
        anyhow::Ok(value)
    })
    .await
}
