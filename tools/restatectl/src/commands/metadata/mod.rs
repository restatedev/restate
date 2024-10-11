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
use std::str::FromStr;

use anyhow::Context;
use bytestring::ByteString;
use cling::prelude::*;

use restate_core::metadata_store::MetadataStoreClient;
use restate_metadata_store::local::create_client;
use restate_metadata_store::Precondition;
use restate_rocksdb::RocksDbManager;
use restate_types::config::{Configuration, MetadataStoreClientOptions};
use restate_types::live::Live;
use restate_types::net::AdvertisedAddress;
use restate_types::storage::{StorageDecode, StorageEncode};
use restate_types::{flexbuffers_storage_encode_decode, Version, Versioned};
use tracing::debug;

use crate::environment::metadata_store;
use crate::environment::task_center::run_in_task_center;

mod get;
mod logs;
mod patch;

#[derive(Run, Subcommand, Clone)]
pub enum Metadata {
    /// Get a single key's value from the metadata store
    Get(get::GetValueOpts),
    /// Patch a value stored in the metadata store
    Patch(patch::PatchValueOpts),
    /// Logs metadata manipulation
    #[clap(subcommand)]
    Logs(logs::Logs),
}

#[derive(Args, Clone, Debug)]
#[clap()]
pub struct MetadataCommonOpts {
    /// Metadata store server address; for Etcd addresses use comma-separated list
    #[arg(short, long = "address", default_value = "http://127.0.0.1:5123")]
    address: String,

    /// Metadata store access mode
    #[arg(long, default_value_t)]
    access_mode: MetadataAccessMode,

    /// Service type for access mode = "remote"
    #[arg(long, default_value_t)]
    remote_service_type: RemoteServiceType,

    /// Restate configuration file for access mode = "direct"
    #[arg(
        short,
        long = "config-file",
        env = "RESTATE_CONFIG",
        value_name = "FILE"
    )]
    config_file: Option<PathBuf>,
}

#[derive(clap::ValueEnum, Clone, Default, Debug, strum::Display)]
#[strum(serialize_all = "kebab-case")]
enum MetadataAccessMode {
    /// Connect to a remote metadata server at the specified address
    #[default]
    Remote,
    /// Open a local metadata store database directory directly
    Direct,
}

#[derive(clap::ValueEnum, Clone, Default, Debug, strum::Display)]
#[strum(serialize_all = "kebab-case")]
enum RemoteServiceType {
    #[default]
    Restate,
    Etcd,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GenericMetadataValue {
    // We assume that the concrete serialized type's encoded version field is called "version".
    version: Version,

    #[serde(flatten)]
    data: serde_json::Map<String, serde_json::Value>,
}

flexbuffers_storage_encode_decode!(GenericMetadataValue);

impl GenericMetadataValue {
    pub fn to_json_value(&self) -> serde_json::Value {
        serde_json::Value::Object(self.data.clone())
    }
}

impl Versioned for GenericMetadataValue {
    fn version(&self) -> Version {
        self.version
    }
}

pub async fn create_metadata_store_client(
    opts: &MetadataCommonOpts,
) -> anyhow::Result<MetadataStoreClient> {
    let client = match opts.remote_service_type {
        RemoteServiceType::Restate => restate_types::config::MetadataStoreClient::Embedded {
            address: AdvertisedAddress::from_str(opts.address.as_str())
                .map_err(|e| anyhow::anyhow!("Failed to parse address: {}", e))?,
        },
        RemoteServiceType::Etcd => restate_types::config::MetadataStoreClient::Etcd {
            addresses: opts
                .address
                .split(',')
                .map(|s| s.to_string())
                .collect::<Vec<String>>(),
        },
    };

    let metadata_store_client_options = MetadataStoreClientOptions {
        metadata_store_client: client,
        ..MetadataStoreClientOptions::default()
    };

    create_client(metadata_store_client_options)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create metadata store client: {}", e))
}

pub async fn get_value<K, T>(opts: &MetadataCommonOpts, key: K) -> anyhow::Result<Option<T>>
where
    K: AsRef<str>,
    T: Versioned + StorageDecode,
{
    let value = match opts.access_mode {
        MetadataAccessMode::Remote => get_value_remote(opts, key).await?,
        MetadataAccessMode::Direct => get_value_direct(opts, key).await?,
    };

    Ok(value)
}

async fn get_value_remote<K, T>(opts: &MetadataCommonOpts, key: K) -> anyhow::Result<Option<T>>
where
    K: AsRef<str>,
    T: Versioned + StorageDecode,
{
    let metadata_store_client = create_metadata_store_client(opts).await?;

    metadata_store_client
        .get(ByteString::from(key.as_ref()))
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get value: {}", e))
}

async fn get_value_direct<K, T>(opts: &MetadataCommonOpts, key: K) -> anyhow::Result<Option<T>>
where
    K: AsRef<str>,
    T: Versioned + StorageDecode,
{
    run_in_task_center(
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

            let value = metadata_store_client
                .get(ByteString::from(key.as_ref()))
                .await
                .map_err(|e| anyhow::anyhow!("Failed to get value: {}", e))?;

            rocksdb_manager.shutdown().await;
            anyhow::Ok(value)
        },
    )
    .await
}

pub async fn set_value<K, T>(
    opts: &MetadataCommonOpts,
    key: K,
    value: &T,
    version: Version,
) -> anyhow::Result<()>
where
    K: Into<ByteString>,
    T: Versioned + StorageEncode,
{
    match opts.access_mode {
        MetadataAccessMode::Remote => set_value_remote(opts, key, value, version).await,
        MetadataAccessMode::Direct => set_value_direct(opts, key, value, version).await,
    }
}

async fn set_value_remote<K, T>(
    opts: &MetadataCommonOpts,
    key: K,
    value: &T,
    version: Version,
) -> anyhow::Result<()>
where
    K: Into<ByteString>,
    T: Versioned + StorageEncode,
{
    let metadata_store_client = create_metadata_store_client(opts).await?;

    metadata_store_client
        .put(key.into(), value, Precondition::MatchesVersion(version))
        .await
        .context("Failed to set metadata key")
}

async fn set_value_direct<K, T>(
    opts: &MetadataCommonOpts,
    key: K,
    value: &T,
    version: Version,
) -> anyhow::Result<()>
where
    K: Into<ByteString>,
    T: Versioned + StorageEncode,
{
    run_in_task_center(
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

            metadata_store_client
                .put(key.into(), value, Precondition::MatchesVersion(version))
                .await
                .context("Failed to set metadata key")?;

            rocksdb_manager.shutdown().await;
            Ok(())
        },
    )
    .await
}
