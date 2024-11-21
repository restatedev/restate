// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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

use cling::prelude::*;

use restate_core::metadata_store::MetadataStoreClient;
use restate_metadata_store::local::create_client;
use restate_types::config::MetadataStoreClientOptions;
use restate_types::net::AdvertisedAddress;
use restate_types::{flexbuffers_storage_encode_decode, Version, Versioned};

mod get;
mod patch;
mod put;

#[derive(Run, Subcommand, Clone)]
pub enum Metadata {
    /// Get a single key's value from the metadata store
    Get(get::GetValueOpts),
    /// Patch a value stored in the metadata store
    Patch(patch::PatchValueOpts),
    /// Replace a single key's value from the metastore
    Put(put::PutValueOpts),
}

#[derive(Args, Clone, Debug)]
#[clap()]
pub struct MetadataCommonOpts {
    /// Metadata store server address; for Etcd addresses use comma-separated list
    #[arg(
        short,
        long = "address",
        default_value = "http://127.0.0.1:5123",
        env = "RESTATE_METADATA_ADDRESS"
    )]
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
