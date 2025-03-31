// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod get;
mod patch;
mod put;

use std::path::PathBuf;

use cling::prelude::*;

use restate_core::protobuf::metadata_proxy_svc::{GetRequest, new_metadata_proxy_client};
use restate_types::protobuf::metadata::VersionedValue;
use restate_types::storage::StorageCodec;
use restate_types::{Version, Versioned, flexbuffers_storage_encode_decode};

use crate::connection::ConnectionInfo;

#[derive(Run, Subcommand, Clone)]
pub enum Metadata {
    /// Get a single key's value from the metadata store
    Get(get::GetValueOpts),
    /// Patch a value stored in the metadata store
    Patch(patch::PatchValueOpts),
    /// Replace a single key's value from the metadata store
    Put(put::PutValueOpts),
}

#[derive(Args, Clone, Debug)]
#[clap()]
pub struct MetadataCommonOpts {
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

#[derive(clap::ValueEnum, Clone, Default, Debug, strum::Display, PartialEq)]
#[strum(serialize_all = "kebab-case")]
enum RemoteServiceType {
    #[default]
    Restate,
    Etcd,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GenericMetadataValue {
    version: Version,

    #[serde(flatten)]
    fields: serde_json::Map<String, serde_json::Value>,
}

flexbuffers_storage_encode_decode!(GenericMetadataValue);

impl GenericMetadataValue {
    pub fn to_json_value(&self) -> serde_json::Value {
        serde_json::Value::Object(self.fields.clone())
    }
}

impl Versioned for GenericMetadataValue {
    fn version(&self) -> Version {
        self.version
    }
}

impl TryFrom<VersionedValue> for GenericMetadataValue {
    type Error = anyhow::Error;
    fn try_from(mut versioned_value: VersionedValue) -> Result<Self, Self::Error> {
        let version: Version = versioned_value
            .version
            .ok_or_else(|| anyhow::anyhow!("version is required"))?
            .into();

        let value: GenericMetadataValue = StorageCodec::decode(&mut versioned_value.bytes)?;
        if value.version != version {
            anyhow::bail!("returned payload and metadata object versions must align");
        }

        Ok(value)
    }
}

impl TryFrom<GenericMetadataValue> for VersionedValue {
    type Error = anyhow::Error;

    fn try_from(value: GenericMetadataValue) -> Result<Self, Self::Error> {
        let mut buf = bytes::BytesMut::new();
        StorageCodec::encode(&value, &mut buf)?;

        Ok(Self {
            version: Some(value.version.into()),
            bytes: buf.into(),
        })
    }
}

async fn get_value(
    connection: &ConnectionInfo,
    key: impl AsRef<str>,
) -> anyhow::Result<Option<GenericMetadataValue>> {
    let key = key.as_ref();
    let response = connection
        .try_each(None, |channel| async {
            new_metadata_proxy_client(channel)
                .get(GetRequest {
                    key: key.to_owned(),
                })
                .await
        })
        .await?;

    let response = response.into_inner();
    let Some(value) = response.value else {
        return Ok(None);
    };

    Ok(Some(value.try_into()?))
}
