// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io::{IsTerminal, Write};

use anyhow::Context;
use bytes::Buf;
use clap::Parser;
use cling::{Collect, Run};

use restate_cli_util::c_eprintln;
use restate_types::epoch::EpochMetadata;
use restate_types::identifiers::PartitionId;
use restate_types::logs::metadata::Logs;
use restate_types::metadata_store::keys::{
    BIFROST_CONFIG_KEY, NODES_CONFIG_KEY, PARTITION_PROCESSOR_EPOCH_PREFIX, PARTITION_TABLE_KEY,
    SCHEMA_INFORMATION_KEY,
};
use restate_types::nodes_config::NodesConfiguration;
use restate_types::partition_table::PartitionTable;
use restate_types::protobuf::metadata::VersionedValue;
use restate_types::schema::SchemaUnindexed;
use restate_types::storage::{self, StorageCodec, StorageCodecKind, StorageDecode};
use restate_types::{Version, Versioned};

use crate::commands::metadata::MetadataCommonOpts;
use crate::connection::ConnectionInfo;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[clap()]
#[cling(run = "get_value")]
pub struct GetValueOpts {
    #[clap(flatten)]
    metadata: MetadataCommonOpts,

    /// The key to get. Without `--raw`, only well-known keys are supported:
    /// nodes_config, bifrost_config, partition_table, schema_registry and
    /// pp_epoch_<partition-id>. Use `--raw` to fetch any other key.
    #[arg(short, long)]
    key: String,

    /// Write the stored value bytes to stdout as-is, without decoding. Works with
    /// any key, including ones that cannot be decoded otherwise. Nothing is
    /// stripped: the output includes the storage encoding header. The value's
    /// version is printed to stderr.
    #[arg(long, default_value_t = false)]
    raw: bool,
}

/// Metadata keys whose values can be decoded into their typed representation.
enum KnownKey {
    NodesConfig,
    Logs,
    PartitionTable,
    Schema,
    Epoch,
}

impl KnownKey {
    fn parse(key: &str) -> Option<Self> {
        match key {
            k if k == &*NODES_CONFIG_KEY => Some(Self::NodesConfig),
            k if k == &*BIFROST_CONFIG_KEY => Some(Self::Logs),
            k if k == &*PARTITION_TABLE_KEY => Some(Self::PartitionTable),
            k if k == &*SCHEMA_INFORMATION_KEY => Some(Self::Schema),
            k => k
                .strip_prefix(PARTITION_PROCESSOR_EPOCH_PREFIX)
                .and_then(|id| id.strip_prefix('_'))
                .and_then(|id| id.parse::<PartitionId>().ok())
                .map(|_| Self::Epoch),
        }
    }
}

async fn get_value(connection: &ConnectionInfo, opts: &GetValueOpts) -> anyhow::Result<()> {
    if opts.raw {
        return get_raw_value(connection, opts).await;
    }

    let known_key = KnownKey::parse(&opts.key);

    let Some(value) = super::get_raw_value(connection, &opts.key).await? else {
        anyhow::bail!("key '{}' does not exist", opts.key);
    };

    let value = match known_key {
        None => decode_generic_json(value)?,
        Some(known_key) => match known_key {
            KnownKey::NodesConfig => decode_json::<NodesConfiguration>(value)?,
            KnownKey::Logs => decode_json::<Logs>(value)?,
            KnownKey::PartitionTable => decode_json::<PartitionTable>(value)?,
            KnownKey::Schema => decode_json::<SchemaUnindexed>(value)?,
            KnownKey::Epoch => decode_json::<EpochMetadata>(value)?,
        },
    };

    println!("{}", serde_json::to_string_pretty(&value)?);

    Ok(())
}

fn decode_json<T: StorageDecode + Versioned + serde::Serialize>(
    mut value: VersionedValue,
) -> anyhow::Result<serde_json::Value> {
    let version: Version = value
        .version
        .ok_or_else(|| anyhow::anyhow!("version is required"))?
        .into();

    let decoded: T = StorageCodec::decode(&mut value.bytes)?;
    if decoded.version() != version {
        anyhow::bail!("returned payload and metadata object versions must align");
    }

    Ok(serde_json::to_value(&decoded)?)
}

fn decode_generic_json(mut value: VersionedValue) -> anyhow::Result<serde_json::Value> {
    let version: Version = value
        .version
        .ok_or_else(|| anyhow::anyhow!("version is required"))?
        .into();

    anyhow::ensure!(
        value.bytes.has_remaining(),
        "value is empty; there is nothing to decode"
    );
    let header = value.bytes.get_u8();
    let codec = StorageCodecKind::try_from(header).with_context(|| {
        format!(
            "unrecognized storage codec header byte {header:#04x}; the value was likely not written \
            with the storage codec and cannot be decoded. Use --raw to fetch the bytes as-is"
        )
    })?;
    let decoded: serde_json::Value = storage::decode::decode_serde(value.bytes, codec).with_context(|| {
        format!("value is stored with the {codec} codec, which cannot be decoded without knowing its type; use --raw")
    })?;

    if let Some(v) = decoded.get("version").and_then(|v| v.as_u64())
        && Version::from(v as u32) != version
    {
        anyhow::bail!("returned payload and metadata object versions must align");
    }

    Ok(decoded)
}

async fn get_raw_value(connection: &ConnectionInfo, opts: &GetValueOpts) -> anyhow::Result<()> {
    let Some(value) = super::get_raw_value(connection, &opts.key).await? else {
        anyhow::bail!("key '{}' does not exist", opts.key);
    };

    let version: Version = value
        .version
        .ok_or_else(|| anyhow::anyhow!("version is required"))?
        .into();

    let mut stdout = std::io::stdout().lock();
    if stdout.is_terminal() {
        anyhow::bail!(
            "Refusing to write binary data to a terminal. Redirect stdout to a file or pipe"
        );
    }

    c_eprintln!("version: {version}");

    success_on_broken_pipe(stdout.write_all(&value.bytes))?;
    success_on_broken_pipe(stdout.flush())?;

    Ok(())
}

// Handles broken pipe gracefull so commands like this `get ... --raw | head -c 0)` should
// succeed
fn success_on_broken_pipe(result: Result<(), std::io::Error>) -> Result<(), std::io::Error> {
    match result {
        Ok(_) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::BrokenPipe => Ok(()),
        Err(err) => Err(err),
    }
}
