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
use json_patch::Patch;
use restate_types::metadata::Precondition;
use serde_json::Value;
use tracing::debug;

use restate_core::metadata_store::MetadataStoreClient;
use restate_rocksdb::RocksDbManager;
use restate_types::config::Configuration;
use restate_types::live::Live;
use restate_types::Version;

use crate::commands::metadata::{
    create_metadata_store_client, GenericMetadataValue, MetadataAccessMode, MetadataCommonOpts,
};
use crate::environment::metadata_store::start_metadata_store;
use crate::environment::task_center::run_in_task_center;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[clap()]
#[cling(run = "patch_value")]
pub struct PatchValueOpts {
    #[clap(flatten)]
    pub metadata: MetadataCommonOpts,

    /// The key to patch
    #[arg(short, long)]
    pub key: String,

    /// The JSON document to put
    #[arg(short, long)]
    pub patch: String,

    /// Expected version for conditional update
    #[arg(short = 'e', long)]
    pub version: Option<u32>,

    /// Preview the change without applying it
    #[arg(short = 'n', long, default_value_t = false)]
    pub dry_run: bool,
}

pub(crate) async fn patch_value(opts: &PatchValueOpts) -> anyhow::Result<()> {
    let patch = serde_json::from_str(opts.patch.as_str())
        .map_err(|e| anyhow::anyhow!("Parsing JSON patch: {}", e))?;

    let value = match opts.metadata.access_mode {
        MetadataAccessMode::Remote => patch_value_remote(opts, patch).await?,
        MetadataAccessMode::Direct => patch_value_direct(opts, patch).await?,
    };

    let value = serde_json::to_string_pretty(&value).map_err(|e| anyhow::anyhow!(e))?;
    println!("{value}");

    Ok(())
}

async fn patch_value_remote(
    opts: &PatchValueOpts,
    patch: Patch,
) -> anyhow::Result<Option<GenericMetadataValue>> {
    let metadata_store_client = create_metadata_store_client(&opts.metadata).await?;
    Ok(Some(
        patch_value_inner(opts, &patch, &metadata_store_client).await?,
    ))
}

async fn patch_value_direct(
    opts: &PatchValueOpts,
    patch: Patch,
) -> anyhow::Result<Option<GenericMetadataValue>> {
    let value = run_in_task_center(opts.metadata.config_file.as_ref(), |config| async move {
        let rocksdb_manager = RocksDbManager::init(Configuration::mapped_updateable(|c| &c.common));
        debug!("RocksDB Initialized");

        let metadata_store_client = start_metadata_store(
            config.common.metadata_store_client.clone(),
            &config.metadata_store,
            Live::from_value(config.metadata_store.clone())
                .map(|c| &c.rocksdb)
                .boxed(),
        )
        .await?;
        debug!("Metadata store client created");

        let result = patch_value_inner(opts, &patch, &metadata_store_client).await;

        rocksdb_manager.shutdown().await;
        result
    })
    .await?;

    Ok(Some(value))
}

async fn patch_value_inner(
    opts: &PatchValueOpts,
    patch: &Patch,
    metadata_store_client: &MetadataStoreClient,
) -> anyhow::Result<GenericMetadataValue> {
    let value: Option<GenericMetadataValue> = metadata_store_client
        .get(ByteString::from(opts.key.as_str()))
        .await?;

    let current_version = value
        .as_ref()
        .map(|v| v.version)
        .unwrap_or(Version::INVALID);

    if let Some(expected_version) = opts.version {
        if current_version != Version::from(expected_version) {
            anyhow::bail!(
                "Version mismatch: expected v{}, got {:#} from store",
                expected_version,
                current_version,
            );
        }
    }

    let mut document = value.map(|v| v.to_json_value()).unwrap_or(Value::Null);
    let value = match json_patch::patch(&mut document, patch) {
        Ok(_) => {
            let new_value = GenericMetadataValue {
                version: current_version.next(),
                data: serde_json::from_value(document.clone()).map_err(|e| anyhow::anyhow!(e))?,
            };
            if !opts.dry_run {
                debug!(
                    "Updating metadata key '{}' with expected {:?} version to {:?}",
                    opts.key, current_version, new_value.version
                );
                metadata_store_client
                    .put(
                        ByteString::from(opts.key.as_str()),
                        &new_value,
                        if current_version == Version::INVALID {
                            Precondition::DoesNotExist
                        } else {
                            Precondition::MatchesVersion(current_version)
                        },
                    )
                    .await
                    .map_err(|e| anyhow::anyhow!("Store update failed: {}", e))?
            } else {
                println!("Dry run - updated value:\n");
            }
            Ok(new_value)
        }
        Err(e) => Err(anyhow::anyhow!("Patch failed: {}", e)),
    }?;
    Ok(value)
}
