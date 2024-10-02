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

use restate_core::metadata_store::Precondition;
use restate_rocksdb::RocksDbManager;
use restate_types::config::Configuration;
use restate_types::live::Live;
use restate_types::Version;

use crate::commands::metadata::GenericMetadataValue;
use crate::environment::metadata_store;
use crate::environment::task_center::run_in_task_center;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[clap()]
#[cling(run = "patch_value")]
pub struct PatchValueOpts {
    /// Set a configuration file to use for Restate.
    /// For more details, check the documentation.
    #[arg(
        short,
        long = "config-file",
        env = "RESTATE_CONFIG",
        value_name = "FILE"
    )]
    config_file: Option<PathBuf>,

    /// The key to patch
    #[arg(short, long)]
    key: String,

    /// The JSON patch to apply to value
    #[arg(short, long)]
    patch: String,

    /// Expected version for conditional update
    #[arg(short = 'e', long)]
    version: Option<u32>,

    /// Preview the change without applying it
    #[arg(short = 'n', long, default_value_t = false)]
    dry_run: bool,
}

async fn patch_value(opts: &PatchValueOpts) -> anyhow::Result<()> {
    let patch: json_patch::Patch = serde_json::from_str(opts.patch.as_str())
        .map_err(|e| anyhow::anyhow!("Parsing JSON patch: {}", e))?;

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

            let value: GenericMetadataValue = metadata_store_client
                .get(ByteString::from(opts.key.as_str()))
                .await?
                .ok_or(anyhow::anyhow!("Key not found: '{}'", opts.key))?;

            if let Some(expected_version) = opts.version {
                if value.version != Version::from(expected_version) {
                    anyhow::bail!(
                        "Version mismatch: expected v{}, got {:#} from store",
                        expected_version,
                        value.version
                    );
                }
            }

            let mut document = value.to_json_value();
            let result = match json_patch::patch(&mut document, &patch) {
                Ok(_) => {
                    let new_value = GenericMetadataValue {
                        version: value.version.next(),
                        data: serde_json::from_value(document.clone())
                            .map_err(|e| anyhow::anyhow!(e))?,
                    };

                    if !opts.dry_run {
                        debug!(
                            "Updating metadata key '{}' with expected {:?} version to {:?}",
                            opts.key, value.version, new_value.version
                        );

                        metadata_store_client
                            .put(
                                ByteString::from(opts.key.as_str()),
                                &new_value,
                                Precondition::MatchesVersion(value.version),
                            )
                            .await
                            .map_err(|e| anyhow::anyhow!("Store update failed: {}", e))?
                    } else {
                        println!("Dry run - updated value:\n");
                    }

                    Ok(new_value)
                }
                Err(e) => Err(anyhow::anyhow!("Patch failed: {}", e)),
            };

            rocksdb_manager.shutdown().await;
            result
        },
    )
    .await?;

    let value = serde_json::to_string_pretty(&value).map_err(|e| anyhow::anyhow!(e))?;
    println!("{}", value);

    Ok(())
}
