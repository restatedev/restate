// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::Context;
use clap::Parser;
use cling::{Collect, Run};
use json_patch::Patch;
use serde_json::Value;
use tonic::Code;

use restate_core::protobuf::metadata_proxy_svc::{PutRequest, new_metadata_proxy_client};
use restate_types::Version;
use restate_types::metadata::Precondition;

use crate::commands::metadata::MetadataCommonOpts;
use crate::connection::{ConnectionInfo, NodeOperationError};

use super::GenericMetadataValue;

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
    #[arg(long)]
    pub version: Option<u32>,

    /// Preview the change without applying it
    #[arg(short = 'n', long, default_value_t = false)]
    pub dry_run: bool,
}

pub(crate) async fn patch_value(
    connection: &ConnectionInfo,
    opts: &PatchValueOpts,
) -> anyhow::Result<()> {
    let patch = serde_json::from_str(opts.patch.as_str())
        .map_err(|e| anyhow::anyhow!("Parsing JSON patch: {}", e))?;

    let value = patch_value_inner(connection, opts, patch).await?;

    let value = serde_json::to_string_pretty(&value).map_err(|e| anyhow::anyhow!(e))?;
    println!("{value}");

    Ok(())
}

async fn patch_value_inner(
    connection: &ConnectionInfo,
    opts: &PatchValueOpts,
    patch: Patch,
) -> anyhow::Result<GenericMetadataValue> {
    let value = super::get_value(connection, &opts.key).await?;

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
    json_patch::patch(&mut document, &patch).context("Patch failed")?;

    let new_value = GenericMetadataValue {
        version: current_version.next(),
        fields: serde_json::from_value(document.clone()).map_err(|e| anyhow::anyhow!(e))?,
    };

    let precondition = if current_version == Version::INVALID {
        Precondition::DoesNotExist
    } else {
        Precondition::MatchesVersion(current_version)
    };

    let request = PutRequest {
        key: opts.key.clone(),
        precondition: Some(precondition.into()),
        value: Some(new_value.clone().try_into()?),
    };

    connection
        .try_each(None, |channel| async {
            new_metadata_proxy_client(channel)
                .put(request.clone())
                .await
                .map_err(|err| {
                    if err.code() == Code::FailedPrecondition {
                        NodeOperationError::Terminal(err)
                    } else {
                        NodeOperationError::RetryElsewhere(err)
                    }
                })
        })
        .await?;

    Ok(new_value)
}
