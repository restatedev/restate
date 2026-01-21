// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use clap::Parser;
use cling::{Collect, Run};

use crate::commands::metadata::MetadataCommonOpts;
use crate::connection::ConnectionInfo;

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
    let value = super::get_value(connection, &opts.key).await?;

    let value = serde_json::to_string_pretty(&value).map_err(|e| anyhow::anyhow!(e))?;
    println!("{value}");

    Ok(())
}
