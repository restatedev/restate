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

use clap::Parser;
use cling::{Collect, Run};

use restate_cli_util::c_eprintln;
use restate_types::Version;

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

    /// Write the stored value bytes to stdout as-is, without decoding. Nothing is
    /// stripped: the output includes the storage encoding header. The value's
    /// version is printed to stderr.
    #[arg(long, default_value_t = false)]
    raw: bool,
}

async fn get_value(connection: &ConnectionInfo, opts: &GetValueOpts) -> anyhow::Result<()> {
    if opts.raw {
        return get_raw_value(connection, opts).await;
    }

    let value = super::get_value(connection, &opts.key).await?;

    let value = serde_json::to_string_pretty(&value).map_err(|e| anyhow::anyhow!(e))?;
    println!("{value}");

    Ok(())
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

    stdout.write_all(&value.bytes)?;
    stdout.flush()?;

    Ok(())
}
