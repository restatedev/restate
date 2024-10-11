// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
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

use crate::commands::metadata::{GenericMetadataValue, MetadataCommonOpts};

use super::get_value;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[clap()]
#[cling(run = "show_value")]
pub struct GetValueOpts {
    #[clap(flatten)]
    pub(crate) metadata: MetadataCommonOpts,

    /// The key to get
    #[arg(short, long)]
    pub(crate) key: String,
}

pub(crate) async fn show_value(opts: &GetValueOpts) -> anyhow::Result<()> {
    let value: Option<GenericMetadataValue> = get_value(&opts.metadata, &opts.key).await?;

    serde_json::to_writer_pretty(std::io::stdout(), &value)?;
    Ok(())
}
