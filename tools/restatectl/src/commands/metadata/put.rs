// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::commands::metadata::patch::{patch_value, PatchValueOpts};
use crate::commands::metadata::MetadataCommonOpts;
use clap::Parser;
use cling::{Collect, Run};
use serde_json::Value;
use std::collections::HashMap;
use std::fs;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[clap()]
#[cling(run = "put_value")]
pub struct PutValueOpts {
    #[clap(flatten)]
    metadata: MetadataCommonOpts,

    /// The key to patch
    #[arg(short, long)]
    key: String,

    /// The JSON document to store
    #[arg(short, long)]
    doc: Option<String>,

    /// The local path to the JSON document to store
    #[arg(short, long)]
    path: Option<String>,

    /// Expected version for conditional update
    #[arg(short = 'e', long)]
    version: Option<u32>,

    /// Preview the change without applying it
    #[arg(short = 'n', long, default_value_t = false)]
    dry_run: bool,
}

async fn put_value(opts: &PutValueOpts) -> anyhow::Result<()> {
    let opts = opts.clone();

    let doc_body = if let Some(doc) = opts.doc {
        doc
    } else if let Some(path) = opts.path {
        fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("Unable to read the document: {}", e))?
    } else {
        anyhow::bail!("Please specify either doc or path");
    };

    let mut doc: Value = serde_json::from_str(&doc_body)
        .map_err(|e| anyhow::anyhow!("Parsing JSON value: {}", e))?;

    if let Some(obj) = doc.as_object_mut() {
        // make sure that the value does not contain the version field.
        obj.remove("version");
    }

    let mut patch_command = HashMap::<&'static str, Value>::new();
    patch_command.insert("op", "replace".into());
    patch_command.insert("path", "".into());
    patch_command.insert("value", doc);

    let patch = vec![patch_command];
    let patch_as_str = serde_json::to_string_pretty(&patch).map_err(|e| anyhow::anyhow!(e))?;

    let patch_opts = PatchValueOpts {
        metadata: opts.metadata,
        key: opts.key,
        patch: patch_as_str,
        version: opts.version,
        dry_run: opts.dry_run,
    };

    patch_value(&patch_opts).await
}
