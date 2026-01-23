// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use clap::Parser;
use clap_stdin::FileOrStdin;
use cling::{Collect, Run};
use serde_json::Value;

use crate::commands::metadata::MetadataCommonOpts;
use crate::commands::metadata::patch::{PatchValueOpts, patch_value};
use crate::connection::ConnectionInfo;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[clap()]
#[cling(run = "put_value")]
pub struct PutValueOpts {
    #[clap(flatten)]
    metadata: MetadataCommonOpts,

    /// The key to patch
    #[arg(short, long)]
    key: String,

    /// The JSON document to store, can be read from stdin or a file path
    doc: FileOrStdin,

    /// Expected version for conditional update
    #[arg(long)]
    version: Option<u32>,

    /// Preview the change without applying it
    #[arg(short = 'n', long, default_value_t = false)]
    dry_run: bool,
}

async fn put_value(connection: &ConnectionInfo, opts: &PutValueOpts) -> anyhow::Result<()> {
    let opts = opts.clone();

    let doc_body = opts.doc.contents()?;

    let mut doc: Value = serde_json::from_str(&doc_body)?;

    if let Some(obj) = doc.as_object_mut() {
        // make sure that the value does not contain the version field.
        obj.remove("version");
    }

    let mut patch_command = HashMap::<&'static str, Value>::new();
    patch_command.insert("op", "replace".into());
    patch_command.insert("path", "".into());
    patch_command.insert("value", doc);

    let patch = vec![patch_command];
    let patch_as_str = serde_json::to_string_pretty(&patch)?;

    let patch_opts = PatchValueOpts {
        metadata: opts.metadata,
        key: opts.key,
        patch: patch_as_str,
        version: opts.version,
        dry_run: opts.dry_run,
    };

    patch_value(connection, &patch_opts).await
}
