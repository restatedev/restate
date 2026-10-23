// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io::Read;
use std::path::PathBuf;

use anyhow::{Context, Result};
use bytes::Bytes;
use cling::prelude::*;

use crate::cli_env::CliEnv;
use crate::ui::fmt::DryRun;

/// Resolve a named signal of an invocation, with an optional payload
///
/// The payload is sent as is: it must be in the format the handler awaiting the signal
/// expects, usually JSON. Signals sent to invocations that are completed or don't exist
/// are ignored.
#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_resolve")]
#[command(after_help = after_help!(
    examples: [
        "restate invocations signal resolve inv_1gdJBtdVEcM942bjcDmb1c1khoaJe11Hbz approved --payload '{\"ok\": true}' --yes",
        "restate invocations signal resolve inv_1gdJBtdVEcM942bjcDmb1c1khoaJe11Hbz approved --payload-file payload.json --dry-run",
    ],
    learn_more: "https://docs.restate.dev/services/invocation/managing-invocations",
))]
pub struct Resolve {
    /// Invocation id (`inv_...`)
    invocation_id: String,
    /// Name of the signal
    name: String,

    /// Payload of the signal
    #[clap(long, conflicts_with = "payload_file")]
    payload: Option<String>,
    /// File containing the payload of the signal, `-` to read it from stdin
    #[clap(long)]
    payload_file: Option<PathBuf>,

    #[clap(flatten)]
    dry_run: DryRun,
}

pub async fn run_resolve(State(env): State<CliEnv>, opts: &Resolve) -> Result<()> {
    let payload = match (&opts.payload, &opts.payload_file) {
        (Some(payload), _) => Bytes::from(payload.clone()),
        (None, Some(path)) if path.as_os_str() == "-" => {
            let mut buf = Vec::new();
            std::io::stdin()
                .read_to_end(&mut buf)
                .context("failed to read the payload from stdin")?;
            Bytes::from(buf)
        }
        (None, Some(path)) => Bytes::from(
            std::fs::read(path)
                .with_context(|| format!("failed to read the payload from {}", path.display()))?,
        ),
        (None, None) => Bytes::new(),
    };

    super::send_signal(
        &env,
        &opts.invocation_id,
        &opts.name,
        format!("resolve ({} bytes payload)", payload.len()),
        "application/octet-stream",
        payload,
        &opts.dry_run,
    )
    .await
}
