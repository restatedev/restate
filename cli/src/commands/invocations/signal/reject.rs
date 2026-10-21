// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::Result;
use bytes::Bytes;
use cling::prelude::*;

use restate_admin_rest_model::invocations::{SIGNAL_FAILURE_CONTENT_TYPE, SignalFailure};

use crate::cli_env::CliEnv;
use crate::error::RestateCliError;
use crate::ui::fmt::DryRun;

/// Reject a named signal of an invocation with a failure
///
/// The handler awaiting the signal gets a terminal error with the given message and code.
/// Signals sent to invocations that are completed or don't exist are ignored.
#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_reject")]
#[command(after_help = after_help!(
    examples: [
        "restate invocations signal reject inv_1gdJBtdVEcM942bjcDmb1c1khoaJe11Hbz approved --message 'Request denied' --yes",
        "restate invocations signal reject inv_1gdJBtdVEcM942bjcDmb1c1khoaJe11Hbz approved --message 'Request denied' --code 403 --dry-run",
    ],
    learn_more: "https://docs.restate.dev/services/invocation/managing-invocations",
))]
pub struct Reject {
    /// Invocation id (`inv_...`)
    invocation_id: String,
    /// Name of the signal
    name: String,

    /// Failure message
    #[clap(long)]
    message: String,
    /// Failure code [default: 500]
    #[clap(long)]
    code: Option<u16>,

    #[clap(flatten)]
    dry_run: DryRun,
}

pub async fn run_reject(State(env): State<CliEnv>, opts: &Reject) -> Result<()> {
    if opts.message.is_empty() {
        return Err(RestateCliError::bad_input("the failure message must not be empty").into());
    }

    let failure = SignalFailure {
        message: opts.message.clone(),
        code: opts.code,
        metadata: Default::default(),
    };
    let body = Bytes::from(serde_json::to_vec(&failure)?);

    super::send_signal(
        &env,
        &opts.invocation_id,
        &opts.name,
        format!(
            "reject (code {}: {})",
            opts.code.unwrap_or(500),
            opts.message
        ),
        SIGNAL_FAILURE_CONTENT_TYPE,
        body,
        &opts.dry_run,
    )
    .await
}
