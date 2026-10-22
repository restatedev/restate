// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod reject;
mod resolve;

use anyhow::Result;
use bytes::Bytes;
use cling::prelude::*;
use serde_json::json;

use crate::cli_env::CliEnv;
use crate::clients::{AdminClient, AdminClientInterface};
use crate::commands::invocations::parse_invocation_id;
use crate::error::RestateCliError;
use crate::ui::fmt::{DryRun, Field, Formatter, IncludeFormatting, Outcome, OutputFormatter};

// Commands are documented on their own struct.
#[derive(Run, Subcommand, Clone)]
pub enum Signal {
    Resolve(resolve::Resolve),
    Reject(reject::Reject),
}

/// Plan, confirm and send the signal `name` to the invocation `invocation_id`.
/// `result` describes the signal result in the plan.
async fn send_signal(
    env: &CliEnv,
    invocation_id: &str,
    name: &str,
    result: String,
    content_type: &'static str,
    body: Bytes,
    dry_run: &DryRun,
) -> Result<()> {
    let invocation_id = parse_invocation_id(invocation_id)?.to_string();
    // `.` and `..` can't be sent as path segments: the URL normalizes them away.
    if matches!(name, "" | "." | "..") {
        return Err(
            RestateCliError::bad_input("the signal name must not be empty, `.` or `..`").into(),
        );
    }

    let client = AdminClient::new(env).await?;

    let mut f = Formatter::new();
    f.detail(
        "signal",
        [
            ("invocation_id", Field::new(invocation_id.as_str())),
            ("name", Field::new(name)),
            ("result", Field::new(result.as_str())),
        ],
    );
    // The plan, JSON-only: the human output already describes the signal to send.
    f.value(
        "changes",
        Field::json_only(json!([{
            "invocation_id": invocation_id,
            "signal": name,
            "result": result,
        }])),
    );
    f.confirm(
        dry_run,
        &format!("Send signal '{name}' to invocation {invocation_id}?"),
    )?;

    client
        .send_invocation_signal(&invocation_id, name, content_type, body)
        .await?
        .into_success_status()
        .await?;

    f.outcome(
        "result",
        Field::with_display(
            "sent",
            format!("Sent signal '{name}' to invocation {invocation_id}"),
        ),
        Outcome::Success,
    );
    f.next_step(
        &format!("restate invocations journal {invocation_id}"),
        "inspect the invocation's journal",
        IncludeFormatting::Yes,
    );
    f.finish()
}
