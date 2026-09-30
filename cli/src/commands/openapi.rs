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
use cling::prelude::*;

use restate_cli_util::c_println;

use crate::cli_env::CliEnv;
use crate::clients::AdminClient;

/// Print the Admin API's OpenAPI spec (JSON), to discover and call the admin API directly.
///
/// Use it for operations the CLI doesn't cover, calling the admin API (e.g. with curl) at the admin URL shown by `restate whoami`.
#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run")]
#[command(after_help = after_help!(
    examples: [
        "restate openapi > admin-api.json",
        "restate openapi | jq '.paths | keys'",
    ],
))]
pub struct OpenApi {}

async fn run(State(env): State<CliEnv>) -> Result<()> {
    let client = AdminClient::new(&env).await?;
    // Always JSON, with or without `--json`: the spec is the output.
    let spec: serde_json::Value = client
        .run(reqwest::Method::GET, client.versioned_url(["openapi"]))
        .await?
        .into_body()
        .await?;
    c_println!("{}", serde_json::to_string_pretty(&spec)?);
    Ok(())
}
