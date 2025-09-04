// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::cli_env::CliEnv;
use crate::clients::{AdminClient, AdminClientInterface};
use anyhow::{Context, Result};
use cling::prelude::*;
use restate_admin_rest_model::services::ModifyServiceRequest;
use restate_types::invocation::ServiceType;
use std::fs::File;
use std::io;
use tempfile::tempdir;

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_edit")]
pub struct Edit {
    /// Service name
    service: String,
}

pub async fn run_edit(State(env): State<CliEnv>, opts: &Edit) -> Result<()> {
    edit(&env, opts).await
}

async fn edit(env: &CliEnv, opts: &Edit) -> Result<()> {
    let admin_client = AdminClient::new(env).await?;
    let svc = admin_client
        .get_service(&opts.service)
        .await?
        .into_body()
        .await?;

    // Prepare file to edit
    let tempdir = tempdir().context("unable to create a temporary directory")?;
    let edit_file_path = tempdir.path().join(".restate_edit");
    write_out_edit_toml(
        &mut File::create(edit_file_path.clone()).context("Failed to create a temp file")?,
        svc.ty,
    )?;

    // Edit file
    env.open_default_editor(&edit_file_path)?;

    // Now load back file into string and parse it
    let modify_request: ModifyServiceRequest = toml::from_str(
        &std::fs::read_to_string(edit_file_path).context("Cannot read the edited config file")?,
    )
    .context("Cannot parse the edited config file")?;

    super::patch::apply_service_configuration_patch(&opts.service, admin_client, modify_request)
        .await
}

// TODO generate this file from the JsonSchema
fn write_out_edit_toml(w: &mut impl io::Write, service_type: ServiceType) -> Result<()> {
    writeln!(w, "# -- Service configuration changes\n")?;
    writeln!(
        w,
        "# NOTE: Service re-discovery will update the settings based on the service endpoint configuration.\n\n"
    )?;

    write_prefixed_lines(w, "# ", super::view::PUBLIC_DESCRIPTION)?;
    writeln!(w, "# Example:")?;
    writeln!(w, "# public = true")?;
    writeln!(w)?;

    write_prefixed_lines(
        w,
        "# ",
        super::patch::IDEMPOTENCY_RETENTION_EDIT_DESCRIPTION,
    )?;
    writeln!(w, "# Example:")?;
    writeln!(w, "# idempotency_retention = \"10hours\"")?;
    writeln!(w)?;

    if service_type == ServiceType::Workflow {
        write_prefixed_lines(w, "# ", super::patch::WORKFLOW_RETENTION_EDIT_DESCRIPTION)?;
        writeln!(w, "# Example:")?;
        writeln!(w, "# workflow_completion_retention = \"10days\"")?;
        writeln!(w)?;
    }

    write_prefixed_lines(w, "# ", super::patch::JOURNAL_RETENTION_EDIT_DESCRIPTION)?;
    writeln!(w, "# Example:")?;
    writeln!(w, "# journal_retention = \"2days\"")?;
    writeln!(w)?;

    write_prefixed_lines(w, "# ", super::patch::INACTIVITY_TIMEOUT_EDIT_DESCRIPTION)?;
    writeln!(w, "# Example:")?;
    writeln!(w, "# inactivity_timeout = \"1min\"")?;
    writeln!(w)?;

    write_prefixed_lines(w, "# ", super::patch::ABORT_TIMEOUT_EDIT_DESCRIPTION)?;
    writeln!(w, "# Example:")?;
    writeln!(w, "# abort_timeout = \"10min\"")?;
    writeln!(w)?;

    Ok(())
}

fn write_prefixed_lines(w: &mut impl io::Write, prefix: &str, lines: &str) -> io::Result<()> {
    for line in lines.lines() {
        writeln!(w, "{prefix}{line}")?;
    }
    Ok(())
}
