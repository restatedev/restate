// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod describe;
mod list;
mod register;
mod remove;

use anyhow::Result;
use cling::prelude::*;

use restate_cli_util::exit;

use crate::clients::{AdminClient, AdminClientInterface, Deployment};
use crate::ui::deployments::render_deployment_url;

#[derive(Run, Subcommand, Clone)]
#[clap(visible_alias = "dp", alias = "deployment")]
// `Register`'s GCP/Lambda auth flags make it much larger than its sibling variants, but this is a
// short-lived CLI command struct constructed once per invocation, so the allocation churn a
// smaller enum would trade for isn't worth the indirection.
#[allow(clippy::large_enum_variant)]
pub enum Deployments {
    /// List the registered deployments
    List(list::List),
    /// Add or update deployments through deployment discovery
    Register(register::Register),
    /// Prints detailed information about a given deployment
    Describe(describe::Describe),
    /// Remove a drained deployment
    Remove(remove::Remove),
}

/// The deployment id for `input`: a deployment id as is, or the id of the deployment
/// registered at that endpoint URL / Lambda ARN.
async fn resolve_deployment_id(client: &AdminClient, input: &str) -> Result<String> {
    if input.starts_with("dp_") {
        return Ok(input.to_owned());
    }
    let normalize = |s: &str| {
        let s = s.trim().trim_end_matches('/');
        if s.contains("://") || s.starts_with("arn:") {
            s.to_owned()
        } else {
            format!("http://{s}")
        }
    };
    let wanted = normalize(input);
    let deployments = client
        .get_deployments()
        .await?
        .into_body()
        .await?
        .deployments;
    deployments
        .into_iter()
        .map(Deployment::from_deployment_response)
        .find(|(_, deployment, _)| normalize(&render_deployment_url(deployment)) == wanted)
        .map(|(id, _, _)| id.to_string())
        .ok_or_else(|| {
            exit::NotFound(format!(
                "No deployment is registered for '{input}'. Pass a deployment id (`dp_…`), or \
                 the URL / Lambda ARN a deployment was registered with"
            ))
            .into()
        })
}
