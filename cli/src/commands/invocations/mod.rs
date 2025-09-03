// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod cancel;
mod describe;
mod kill;
mod list;
mod purge;
mod restart_as_new;
mod resume;

use cling::prelude::*;
use restate_types::identifiers::InvocationId;

#[derive(Run, Subcommand, Clone)]
pub enum Invocations {
    /// List invocations
    List(list::List),
    /// Prints detailed information about a given invocation
    Describe(describe::Describe),
    /// Cancel a given invocation, or a set of invocations, and its children
    Cancel(cancel::Cancel),
    /// Cancel a given invocation, or a set of invocations, and its children
    Kill(kill::Kill),
    /// Purge a completed invocation, or a set of invocations. This command affects only completed invocations.
    Purge(purge::Purge),
    /// Restart a completed invocation, or a set of invocations. This command affects only completed invocations. Note: this command doesn't work on workflows yet.
    RestartAsNew(restart_as_new::RestartAsNew),
    /// Resume an invocation, or a set of invocations.
    Resume(resume::Resume),
}

/// See [cancel::Cancel] for more details on query
fn create_query_filter(query: &str) -> String {
    let q = query.trim();
    if let Ok(id) = q.parse::<InvocationId>() {
        format!("id = '{id}'")
    } else {
        match q.find('/').unwrap_or_default() {
            0 => format!("target LIKE '{q}/%'"),
            // If there's one slash, let's add the wildcard depending on the service type,
            // so we discriminate correctly with serviceName/handlerName with workflowName/workflowKey
            1 => format!(
                "(target = '{q}' AND target_service_ty = 'service') OR (target LIKE '{q}/%' AND target_service_ty != 'service'))"
            ),
            // Can only be exact match here
            _ => format!("target LIKE '{q}'"),
        }
    }
}
