// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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
mod journal;
mod kill;
mod list;
mod pause;
mod purge;
mod restart_as_new;
mod resume;

use anyhow::Result;
use cling::prelude::*;

use restate_cli_util::exit;
use restate_types::identifiers::InvocationId;

const DEFAULT_BATCH_INVOCATIONS_OPERATION_LIMIT: usize = 500;
const DEFAULT_BATCH_INVOCATIONS_OPERATION_PRINT_LIMIT: usize =
    DEFAULT_BATCH_INVOCATIONS_OPERATION_LIMIT;

#[derive(Run, Subcommand, Clone)]
pub enum Invocations {
    /// List invocations
    List(list::List),
    /// Prints detailed information about a given invocation
    Describe(describe::Describe),
    /// Show an invocation's journal (metadata by default; add --payload for entry payloads)
    Journal(journal::Journal),
    /// Cancel a given invocation, or a set of invocations, and its children
    Cancel(cancel::Cancel),
    Kill(kill::Kill),
    /// Purge a completed invocation, or a set of invocations. This command affects only completed invocations.
    Purge(purge::Purge),
    /// Restart a completed invocation, or a set of invocations. This command affects only completed invocations. Note: this command doesn't work on workflows yet.
    RestartAsNew(restart_as_new::RestartAsNew),
    /// Resume an invocation, or a set of invocations.
    Resume(resume::Resume),
    /// Pause an invocation, or a set of invocations.
    Pause(pause::Pause),
}

/// Validate an invocation id client-side, so a typo is bad input (exit 2) rather than
/// a server-side SQL error.
fn parse_invocation_id(id: &str) -> Result<InvocationId> {
    id.trim()
        .parse()
        .map_err(|err| exit::BadInput(format!("invalid invocation id '{id}': {err}")).into())
}

/// See [cancel::Cancel] for more details on query
fn create_query_filter(query: &str) -> Result<String> {
    create_prefixed_query_filter(query, "")
}

/// [`create_query_filter`] with `prefix` (e.g. `inv.`) on the column names, for queries
/// joining other tables.
fn create_prefixed_query_filter(query: &str, prefix: &str) -> Result<String> {
    let q = query.trim();
    // Input with the invocation id prefix is taken as an id, and must be a valid one.
    if q.starts_with("inv_") {
        return Ok(format!("{prefix}id = '{}'", parse_invocation_id(q)?));
    }
    Ok(match q.matches('/').count() {
        0 => format!("{prefix}target LIKE '{q}/%'"),
        // If there's one slash, let's add the wildcard depending on the service type,
        // so we discriminate correctly with serviceName/handlerName with workflowName/workflowKey
        1 => format!(
            "(({prefix}target = '{q}' AND {prefix}target_service_ty = 'service') OR ({prefix}target LIKE '{q}/%' AND {prefix}target_service_ty != 'service'))"
        ),
        // Can only be exact match here
        _ => format!("{prefix}target LIKE '{q}'"),
    })
}

#[cfg(test)]
mod tests {
    use super::create_query_filter;

    #[test]
    fn creates_target_query_filters() {
        assert_eq!(
            create_query_filter("MyService").unwrap(),
            "target LIKE 'MyService/%'"
        );
        assert_eq!(
            create_query_filter("Chat/session456").unwrap(),
            "((target = 'Chat/session456' AND target_service_ty = 'service') OR (target LIKE 'Chat/session456/%' AND target_service_ty != 'service'))"
        );
        assert_eq!(
            create_query_filter("Chat/session456/send").unwrap(),
            "target LIKE 'Chat/session456/send'"
        );
    }

    #[test]
    fn rejects_malformed_invocation_ids() {
        assert!(
            create_query_filter("inv_bogus")
                .unwrap_err()
                .to_string()
                .starts_with("invalid invocation id 'inv_bogus'")
        );
    }
}
