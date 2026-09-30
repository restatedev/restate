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

use restate_types::identifiers::InvocationId;

use crate::error::RestateCliError;

const DEFAULT_BATCH_INVOCATIONS_OPERATION_LIMIT: usize = 500;
const DEFAULT_BATCH_INVOCATIONS_OPERATION_PRINT_LIMIT: usize =
    DEFAULT_BATCH_INVOCATIONS_OPERATION_LIMIT;

/// Help of the `<QUERY>` argument of the commands acting on a set of invocations.
const QUERY_HELP: &str =
    "Invocation id, or target: `Service`, `Service/handler`, `Object/key`, `Object/key/handler`";
const QUERY_LONG_HELP: &str = "\
Which invocations: an invocation id (`inv_...`), or all the invocations of a target:
  * `Service`: of any handler of a service, virtual object or workflow
  * `Service/handler`: of one handler of a service
  * `Object/key`: of one key of a virtual object or workflow
  * `Object/key/handler`: of one handler of a virtual object or workflow key
A two-part query is `Object/key` for virtual objects and workflows, else `Service/handler`.
Names and keys match exactly: `Cart/u1` doesn't match `Cart/u10`.";

// Commands are documented on their own struct.
#[derive(Run, Subcommand, Clone)]
pub enum Invocations {
    List(list::List),
    Describe(describe::Describe),
    Journal(journal::Journal),
    Cancel(cancel::Cancel),
    Kill(kill::Kill),
    Purge(purge::Purge),
    RestartAsNew(restart_as_new::RestartAsNew),
    Resume(resume::Resume),
    Pause(pause::Pause),
}

/// Validate an invocation id client-side, so a typo is bad input (exit 2) rather than
/// a server-side SQL error.
fn parse_invocation_id(id: &str) -> Result<InvocationId> {
    id.trim().parse().map_err(|err| {
        RestateCliError::bad_input(format!("invalid invocation id '{id}': {err}")).into()
    })
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
