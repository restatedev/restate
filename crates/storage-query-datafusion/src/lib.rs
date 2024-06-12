// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod analyzer;
pub mod context;
mod deployment;
mod idempotency;
mod inbox;
mod invocation_state;
mod invocation_status;
mod journal;
mod keyed_service_status;
mod partition_store_scanner;
mod physical_optimizer;
mod promise;
mod service;
mod state;
mod table_macro;
mod table_providers;
mod table_util;

pub use context::BuildError;

#[cfg(feature = "table_docs")]
pub fn table_docs(table: &str) -> Vec<(&str, &str, &str)> {
    match table {
        "sys_journal" => Vec::from(journal::schema::TABLE_DOCS),
        "state" => Vec::from(state::schema::TABLE_DOCS),
        "sys_keyed_service_status" => Vec::from(keyed_service_status::schema::TABLE_DOCS),
        "sys_inbox" => Vec::from(inbox::schema::TABLE_DOCS),
        "sys_deployment" => Vec::from(deployment::schema::TABLE_DOCS),
        "sys_service" => Vec::from(service::schema::TABLE_DOCS),
        "sys_idempotency" => Vec::from(idempotency::schema::TABLE_DOCS),
        "sys_promise" => Vec::from(promise::schema::TABLE_DOCS),
        "sys_invocation" => {
            // We need to compile this manually, due to the fact that it's a view.
            use std::collections::HashMap;
            let mut sys_invocation_state: HashMap<&'static str, (&'static str, &'static str)> =
                invocation_state::schema::TABLE_DOCS
                    .iter()
                    .map(|(name, ty, desc)| (*name, (*ty, *desc)))
                    .collect();
            let mut sys_invocation_status: HashMap<&'static str, (&'static str, &'static str)> =
                invocation_status::schema::TABLE_DOCS
                    .iter()
                    .map(|(name, ty, desc)| (*name, (*ty, *desc)))
                    .collect();

            fn copy_desc<'a>(
                map: &mut HashMap<&'a str, (&'a str, &'a str)>,
                name: &str,
            ) -> (&'a str, &'a str, &'a str) {
                let (name, (ty, desc)) = map.remove_entry(name).unwrap();
                (name, ty, desc)
            }

            vec![
                copy_desc(&mut sys_invocation_status, "id"),
                copy_desc(&mut sys_invocation_status, "target"),
                copy_desc(&mut sys_invocation_status, "target_service_name"),
                copy_desc(&mut sys_invocation_status, "target_service_key"),
                copy_desc(&mut sys_invocation_status, "target_handler_name"),
                copy_desc(&mut sys_invocation_status, "target_service_ty"),
                copy_desc(&mut sys_invocation_status, "invoked_by"),
                copy_desc(&mut sys_invocation_status, "invoked_by_service_name"),
                copy_desc(&mut sys_invocation_status, "invoked_by_id"),
                copy_desc(&mut sys_invocation_status, "invoked_by_target"),
                copy_desc(&mut sys_invocation_status, "pinned_deployment_id"),
                copy_desc(&mut sys_invocation_status, "trace_id"),
                copy_desc(&mut sys_invocation_status, "journal_size"),
                copy_desc(&mut sys_invocation_status, "created_at"),
                copy_desc(&mut sys_invocation_status, "modified_at"),

                copy_desc(&mut sys_invocation_state, "retry_count"),
                copy_desc(&mut sys_invocation_state, "last_start_at"),
                copy_desc(&mut sys_invocation_state, "next_retry_at"),
                copy_desc(&mut sys_invocation_state, "last_attempt_deployment_id"),
                copy_desc(&mut sys_invocation_state, "last_attempt_server"),
                copy_desc(&mut sys_invocation_state, "last_failure"),
                copy_desc(&mut sys_invocation_state, "last_failure_error_code"),
                copy_desc(&mut sys_invocation_state, "last_failure_related_entry_index"),
                copy_desc(&mut sys_invocation_state, "last_failure_related_entry_name"),
                copy_desc(&mut sys_invocation_state, "last_failure_related_entry_type"),

                ("status", "Utf8", "Either `pending` or `ready` or `running` or `backing-off` or `suspended` or `completed`.")
            ]
        }
        _ => panic!("Unknown table '{table}'"),
    }
}

#[cfg(test)]
pub(crate) mod mocks;

#[cfg(test)]
mod tests;
