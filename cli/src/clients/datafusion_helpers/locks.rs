// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Active (locked) keys of keyed services, read from the vqueues tables.

use std::collections::BTreeMap;

use anyhow::Result;
use chrono::{DateTime, Local};
use serde::Deserialize;

use super::InvocationState;
use crate::clients::DataFusionHttpClient;

/// A key lock and the invocation holding it.
#[derive(Debug, Clone, Deserialize)]
pub struct LockedKey {
    #[serde(skip)]
    pub key: String,
    lock_name: String,
    pub scope: Option<String>,
    pub acquired_at: Option<DateTime<Local>>,
    /// The invocation (or other operation) holding the lock.
    pub acquired_by: Option<String>,
    /// Holder details, `None` if the holder is not an invocation.
    pub handler: Option<String>,
    pub status: Option<InvocationState>,
    pub modified_at: Option<DateTime<Local>>,
    pub last_start_at: Option<DateTime<Local>>,
    pub next_retry_at: Option<DateTime<Local>>,
    pub retry_count: Option<u64>,
    /// Entries waiting in the key's inbox behind the lock holder.
    pub num_queued: u64,
}

/// Locked keys grouped by service name, most queued first.
pub type LockedKeysMap = BTreeMap<String, Vec<LockedKey>>;

/// Columns the locked-keys query needs; absent on servers without vqueues.
const REQUIRED_COLUMNS: &[(&str, &[&str])] = &[
    (
        "sys_locks",
        &["lock_name", "scope", "acquired_at", "acquired_by"],
    ),
    ("sys_vqueue_meta", &["id", "scope", "lock_name"]),
    (
        "sys_vqueue_entry_status",
        &[
            "vqueue_id",
            "stage",
            "has_lock",
            "entry_kind",
            "entry_id",
            "status",
            "next_at",
            "retry_count_since_last_stored_command",
        ],
    ),
];

/// Returns the locked keys of the given services, or `None` if the server has no vqueues
/// tables to derive them from.
pub async fn get_locked_keys(
    client: &DataFusionHttpClient,
    services_filter: impl IntoIterator<Item = impl AsRef<str>>,
) -> Result<Option<LockedKeysMap>> {
    let services = services_filter
        .into_iter()
        .map(|x| format!("'{}'", x.as_ref()))
        .collect::<Vec<_>>();
    if services.is_empty() || !supports_locks(client).await? {
        return Ok(None);
    }

    let query = format!(
        "SELECT
            l.lock_name, l.scope, l.acquired_at, l.acquired_by,
            i.target_handler_name AS handler,
            CASE WHEN es.stage = 'inbox' AND es.status = 'backing-off' THEN 'backing-off'
                ELSE i.status END AS status,
            i.modified_at, i.last_start_at,
            es.next_at AS next_retry_at, es.retry_count_since_last_stored_command AS retry_count,
            COALESCE(q.num_queued, 0) AS num_queued
        FROM sys_locks l
        LEFT JOIN sys_invocation i ON i.id = l.acquired_by
        LEFT JOIN sys_vqueue_entry_status es
            ON es.entry_kind = 'invocation' AND es.entry_id = l.acquired_by
        LEFT JOIN (
            SELECT m.lock_name, m.scope, COUNT(1) AS num_queued
            FROM sys_vqueue_entry_status e JOIN sys_vqueue_meta m ON e.vqueue_id = m.id
            WHERE e.stage = 'inbox' AND NOT e.has_lock AND m.lock_name IS NOT NULL
            GROUP BY m.lock_name, m.scope
        ) q ON q.lock_name = l.lock_name AND q.scope IS NOT DISTINCT FROM l.scope
        WHERE split_part(l.lock_name, '/', 1) IN ({})
        ORDER BY num_queued DESC, l.acquired_at ASC",
        services.join(",")
    );

    let mut map = LockedKeysMap::new();
    for mut row in client.run_json_query::<LockedKey>(query).await? {
        let (service, key) = row
            .lock_name
            .split_once('/')
            .unwrap_or((&row.lock_name, ""));
        let service = service.to_owned();
        key.clone_into(&mut row.key);
        map.entry(service).or_default().push(row);
    }
    Ok(Some(map))
}

async fn supports_locks(client: &DataFusionHttpClient) -> Result<bool> {
    let filter = REQUIRED_COLUMNS
        .iter()
        .map(|(table, columns)| {
            let columns = columns.iter().map(|c| format!("'{c}'")).collect::<Vec<_>>();
            format!(
                "(table_name = '{table}' AND column_name IN ({}))",
                columns.join(",")
            )
        })
        .collect::<Vec<_>>()
        .join(" OR ");
    let expected: usize = REQUIRED_COLUMNS.iter().map(|(_, c)| c.len()).sum();
    let found = client
        .run_count_agg_query(format!(
            "SELECT COUNT(*) FROM information_schema.columns WHERE {filter}"
        ))
        .await?;
    Ok(found as usize == expected)
}
