// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A set of common queries needed by the CLI

use std::collections::HashMap;
use std::fmt::Display;

use anyhow::Result;
use bytes::Bytes;
use chrono::{DateTime, Local};
use restate_types::SemanticRestateVersion;
use serde::Deserialize;
use serde_with::serde_as;

use restate_types::identifiers::DeploymentId;
use restate_types::identifiers::ServiceId;
use restate_types::journal_events::Event;

use super::{
    HandlerStateStats, Invocation, InvocationCompletion, InvocationState, JournalEntryRow,
    JournalEventRow, JournalFetch, ServiceHandlerUsage, ServiceStatusMap, SimpleInvocation,
    event_failure,
};

use crate::clients::DataFusionHttpClient;

pub async fn find_active_invocations_simple(
    client: &DataFusionHttpClient,
    filter: &str,
) -> Result<Vec<SimpleInvocation>> {
    let query = format!("SELECT id, target, status FROM sys_invocation_status WHERE {filter}");
    let mut invocations = client.run_json_query::<SimpleInvocation>(query).await?;

    // Refine the raw `invoked` into the status shown elsewhere (`ready`, `running`,
    // `backing-off`, ...): one point-read on `sys_invocation` plus the VQueue overlay.
    let invoked: Vec<&str> = invocations
        .iter()
        .filter(|inv| inv.status == "invoked")
        .map(|inv| inv.id.as_str())
        .collect();
    if invoked.is_empty() {
        return Ok(invocations);
    }
    #[derive(Deserialize)]
    struct StatusRow {
        id: String,
        status: String,
    }
    let query = format!(
        "SELECT id, status FROM sys_invocation WHERE id IN ({})",
        sql_id_list(invoked.iter().copied())
    );
    let mut statuses: HashMap<String, String> = client
        .run_json_query::<StatusRow>(query)
        .await?
        .into_iter()
        .map(|row| (row.id, row.status))
        .collect();
    for (id, row) in vqueue_entry_statuses(client, invoked.into_iter()).await? {
        if row.is_backing_off() {
            statuses.insert(id, InvocationState::BackingOff.to_string());
        }
    }
    for inv in &mut invocations {
        if let Some(status) = statuses.remove(&inv.id) {
            inv.status = status;
        }
    }
    Ok(invocations)
}

pub async fn count_deployment_active_inv(
    client: &DataFusionHttpClient,
    deployment_id: &DeploymentId,
) -> Result<i64> {
    Ok(client
        .run_count_agg_query(format!(
            "SELECT COUNT(1) AS inv_count
            FROM sys_invocation_status
            WHERE pinned_deployment_id = '{deployment_id}' AND status != 'completed'"
        ))
        .await?)
}

pub async fn count_deployment_active_inv_by_method(
    client: &DataFusionHttpClient,
    deployment_id: &DeploymentId,
) -> Result<Vec<ServiceHandlerUsage>> {
    let query = format!(
        "SELECT
            target_service_name as service,
            target_handler_name as handler,
            COUNT(1) AS inv_count
            FROM sys_invocation_status
            WHERE pinned_deployment_id = '{deployment_id}' AND status != 'completed'
            GROUP BY pinned_deployment_id, target_service_name, target_handler_name"
    );

    Ok(client.run_json_query::<ServiceHandlerUsage>(query).await?)
}

#[derive(Deserialize)]
struct ServiceStatusQueryResult {
    target_service_name: String,
    target_handler_name: String,
    status: InvocationState,
    #[serde(flatten)]
    stats: HandlerStateStats,
}

pub async fn get_service_status(
    client: &DataFusionHttpClient,
    services_filter: impl IntoIterator<Item = impl AsRef<str>>,
) -> Result<ServiceStatusMap> {
    let mut status_map = ServiceStatusMap::default();

    let query_filter = format!(
        "({})",
        services_filter
            .into_iter()
            .map(|x| format!("'{}'", x.as_ref()))
            .collect::<Vec<_>>()
            .join(",")
    );
    // `sys_invocation` already reports inboxed invocations as `pending`.
    let query = format!(
        "SELECT
            target_service_name,
            target_handler_name,
            status,
            COUNT(1) as num_invocations,
            MIN(created_at) as oldest_at,
            FIRST_VALUE(id ORDER BY created_at ASC) as oldest_invocation
        FROM sys_invocation
        WHERE status != 'completed' AND target_service_name IN {query_filter}
        GROUP BY target_service_name, target_handler_name, status
        ORDER BY target_handler_name"
    );
    let rows = client
        .run_json_query::<ServiceStatusQueryResult>(query)
        .await?;
    for row in rows {
        status_map.set_handler_stats(
            &row.target_service_name,
            &row.target_handler_name,
            row.status,
            row.stats,
        );
    }

    Ok(status_map)
}

#[derive(Deserialize)]
struct InvocationQueryResult {
    last_start_at: Option<DateTime<Local>>,
    completion_result: Option<String>,
    completion_failure: Option<String>,
    #[serde(flatten)]
    invocation: Invocation,
}

impl From<InvocationQueryResult> for Invocation {
    fn from(value: InvocationQueryResult) -> Self {
        // Running duration
        let current_attempt_duration = if value.invocation.status == InvocationState::Running {
            value
                .last_start_at
                .map(|last_start| Local::now().signed_duration_since(last_start))
        } else {
            None
        };

        let last_attempt_started_at = if value.invocation.status == InvocationState::BackingOff {
            value.last_start_at
        } else {
            None
        };

        Invocation {
            current_attempt_duration,
            last_attempt_started_at,
            completion: InvocationCompletion::from_sql(
                value.completion_result,
                value.completion_failure,
            ),
            ..value.invocation
        }
    }
}

// we don't want to scan the table indefinitely to get a count, so we stop after this many rows are checked
const COUNT_LIMIT: usize = 50000;

pub enum CountEstimate {
    Exact(usize),
    LowerBound(usize),
}

impl CountEstimate {
    fn from_rows(received_less_than_limit: bool, rows: usize, minimum_count: usize) -> Self {
        if received_less_than_limit {
            // if we receive less rows than we asked for, its the full set
            Self::Exact(rows)
        } else if rows > minimum_count {
            // if we receive row_limit rows, and its more than our count estimate, then the rows must be very sparse.
            // our best guess for a lower bound has to be the number of rows we received
            Self::LowerBound(rows)
        } else {
            // otherwise, the count in the first 50k invocations is a pretty good lower bound
            Self::LowerBound(minimum_count)
        }
    }
}

impl Display for CountEstimate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CountEstimate::Exact(count) => count.fmt(f),
            CountEstimate::LowerBound(count) => write!(f, "{count}+"),
        }
    }
}

pub async fn find_and_count_active_invocations(
    client: &DataFusionHttpClient,
    filter: &str,
    order: &str,
    limit: usize,
) -> Result<(Vec<Invocation>, CountEstimate)> {
    let count_fut = count_active_invocations_lower_bound(client, filter);
    let inv_fut = find_active_invocations(client, filter, order, limit);

    let (count_lower_bound, inv) = tokio::join!(count_fut, inv_fut);
    let count_lower_bound = count_lower_bound?;
    let (invocations, received_less_than_limit) = inv?;

    let count_estimate = CountEstimate::from_rows(
        received_less_than_limit,
        invocations.len(),
        count_lower_bound as usize,
    );

    Ok((invocations, count_estimate))
}

pub async fn count_active_invocations_lower_bound(
    client: &DataFusionHttpClient,
    filter: &str,
) -> Result<i64> {
    // how does this work?
    // 1. we take the first N unfiltered rows of the table. in practice this will probably be weighted
    // towards local partitions, but for sufficiently large N we should get a good sample of invocations
    // 2. we apply a filter to those <=N rows
    // 3. we count how many rows matched that filter
    // this ensures that we never scan more than N rows, even if the filter is very strict.
    // we return a lower bound that is always <=N, but we will only use it if its more than limit on the main query.
    let count_query = format!(
        "SELECT COUNT(1) FROM (SELECT
            *
        FROM sys_invocation inv
        LIMIT {COUNT_LIMIT}
        )
        {filter}"
    );

    Ok(client.run_count_agg_query(count_query).await?)
}

pub async fn find_active_invocations(
    client: &DataFusionHttpClient,
    filter: &str,
    order: &str,
    limit: usize,
) -> Result<(Vec<Invocation>, bool)> {
    let (mut invocations, received_less_than_limit) = if client
        .server_version()
        // any 1.5.x including prereleases
        .is_equal_or_newer_than(&SemanticRestateVersion::new(1, 4, u64::MAX))
    {
        find_active_invocations_post_1_5(client, filter, order, limit).await?
    } else {
        find_active_invocations_pre_1_5(client, filter, order, limit).await?
    };
    apply_vqueue_overlay(client, &mut invocations).await?;
    apply_last_failure_events(client, &mut invocations).await?;
    Ok((invocations, received_less_than_limit))
}

/// Quoted, comma-separated SQL list of `ids`, for `IN (...)`.
fn sql_id_list<'a>(ids: impl IntoIterator<Item = &'a str>) -> String {
    ids.into_iter()
        .map(|id| format!("'{id}'"))
        .collect::<Vec<_>>()
        .join(",")
}

/// Ids of invocations backing off in their VQueue (Restate 1.7+), for status filters.
const VQUEUE_BACKING_OFF_IDS: &str =
    "SELECT entry_id FROM sys_vqueues WHERE stage = 'inbox' AND status = 'backing-off'";

/// SQL predicate on `sys_invocation inv` matching `statuses`.
///
/// Mirrors the web UI (restate-web-ui `convertFilters.ts` `vqueueStatusClause`): on
/// VQueue-backed servers `sys_invocation` reports a backing-off invocation as `ready`,
/// so `backing-off` and `ready` are refined through `sys_vqueues`.
pub async fn invocation_status_filter(
    client: &DataFusionHttpClient,
    statuses: &[InvocationState],
) -> Result<String> {
    let refine = (statuses.contains(&InvocationState::BackingOff)
        != statuses.contains(&InvocationState::Ready))
        && client
            .check_columns_exists("sys_vqueues", &["entry_id", "stage", "status"])
            .await?;
    let clauses: Vec<String> = statuses
        .iter()
        .map(|status| match status {
            InvocationState::BackingOff if refine => format!(
                "(inv.status = 'backing-off' OR (inv.status = 'ready' AND inv.id IN ({VQUEUE_BACKING_OFF_IDS})))"
            ),
            InvocationState::Ready if refine => {
                format!("(inv.status = 'ready' AND inv.id NOT IN ({VQUEUE_BACKING_OFF_IDS}))")
            }
            status => format!("inv.status = '{status}'"),
        })
        .collect();
    Ok(format!("({})", clauses.join(" OR ")))
}

#[derive(Deserialize)]
struct VqueueEntryStatusRow {
    entry_id: String,
    stage: String,
    status: String,
    next_at: Option<DateTime<Local>>,
    latest_attempt_at: Option<DateTime<Local>>,
    retry_count_since_last_stored_command: Option<u64>,
}

impl VqueueEntryStatusRow {
    fn is_backing_off(&self) -> bool {
        self.stage == "inbox" && self.status == "backing-off"
    }
}

/// Point-read the live VQueue entry status (Restate 1.7+) of `ids`, keyed by id. Empty
/// on servers without VQueues.
async fn vqueue_entry_statuses<'a>(
    client: &DataFusionHttpClient,
    ids: impl ExactSizeIterator<Item = &'a str>,
) -> Result<HashMap<String, VqueueEntryStatusRow>> {
    if ids.len() == 0
        || !client
            .check_columns_exists(
                "sys_vqueue_entry_status",
                &[
                    "entry_id",
                    "entry_kind",
                    "stage",
                    "status",
                    "next_at",
                    "latest_attempt_at",
                    "retry_count_since_last_stored_command",
                ],
            )
            .await?
    {
        return Ok(HashMap::new());
    }
    let query = format!(
        "SELECT entry_id, stage, status, next_at, latest_attempt_at, retry_count_since_last_stored_command
         FROM sys_vqueue_entry_status
         WHERE entry_kind = 'invocation' AND entry_id IN ({})",
        sql_id_list(ids)
    );
    Ok(client
        .run_json_query::<VqueueEntryStatusRow>(query)
        .await?
        .into_iter()
        .map(|row| (row.entry_id.clone(), row))
        .collect())
}

/// Overlay the live VQueue entry status, like the web UI does (restate-web-ui
/// `convertInvocation.ts` `applyVqueueOverlay`): on VQueue-backed servers
/// `sys_invocation` shows a retrying invocation as `ready` and leaves the retry columns
/// null. This is the only source of retry count / next retry. One point-read for the
/// whole page.
async fn apply_vqueue_overlay(
    client: &DataFusionHttpClient,
    invocations: &mut [Invocation],
) -> Result<()> {
    let rows = vqueue_entry_statuses(client, invocations.iter().map(|inv| inv.id.as_str())).await?;
    for inv in invocations.iter_mut() {
        let Some(row) = rows.get(&inv.id) else {
            continue;
        };
        if row.is_backing_off() {
            inv.status = InvocationState::BackingOff;
            inv.next_retry_at = row.next_at;
            inv.last_attempt_started_at = row.latest_attempt_at.or(inv.last_attempt_started_at);
        }
        inv.num_retries = row.retry_count_since_last_stored_command;
    }
    Ok(())
}

#[derive(Deserialize)]
struct LastFailureEventRow {
    id: String,
    event_json: Option<String>,
}

/// Fill the last failure of backing-off and paused invocations from their latest
/// `TransientError` / `Paused` journal event (the only source of failures), like the web
/// UI (restate-web-ui `convertInvocation.ts` `applyTransientError`, `getPausedError.ts`).
/// One aggregate query for the whole page.
async fn apply_last_failure_events(
    client: &DataFusionHttpClient,
    invocations: &mut [Invocation],
) -> Result<()> {
    let ids: Vec<&str> = invocations
        .iter()
        .filter(|inv| {
            matches!(
                inv.status,
                InvocationState::BackingOff | InvocationState::Paused
            )
        })
        .map(|inv| inv.id.as_str())
        .collect();
    if ids.is_empty()
        || !client
            .check_columns_exists(
                "sys_journal_events",
                &["id", "appended_at", "event_type", "event_json"],
            )
            .await?
    {
        return Ok(());
    }
    let query = format!(
        "SELECT id, LAST_VALUE(event_json ORDER BY appended_at) AS event_json
         FROM sys_journal_events
         WHERE id IN ({}) AND event_type IN ('TransientError', 'Paused')
         GROUP BY id",
        sql_id_list(ids)
    );
    let events: HashMap<String, Event> = client
        .run_json_query::<LastFailureEventRow>(query)
        .await?
        .into_iter()
        .filter_map(|row| Some((row.id, serde_json::from_str(&row.event_json?).ok()?)))
        .collect();

    for inv in invocations.iter_mut() {
        let Some(failure) = events.get(&inv.id).and_then(event_failure) else {
            continue;
        };
        inv.last_failure_message = Some(format!(
            "[{}] {}",
            u16::from(failure.error_code),
            failure.error_message
        ));
        inv.last_failure_entry_name = failure.related_command_name.clone();
        inv.last_failure_entry_ty = failure.related_command_type.map(|ty| ty.to_string());
    }
    Ok(())
}

async fn find_active_invocations_pre_1_5(
    client: &DataFusionHttpClient,
    filter: &str,
    order: &str,
    limit: usize,
) -> Result<(Vec<Invocation>, bool)> {
    let has_restate_1_2_columns = client
        .check_columns_exists("sys_invocation", &["idempotency_key"])
        .await?;
    let select_idempotency_key = if has_restate_1_2_columns {
        "idempotency_key"
    } else {
        "CAST(NULL as STRING) AS idempotency_key"
    };

    let query = format!(
        "WITH invocations as (SELECT
            inv.id,
            inv.target,
            inv.target_service_ty,
            inv.target_service_name,
            {select_idempotency_key},
            inv.status,
            inv.created_at,
            inv.modified_at as state_modified_at,
            inv.modified_at,
            inv.pinned_deployment_id,
            inv.last_attempt_deployment_id,
            inv.last_attempt_server,
            inv.last_start_at,
            inv.invoked_by_id,
            inv.invoked_by_target,
            inv.trace_id,
            inv.completion_result,
            inv.completion_failure
        FROM sys_invocation inv
        {filter}
        {order}
        LIMIT {limit})

        SELECT
            inv.*,
            dp.id IS NOT NULL as pinned_deployment_exists
        FROM sys_deployment dp
        RIGHT JOIN invocations inv ON dp.id = inv.pinned_deployment_id
        {order}"
    );

    let rows = client
        .run_json_query::<InvocationQueryResult>(query)
        .await?;

    let received_less_than_limit = rows.len() < limit;

    Ok((
        rows.into_iter().map(Invocation::from).collect(),
        received_less_than_limit,
    ))
}

async fn find_active_invocations_post_1_5(
    client: &DataFusionHttpClient,
    filter: &str,
    order: &str,
    limit: usize,
) -> Result<(Vec<Invocation>, bool)> {
    let id_query = format!(
        "SELECT
            id
        FROM sys_invocation inv
        {filter}
        {order}
        LIMIT {limit}"
    );

    #[derive(Deserialize)]
    struct InvocationIdRow {
        id: String,
    }

    let id_rows = client.run_json_query::<InvocationIdRow>(id_query).await?;

    // to be sure we received less than the limit we have to consider the length of the id rows.
    // as some invocations may not longer match the filters, invocations can be a smaller list
    // than the id list, and that count dropping below $limit doesn't mean that we have a full set.
    let received_less_than_limit = id_rows.len() < limit;

    let invocations = describe_invocations_post_1_5(
        client,
        id_rows.into_iter().take(limit).map(|row| row.id).collect(),
        filter,
        order,
    )
    .await?;

    Ok((invocations, received_less_than_limit))
}

// from 1.5, multipoint reads like `id in ("inv_a", "inv_b") are very efficient and can be used.
// before 1.5, they are just another scan, so it generally doubles your query time
async fn describe_invocations_post_1_5(
    client: &DataFusionHttpClient,
    invocation_ids: Vec<String>,
    // we check the filter again, in case some rows no longer fit into it
    filter: &str,
    order: &str,
) -> Result<Vec<Invocation>> {
    if invocation_ids.is_empty() {
        return Ok(Vec::new());
    }

    let ids_filter = if filter.is_empty() {
        format!("WHERE inv.id in ('{}')", invocation_ids.join("','"))
    } else {
        format!("{filter} AND inv.id in ('{}')", invocation_ids.join("','"))
    };

    // the join direction doesn't matter as both sides of the join are small
    let query = format!(
        "SELECT
            inv.id,
            inv.target,
            inv.target_service_ty,
            inv.idempotency_key,
            inv.status,
            inv.created_at,
            inv.modified_at as state_modified_at,
            inv.pinned_deployment_id,
            inv.last_attempt_deployment_id,
            inv.last_attempt_server,
            inv.last_start_at,
            inv.invoked_by_id,
            inv.invoked_by_target,
            inv.trace_id,
            inv.completion_result,
            inv.completion_failure,
            inv.inboxed_at,
            inv.scheduled_at,
            inv.scheduled_start_at,
            inv.running_at,
            inv.completed_at,
            dp.id IS NOT NULL as pinned_deployment_exists
        FROM sys_invocation inv
        LEFT JOIN sys_deployment dp ON dp.id = inv.pinned_deployment_id
        {ids_filter}
        {order}"
    );
    let rows = client
        .run_json_query::<InvocationQueryResult>(query)
        .await?;

    Ok(rows.into_iter().map(Invocation::from).collect())
}

pub async fn get_service_invocations(
    client: &DataFusionHttpClient,
    service: &str,
    limit_active: usize,
) -> Result<Vec<Invocation>> {
    // Active invocations analysis
    Ok(find_active_invocations(
        client,
        &format!("WHERE inv.target_service_name = '{service}'"),
        "ORDER BY inv.modified_at DESC, inv.id",
        limit_active,
    )
    .await?
    .0)
}

pub async fn get_invocation(
    client: &DataFusionHttpClient,
    invocation_id: &str,
) -> Result<Option<Invocation>> {
    Ok(
        find_active_invocations(client, &format!("WHERE inv.id = '{invocation_id}'"), "", 1)
            .await?
            .0
            .pop(),
    )
}

#[derive(Debug, Clone, Deserialize)]
struct JournalRowQueryResult {
    index: u32,
    entry_type: String,
    name: Option<String>,
    appended_at: Option<DateTime<Local>>,
    entry_lite_json: Option<String>,
    entry_json: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct JournalStats {
    max_index: Option<u32>,
    #[serde(default)]
    count: i64,
}

/// Number of entries in an invocation's journal.
pub async fn get_journal_length(client: &DataFusionHttpClient, invocation_id: &str) -> Result<u64> {
    Ok(u64::try_from(journal_stats(client, invocation_id).await?.count).unwrap_or(0))
}

async fn journal_stats(client: &DataFusionHttpClient, invocation_id: &str) -> Result<JournalStats> {
    let query = format!(
        "SELECT MAX(sj.index) AS max_index, COUNT(*) AS count \
         FROM sys_journal sj WHERE sj.id = '{invocation_id}'"
    );
    Ok(client
        .run_json_query::<JournalStats>(query)
        .await?
        .into_iter()
        .next()
        .unwrap_or(JournalStats {
            max_index: None,
            count: 0,
        }))
}

/// Fetch a slice of an invocation's journal from `sys_journal`, defaulting to the
/// lightweight `entry_lite_json` metadata projection; `include_payload` additionally
/// pulls the full `entry_json`. Entries are returned ordered by ascending index.
pub async fn get_journal(
    client: &DataFusionHttpClient,
    invocation_id: &str,
    fetch: JournalFetch,
    include_payload: bool,
) -> Result<Vec<JournalEntryRow>> {
    // Column-existence gating for backward compatibility with older servers.
    let has_appended_at = client
        .check_columns_exists("sys_journal", &["appended_at"])
        .await?;
    let select_appended = if has_appended_at {
        "sj.appended_at"
    } else {
        "CAST(NULL as TIMESTAMP) AS appended_at"
    };

    let has_entry_lite = client
        .check_columns_exists("sys_journal", &["entry_lite_json"])
        .await?;
    let select_lite = if has_entry_lite {
        "sj.entry_lite_json"
    } else {
        "CAST(NULL as STRING) AS entry_lite_json"
    };

    let has_entry_json = client
        .check_columns_exists("sys_journal", &["entry_json"])
        .await?;
    // Only pull payloads when explicitly requested (they can be large).
    let select_full = if include_payload && has_entry_json {
        "sj.entry_json"
    } else {
        "CAST(NULL as STRING) AS entry_json"
    };

    let index_filter = match fetch {
        JournalFetch::One(index) => format!(" AND sj.index = {index}"),
        JournalFetch::Range(start, end) => {
            let mut filter = String::new();
            if let Some(start) = start {
                filter.push_str(&format!(" AND sj.index >= {start}"));
            }
            if let Some(end) = end {
                filter.push_str(&format!(" AND sj.index <= {end}"));
            }
            filter
        }
        JournalFetch::All => String::new(),
        JournalFetch::Preview { head, tail } => {
            let stats = journal_stats(client, invocation_id).await?;
            let count = u64::try_from(stats.count).unwrap_or(0);
            if count > u64::from(head) + u64::from(tail) {
                let max_index = stats.max_index.unwrap_or(0);
                let tail_start = (max_index + 1).saturating_sub(tail);
                format!(" AND (sj.index < {head} OR sj.index >= {tail_start})")
            } else {
                // Small journal: fetch everything, no elision needed.
                String::new()
            }
        }
    };

    let query = format!(
        "SELECT
            sj.index,
            sj.entry_type,
            sj.name,
            {select_appended},
            {select_lite},
            {select_full}
        FROM sys_journal sj
        WHERE sj.id = '{invocation_id}'{index_filter}
        ORDER BY sj.index ASC",
    );

    let entries = client
        .run_json_query::<JournalRowQueryResult>(query)
        .await?
        .into_iter()
        .map(|row| JournalEntryRow {
            index: row.index,
            entry_type: row.entry_type,
            name: row.name,
            appended_at: row.appended_at,
            lite: row
                .entry_lite_json
                .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok()),
            full: row
                .entry_json
                .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok()),
        })
        .collect();

    Ok(entries)
}

#[derive(Debug, Clone, Deserialize)]
struct JournalEventQueryResult {
    after_journal_entry_index: u32,
    appended_at: Option<DateTime<Local>>,
    event_type: String,
    event_json: Option<String>,
}

/// Fetch the most recent journal events for an invocation from `sys_journal_events`,
/// returned oldest-first. Empty when the server predates the table.
pub async fn get_journal_events(
    client: &DataFusionHttpClient,
    invocation_id: &str,
    limit: usize,
) -> Result<Vec<JournalEventRow>> {
    // The table only exists on newer servers; skip gracefully otherwise.
    let has_events = client
        .check_columns_exists(
            "sys_journal_events",
            &[
                "after_journal_entry_index",
                "appended_at",
                "event_type",
                "event_json",
            ],
        )
        .await?;
    if !has_events {
        return Ok(Vec::new());
    }

    let query = format!(
        "SELECT sje.after_journal_entry_index, sje.appended_at, sje.event_type, sje.event_json \
         FROM sys_journal_events sje WHERE sje.id = '{invocation_id}' \
         ORDER BY sje.appended_at DESC, sje.after_journal_entry_index DESC LIMIT {limit}"
    );

    let mut events: Vec<JournalEventRow> = client
        .run_json_query::<JournalEventQueryResult>(query)
        .await?
        .into_iter()
        .map(|row| JournalEventRow {
            after_journal_entry_index: row.after_journal_entry_index,
            appended_at: row.appended_at,
            event_type: row.event_type,
            event: row
                .event_json
                .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok()),
        })
        .collect();

    // Return oldest-first for display.
    events.reverse();
    Ok(events)
}

#[serde_as]
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct StateKeysQueryResult {
    service_name: String,
    service_key: String,
    key: String,
    #[serde_as(as = "serde_with::hex::Hex")]
    value: Vec<u8>,
}

pub(crate) async fn get_state_keys(
    client: &DataFusionHttpClient,
    service: &str,
    key: Option<&str>,
) -> Result<HashMap<ServiceId, HashMap<String, Bytes>>> {
    let filter = if let Some(k) = key {
        format!("service_name = '{service}' AND service_key = '{k}'")
    } else {
        format!("service_name = '{service}'")
    };
    let sql = format!("SELECT service_name, service_key, key, value FROM state WHERE {filter}");
    let query_result_iter = client.run_json_query::<StateKeysQueryResult>(sql).await?;

    #[allow(clippy::mutable_key_type)]
    let mut user_state: HashMap<ServiceId, HashMap<String, Bytes>> = HashMap::new();
    for row in query_result_iter {
        user_state
            // todo(tillrohrmann) allow specifying the scope
            .entry(ServiceId::new(None, row.service_name, row.service_key))
            .or_default()
            .insert(row.key, Bytes::from(row.value));
    }
    Ok(user_state)
}
