// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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
use restate_types::identifiers::{AwakeableIdentifier, InvocationId, ServiceId};

use super::{
    HandlerStateStats, Invocation, InvocationCompletion, InvocationState, JournalEntry,
    JournalEntryTypeV1, JournalEntryV1, JournalEntryV2, LockedKeyInfo, OutgoingInvoke,
    ServiceHandlerLockedKeysMap, ServiceHandlerUsage, ServiceStatusMap, SimpleInvocation,
};

use crate::clients::DataFusionHttpClient;

static JOURNAL_QUERY_LIMIT: usize = 100;

pub async fn find_active_invocations_simple(
    client: &DataFusionHttpClient,
    filter: &str,
) -> Result<Vec<SimpleInvocation>> {
    let query = format!("SELECT id, target FROM sys_invocation_status WHERE {filter}");
    Ok(client.run_json_query::<SimpleInvocation>(query).await?)
}

pub async fn count_deployment_active_inv(
    client: &DataFusionHttpClient,
    deployment_id: &DeploymentId,
) -> Result<i64> {
    Ok(client
        .run_count_agg_query(format!(
            "SELECT COUNT(1) AS inv_count
            FROM sys_invocation_status
            WHERE pinned_deployment_id = '{deployment_id}'"
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
            WHERE pinned_deployment_id = '{deployment_id}'
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
    // Inbox analysis (pending invocations)....
    {
        let query = format!(
            "SELECT
                target_service_name,
                target_handler_name,
                'pending' as status,
                COUNT(1) as num_invocations,
                MIN(created_at) as oldest_at,
                FIRST_VALUE(id ORDER BY created_at ASC) as oldest_invocation
             FROM sys_invocation_status
             WHERE status == 'inboxed' AND target_service_name IN {query_filter}
             GROUP BY target_service_name, target_handler_name"
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
    }

    // Active invocations analysis
    {
        let query = format!(
            "
            SELECT
                target_service_name,
                target_handler_name,
                status,
                COUNT(1) as num_invocations,
                MIN(created_at) as oldest_at,
                FIRST_VALUE(id ORDER BY created_at ASC) as oldest_invocation
            FROM sys_invocation
            WHERE target_service_name IN {query_filter}
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
    }

    Ok(status_map)
}

#[derive(Deserialize)]
struct LockedKeysQueryResult {
    service_name: String,
    service_key: String,
    modified_at: Option<DateTime<Local>>,
    last_start_at: Option<DateTime<Local>>,
    #[serde(flatten)]
    info: LockedKeyInfo,
}

pub async fn get_locked_keys_status(
    client: &DataFusionHttpClient,
    services_filter: impl IntoIterator<Item = impl AsRef<str>>,
) -> Result<ServiceHandlerLockedKeysMap> {
    let mut key_map = ServiceHandlerLockedKeysMap::default();
    let quoted_service_names = services_filter
        .into_iter()
        .map(|x| format!("'{}'", x.as_ref()))
        .collect::<Vec<_>>();
    if quoted_service_names.is_empty() {
        return Ok(key_map);
    }

    let query_filter = format!("({})", quoted_service_names.join(","));

    // Inbox analysis (pending invocations)....
    {
        let query = format!(
            "SELECT
                service_name,
                service_key,
                COUNT(1) as num_pending
             FROM sys_inbox
             WHERE service_name IN {query_filter}
             GROUP BY service_name, service_key
             ORDER BY num_pending DESC"
        );
        let rows = client
            .run_json_query::<LockedKeysQueryResult>(query)
            .await?;
        for row in rows {
            key_map.insert(&row.service_name, row.service_key, row.info);
        }
    }

    // Active invocations analysis
    {
        let query = format!(
            "
            SELECT
                target_service_name as service_name,
                target_service_key as service_key,
                status as invocation_status,
                first_value(id) as invocation_holding_lock,
                first_value(target_handler_name) as invocation_method_holding_lock,
                first_value(created_at) as invocation_created_at,
                first_value(modified_at) as modified_at,
                first_value(pinned_deployment_id) as pinned_deployment_id,
                first_value(last_attempt_deployment_id) as last_attempt_deployment_id,
                first_value(last_failure) as last_failure_message,
                first_value(next_retry_at) as next_retry_at,
                first_value(last_start_at) as last_start_at,
                0 as num_pending,
                sum(retry_count) as num_retries
            FROM sys_invocation
            WHERE status != 'pending' AND target_service_name IN {query_filter}
            GROUP BY target_service_name, target_service_key, status"
        );

        let rows = client
            .run_json_query::<LockedKeysQueryResult>(query)
            .await?;
        for row in rows {
            let info = key_map.locked_key_info_mut(&row.service_name, &row.service_key);

            info.invocation_status = row.info.invocation_status;
            info.invocation_holding_lock = row.info.invocation_holding_lock;
            info.invocation_method_holding_lock = row.info.invocation_method_holding_lock;
            info.invocation_created_at = row.info.invocation_created_at;

            // Running duration
            if row.info.invocation_status == Some(InvocationState::Running) {
                info.invocation_attempt_duration = row
                    .last_start_at
                    .map(|last_start| Local::now().signed_duration_since(last_start));
            }

            // State duration
            info.invocation_state_duration = row
                .modified_at
                .map(|last_modified| Local::now().signed_duration_since(last_modified));

            // Retries
            info.num_retries = row.info.num_retries;
            info.next_retry_at = row.info.next_retry_at;
            info.pinned_deployment_id = row.info.pinned_deployment_id;
            info.last_failure_message = row.info.last_failure_message;
            info.last_attempt_deployment_id = row.info.last_attempt_deployment_id;
        }
    }

    Ok(key_map)
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
    if client
        .server_version()
        // any 1.5.x including prereleases
        .is_equal_or_newer_than(&SemanticRestateVersion::new(1, 4, u64::MAX))
    {
        find_active_invocations_post_1_5(client, filter, order, limit).await
    } else {
        find_active_invocations_pre_1_5(client, filter, order, limit).await
    }
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
            inv.pinned_deployment_id,
            inv.retry_count as num_retries,
            inv.last_failure as last_failure_message,
            inv.last_failure_related_entry_index as last_failure_entry_index,
            inv.last_failure_related_entry_name as last_failure_entry_name,
            inv.last_failure_related_entry_type as last_failure_entry_ty,
            inv.last_attempt_deployment_id,
            inv.last_attempt_server,
            inv.next_retry_at,
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
            inv.retry_count as num_retries,
            inv.last_failure as last_failure_message,
            inv.last_failure_related_entry_index as last_failure_entry_index,
            inv.last_failure_related_entry_name as last_failure_entry_name,
            inv.last_failure_related_entry_type as last_failure_entry_ty,
            inv.last_attempt_deployment_id,
            inv.last_attempt_server,
            inv.next_retry_at,
            inv.last_start_at,
            inv.invoked_by_id,
            inv.invoked_by_target,
            inv.trace_id,
            inv.completion_result,
            inv.completion_failure,
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
        "ORDER BY inv.created_at DESC, inv.id",
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

#[derive(Debug, Clone, PartialEq, Deserialize)]
struct JournalQueryResult {
    index: u32,
    entry_type: String,
    #[serde(default)]
    completed: bool,
    invoked_id: Option<String>,
    invoked_target: Option<String>,
    sleep_wakeup_at: Option<DateTime<Local>>,
    name: Option<String>,
    promise_name: Option<String>,

    // --- V2 columns
    version: u32,
    appended_at: Option<DateTime<Local>>,
    entry_json: Option<String>,
}

pub async fn get_invocation_journal(
    client: &DataFusionHttpClient,
    invocation_id: &str,
) -> Result<Vec<JournalEntry>> {
    let has_restate_1_1_promise_name_column = client
        .check_columns_exists("sys_journal", &["promise_name"])
        .await?;
    let select_promise_column = if has_restate_1_1_promise_name_column {
        "sj.promise_name"
    } else {
        "CAST(NULL as STRING) AS promise_name"
    };
    let has_restate_1_2_columns = client
        .check_columns_exists("sys_journal", &["version", "entry_json", "appended_at"])
        .await?;
    let select_restate_1_2_columns = if has_restate_1_2_columns {
        "sj.version, sj.entry_json, sj.appended_at"
    } else {
        "CAST(1 as INT UNSIGNED) AS version, CAST(NULL as STRING) AS entry_json, CAST(NULL as TIMESTAMP) AS appended_at"
    };

    // We are only looking for one...
    // Let's get journal details.
    let query = format!(
        "SELECT
            sj.index,
            sj.entry_type,
            sj.completed,
            sj.invoked_id,
            sj.invoked_target,
            sj.sleep_wakeup_at,
            sj.name,
            {select_promise_column},
            {select_restate_1_2_columns}
        FROM sys_journal sj
        WHERE
            sj.id = '{invocation_id}'
        ORDER BY index DESC
        LIMIT {JOURNAL_QUERY_LIMIT}",
    );

    let my_invocation_id: InvocationId = invocation_id.parse().expect("Invocation ID is not valid");

    let mut journal: Vec<_> = client
        .run_json_query::<JournalQueryResult>(query)
        .await?
        .into_iter()
        .map(|row| {
            if row.version == 1 {
                let entry_type = match row.entry_type.as_str() {
                    "Sleep" => JournalEntryTypeV1::Sleep {
                        wakeup_at: row.sleep_wakeup_at,
                    },
                    "Call" => JournalEntryTypeV1::Call(OutgoingInvoke {
                        invocation_id: row.invoked_id,
                        invoked_target: row.invoked_target,
                    }),
                    "OneWayCall" => JournalEntryTypeV1::OneWayCall(OutgoingInvoke {
                        invocation_id: row.invoked_id,
                        invoked_target: row.invoked_target,
                    }),
                    "Awakeable" => JournalEntryTypeV1::Awakeable(AwakeableIdentifier::new(
                        my_invocation_id,
                        row.index,
                    )),
                    "GetState" => JournalEntryTypeV1::GetState,
                    "SetState" => JournalEntryTypeV1::SetState,
                    "ClearState" => JournalEntryTypeV1::ClearState,
                    "Run" => JournalEntryTypeV1::Run,
                    "GetPromise" => JournalEntryTypeV1::GetPromise(row.promise_name),
                    t => JournalEntryTypeV1::Other(t.to_owned()),
                };

                Ok(JournalEntry::V1(JournalEntryV1 {
                    seq: row.index,
                    entry_type,
                    completed: row.completed,
                    name: row.name,
                }))
            } else if row.version == 2 {
                Ok(JournalEntry::V2(JournalEntryV2 {
                    seq: row.index,
                    entry_type: row.entry_type,
                    name: row.name,
                    entry: row.entry_json.and_then(|j| serde_json::from_str(&j).ok()),
                    appended_at: row.appended_at,
                }))
            } else {
                anyhow::bail!(
                    "The row version is unknown, cannot parse the journal: {}",
                    row.version
                )
            }
        })
        .collect::<Result<Vec<_>, _>>()?;

    // Sort by seq.
    journal.reverse();
    Ok(journal)
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
            .entry(ServiceId::new(row.service_name, row.service_key))
            .or_default()
            .insert(row.key, Bytes::from(row.value));
    }
    Ok(user_state)
}
