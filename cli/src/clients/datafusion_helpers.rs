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
use std::str::FromStr;

use anyhow::Result;
use bytes::Bytes;
use chrono::{DateTime, Duration, Local};
use clap::ValueEnum;

use restate_service_protocol::awakeable_id::AwakeableIdentifier;
use restate_types::identifiers::DeploymentId;
use restate_types::identifiers::{InvocationId, ServiceId};
use restate_types::invocation::ServiceType;
use serde::Deserialize;
use serde_with::{serde_as, DeserializeAs};

use super::DataFusionHttpClient;

static JOURNAL_QUERY_LIMIT: usize = 100;

#[derive(
    ValueEnum, Copy, Clone, Eq, Hash, PartialEq, Debug, Default, serde_with::DeserializeFromStr,
)]
pub enum InvocationState {
    #[default]
    #[clap(hide = true)]
    Unknown,
    Scheduled,
    Pending,
    Ready,
    Running,
    Suspended,
    BackingOff,
    Killed,
    Completed,
}

impl FromStr for InvocationState {
    type Err = std::convert::Infallible;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "pending" => Self::Pending,
            "scheduled" => Self::Scheduled,
            "ready" => Self::Ready,
            "running" => Self::Running,
            "suspended" => Self::Suspended,
            "backing-off" => Self::BackingOff,
            "completed" => Self::Completed,
            "killed" => Self::Killed,
            _ => Self::Unknown,
        })
    }
}

impl Display for InvocationState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InvocationState::Unknown => write!(f, "unknown"),
            InvocationState::Pending => write!(f, "pending"),
            InvocationState::Scheduled => write!(f, "scheduled"),
            InvocationState::Ready => write!(f, "ready"),
            InvocationState::Running => write!(f, "running"),
            InvocationState::Suspended => write!(f, "suspended"),
            InvocationState::BackingOff => write!(f, "backing-off"),
            InvocationState::Killed => write!(f, "killed"),
            InvocationState::Completed => write!(f, "completed"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct OutgoingInvoke {
    pub invocation_id: Option<String>,
    pub invoked_target: Option<String>,
}

#[derive(Debug, Clone)]
pub struct JournalEntry {
    pub seq: u32,
    pub entry_type: JournalEntryType,
    completed: bool,
    pub name: Option<String>,
}

impl JournalEntry {
    pub fn is_completed(&self) -> bool {
        if self.entry_type.is_completable() {
            self.completed
        } else {
            true
        }
    }

    pub fn should_present(&self) -> bool {
        self.entry_type.should_present()
    }
}

#[derive(Debug, Clone)]
pub enum JournalEntryType {
    Sleep {
        wakeup_at: Option<chrono::DateTime<Local>>,
    },
    Call(OutgoingInvoke),
    OneWayCall(OutgoingInvoke),
    Awakeable(AwakeableIdentifier),
    GetState,
    SetState,
    ClearState,
    Run,
    /// GetPromise is the blocking promise API,
    ///  PeekPromise is the non-blocking variant (we don't need to show it)
    GetPromise(Option<String>),
    Other(String),
}

impl JournalEntryType {
    fn is_completable(&self) -> bool {
        matches!(
            self,
            JournalEntryType::Sleep { .. }
                | JournalEntryType::Call(_)
                | JournalEntryType::Awakeable(_)
                | JournalEntryType::GetState
                | JournalEntryType::GetPromise(_)
        )
    }

    fn should_present(&self) -> bool {
        matches!(
            self,
            JournalEntryType::Sleep { .. }
                | JournalEntryType::Call(_)
                | JournalEntryType::OneWayCall(_)
                | JournalEntryType::Awakeable(_)
                | JournalEntryType::Run
                | JournalEntryType::GetPromise(_)
        )
    }
}

impl Display for JournalEntryType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JournalEntryType::Sleep { .. } => write!(f, "Sleep"),
            JournalEntryType::Call(_) => write!(f, "Call"),
            JournalEntryType::OneWayCall(_) => write!(f, "Send"),
            JournalEntryType::Awakeable(_) => write!(f, "Awakeable"),
            JournalEntryType::GetState => write!(f, "GetState"),
            JournalEntryType::SetState => write!(f, "SetState"),
            JournalEntryType::ClearState => write!(f, "ClearState"),
            JournalEntryType::Run => write!(f, "Run"),
            JournalEntryType::GetPromise(_) => write!(f, "Promise"),
            JournalEntryType::Other(s) => write!(f, "{s}"),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct SimpleInvocation {
    pub id: String,
    pub target: String,
    pub status: InvocationState,
}

pub async fn find_active_invocations_simple(
    client: &DataFusionHttpClient,
    filter: &str,
) -> Result<Vec<SimpleInvocation>> {
    let query = format!("SELECT id, target, status FROM sys_invocation WHERE {filter}");
    Ok(client.run_json_query::<SimpleInvocation>(query).await?)
}

#[derive(Debug, Clone)]
pub enum InvocationCompletion {
    Success,
    Failure(String),
}

impl InvocationCompletion {
    fn from_sql(
        completion_result: Option<String>,
        completion_failure: Option<String>,
    ) -> Option<InvocationCompletion> {
        match (completion_result.as_deref(), completion_failure) {
            (Some("success"), None) => Some(InvocationCompletion::Success),
            (Some("failure"), None) => Some(InvocationCompletion::Failure("Unknown".to_owned())),
            (Some("failure"), Some(failure)) => Some(InvocationCompletion::Failure(failure)),
            _ => None,
        }
    }
}

#[serde_as]
#[derive(Debug, Clone, Deserialize)]
pub struct Invocation {
    pub id: String,
    pub target: String,
    #[serde_as(as = "DatafusionServiceType")]
    pub target_service_ty: ServiceType,
    pub created_at: chrono::DateTime<Local>,
    // None if invoked directly (e.g. ingress)
    pub invoked_by_id: Option<String>,
    pub invoked_by_target: Option<String>,
    pub status: InvocationState,
    #[serde(skip)]
    pub completion: Option<InvocationCompletion>,
    pub trace_id: Option<String>,
    pub idempotency_key: Option<String>,

    // If it **requires** this deployment.
    pub pinned_deployment_id: Option<String>,
    #[serde(skip)]
    pub pinned_deployment_exists: bool,
    // Last attempted deployment
    pub last_attempt_deployment_id: Option<String>,
    pub last_attempt_server: Option<String>,

    // if running, how long has it been running?
    pub current_attempt_duration: Option<Duration>,
    // E.g. If suspended, since when?
    pub state_modified_at: Option<DateTime<Local>>,

    // If backing-off
    pub num_retries: Option<u64>,
    pub next_retry_at: Option<DateTime<Local>>,

    pub last_attempt_started_at: Option<DateTime<Local>>,
    // Last attempt failed?
    pub last_failure_message: Option<String>,
    pub last_failure_entry_index: Option<u64>,
    pub last_failure_entry_name: Option<String>,
    pub last_failure_entry_ty: Option<String>,
}

pub async fn count_deployment_active_inv(
    client: &DataFusionHttpClient,
    deployment_id: &DeploymentId,
) -> Result<i64> {
    Ok(client
        .run_count_agg_query(format!(
            "SELECT COUNT(id) AS inv_count \
            FROM sys_invocation_status \
            WHERE pinned_deployment_id = '{deployment_id}' \
            GROUP BY pinned_deployment_id"
        ))
        .await?)
}

#[derive(Deserialize)]
pub struct ServiceHandlerUsage {
    pub service: String,
    pub handler: String,
    pub inv_count: i64,
}

/// Key is service name
#[derive(Clone, Default)]
pub struct ServiceStatusMap(HashMap<String, ServiceStatus>);

impl ServiceStatusMap {
    fn set_handler_stats(
        &mut self,
        service: &str,
        handler: &str,
        state: InvocationState,
        stats: HandlerStateStats,
    ) {
        let comp_handlers = self
            .0
            .entry(service.to_owned())
            .or_insert_with(|| ServiceStatus {
                handlers: HashMap::new(),
            });

        let handler_info = comp_handlers
            .handlers
            .entry(handler.to_owned())
            .or_insert_with(|| HandlerInfo {
                per_state_totals: HashMap::new(),
            });

        handler_info.per_state_totals.insert(state, stats);
    }

    pub fn get_service_status(&self, service: &str) -> Option<&ServiceStatus> {
        self.0.get(service)
    }
}

#[derive(Default, Clone)]
pub struct ServiceStatus {
    handlers: HashMap<String, HandlerInfo>,
}

impl ServiceStatus {
    pub fn get_handler_stats(
        &self,
        state: InvocationState,
        method: &str,
    ) -> Option<&HandlerStateStats> {
        self.handlers.get(method).and_then(|x| x.get_stats(state))
    }

    pub fn get_handler(&self, handler: &str) -> Option<&HandlerInfo> {
        self.handlers.get(handler)
    }
}

#[derive(Default, Clone)]
pub struct HandlerInfo {
    per_state_totals: HashMap<InvocationState, HandlerStateStats>,
}

impl HandlerInfo {
    pub fn get_stats(&self, state: InvocationState) -> Option<&HandlerStateStats> {
        self.per_state_totals.get(&state)
    }

    pub fn oldest_non_suspended_invocation_state(
        &self,
    ) -> Option<(InvocationState, &HandlerStateStats)> {
        let mut oldest: Option<(InvocationState, &HandlerStateStats)> = None;
        for (state, stats) in &self.per_state_totals {
            if state == &InvocationState::Suspended {
                continue;
            }
            if oldest.is_none() || oldest.is_some_and(|oldest| stats.oldest_at < oldest.1.oldest_at)
            {
                oldest = Some((*state, stats));
            }
        }
        oldest
    }
}

#[derive(Deserialize, Clone)]
pub struct HandlerStateStats {
    pub num_invocations: i64,
    pub oldest_at: chrono::DateTime<Local>,
    pub oldest_invocation: String,
}

pub async fn count_deployment_active_inv_by_method(
    client: &DataFusionHttpClient,
    deployment_id: &DeploymentId,
) -> Result<Vec<ServiceHandlerUsage>> {
    let query = format!(
        "SELECT
            target_service_name as service,
            target_handler_name as handler,
            COUNT(id) AS inv_count
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
                target_service_name as service,
                target_handler_name as handler,
                'pending' as status,
                COUNT(id) as num_invocations,
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
                COUNT(id) as num_invocations,
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

// Service -> Locked Keys
#[derive(Default)]
pub struct ServiceHandlerLockedKeysMap {
    services: HashMap<String, HashMap<String, LockedKeyInfo>>,
}

#[derive(Clone, Default, Debug, Deserialize)]
pub struct LockedKeyInfo {
    pub num_pending: i64,
    // Who is holding the lock
    pub invocation_holding_lock: Option<String>,
    pub invocation_method_holding_lock: Option<String>,
    pub invocation_status: Option<InvocationState>,
    pub invocation_created_at: Option<DateTime<Local>>,
    // if running, how long has it been running?
    pub invocation_attempt_duration: Option<Duration>,
    // E.g. If suspended, how long has it been suspended?
    pub invocation_state_duration: Option<Duration>,

    pub num_retries: Option<u64>,
    pub next_retry_at: Option<DateTime<Local>>,
    pub pinned_deployment_id: Option<String>,
    // Last attempt failed?
    pub last_failure_message: Option<String>,
    pub last_attempt_deployment_id: Option<String>,
}

impl ServiceHandlerLockedKeysMap {
    fn insert(&mut self, service: &str, key: String, info: LockedKeyInfo) {
        let locked_keys = self.services.entry(service.to_owned()).or_default();
        locked_keys.insert(key.to_owned(), info);
    }

    fn locked_key_info_mut(&mut self, service: &str, key: &str) -> &mut LockedKeyInfo {
        let locked_keys = self.services.entry(service.to_owned()).or_default();
        locked_keys.entry(key.to_owned()).or_default()
    }

    pub fn into_inner(self) -> HashMap<String, HashMap<String, LockedKeyInfo>> {
        self.services
    }

    pub fn is_empty(&self) -> bool {
        self.services.is_empty()
    }
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
                COUNT(id) as num_pending
             FROM sys_inbox
             WHERE service_name IN {query_filter}
             GROUP BY service_name, service_key
             ORDER BY COUNT(id) DESC"
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
    known_deployment_id: Option<String>,
    completion_result: Option<String>,
    completion_failure: Option<String>,
    #[serde(flatten)]
    invocation: Invocation,
    full_count: usize,
}

pub async fn find_active_invocations(
    client: &DataFusionHttpClient,
    filter: &str,
    post_filter: &str,
    order: &str,
    limit: usize,
) -> Result<(Vec<Invocation>, usize)> {
    // Check if columns completion_result and completion_failure are available.
    // Those were introduced in Restate 1.1
    let has_restate_1_1_completion_columns = client
        .check_columns_exists(
            "sys_invocation",
            &["completion_result", "completion_failure"],
        )
        .await?;
    let select_completion_columns = if has_restate_1_1_completion_columns {
        "inv.completion_result, inv.completion_failure"
    } else {
        "CAST(NULL as STRING) AS completion_result, CAST(NULL as STRING) AS completion_failure"
    };

    let has_restate_1_2_columns = client
        .check_columns_exists("sys_invocation", &["idempotency_key"])
        .await?;
    let select_idempotency_key = if has_restate_1_2_columns {
        "inv.idempotency_key"
    } else {
        "CAST(NULL as STRING) AS idempotency_key"
    };

    let mut full_count = 0;
    let mut active = vec![];
    let query = format!(
        "WITH enriched_invocations AS
        (SELECT
            inv.id,
            inv.target,
            inv.target_service_ty,
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
            svc.deployment_id as comp_latest_deployment,
            dp.id as known_deployment_id,
            inv.trace_id,
            {select_completion_columns}
        FROM sys_invocation inv
        LEFT JOIN sys_service svc ON svc.name = inv.target_service_name
        LEFT JOIN sys_deployment dp ON dp.id = inv.pinned_deployment_id
        {filter}
        {order}
        )
        SELECT *, COUNT(*) OVER() AS full_count from enriched_invocations
        {post_filter}
        LIMIT {limit}"
    );
    let rows = client
        .run_json_query::<InvocationQueryResult>(query)
        .await?;
    for row in rows {
        // Running duration
        let current_attempt_duration = if row.invocation.status == InvocationState::Running {
            row.last_start_at
                .map(|last_start| Local::now().signed_duration_since(last_start))
        } else {
            None
        };

        let last_attempt_started_at = if row.invocation.status == InvocationState::BackingOff {
            row.last_start_at
        } else {
            None
        };

        active.push(Invocation {
            current_attempt_duration,
            last_attempt_started_at,
            pinned_deployment_exists: row.known_deployment_id.is_some(),
            completion: InvocationCompletion::from_sql(
                row.completion_result,
                row.completion_failure,
            ),
            ..row.invocation
        });

        full_count = row.full_count;
    }
    Ok((active, full_count))
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
        "",
        "ORDER BY inv.created_at DESC",
        limit_active,
    )
    .await?
    .0)
}

#[derive(serde_with::DeserializeFromStr)]
enum DatafusionServiceType {
    Service,
    VirtualObject,
    Workflow,
}

impl From<DatafusionServiceType> for ServiceType {
    fn from(value: DatafusionServiceType) -> Self {
        match value {
            DatafusionServiceType::Service => Self::Service,
            DatafusionServiceType::VirtualObject => Self::VirtualObject,
            DatafusionServiceType::Workflow => Self::Workflow,
        }
    }
}

impl<'de> DeserializeAs<'de, ServiceType> for DatafusionServiceType {
    fn deserialize_as<D>(deserializer: D) -> std::result::Result<ServiceType, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(DatafusionServiceType::deserialize(deserializer)?.into())
    }
}

impl FromStr for DatafusionServiceType {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Ok(match s {
            "service" => Self::Service,
            "virtual_object" => Self::VirtualObject,
            "workflow" => Self::Workflow,
            _ => return Err("Unexpected instance type".into()),
        })
    }
}

pub async fn get_invocation(
    client: &DataFusionHttpClient,
    invocation_id: &str,
) -> Result<Option<Invocation>> {
    Ok(find_active_invocations(
        client,
        &format!("WHERE inv.id = '{invocation_id}'"),
        "",
        "",
        1,
    )
    .await?
    .0
    .pop())
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
            {select_promise_column}
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
            let entry_type = match row.entry_type.as_str() {
                "Sleep" => JournalEntryType::Sleep {
                    wakeup_at: row.sleep_wakeup_at.map(Into::into),
                },
                "Call" => JournalEntryType::Call(OutgoingInvoke {
                    invocation_id: row.invoked_id,
                    invoked_target: row.invoked_target,
                }),
                "OneWayCall" => JournalEntryType::OneWayCall(OutgoingInvoke {
                    invocation_id: row.invoked_id,
                    invoked_target: row.invoked_target,
                }),
                "Awakeable" => JournalEntryType::Awakeable(AwakeableIdentifier::new(
                    my_invocation_id,
                    row.index,
                )),
                "GetState" => JournalEntryType::GetState,
                "SetState" => JournalEntryType::SetState,
                "ClearState" => JournalEntryType::ClearState,
                "Run" => JournalEntryType::Run,
                "GetPromise" => JournalEntryType::GetPromise(row.promise_name),
                t => JournalEntryType::Other(t.to_owned()),
            };

            JournalEntry {
                seq: row.index,
                entry_type,
                completed: row.completed,
                name: row.name,
            }
        })
        .collect();

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
