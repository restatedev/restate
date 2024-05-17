// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
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

use super::DataFusionHttpClient;

use arrow::array::{Array, ArrayAccessor, AsArray, StringArray};
use arrow::datatypes::{ArrowTemporalType, Date64Type};
use arrow::record_batch::RecordBatch;
use clap::ValueEnum;
use restate_meta_rest_model::deployments::DeploymentId;

use anyhow::Result;
use arrow_convert::{ArrowDeserialize, ArrowField};
use bytes::Bytes;
use chrono::{DateTime, Duration, Local, TimeZone};
use restate_meta_rest_model::services::ServiceType;
use restate_service_protocol::awakeable_id::AwakeableIdentifier;
use restate_types::identifiers::{InvocationId, ServiceId};

static JOURNAL_QUERY_LIMIT: usize = 100;

trait OptionalArrowLocalDateTime {
    fn value_as_local_datetime_opt(&self, index: usize) -> Option<chrono::DateTime<Local>>;
}

impl<T> OptionalArrowLocalDateTime for &arrow::array::PrimitiveArray<T>
where
    T: ArrowTemporalType,
    i64: From<T::Native>,
{
    fn value_as_local_datetime_opt(&self, index: usize) -> Option<chrono::DateTime<Local>> {
        if !self.is_null(index) {
            self.value_as_datetime(index)
                .map(|naive| Local.from_utc_datetime(&naive))
        } else {
            None
        }
    }
}

trait OptionalArrowValue
where
    Self: ArrayAccessor,
{
    fn value_opt(&self, index: usize) -> Option<<Self as ArrayAccessor>::Item> {
        if !self.is_null(index) {
            Some(self.value(index))
        } else {
            None
        }
    }
}

impl<T> OptionalArrowValue for T where T: ArrayAccessor {}

trait OptionalArrowOwnedString
where
    Self: ArrayAccessor,
{
    fn value_string_opt(&self, index: usize) -> Option<String>;
    fn value_string(&self, index: usize) -> String;
}

impl OptionalArrowOwnedString for &StringArray {
    fn value_string_opt(&self, index: usize) -> Option<String> {
        if !self.is_null(index) {
            Some(self.value(index).to_owned())
        } else {
            None
        }
    }

    fn value_string(&self, index: usize) -> String {
        self.value(index).to_owned()
    }
}

fn value_as_string(batch: &RecordBatch, col: usize, row: usize) -> String {
    batch.column(col).as_string::<i32>().value_string(row)
}

fn value_as_string_opt(batch: &RecordBatch, col: usize, row: usize) -> Option<String> {
    batch.column(col).as_string::<i32>().value_string_opt(row)
}

fn value_as_i64(batch: &RecordBatch, col: usize, row: usize) -> i64 {
    batch
        .column(col)
        .as_primitive::<arrow::datatypes::Int64Type>()
        .value(row)
}

fn value_as_u64_opt(batch: &RecordBatch, col: usize, row: usize) -> Option<u64> {
    batch
        .column(col)
        .as_primitive::<arrow::datatypes::UInt64Type>()
        .value_opt(row)
}

fn value_as_dt_opt(batch: &RecordBatch, col: usize, row: usize) -> Option<chrono::DateTime<Local>> {
    batch
        .column(col)
        .as_primitive::<arrow::datatypes::Date64Type>()
        .value_as_local_datetime_opt(row)
}

#[derive(ValueEnum, Copy, Clone, Eq, Hash, PartialEq, Debug, Default)]
pub enum InvocationState {
    #[default]
    #[clap(hide = true)]
    Unknown,
    Pending,
    Ready,
    Running,
    Suspended,
    BackingOff,
    Completed,
}

impl FromStr for InvocationState {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "pending" => Self::Pending,
            "ready" => Self::Ready,
            "running" => Self::Running,
            "suspended" => Self::Suspended,
            "backing-off" => Self::BackingOff,
            "completed" => Self::Completed,
            _ => Self::Unknown,
        })
    }
}

impl Display for InvocationState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InvocationState::Unknown => write!(f, "unknown"),
            InvocationState::Pending => write!(f, "pending"),
            InvocationState::Ready => write!(f, "ready"),
            InvocationState::Running => write!(f, "running"),
            InvocationState::Suspended => write!(f, "suspended"),
            InvocationState::BackingOff => write!(f, "backing-off"),
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
    SideEffect,
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
        )
    }

    fn should_present(&self) -> bool {
        matches!(
            self,
            JournalEntryType::Sleep { .. }
                | JournalEntryType::Call(_)
                | JournalEntryType::OneWayCall(_)
                | JournalEntryType::Awakeable(_)
                | JournalEntryType::SideEffect
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
            JournalEntryType::SideEffect => write!(f, "SideEffect"),
            JournalEntryType::Other(s) => write!(f, "{}", s),
        }
    }
}

#[derive(Debug, Clone)]
pub struct InvocationDetailed {
    pub invocation: Invocation,
    pub journal: Vec<JournalEntry>,
}

#[derive(Debug, Clone)]
pub struct Invocation {
    pub id: String,
    pub target: String,
    pub target_service_ty: ServiceType,
    pub created_at: chrono::DateTime<Local>,
    // None if invoked directly (e.g. ingress)
    pub invoked_by_id: Option<String>,
    pub invoked_by_target: Option<String>,
    pub status: InvocationState,
    pub trace_id: Option<String>,

    // If it **requires** this deployment.
    pub pinned_deployment_id: Option<String>,
    pub pinned_deployment_exists: bool,
    pub deployment_id_at_latest_svc_revision: String,
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
            WHERE pinned_deployment_id = '{}' \
            GROUP BY pinned_deployment_id",
            deployment_id
        ))
        .await?)
}

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

#[derive(Clone)]
pub struct HandlerStateStats {
    pub num_invocations: i64,
    pub oldest_at: chrono::DateTime<Local>,
    pub oldest_invocation: String,
}

pub async fn count_deployment_active_inv_by_method(
    client: &DataFusionHttpClient,
    deployment_id: &DeploymentId,
) -> Result<Vec<ServiceHandlerUsage>> {
    let mut output = vec![];

    let query = format!(
        "SELECT 
            target_service_name,
            target_handler_name,
            COUNT(id) AS inv_count
            FROM sys_invocation_status
            WHERE pinned_deployment_id = '{}'
            GROUP BY pinned_deployment_id, target_service_name, target_handler_name",
        deployment_id
    );

    for batch in client.run_query(query).await?.batches {
        for i in 0..batch.num_rows() {
            output.push(ServiceHandlerUsage {
                service: value_as_string(&batch, 0, i),
                handler: value_as_string(&batch, 1, i),
                inv_count: value_as_i64(&batch, 2, i),
            });
        }
    }
    Ok(output)
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
                COUNT(id),
                MIN(created_at),
                FIRST_VALUE(id ORDER BY created_at ASC)
             FROM sys_invocation_status
             WHERE status == 'inboxed' AND target_service_name IN {}
             GROUP BY target_service_name, target_handler_name",
            query_filter
        );
        let resp = client.run_query(query).await?;
        for batch in resp.batches {
            for i in 0..batch.num_rows() {
                let service = batch.column(0).as_string::<i32>().value_string(i);
                let handler = batch.column(1).as_string::<i32>().value_string(i);
                let num_invocations = batch
                    .column(2)
                    .as_primitive::<arrow::datatypes::Int64Type>()
                    .value(i);
                let oldest_at = batch
                    .column(3)
                    .as_primitive::<arrow::datatypes::Date64Type>()
                    .value_as_local_datetime_opt(i)
                    .unwrap();

                let oldest_invocation = batch.column(4).as_string::<i32>().value_string(i);

                let stats = HandlerStateStats {
                    num_invocations,
                    oldest_at,
                    oldest_invocation,
                };
                status_map.set_handler_stats(&service, &handler, InvocationState::Pending, stats);
            }
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
                COUNT(id),
                MIN(created_at),
                FIRST_VALUE(id ORDER BY created_at ASC)
            FROM sys_invocation
            WHERE target_service_name IN {}
            GROUP BY target_service_name, target_handler_name, status
            ORDER BY target_handler_name",
            query_filter
        );
        let resp = client.run_query(query).await?;
        for batch in resp.batches {
            for i in 0..batch.num_rows() {
                let service = value_as_string(&batch, 0, i);
                let handler = value_as_string(&batch, 1, i);
                let status = value_as_string(&batch, 2, i);

                let stats = HandlerStateStats {
                    num_invocations: value_as_i64(&batch, 3, i),
                    oldest_at: value_as_dt_opt(&batch, 4, i).unwrap(),
                    oldest_invocation: value_as_string(&batch, 5, i),
                };

                status_map.set_handler_stats(&service, &handler, status.parse().unwrap(), stats);
            }
        }
    }

    Ok(status_map)
}

// Service -> Locked Keys
#[derive(Default)]
pub struct ServiceHandlerLockedKeysMap {
    services: HashMap<String, HashMap<String, LockedKeyInfo>>,
}

#[derive(Clone, Default, Debug)]
pub struct LockedKeyInfo {
    pub num_pending: i64,
    pub oldest_pending: Option<chrono::DateTime<Local>>,
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
                COUNT(id),
                MIN(created_at)
             FROM sys_inbox
             WHERE service_name IN {}
             GROUP BY service_name, service_key
             ORDER BY COUNT(id) DESC",
            query_filter
        );
        let resp = client.run_query(query).await?;
        for batch in resp.batches {
            for i in 0..batch.num_rows() {
                let service = batch.column(0).as_string::<i32>().value(i);
                let key = value_as_string(&batch, 1, i);
                let num_pending = value_as_i64(&batch, 2, i);
                let oldest_pending = value_as_dt_opt(&batch, 3, i);

                let info = LockedKeyInfo {
                    num_pending,
                    oldest_pending,
                    ..LockedKeyInfo::default()
                };
                key_map.insert(service, key, info);
            }
        }
    }

    // Active invocations analysis
    {
        let query = format!(
            "
            SELECT
                target_service_name,
                target_service_key,
                status,
                first_value(id),
                first_value(target_handler_name),
                first_value(created_at),
                first_value(modified_at),
                first_value(pinned_deployment_id),
                first_value(last_attempt_deployment_id),
                first_value(last_failure),
                first_value(next_retry_at),
                first_value(last_start_at),
                sum(retry_count)
            FROM sys_invocation
            WHERE status != 'pending' AND target_service_name IN {}
            GROUP BY target_service_name, target_service_key, status",
            query_filter
        );

        let resp = client.run_query(query).await?;
        for batch in resp.batches {
            for i in 0..batch.num_rows() {
                let service = value_as_string(&batch, 0, i);
                let key = value_as_string(&batch, 1, i);
                let status = batch
                    .column(2)
                    .as_string::<i32>()
                    .value(i)
                    .parse()
                    .expect("Unexpected status");
                let id = value_as_string_opt(&batch, 3, i);
                let handler = value_as_string_opt(&batch, 4, i);
                let created_at = value_as_dt_opt(&batch, 5, i);
                let modified_at = value_as_dt_opt(&batch, 6, i);
                let pinned_deployment_id = value_as_string_opt(&batch, 7, i);
                let last_attempt_eps = batch.column(8).as_string::<i32>();
                let last_failure_message = value_as_string_opt(&batch, 9, i);
                let next_retry_at = value_as_dt_opt(&batch, 10, i);
                let last_start = value_as_dt_opt(&batch, 11, i);
                let num_retries = value_as_u64_opt(&batch, 12, i);

                let info = key_map.locked_key_info_mut(&service, &key);

                info.invocation_status = Some(status);
                info.invocation_holding_lock = id;
                info.invocation_method_holding_lock = handler;
                info.invocation_created_at = created_at;

                // Running duration
                if status == InvocationState::Running {
                    info.invocation_attempt_duration =
                        last_start.map(|last_start| Local::now().signed_duration_since(last_start));
                }

                // State duration
                info.invocation_state_duration = modified_at
                    .map(|last_modified| Local::now().signed_duration_since(last_modified));

                // Retries
                info.num_retries = num_retries;
                info.next_retry_at = next_retry_at;
                info.pinned_deployment_id = pinned_deployment_id;
                info.last_failure_message = last_failure_message;
                info.last_attempt_deployment_id = last_attempt_eps.value_string_opt(i);
            }
        }
    }

    Ok(key_map)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RestateDateTime(DateTime<Local>);

impl From<RestateDateTime> for DateTime<Local> {
    fn from(value: RestateDateTime) -> Self {
        value.0
    }
}

impl arrow_convert::field::ArrowField for RestateDateTime {
    type Type = Self;

    #[inline]
    fn data_type() -> arrow::datatypes::DataType {
        arrow::datatypes::DataType::Date64
    }
}

impl arrow_convert::deserialize::ArrowDeserialize for RestateDateTime {
    type ArrayType = arrow::array::Date64Array;

    #[inline]
    fn arrow_deserialize(v: Option<i64>) -> Option<Self> {
        v.and_then(arrow::temporal_conversions::as_datetime::<Date64Type>)
            .map(|naive| Local.from_utc_datetime(&naive))
            .map(RestateDateTime)
    }
}

// enable Vec<RestateDateTime>
arrow_convert::arrow_enable_vec_for_type!(RestateDateTime);

#[derive(Debug, Clone, PartialEq, ArrowField, ArrowDeserialize)]
struct InvocationRowResult {
    id: Option<String>,
    target: Option<String>,
    target_service_ty: Option<String>,
    status: String,
    created_at: Option<RestateDateTime>,
    modified_at: Option<RestateDateTime>,
    pinned_deployment_id: Option<String>,
    retry_count: Option<u64>,
    last_failure: Option<String>,
    last_failure_related_entry_index: Option<u64>,
    last_failure_related_entry_name: Option<String>,
    last_failure_related_entry_type: Option<String>,
    last_attempt_deployment_id: Option<String>,
    last_attempt_server: Option<String>,
    next_retry_at: Option<RestateDateTime>,
    last_start_at: Option<RestateDateTime>,
    invoked_by_id: Option<String>,
    invoked_by_target: Option<String>,
    comp_latest_deployment: Option<String>,
    known_deployment_id: Option<String>,
    trace_id: Option<String>,
    full_count: Option<i64>,
}

pub async fn find_active_invocations(
    client: &DataFusionHttpClient,
    filter: &str,
    post_filter: &str,
    order: &str,
    limit: usize,
) -> Result<(Vec<Invocation>, usize)> {
    let mut full_count = 0;
    let mut active = vec![];
    let query = format!(
        "WITH enriched_invocations AS
        (SELECT
            inv.id,
            inv.target,
            inv.target_service_ty,
            inv.status,
            inv.created_at,
            inv.modified_at,
            inv.pinned_deployment_id,
            inv.retry_count,
            inv.last_failure,
            inv.last_failure_related_entry_index,
            inv.last_failure_related_entry_name,
            inv.last_failure_related_entry_type,
            inv.last_attempt_deployment_id,
            inv.last_attempt_server,
            inv.next_retry_at,
            inv.last_start_at,
            inv.invoked_by_id,
            inv.invoked_by_target,
            svc.deployment_id as comp_latest_deployment,
            dp.id as known_deployment_id,
            inv.trace_id
        FROM sys_invocation inv
        LEFT JOIN sys_service svc ON svc.name = inv.target_service_name
        LEFT JOIN sys_deployment dp ON dp.id = inv.pinned_deployment_id
        {}
        {}
        )
        SELECT *, COUNT(*) OVER() AS full_count from enriched_invocations
        {}
        LIMIT {}",
        filter, order, post_filter, limit,
    );
    let rows = client
        .run_query_and_map_results::<InvocationRowResult>(query)
        .await?;
    for row in rows {
        let status = row.status.parse().expect("Unexpected status");

        // Running duration
        let last_start: Option<DateTime<Local>> = row.last_start_at.map(Into::into);
        let current_attempt_duration = if status == InvocationState::Running {
            last_start.map(|last_start| Local::now().signed_duration_since(last_start))
        } else {
            None
        };

        let last_attempt_started_at = if status == InvocationState::BackingOff {
            last_start
        } else {
            None
        };

        active.push(Invocation {
            id: row.id.expect("id"),
            target: row.target.expect("target"),
            target_service_ty: parse_service_type(&row.target_service_ty.expect("target")),
            status,
            created_at: row.created_at.expect("created_at").into(),
            invoked_by_id: row.invoked_by_id,
            invoked_by_target: row.invoked_by_target,
            state_modified_at: row.modified_at.map(Into::into),
            num_retries: row.retry_count,
            next_retry_at: row.next_retry_at.map(Into::into),
            pinned_deployment_id: row.pinned_deployment_id,
            pinned_deployment_exists: row.known_deployment_id.is_some(),
            deployment_id_at_latest_svc_revision: row
                .comp_latest_deployment
                .expect("comp_latest_deployment"),
            last_failure_message: row.last_failure,
            last_failure_entry_index: row.last_failure_related_entry_index,
            last_failure_entry_name: row.last_failure_related_entry_name,
            last_attempt_deployment_id: row.last_attempt_deployment_id,
            last_attempt_server: row.last_attempt_server,
            trace_id: row.trace_id,
            current_attempt_duration,
            last_attempt_started_at,
            last_failure_entry_ty: row.last_failure_related_entry_type,
        });

        full_count = row.full_count.expect("full_count") as usize;
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
        &format!("WHERE inv.target_service_name = '{}'", service),
        "",
        "ORDER BY inv.created_at DESC",
        limit_active,
    )
    .await?
    .0)
}

#[allow(dead_code)]
fn parse_service_type(s: &str) -> ServiceType {
    match s {
        "service" => ServiceType::Service,
        "virtual_object" => ServiceType::VirtualObject,
        "workflow" => ServiceType::Workflow,
        _ => panic!("Unexpected instance type"),
    }
}

pub async fn get_invocation(
    client: &DataFusionHttpClient,
    invocation_id: &str,
) -> Result<Option<Invocation>> {
    Ok(find_active_invocations(
        client,
        &format!("WHERE inv.id = '{}'", invocation_id),
        "",
        "",
        1,
    )
    .await?
    .0
    .pop())
}

#[derive(Debug, Clone, PartialEq, ArrowField, ArrowDeserialize)]
struct JournalRowResult {
    index: Option<u32>,
    entry_type: Option<String>,
    completed: Option<bool>,
    invoked_id: Option<String>,
    invoked_target: Option<String>,
    sleep_wakeup_at: Option<RestateDateTime>,
    name: Option<String>,
}

pub async fn get_invocation_journal(
    client: &DataFusionHttpClient,
    invocation_id: &str,
) -> Result<Vec<JournalEntry>> {
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
            sj.name
        FROM sys_journal sj
        WHERE
            sj.id = '{}'
        ORDER BY index DESC
        LIMIT {}",
        invocation_id, JOURNAL_QUERY_LIMIT,
    );

    let my_invocation_id: InvocationId = invocation_id.parse().expect("Invocation ID is not valid");

    let mut journal: Vec<_> = client
        .run_query_and_map_results::<JournalRowResult>(query)
        .await?
        .map(|row| {
            let index = row.index.expect("index");
            let entry_type = match row.entry_type.expect("entry_type").as_str() {
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
                "Awakeable" => {
                    JournalEntryType::Awakeable(AwakeableIdentifier::new(my_invocation_id, index))
                }
                "GetState" => JournalEntryType::GetState,
                "SetState" => JournalEntryType::SetState,
                "ClearState" => JournalEntryType::ClearState,
                "Run" => JournalEntryType::SideEffect,
                t => JournalEntryType::Other(t.to_owned()),
            };

            JournalEntry {
                seq: index,
                entry_type,
                completed: row.completed.unwrap_or_default(),
                name: row.name,
            }
        })
        .collect();

    // Sort by seq.
    journal.reverse();
    Ok(journal)
}

#[derive(Debug, Clone, PartialEq, ArrowField, ArrowDeserialize)]
pub struct StateKeysQueryResult {
    service_name: Option<String>,
    service_key: Option<String>,
    key: Option<String>,
    value: Option<Vec<u8>>,
}

pub(crate) async fn get_state_keys(
    client: &DataFusionHttpClient,
    service: &str,
    key: Option<&str>,
) -> Result<HashMap<ServiceId, HashMap<String, Bytes>>> {
    let filter = if let Some(k) = key {
        format!("service_name = '{}' AND service_key = '{}'", service, k)
    } else {
        format!("service_name = '{}'", service)
    };
    let sql = format!(
        "SELECT service_name, service_key, key, value FROM state WHERE {}",
        filter
    );
    let query_result_iter = client
        .run_query_and_map_results::<StateKeysQueryResult>(sql)
        .await?;

    #[allow(clippy::mutable_key_type)]
    let mut user_state: HashMap<ServiceId, HashMap<String, Bytes>> = HashMap::new();
    for row in query_result_iter {
        user_state
            .entry(ServiceId::new(
                row.service_name.expect("service_name"),
                row.service_key.expect("service_key"),
            ))
            .or_default()
            .insert(row.key.expect("key"), Bytes::from(row.value.expect("key")));
    }
    Ok(user_state)
}
