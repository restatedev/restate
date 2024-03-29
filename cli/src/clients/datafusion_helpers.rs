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
use arrow::datatypes::ArrowTemporalType;
use arrow::record_batch::RecordBatch;
use clap::ValueEnum;
use restate_meta_rest_model::deployments::DeploymentId;

use anyhow::Result;
use chrono::{DateTime, Duration, Local, TimeZone};
use restate_meta_rest_model::components::ComponentType;
use restate_service_protocol::awakeable_id::AwakeableIdentifier;
use restate_types::identifiers::InvocationId;

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
        }
    }
}

#[derive(Debug, Clone)]
pub struct OutgoingInvoke {
    pub invocation_id: Option<String>,
    pub invoked_component: Option<String>,
    pub invoked_handler: Option<String>,
    pub invoked_component_key: Option<String>,
}

#[derive(Debug, Clone)]
pub struct JournalEntry {
    pub seq: u32,
    pub entry_type: JournalEntryType,
    completed: bool,
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
    Invoke(OutgoingInvoke),
    BackgroundInvoke(OutgoingInvoke),
    Awakeable(AwakeableIdentifier),
    GetState,
    SetState,
    ClearState,
    Other(String),
}

impl JournalEntryType {
    fn is_completable(&self) -> bool {
        matches!(
            self,
            JournalEntryType::Sleep { .. }
                | JournalEntryType::Invoke(_)
                | JournalEntryType::Awakeable(_)
                | JournalEntryType::GetState
        )
    }

    fn should_present(&self) -> bool {
        matches!(
            self,
            JournalEntryType::Sleep { .. }
                | JournalEntryType::Invoke(_)
                | JournalEntryType::BackgroundInvoke(_)
                | JournalEntryType::Awakeable(_)
        )
    }
}

impl Display for JournalEntryType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JournalEntryType::Sleep { .. } => write!(f, "Sleep"),
            JournalEntryType::Invoke(_) => write!(f, "Invoke"),
            JournalEntryType::BackgroundInvoke(_) => write!(f, "BackgroundInvoke"),
            JournalEntryType::Awakeable(_) => write!(f, "Awakeable"),
            JournalEntryType::GetState => write!(f, "GetState"),
            JournalEntryType::SetState => write!(f, "SetState"),
            JournalEntryType::ClearState => write!(f, "ClearState"),
            JournalEntryType::Other(s) => write!(f, "{}", s),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct InvocationDetailed {
    pub invocation: Invocation,
    pub journal: Vec<JournalEntry>,
}

#[derive(Debug, Clone, Default)]
pub struct Invocation {
    pub id: String,
    pub component: String,
    pub handler: String,
    pub key: Option<String>, // Set only on keyed service
    pub created_at: chrono::DateTime<Local>,
    // None if invoked directly (e.g. ingress)
    pub invoked_by_id: Option<String>,
    pub invoked_by_component: Option<String>,
    pub status: InvocationState,
    pub trace_id: Option<String>,

    // If it **requires** this deployment.
    pub pinned_deployment_id: Option<String>,
    pub pinned_deployment_exists: bool,
    pub deployment_id_at_latest_svc_revision: String,
    // Last attempted deployment
    pub last_attempt_deployment_id: Option<String>,

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

pub struct ComponentHandlerUsage {
    pub component: String,
    pub handler: String,
    pub inv_count: i64,
}

/// Key is component name
#[derive(Clone, Default)]
pub struct ComponentStatusMap(HashMap<String, ComponentStatus>);

impl ComponentStatusMap {
    fn set_handler_stats(
        &mut self,
        component: &str,
        handler: &str,
        state: InvocationState,
        stats: HandlerStateStats,
    ) {
        let comp_handlers = self
            .0
            .entry(component.to_owned())
            .or_insert_with(|| ComponentStatus {
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

    pub fn get_component_status(&self, component: &str) -> Option<&ComponentStatus> {
        self.0.get(component)
    }
}

#[derive(Default, Clone)]
pub struct ComponentStatus {
    handlers: HashMap<String, HandlerInfo>,
}

impl ComponentStatus {
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
) -> Result<Vec<ComponentHandlerUsage>> {
    let mut output = vec![];

    let query = format!(
        "SELECT 
            component,
            handler,
            COUNT(id) AS inv_count
            FROM sys_invocation_status
            WHERE pinned_deployment_id = '{}'
            GROUP BY pinned_deployment_id, component, handler",
        deployment_id
    );

    for batch in client.run_query(query).await?.batches {
        for i in 0..batch.num_rows() {
            output.push(ComponentHandlerUsage {
                component: value_as_string(&batch, 0, i),
                handler: value_as_string(&batch, 1, i),
                inv_count: value_as_i64(&batch, 2, i),
            });
        }
    }
    Ok(output)
}

pub async fn get_components_status(
    client: &DataFusionHttpClient,
    components_filter: impl IntoIterator<Item = impl AsRef<str>>,
) -> Result<ComponentStatusMap> {
    let mut status_map = ComponentStatusMap::default();

    let query_filter = format!(
        "({})",
        components_filter
            .into_iter()
            .map(|x| format!("'{}'", x.as_ref()))
            .collect::<Vec<_>>()
            .join(",")
    );
    // Inbox analysis (pending invocations)....
    {
        let query = format!(
            "SELECT 
                component,
                handler,
                COUNT(id),
                MIN(created_at),
                FIRST_VALUE(id ORDER BY created_at ASC)
             FROM sys_inbox WHERE component IN {}
             GROUP BY component, handler",
            query_filter
        );
        let resp = client.run_query(query).await?;
        for batch in resp.batches {
            for i in 0..batch.num_rows() {
                let component = batch.column(0).as_string::<i32>().value_string(i);
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
                status_map.set_handler_stats(&component, &handler, InvocationState::Pending, stats);
            }
        }
    }

    // Active invocations analysis
    {
        let query = format!(
            "WITH enriched_invokes AS
            (SELECT
                ss.component,
                ss.handler,
                CASE
                 WHEN ss.status = 'suspended' THEN 'suspended'
                 WHEN sis.in_flight THEN 'running'
                 WHEN ss.status = 'invoked' AND retry_count > 0 THEN 'backing-off'
                 ELSE 'ready'
                END AS combined_status,
                ss.id,
                ss.created_at
            FROM sys_invocation_status ss
            LEFT JOIN sys_invocation_state sis ON ss.id = sis.id
            WHERE ss.component IN {}
            )
            SELECT component, handler, combined_status, COUNT(id), MIN(created_at), FIRST_VALUE(id ORDER BY created_at ASC)
            FROM enriched_invokes GROUP BY component, handler, combined_status ORDER BY method",
            query_filter
        );
        let resp = client.run_query(query).await?;
        for batch in resp.batches {
            for i in 0..batch.num_rows() {
                let component = value_as_string(&batch, 0, i);
                let handler = value_as_string(&batch, 1, i);
                let status = value_as_string(&batch, 2, i);

                let stats = HandlerStateStats {
                    num_invocations: value_as_i64(&batch, 3, i),
                    oldest_at: value_as_dt_opt(&batch, 4, i).unwrap(),
                    oldest_invocation: value_as_string(&batch, 5, i),
                };

                status_map.set_handler_stats(&component, &handler, status.parse().unwrap(), stats);
            }
        }
    }

    Ok(status_map)
}

// Component -> Locked Keys
#[derive(Default)]
pub struct ComponentHandlerLockedKeysMap {
    components: HashMap<String, HashMap<String, LockedKeyInfo>>,
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

impl ComponentHandlerLockedKeysMap {
    fn insert(&mut self, component: &str, key: String, info: LockedKeyInfo) {
        let locked_keys = self.components.entry(component.to_owned()).or_default();
        locked_keys.insert(key.to_owned(), info);
    }

    fn locked_key_info_mut(&mut self, component: &str, key: &str) -> &mut LockedKeyInfo {
        let locked_keys = self.components.entry(component.to_owned()).or_default();
        locked_keys.entry(key.to_owned()).or_default()
    }

    pub fn into_inner(self) -> HashMap<String, HashMap<String, LockedKeyInfo>> {
        self.components
    }

    pub fn is_empty(&self) -> bool {
        self.components.is_empty()
    }
}

pub async fn get_locked_keys_status(
    client: &DataFusionHttpClient,
    components_filter: impl IntoIterator<Item = impl AsRef<str>>,
) -> Result<ComponentHandlerLockedKeysMap> {
    let mut key_map = ComponentHandlerLockedKeysMap::default();
    let quoted_component_names = components_filter
        .into_iter()
        .map(|x| format!("'{}'", x.as_ref()))
        .collect::<Vec<_>>();
    if quoted_component_names.is_empty() {
        return Ok(key_map);
    }

    let query_filter = format!("({})", quoted_component_names.join(","));

    // Inbox analysis (pending invocations)....
    {
        let query = format!(
            "SELECT 
                component,
                component_key,
                COUNT(id),
                MIN(created_at)
             FROM sys_inbox
             WHERE component IN {}
             GROUP BY component, component_key
             ORDER BY COUNT(id) DESC",
            query_filter
        );
        let resp = client.run_query(query).await?;
        for batch in resp.batches {
            for i in 0..batch.num_rows() {
                let component = batch.column(0).as_string::<i32>().value(i);
                let key = value_as_string(&batch, 1, i);
                let num_pending = value_as_i64(&batch, 2, i);
                let oldest_pending = value_as_dt_opt(&batch, 3, i);

                let info = LockedKeyInfo {
                    num_pending,
                    oldest_pending,
                    ..LockedKeyInfo::default()
                };
                key_map.insert(component, key, info);
            }
        }
    }

    // Active invocations analysis
    {
        let query = format!(
            "WITH enriched_invokes AS
            (SELECT
                ss.component,
                ss.handler,
                ss.component_key,
                CASE
                 WHEN ss.status = 'suspended' THEN 'suspended'
                 WHEN sis.in_flight THEN 'running'
                 WHEN ss.status = 'invoked' AND retry_count > 0 THEN 'backing-off'
                 ELSE 'ready'
                END AS combined_status,
                ss.id,
                ss.created_at,
                ss.modified_at,
                ss.pinned_deployment_id,
                sis.retry_count,
                sis.last_failure,
                sis.last_attempt_deployment_id,
                sis.next_retry_at,
                sis.last_start_at
            FROM sys_invocation_status ss
            LEFT JOIN sys_invocation_state sis ON ss.id = sis.id
            WHERE ss.service IN {}
            )
            SELECT
                component,
                component_key,
                combined_status,
                first_value(id),
                first_value(handler),
                first_value(created_at),
                first_value(modified_at),
                first_value(pinned_deployment_id),
                first_value(last_attempt_deployment_id),
                first_value(last_failure),
                first_value(next_retry_at),
                first_value(last_start_at),
                sum(retry_count)
            FROM enriched_invokes GROUP BY component, component_key, combined_status",
            query_filter
        );

        let resp = client.run_query(query).await?;
        for batch in resp.batches {
            for i in 0..batch.num_rows() {
                let component = value_as_string(&batch, 0, i);
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

                let info = key_map.locked_key_info_mut(&component, &key);

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
            ss.id,
            ss.component,
            ss.handler,
            ss.component_key,
            CASE
             WHEN ss.status = 'suspended' THEN 'suspended'
             WHEN sis.in_flight THEN 'running'
             WHEN ss.status = 'invoked' AND retry_count > 0 THEN 'backing-off'
             ELSE 'ready'
            END AS combined_status,
            ss.created_at,
            ss.modified_at,
            ss.pinned_deployment_id,
            sis.retry_count,
            sis.last_failure,
            sis.last_attempt_deployment_id,
            sis.next_retry_at,
            sis.last_start_at,
            ss.invoked_by_id,
            ss.invoked_by_component,
            comp.ty,
            comp.deployment_id as comp_latest_deployment,
            dp.id as known_deployment_id,
            ss.trace_id
        FROM sys_invocation_status ss
        LEFT JOIN sys_invocation_state sis ON ss.id = sis.id
        LEFT JOIN sys_component comp ON comp.name = ss.component
        LEFT JOIN sys_deployment dp ON dp.id = ss.pinned_deployment_id
        {}
        {}
        )
        SELECT *, COUNT(*) OVER() AS full_count from enriched_invocations
        {}
        LIMIT {}",
        filter, order, post_filter, limit,
    );
    let resp = client.run_query(query).await?;
    for batch in resp.batches {
        for i in 0..batch.num_rows() {
            if full_count == 0 {
                full_count = value_as_i64(&batch, batch.num_columns() - 1, i) as usize;
            }
            let id = value_as_string(&batch, 0, i);
            let component = value_as_string(&batch, 1, i);
            let handler = value_as_string(&batch, 2, i);
            let component_key = value_as_string_opt(&batch, 3, i);
            let status: InvocationState = value_as_string(&batch, 4, i)
                .parse()
                .expect("Unexpected status");
            let created_at = value_as_dt_opt(&batch, 5, i).expect("Missing created_at");

            let state_modified_at = value_as_dt_opt(&batch, 6, i);

            let pinned_deployment_id = value_as_string_opt(&batch, 7, i);

            let num_retries = value_as_u64_opt(&batch, 8, i);

            let last_failure_message = value_as_string_opt(&batch, 9, i);
            let last_attempt_deployment_id = value_as_string_opt(&batch, 10, i);

            let next_retry_at = value_as_dt_opt(&batch, 11, i);
            let last_start = value_as_dt_opt(&batch, 12, i);

            let invoked_by_id = value_as_string_opt(&batch, 13, i);
            let invoked_by_component = value_as_string_opt(&batch, 14, i);
            let component_type = parse_component_type(&value_as_string(&batch, 15, i));
            let deployment_id_at_latest_svc_revision = value_as_string(&batch, 16, i);

            let existing_pinned_deployment_id = value_as_string_opt(&batch, 17, i);
            let trace_id = value_as_string_opt(&batch, 18, i);

            let key = if component_type == ComponentType::VirtualObject {
                component_key
            } else {
                None
            };

            let mut invocation = Invocation {
                id,
                status,
                component,
                key,
                handler,
                created_at,
                invoked_by_id,
                invoked_by_component,
                state_modified_at,
                num_retries,
                next_retry_at,
                pinned_deployment_id,
                pinned_deployment_exists: existing_pinned_deployment_id.is_some(),
                deployment_id_at_latest_svc_revision,
                last_failure_message,
                last_attempt_deployment_id,
                trace_id,
                ..Default::default()
            };

            // Running duration
            if status == InvocationState::Running {
                invocation.current_attempt_duration =
                    last_start.map(|last_start| Local::now().signed_duration_since(last_start));
            }

            if invocation.status == InvocationState::BackingOff {
                invocation.last_attempt_started_at = last_start;
            }

            active.push(invocation);
        }
    }
    Ok((active, full_count))
}

pub async fn find_inbox_invocations(
    client: &DataFusionHttpClient,
    filter: &str,
    order: &str,
    limit: usize,
) -> Result<(Vec<Invocation>, usize)> {
    let mut inbox: Vec<Invocation> = Vec::new();
    // Inbox...
    let mut full_count = 0;
    {
        let query = format!(
            "WITH inbox_table AS
            (SELECT
                ss.component,
                ss.handler,
                ss.id,
                ss.created_at,
                ss.invoked_by_id,
                ss.invoked_by_component,
                ss.component_key,
                comp.ty,
                ss.trace_id
             FROM sys_inbox ss
             LEFT JOIN sys_component comp ON comp.name = ss.component
             {}
             {}
            )
            SELECT *, COUNT(*) OVER() AS full_count FROM inbox_table
            LIMIT {}",
            filter, order, limit
        );
        let resp = client.run_query(query).await?;
        for batch in resp.batches {
            for i in 0..batch.num_rows() {
                if full_count == 0 {
                    full_count = value_as_i64(&batch, batch.num_columns() - 1, i) as usize;
                }
                let component_type = parse_component_type(&value_as_string(&batch, 7, i));
                let key = if component_type == ComponentType::VirtualObject {
                    value_as_string_opt(&batch, 6, i)
                } else {
                    None
                };

                let invocation = Invocation {
                    status: InvocationState::Pending,
                    component: value_as_string(&batch, 0, i),
                    handler: value_as_string(&batch, 1, i),
                    id: value_as_string(&batch, 2, i),
                    created_at: value_as_dt_opt(&batch, 3, i).expect("Missing created_at"),
                    key,
                    invoked_by_id: value_as_string_opt(&batch, 4, i),
                    invoked_by_component: value_as_string_opt(&batch, 5, i),
                    trace_id: value_as_string_opt(&batch, 8, i),
                    ..Default::default()
                };
                inbox.push(invocation);
            }
        }
    }
    Ok((inbox, full_count))
}

pub async fn get_component_invocations(
    client: &DataFusionHttpClient,
    component: &str,
    limit_inbox: usize,
    limit_active: usize,
) -> Result<(Vec<Invocation>, Vec<Invocation>)> {
    // Inbox...
    let inbox: Vec<Invocation> = find_inbox_invocations(
        client,
        &format!("WHERE ss.component = '{}'", component),
        "ORDER BY ss.created_at DESC",
        limit_inbox,
    )
    .await?
    .0;

    // Active invocations analysis
    let active: Vec<Invocation> = find_active_invocations(
        client,
        &format!("WHERE ss.component = '{}'", component),
        "",
        "ORDER BY ss.created_at DESC",
        limit_active,
    )
    .await?
    .0;

    Ok((inbox, active))
}

fn parse_component_type(s: &str) -> ComponentType {
    match s {
        "service" => ComponentType::Service,
        "virtual_object" => ComponentType::VirtualObject,
        _ => panic!("Unexpected instance type"),
    }
}

pub async fn get_invocation(
    client: &DataFusionHttpClient,
    invocation_id: &str,
) -> Result<Option<Invocation>> {
    // Is it in inbox?
    let result =
        find_inbox_invocations(client, &format!("WHERE ss.id = '{}'", invocation_id), "", 1)
            .await?
            .0
            .pop();

    if result.is_none() {
        // Maybe it's active
        return Ok(find_active_invocations(
            client,
            &format!("WHERE ss.id = '{}'", invocation_id),
            "",
            "",
            1,
        )
        .await?
        .0
        .pop());
    }

    Ok(result)
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
            sj.invoked_component,
            sj.invoked_handler,
            sj.invoked_component_key,
            sj.sleep_wakeup_at
        FROM sys_journal sj
        WHERE
            sj.invocation_id = '{}'
        ORDER BY index DESC
        LIMIT {}",
        invocation_id, JOURNAL_QUERY_LIMIT,
    );

    let my_invocation_id: InvocationId = invocation_id.parse().expect("Invocation ID is not valid");
    let resp = client.run_query(query).await?;
    let mut journal = vec![];
    for batch in resp.batches {
        for i in 0..batch.num_rows() {
            let index = batch
                .column(0)
                .as_primitive::<arrow::datatypes::UInt32Type>()
                .value(i);

            let entry_type = value_as_string(&batch, 1, i);
            let completed = batch.column(2).as_boolean().value(i);
            let outgoing_invocation_id = value_as_string_opt(&batch, 3, i);
            let invoked_component = value_as_string_opt(&batch, 4, i);
            let invoked_handler = value_as_string_opt(&batch, 5, i);
            let invoked_component_key = value_as_string_opt(&batch, 6, i);
            let wakeup_at = value_as_dt_opt(&batch, 7, i);

            let entry_type = match entry_type.as_str() {
                "Sleep" => JournalEntryType::Sleep { wakeup_at },
                "Invoke" => JournalEntryType::Invoke(OutgoingInvoke {
                    invocation_id: outgoing_invocation_id,
                    invoked_component,
                    invoked_handler,
                    invoked_component_key,
                }),
                "BackgroundInvoke" => JournalEntryType::BackgroundInvoke(OutgoingInvoke {
                    invocation_id: outgoing_invocation_id,
                    invoked_component,
                    invoked_handler,
                    invoked_component_key,
                }),
                "Awakeable" => JournalEntryType::Awakeable(AwakeableIdentifier::new(
                    my_invocation_id.clone(),
                    index,
                )),
                "GetState" => JournalEntryType::GetState,
                "SetState" => JournalEntryType::SetState,
                "ClearState" => JournalEntryType::ClearState,
                t => JournalEntryType::Other(t.to_owned()),
            };

            journal.push(JournalEntry {
                seq: index,
                entry_type,
                completed,
            });
        }
    }

    // Sort by seq.
    journal.reverse();
    Ok(journal)
}
