// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{collections::HashMap, fmt::Display, str::FromStr};

use anyhow::Result;
use chrono::{DateTime, Duration, Local};
use clap::ValueEnum;
use restate_types::invocation::ServiceType;
use restate_types::journal_events::{Event, TransientErrorEvent};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_with::{DeserializeAs, SerializeAs, serde_as};

mod locks;
mod v2;

pub use locks::*;
pub use v2::*;

#[derive(Deserialize)]
pub struct ServiceHandlerUsage {
    pub service: String,
    pub handler: String,
    pub inv_count: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SimpleInvocation {
    pub id: String,
    pub target: String,
    /// The `sys_invocation_status.status` value (e.g. `completed`, `suspended`), with
    /// `invoked` refined to the live status (e.g. `running`, `backing-off`).
    pub status: String,
}

#[derive(
    ValueEnum,
    Copy,
    Clone,
    Eq,
    Hash,
    PartialEq,
    Debug,
    Default,
    serde_with::DeserializeFromStr,
    serde_with::SerializeDisplay,
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
    Completed,
    Paused,
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
            "paused" => Self::Paused,
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
            InvocationState::Paused => write!(f, "paused"),
            InvocationState::BackingOff => write!(f, "backing-off"),
            InvocationState::Completed => write!(f, "completed"),
        }
    }
}

#[serde_as]
#[derive(Debug, Clone, Deserialize, Serialize)]
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
    pub pinned_deployment_exists: bool,
    // Last attempted deployment
    pub last_attempt_deployment_id: Option<String>,
    pub last_attempt_server: Option<String>,

    // if running, how long has it been running?
    pub current_attempt_duration: Option<Duration>,
    // E.g. If suspended, since when?
    pub state_modified_at: Option<DateTime<Local>>,
    // Lifecycle timestamps, when the invocation went through that stage.
    pub inboxed_at: Option<DateTime<Local>>,
    pub scheduled_at: Option<DateTime<Local>>,
    pub scheduled_start_at: Option<DateTime<Local>>,
    pub running_at: Option<DateTime<Local>>,
    pub completed_at: Option<DateTime<Local>>,

    // If backing-off: from the VQueue entry status (not `sys_invocation`).
    pub num_retries: Option<u64>,
    pub next_retry_at: Option<DateTime<Local>>,

    pub last_attempt_started_at: Option<DateTime<Local>>,
    // Last failure: from the latest `TransientError` / `Paused` journal event.
    pub last_failure_message: Option<String>,
    pub last_failure_entry_name: Option<String>,
    pub last_failure_entry_ty: Option<String>,
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

impl SerializeAs<ServiceType> for DatafusionServiceType {
    fn serialize_as<S>(source: &ServiceType, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let repr = match source {
            ServiceType::Service => "service",
            ServiceType::VirtualObject => "virtual_object",
            ServiceType::Workflow => "workflow",
        };
        serializer.serialize_str(repr)
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

/// Which slice of an invocation's journal to fetch (see [`get_journal`](v2::get_journal)).
#[derive(Debug, Clone, Copy)]
pub enum JournalFetch {
    /// The first `head` and last `tail` entries (a preview of large journals).
    Preview { head: u32, tail: u32 },
    /// The entire journal.
    All,
    /// A single entry by index.
    One(u32),
    /// An inclusive index range; `None` bounds are open.
    Range(Option<u32>, Option<u32>),
}

/// A single journal entry fetched from `sys_journal`. `lite`/`full` are the parsed
/// `entry_lite_json` (metadata projection) and `entry_json` (full payload).
#[derive(Debug, Clone)]
pub struct JournalEntryRow {
    pub index: u32,
    pub entry_type: String,
    pub name: Option<String>,
    pub appended_at: Option<DateTime<Local>>,
    pub lite: Option<Value>,
    pub full: Option<Value>,
}

/// A single event from `sys_journal_events` (decoded events attached to an invocation's
/// timeline, ordered relative to journal entries by `after_journal_entry_index`).
#[derive(Debug, Clone)]
pub struct JournalEventRow {
    pub after_journal_entry_index: u32,
    pub appended_at: Option<DateTime<Local>>,
    pub event_type: String,
    pub event: Option<Value>,
}

impl JournalEventRow {
    /// The typed event; `None` when this CLI can't decode it (e.g. a newer event type).
    pub fn decoded(&self) -> Option<Event> {
        serde_json::from_value(self.event.clone()?).ok()
    }
}

/// The failure an event reports: a transient error, or the one that paused the invocation.
pub fn event_failure(event: &Event) -> Option<&TransientErrorEvent> {
    match event {
        Event::TransientError(failure) => Some(failure),
        Event::Paused(paused) => paused.last_failure.as_ref(),
        _ => None,
    }
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
