use std::{collections::HashMap, fmt::Display, str::FromStr};

use anyhow::Result;
use bytes::Bytes;
use chrono::{DateTime, Duration, Local};
use clap::ValueEnum;
use restate_admin_rest_model::version::AdminApiVersion;
use restate_types::{
    identifiers::{AwakeableIdentifier, DeploymentId, ServiceId},
    invocation::ServiceType,
};
use serde::Deserialize;
use serde_with::{serde_as, DeserializeAs};

use super::DataFusionHttpClient;

mod v1;
mod v2;

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
    pub status: InvocationState,
}

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

#[derive(Debug, Clone)]
pub struct OutgoingInvoke {
    pub invocation_id: Option<String>,
    pub invoked_target: Option<String>,
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

macro_rules! v1_converter_fn {
    ($name:tt, ($($v:tt: $t:ty),+) -> $ret:ty) => {
        pub async fn $name(client: &DataFusionHttpClient, $($v: $t),+) -> $ret {
            match client.admin_api_version() {
                AdminApiVersion::V1 => v1::$name(client, $($v),+).await,
                _ => v2::$name(client, $($v),+).await,
            }
        }
    };
}

v1_converter_fn!(count_deployment_active_inv, (deployment_id: &DeploymentId) -> Result<i64>);
v1_converter_fn!(count_deployment_active_inv_by_method, (deployment_id: &DeploymentId) -> Result<Vec<ServiceHandlerUsage>>);
v1_converter_fn!(find_active_invocations_simple, (filter: &str) -> Result<Vec<SimpleInvocation>>);
v1_converter_fn!(get_invocation, (invocation_id: &str) -> Result<Option<Invocation>>);
v1_converter_fn!(get_invocation_journal, (invocation_id: &str) -> Result<Vec<JournalEntry>>);
v1_converter_fn!(find_active_invocations, (filter: &str, post_filter: &str, order: &str, limit: usize) -> Result<(Vec<Invocation>, usize)>);
v1_converter_fn!(get_service_invocations, (service: &str, limit_active: usize) -> Result<Vec<Invocation>> );
v1_converter_fn!(get_state_keys, (service: &str, key: Option<&str>) -> Result<HashMap<ServiceId, HashMap<String, Bytes>>> );
v1_converter_fn!(get_locked_keys_status, (services_filter: impl IntoIterator<Item = impl AsRef<str>>) -> Result<ServiceHandlerLockedKeysMap> );
v1_converter_fn!(get_service_status, (services_filter: impl IntoIterator<Item = impl AsRef<str>>) -> Result<ServiceStatusMap> );
