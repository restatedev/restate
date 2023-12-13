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

use arrow::array::{as_string_array, Array, AsArray};
use restate_meta_rest_model::endpoints::EndpointId;

use anyhow::Result;
use chrono::{DateTime, Duration, Local, TimeZone};

pub async fn count_deployment_active_inv(
    client: &DataFusionHttpClient,
    endpoint_id: &EndpointId,
) -> Result<i64> {
    Ok(client
        .run_count_agg_query(format!(
            "SELECT COUNT(id) AS inv_count \
            FROM sys_status \
            WHERE pinned_endpoint_id = '{}' \
            GROUP BY pinned_endpoint_id",
            endpoint_id
        ))
        .await?)
}

pub struct ServiceMethodUsage {
    pub service: String,
    pub method: String,
    pub inv_count: i64,
}

pub async fn count_deployment_active_inv_by_method(
    client: &DataFusionHttpClient,
    endpoint_id: &EndpointId,
) -> Result<Vec<ServiceMethodUsage>> {
    let query = format!(
        "SELECT service, method, COUNT(id) AS inv_count \
            FROM sys_status \
            WHERE pinned_endpoint_id = '{}' \
            GROUP BY pinned_endpoint_id, service, method",
        endpoint_id
    );

    let resp = client.run_query(query).await?;
    let batches = resp.batches;

    let mut output = vec![];
    for batch in batches {
        let col = batch.column(0);
        let services = as_string_array(col);
        let col = batch.column(1);
        let methods = as_string_array(col);
        let col = batch.column(2);
        let inv_counts = col.as_primitive::<arrow::datatypes::Int64Type>();

        for i in 0..batch.num_rows() {
            let service = services.value(i).to_owned();
            let method = methods.value(i).to_owned();
            let inv_count = inv_counts.value(i);
            output.push(ServiceMethodUsage {
                service,
                method,
                inv_count,
            });
        }
    }
    Ok(output)
}

// -- Service status helpers and types --

/// Key is service name
#[derive(Clone, Default)]
pub struct ServiceStatusMap(HashMap<String, ServiceStatus>);

impl ServiceStatusMap {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub fn set_method_stats(
        &mut self,
        service: &str,
        method: &str,
        state: EnrichedInvocationState,
        stats: MethodStateStats,
    ) {
        let svc_methods = self
            .0
            .entry(service.to_owned())
            .or_insert_with(|| ServiceStatus {
                methods: HashMap::new(),
            });

        let method_info = svc_methods
            .methods
            .entry(method.to_owned())
            .or_insert_with(|| MethodInfo {
                per_state_totals: HashMap::new(),
            });

        method_info.per_state_totals.insert(state, stats);
    }

    pub fn get_service_status(&self, service: &str) -> Option<&ServiceStatus> {
        self.0.get(service)
    }
}

#[derive(Copy, Clone, Eq, Hash, PartialEq, Debug)]
pub enum EnrichedInvocationState {
    Unknown,
    Pending,
    Ready,
    Running,
    Suspended,
    BackingOff,
}

impl FromStr for EnrichedInvocationState {
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

impl Display for EnrichedInvocationState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EnrichedInvocationState::Unknown => write!(f, "unknown"),
            EnrichedInvocationState::Pending => write!(f, "pending"),
            EnrichedInvocationState::Ready => write!(f, "ready"),
            EnrichedInvocationState::Running => write!(f, "running"),
            EnrichedInvocationState::Suspended => write!(f, "suspended"),
            EnrichedInvocationState::BackingOff => write!(f, "backing-off"),
        }
    }
}

#[derive(Default, Clone)]
pub struct ServiceStatus {
    pub methods: HashMap<String, MethodInfo>,
}

impl ServiceStatus {
    pub fn get_method_stats(
        &self,
        state: EnrichedInvocationState,
        method: &str,
    ) -> Option<&MethodStateStats> {
        self.methods.get(method).and_then(|x| x.get_stats(state))
    }

    pub fn get_method(&self, method: &str) -> Option<&MethodInfo> {
        self.methods.get(method)
    }
}

#[derive(Default, Clone)]
pub struct MethodInfo {
    pub per_state_totals: HashMap<EnrichedInvocationState, MethodStateStats>,
}

impl MethodInfo {
    pub fn get_stats(&self, state: EnrichedInvocationState) -> Option<&MethodStateStats> {
        self.per_state_totals.get(&state)
    }

    pub fn oldest_non_suspended_invocation_state(
        &self,
    ) -> Option<(EnrichedInvocationState, &MethodStateStats)> {
        let mut oldest: Option<(EnrichedInvocationState, &MethodStateStats)> = None;
        for (state, stats) in &self.per_state_totals {
            if state == &EnrichedInvocationState::Suspended {
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
pub struct MethodStateStats {
    pub num_invocations: i64,
    pub oldest_at: chrono::DateTime<Local>,
    pub oldest_invocation: String,
}

pub async fn get_services_status(
    client: &DataFusionHttpClient,
    services_filter: impl IntoIterator<Item = impl AsRef<str>>,
) -> Result<ServiceStatusMap> {
    let mut status_map = ServiceStatusMap::new();

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
                service_name, 
                method,
                COUNT(id),
                MIN(created_at),
                FIRST_VALUE(id ORDER BY created_at ASC)
             FROM sys_inbox WHERE service_name IN {}
             GROUP BY service_name, method",
            query_filter
        );
        let resp = client.run_query(query).await?;
        for batch in resp.batches {
            let col = batch.column(0);
            let services = as_string_array(col);
            let col = batch.column(1);
            let methods = as_string_array(col);
            let col = batch.column(2);
            let inv_counts = col.as_primitive::<arrow::datatypes::Int64Type>();
            let col = batch.column(3);
            let oldest_ats = col.as_primitive::<arrow::datatypes::Date64Type>();
            let col = batch.column(4);
            let oldest_invs = as_string_array(col);

            for i in 0..batch.num_rows() {
                let service = services.value(i).to_owned();
                let method = methods.value(i).to_owned();
                let num_invocations = inv_counts.value(i);
                let oldest_at: DateTime<Local> = Local
                    .timestamp_millis_opt(oldest_ats.value(i))
                    .latest()
                    .expect("Invalid timestamp");

                let oldest_invocation = oldest_invs.value(i).to_owned();

                let stats = MethodStateStats {
                    num_invocations,
                    oldest_at,
                    oldest_invocation,
                };
                status_map.set_method_stats(
                    &service,
                    &method,
                    EnrichedInvocationState::Pending,
                    stats,
                );
            }
        }
    }

    // Active invocations analysis
    {
        let query = format!(
            "WITH enriched_invokes AS
            (SELECT
                ss.service,
                ss.method,
                CASE
                 WHEN ss.status = 'suspended' THEN 'suspended'
                 WHEN sis.in_flight THEN 'running'
                 WHEN ss.status = 'invoked' AND retry_count > 0 THEN 'backing-off'
                 ELSE 'ready'
                END AS combined_status,
                ss.id,
                ss.created_at
            FROM sys_status ss
            LEFT JOIN sys_invocation_state sis ON ss.id = sis.id
            WHERE ss.service IN {}
            )
            SELECT service, method, combined_status, COUNT(id), MIN(created_at), FIRST_VALUE(id
            ORDER BY created_at ASC)
            FROM enriched_invokes GROUP BY service, method, combined_status ORDER BY method",
            query_filter
        );
        let resp = client.run_query(query).await?;
        for batch in resp.batches {
            let col = batch.column(0);
            let services = as_string_array(col);

            let col = batch.column(1);
            let methods = as_string_array(col);

            let col = batch.column(2);
            let statuses = as_string_array(col);

            let col = batch.column(3);
            let inv_counts = col.as_primitive::<arrow::datatypes::Int64Type>();

            let col = batch.column(4);
            let oldest = col.as_primitive::<arrow::datatypes::Date64Type>();

            let col = batch.column(5);
            let oldest_invs = as_string_array(col);

            for i in 0..batch.num_rows() {
                let service = services.value(i).to_owned();
                let method = methods.value(i).to_owned();
                let status = statuses.value(i).to_owned();
                let num_invocations = inv_counts.value(i);
                let oldest_at: DateTime<Local> = Local
                    .timestamp_millis_opt(oldest.value(i))
                    .latest()
                    .expect("Invalid timestamp");

                let oldest_invocation = oldest_invs.value(i).to_owned();

                let stats = MethodStateStats {
                    num_invocations,
                    oldest_at,
                    oldest_invocation,
                };

                status_map.set_method_stats(&service, &method, status.parse().unwrap(), stats);
            }
        }
    }

    Ok(status_map)
}

// Service -> Locked Keys
#[derive(Default)]
pub struct ServiceMethodLockedKeysMap {
    services: HashMap<String, HashMap<String, LockedKeyInfo>>,
}

#[derive(Clone, Default, Debug)]
pub struct LockedKeyInfo {
    pub num_pending: i64,
    pub oldest_pending: Option<chrono::DateTime<Local>>,
    // Who is holding the lock
    pub invocation_holding_lock: Option<String>,
    pub invocation_method_holding_lock: Option<String>,
    pub invocation_status: Option<EnrichedInvocationState>,
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

impl ServiceMethodLockedKeysMap {
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
}

pub async fn get_locked_keys_status(
    client: &DataFusionHttpClient,
    services_filter: impl IntoIterator<Item = impl AsRef<str>>,
) -> Result<ServiceMethodLockedKeysMap> {
    let mut key_map = ServiceMethodLockedKeysMap::default();
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
            "SELECT service_name, service_key, COUNT(id), MIN(created_at)
             FROM sys_inbox WHERE service_name IN {}
             GROUP BY service_name, service_key ORDER BY COUNT(id) DESC",
            query_filter
        );
        let resp = client.run_query(query).await?;
        for batch in resp.batches {
            let col = batch.column(0);
            let services = as_string_array(col);
            let col = batch.column(1);
            let keys = as_string_array(col);
            let col = batch.column(2);
            let inv_counts = col.as_primitive::<arrow::datatypes::Int64Type>();
            let col = batch.column(3);
            let oldest = col.as_primitive::<arrow::datatypes::Date64Type>();

            for i in 0..batch.num_rows() {
                let service = services.value(i);
                let key = keys.value(i);
                let num_pending = inv_counts.value(i);
                let oldest_pending = Some(
                    Local
                        .timestamp_millis_opt(oldest.value(i))
                        .latest()
                        .expect("Invalid timestamp"),
                );

                let info = LockedKeyInfo {
                    num_pending,
                    oldest_pending,
                    ..LockedKeyInfo::default()
                };
                key_map.insert(service, key.to_owned(), info);
            }
        }
    }

    // Active invocations analysis
    {
        let query = format!(
            "WITH enriched_invokes AS
            (SELECT
                ss.service,
                ss.method,
                ss.service_key,
                CASE
                 WHEN ss.status = 'suspended' THEN 'suspended'
                 WHEN sis.in_flight THEN 'running'
                 WHEN ss.status = 'invoked' AND retry_count > 0 THEN 'backing-off'
                 ELSE 'ready'
                END AS combined_status,
                ss.id,
                ss.created_at,
                ss.modified_at,
                ss.pinned_endpoint_id,
                sis.retry_count,
                sis.last_failure,
                sis.last_attempt_endpoint_id,
                sis.next_retry_at,
                sis.last_start_at
            FROM sys_status ss
            LEFT JOIN sys_invocation_state sis ON ss.id = sis.id
            WHERE ss.service IN {}
            )
            SELECT
                service,
                service_key,
                combined_status,
                first_value(id),
                first_value(method),
                first_value(created_at),
                first_value(modified_at),
                first_value(pinned_endpoint_id),
                first_value(last_attempt_endpoint_id),
                first_value(last_failure),
                first_value(next_retry_at),
                first_value(last_start_at),
                sum(retry_count)
            FROM enriched_invokes GROUP BY service, service_key, combined_status",
            query_filter
        );

        let resp = client.run_query(query).await?;
        for batch in resp.batches {
            let col = batch.column(0);
            let services = as_string_array(col);

            let col = batch.column(1);
            let keys = as_string_array(col);

            let col = batch.column(2);
            let statuses = as_string_array(col);

            let col = batch.column(3);
            let ids = as_string_array(col);

            let col = batch.column(4);
            let methods = as_string_array(col);

            let col = batch.column(5);
            let created_ats = col.as_primitive::<arrow::datatypes::Date64Type>();

            let col = batch.column(6);
            let modified_ats = col.as_primitive::<arrow::datatypes::Date64Type>();

            let col = batch.column(7);
            let pinned_eps = as_string_array(col);

            let col = batch.column(8);
            let last_attempt_eps = as_string_array(col);

            let col = batch.column(9);
            let last_failures = as_string_array(col);

            let col = batch.column(10);
            let next_retries = col.as_primitive::<arrow::datatypes::Date64Type>();

            let col = batch.column(11);
            let last_starts = col.as_primitive::<arrow::datatypes::Date64Type>();

            let col = batch.column(12);
            let num_retries = col.as_primitive::<arrow::datatypes::UInt64Type>();

            for i in 0..batch.num_rows() {
                let service = services.value(i);
                let key = keys.value(i);

                let info = key_map.locked_key_info_mut(service, key);

                // current invocation
                let status = statuses.value(i).parse().expect("Unexpected status");
                info.invocation_status = Some(status);
                info.invocation_holding_lock = Some(ids.value(i).to_owned());
                info.invocation_method_holding_lock = Some(methods.value(i).to_owned());
                info.invocation_created_at = Some(
                    Local
                        .timestamp_millis_opt(created_ats.value(i))
                        .latest()
                        .expect("Invalid timestamp"),
                );

                // Running duration
                if status == EnrichedInvocationState::Running && !last_starts.is_null(i) {
                    let last_start = Local
                        .timestamp_millis_opt(last_starts.value(i))
                        .latest()
                        .expect("Invalid timestamp");
                    info.invocation_attempt_duration =
                        Some(Local::now().signed_duration_since(last_start));
                }

                // State duration
                if !modified_ats.is_null(i) {
                    let last_modified = Local
                        .timestamp_millis_opt(modified_ats.value(i))
                        .latest()
                        .expect("Invalid timestamp");
                    info.invocation_state_duration =
                        Some(Local::now().signed_duration_since(last_modified));
                }

                // Num retries
                if !num_retries.is_null(i) {
                    info.num_retries = Some(num_retries.value(i));
                }
                // Next retry
                if !next_retries.is_null(i) {
                    info.next_retry_at = Some(
                        Local
                            .timestamp_millis_opt(next_retries.value(i))
                            .latest()
                            .expect("Invalid timestamp"),
                    );
                }

                if !pinned_eps.is_null(i) {
                    info.pinned_deployment_id = Some(pinned_eps.value(i).to_owned());
                }

                if !last_failures.is_null(i) {
                    info.last_failure_message = Some(last_failures.value(i).to_owned());
                }

                if !last_attempt_eps.is_null(i) {
                    info.last_attempt_deployment_id = Some(last_attempt_eps.value(i).to_owned());
                }
            }
        }
    }

    Ok(key_map)
}
