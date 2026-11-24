// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{borrow::Cow, collections::HashMap, ops::RangeInclusive};

use serde::{Deserialize, Serialize};

use restate_types::net::address::{AdvertisedAddress, HttpIngressPort};
use restate_util_string::ReString;

/// Version of the admin API to allow CLIs to detect if they are incompatible with a server.
///
/// # Important
/// The discriminants of variants must be consecutive without gaps!
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, derive_more::TryFrom, strum::EnumIter,
)]
#[try_from(repr)]
#[repr(u16)]
pub enum AdminApiVersion {
    Unknown = 0,
    V1 = 1,
    V2 = 2,
    V3 = 3,
    /// Added with v1.7.0. Added scope column to sys_state and sys_promise tables. More changes to
    /// follow with the vqueues changes. Removed sys_idempotency table.
    V4 = 4,
    /// Added with v1.8.0. Added AWS-to-GCP workload identity federation to deployment auth.
    V5 = 5,
}

impl AdminApiVersion {
    pub fn as_repr(&self) -> u16 {
        *self as u16
    }

    pub fn choose_max_supported_version(
        client_versions: RangeInclusive<AdminApiVersion>,
        server_versions: RangeInclusive<u16>,
    ) -> Option<AdminApiVersion> {
        if client_versions.end().as_repr() < *server_versions.start()
            || *server_versions.end() < client_versions.start().as_repr()
        {
            // no compatible version if ranges are disjoint
            None
        } else {
            // pick minimum of both ends to obtain maximum supported version
            AdminApiVersion::try_from((*server_versions.end()).min(client_versions.end().as_repr()))
                .ok()
        }
    }
}

/// Admin API version information
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
#[derive(Debug, Serialize, Deserialize)]
pub struct VersionInformation {
    /// # Admin server version
    ///
    /// Version of the admin server
    pub version: String,
    /// # Min admin API version
    ///
    /// Minimum supported admin API version by the admin server
    pub min_admin_api_version: u16,
    /// # Max admin API version
    ///
    /// Maximum supported admin API version by the admin server
    pub max_admin_api_version: u16,
    /// # Ingress endpoint
    ///
    /// Ingress endpoint that the Web UI should use to interact with.
    pub ingress_endpoint: Option<AdvertisedAddress<HttpIngressPort>>,

    /// # Restate experimental features
    ///
    /// List experimental features with their
    /// enabled state.
    pub features: HashMap<Cow<'static, str>, bool>,
    /// # Query engine
    ///
    /// What the SQL query engine offers. Absent on servers older than v1.8.2.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_engine: Option<QueryEngineInformation>,
}

/// What the SQL query engine offers. New fields can be added; clients ignore the ones
/// they don't know.
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryEngineInformation {
    /// # Default query engine
    ///
    /// The engine that runs a query unless the request selects another: `v1` or `v2`.
    #[cfg_attr(feature = "schema", schema(value_type = String))]
    pub default_engine: ReString,
    /// # Statistics tables
    ///
    /// Whether the statistics tables (`sys_service_stats`, `sys_deployment_stats` and
    /// `sys_virtual_object_stats`) answer queries. `false` while queries don't read indexes,
    /// because the tables aren't shown then. Otherwise `true`, from the admin node's start, unless
    /// its last check found a partition whose leader answered and lacks a complete status gauge.
    /// Partitions without a known leader, leaders that are down or don't answer, and leaders whose
    /// partition store isn't open (no registry rows) don't count. The admin node checks every
    /// `admin.query-engine.index-catalog-refresh-interval`.
    pub stats_tables: bool,
}

#[cfg(test)]
mod tests {
    use crate::version::{AdminApiVersion, QueryEngineInformation, VersionInformation};

    #[test]
    fn query_engine_is_optional_and_extensible() {
        let older = r#"{"version":"1.8.1","min_admin_api_version":2,"max_admin_api_version":5,"ingress_endpoint":null,"features":{}}"#;
        let older: VersionInformation = serde_json::from_str(older).unwrap();
        assert_eq!(older.query_engine, None);

        let newer = r#"{"default_engine":"v2","stats_tables":true,"field_from_the_future":1}"#;
        let newer: QueryEngineInformation = serde_json::from_str(newer).unwrap();
        assert_eq!(
            serde_json::to_value(&newer).unwrap(),
            serde_json::json!({"default_engine": "v2", "stats_tables": true})
        );
    }

    #[test]
    fn choose_max_supported_admin_api_version() {
        assert_eq!(
            AdminApiVersion::choose_max_supported_version(
                AdminApiVersion::Unknown..=AdminApiVersion::V1,
                0..=1
            ),
            Some(AdminApiVersion::V1)
        );
        assert_eq!(
            AdminApiVersion::choose_max_supported_version(
                AdminApiVersion::Unknown..=AdminApiVersion::V1,
                0..=10
            ),
            Some(AdminApiVersion::V1)
        );
        assert_eq!(
            AdminApiVersion::choose_max_supported_version(
                AdminApiVersion::Unknown..=AdminApiVersion::V1,
                0..=0
            ),
            Some(AdminApiVersion::Unknown)
        );
        assert_eq!(
            AdminApiVersion::choose_max_supported_version(
                AdminApiVersion::Unknown..=AdminApiVersion::V1,
                42..=42
            ),
            None
        );
    }
}
