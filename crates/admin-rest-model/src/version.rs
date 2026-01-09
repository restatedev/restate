// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::RangeInclusive;

use serde::{Deserialize, Serialize};

use restate_types::net::address::{AdvertisedAddress, HttpIngressPort};

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
}

#[cfg(test)]
mod tests {
    use crate::version::AdminApiVersion;

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
