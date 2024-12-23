// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::{Deserialize, Serialize};
use std::ops::RangeInclusive;

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
}

impl From<AdminApiVersion> for (http::HeaderName, http::HeaderValue) {
    fn from(value: AdminApiVersion) -> Self {
        (
            AdminApiVersion::HEADER_NAME,
            http::HeaderValue::from(value.as_repr()),
        )
    }
}

impl AdminApiVersion {
    const HEADER_NAME: http::HeaderName =
        http::HeaderName::from_static("x-restate-admin-api-version");

    pub fn as_repr(&self) -> u16 {
        *self as u16
    }

    pub fn from_headers(headers: &http::HeaderMap) -> Self {
        let is_cli = matches!(headers.get(http::header::USER_AGENT), Some(value) if value.as_bytes().starts_with(b"restate-cli"));

        match headers.get(Self::HEADER_NAME) {
            Some(value) => match value.to_str() {
                Ok(value) => match value.parse::<u16>() {
                    Ok(value) => match Self::try_from(value) {
                        Ok(value) => value,
                        Err(_) => Self::Unknown,
                    },
                    Err(_) => Self::Unknown,
                },
                Err(_) => Self::Unknown,
            },
            // CLI didn't used to send the version header, but if we know its the CLI, then we can treat that as V1
            None if is_cli => Self::V1,
            None => Self::Unknown,
        }
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

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
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
