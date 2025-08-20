// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

include!(concat!(
    env!("OUT_DIR"),
    "/dev.restate.service.discovery.rs"
));

/// Min/max supported service discovery protocol versions by this server version.
pub const MIN_SERVICE_DISCOVERY_PROTOCOL_VERSION: ServiceDiscoveryProtocolVersion =
    ServiceDiscoveryProtocolVersion::V1;
pub const MAX_SERVICE_DISCOVERY_PROTOCOL_VERSION: ServiceDiscoveryProtocolVersion =
    ServiceDiscoveryProtocolVersion::V4;

impl ServiceDiscoveryProtocolVersion {
    pub fn as_repr(&self) -> i32 {
        i32::from(*self)
    }

    pub fn is_supported(&self) -> bool {
        MIN_SERVICE_DISCOVERY_PROTOCOL_VERSION <= *self
            && *self <= MAX_SERVICE_DISCOVERY_PROTOCOL_VERSION
    }
}
