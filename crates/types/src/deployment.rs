// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::identifiers::DeploymentId;
use crate::service_protocol::ServiceProtocolVersion;

/// Deployment which was chosen to run an invocation on.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PinnedDeployment {
    pub deployment_id: DeploymentId,
    pub service_protocol_version: ServiceProtocolVersion,
}

impl PinnedDeployment {
    pub fn new(
        deployment_id: DeploymentId,
        service_protocol_version: ServiceProtocolVersion,
    ) -> Self {
        Self {
            deployment_id,
            service_protocol_version,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::identifiers::{ResourceId, TimestampAwareId};

    #[test]
    fn test_deployment_id_format() {
        let a = DeploymentId::new();
        assert!(a.timestamp().as_u64() > 0);
        let a_str = a.to_string();
        assert!(a_str.starts_with("dp_"));
        assert_eq!(a_str.len(), DeploymentId::str_encoded_len(),);
        assert_eq!(26, a_str.len());
    }

    #[test]
    fn test_deployment_roundtrip() {
        let a = DeploymentId::new();
        let b: DeploymentId = a.to_string().parse().unwrap();
        assert_eq!(a, b);
        assert_eq!(a.to_string(), b.to_string());
    }
}
