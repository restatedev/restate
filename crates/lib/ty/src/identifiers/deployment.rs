// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::ulid_backed_id;

ulid_backed_id!(Deployment @with_resource_id);

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::identifiers::{DeploymentId, IdDecodeError, ResourceId, TimestampAwareId};

    #[test]
    fn test_deployment_id_from_str() {
        let deployment_id = "dp_11nGQpCRmau6ypL82KH2TnP";
        let from_str_result = DeploymentId::from_str(deployment_id);
        assert!(from_str_result.is_ok());
        assert_eq!(
            from_str_result.unwrap().to_string(),
            deployment_id.to_string()
        );

        let deployment_id = "dp_11nGQpCRmau6ypL82KH2TnP123456";
        let from_str_result = DeploymentId::from_str(deployment_id);
        assert!(from_str_result.is_err());
        assert_eq!(from_str_result.unwrap_err(), IdDecodeError::Length);
    }

    #[test]
    fn test_deployment_id_format() {
        let a = DeploymentId::new();
        assert!(a.timestamp_ms() > 0);
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
