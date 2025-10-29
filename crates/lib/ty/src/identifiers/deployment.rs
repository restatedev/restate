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

    use crate::identifiers::{DeploymentId, IdDecodeError};

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
}
