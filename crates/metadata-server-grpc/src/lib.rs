// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub mod grpc;

use std::collections::HashMap;

use itertools::Itertools;

use restate_types::errors::GenericError;
use restate_types::{PlainNodeId, Version};

#[derive(Debug, thiserror::Error)]
pub enum ProvisionError {
    #[error("failed provisioning: {0}")]
    Internal(GenericError),
}

#[derive(Debug, thiserror::Error)]
#[error("invalid nodes configuration: {0}")]
pub struct InvalidConfiguration(String);

type CreatedAtMillis = i64;

#[derive(Clone, Debug, prost_dto::IntoProst, prost_dto::FromProst, derive_more::Display)]
#[prost(target = "crate::grpc::MetadataServerConfiguration")]
#[display("{version}; [{}]", members.keys().format(", "))]
pub struct MetadataServerConfiguration {
    #[prost(required)]
    pub version: Version,
    pub members: HashMap<PlainNodeId, CreatedAtMillis>,
}

impl MetadataServerConfiguration {
    pub fn contains(&self, node_id: PlainNodeId) -> bool {
        self.members.contains_key(&node_id)
    }

    pub fn num_members(&self) -> usize {
        self.members.len()
    }

    pub fn version(&self) -> Version {
        self.version
    }
}

impl Default for MetadataServerConfiguration {
    fn default() -> Self {
        MetadataServerConfiguration {
            version: Version::INVALID,
            members: HashMap::default(),
        }
    }
}

#[cfg(any(test, feature = "test-util"))]
pub mod tests {
    use restate_types::{Version, Versioned, flexbuffers_storage_encode_decode};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialOrd, PartialEq, Serialize, Deserialize)]
    pub struct Value {
        pub version: Version,
        pub value: u32,
    }

    impl Default for Value {
        fn default() -> Self {
            Self {
                version: Version::MIN,
                value: Default::default(),
            }
        }
    }

    impl Value {
        pub fn new(value: u32) -> Self {
            Value {
                value,
                ..Value::default()
            }
        }

        pub fn next_version(mut self) -> Self {
            self.version = self.version.next();
            self
        }
    }

    impl Versioned for Value {
        fn version(&self) -> Version {
            self.version
        }
    }

    flexbuffers_storage_encode_decode!(Value);
}
