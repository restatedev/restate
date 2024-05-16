// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;
use std::fmt::{Display, Formatter};
use std::mem::size_of;
use std::str::FromStr;

use ulid::Ulid;

use crate::base62_util::base62_max_length_for_type;
use crate::errors::IdDecodeError;
use crate::id_util::{IdDecoder, IdEncoder, IdResourceType};
use crate::identifiers::{DeploymentId, ResourceId, TimestampAwareId};
use crate::service_protocol::ServiceProtocolVersion;
use crate::time::MillisSinceEpoch;

impl ResourceId for DeploymentId {
    const SIZE_IN_BYTES: usize = size_of::<u128>();
    const RESOURCE_TYPE: IdResourceType = IdResourceType::Deployment;
    const STRING_CAPACITY_HINT: usize = base62_max_length_for_type::<u128>();
    fn push_contents_to_encoder(&self, encoder: &mut IdEncoder<Self>) {
        let ulid_raw: u128 = self.0.into();
        encoder.encode_fixed_width(ulid_raw);
    }
}

impl TimestampAwareId for DeploymentId {
    fn timestamp(&self) -> MillisSinceEpoch {
        self.0.timestamp_ms().into()
    }
}

impl FromStr for DeploymentId {
    type Err = IdDecodeError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let mut decoder = IdDecoder::new(input)?;
        // Ensure we are decoding the correct resource type
        if decoder.resource_type != Self::RESOURCE_TYPE {
            return Err(IdDecodeError::TypeMismatch);
        }

        // ulid (u128)
        let raw_ulid: u128 = decoder.cursor.decode_next()?;
        Ok(Self::from(raw_ulid))
    }
}

impl fmt::Display for DeploymentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut encoder = IdEncoder::<Self>::new();
        self.push_contents_to_encoder(&mut encoder);
        fmt::Display::fmt(&encoder.finalize(), f)
    }
}

impl From<u128> for DeploymentId {
    fn from(value: u128) -> Self {
        Self(Ulid::from(value))
    }
}

// Passthrough json schema to the string
#[cfg(feature = "schemars")]
impl schemars::JsonSchema for DeploymentId {
    fn schema_name() -> String {
        <String as schemars::JsonSchema>::schema_name()
    }

    fn json_schema(g: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        <String as schemars::JsonSchema>::json_schema(g)
    }
}

/// Deployment which was chosen to run an invocation on.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PinnedDeployment {
    pub deployment_id: DeploymentId,
    pub service_protocol_version: ServiceProtocolVersion,
}

impl Display for PinnedDeployment {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "id: {}, service protocol version: {}",
            self.deployment_id,
            self.service_protocol_version.as_repr()
        )
    }
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

    #[test]
    fn test_deployment_id_format() {
        let a = DeploymentId::new();
        assert!(a.timestamp().as_u64() > 0);
        let a_str = a.to_string();
        assert!(a_str.starts_with("dp_"));
        assert_eq!(DeploymentId::STRING_CAPACITY_HINT + 4, a_str.len());
        assert_eq!(
            a_str.len(),
            IdEncoder::<DeploymentId>::estimate_buf_capacity()
        );
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
