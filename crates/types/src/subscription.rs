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
use std::mem::size_of;
use std::str::FromStr;

use crate::base62_util::base62_max_length_for_type;
use crate::errors::IdDecodeError;
use crate::id_util::{IdDecoder, IdEncoder, IdResourceType};
use crate::identifiers::{ResourceId, SubscriptionId, TimestampAwareId};
use crate::time::MillisSinceEpoch;
use ulid::Ulid;

impl ResourceId for SubscriptionId {
    const SIZE_IN_BYTES: usize = size_of::<u128>();
    const RESOURCE_TYPE: IdResourceType = IdResourceType::Subscription;
    const STRING_CAPACITY_HINT: usize = base62_max_length_for_type::<u128>();
    fn push_contents_to_encoder(&self, encoder: &mut IdEncoder<Self>) {
        let ulid_raw: u128 = self.0.into();
        encoder.encode_fixed_width(ulid_raw);
    }
}

impl TimestampAwareId for SubscriptionId {
    fn timestamp(&self) -> MillisSinceEpoch {
        self.0.timestamp_ms().into()
    }
}

impl FromStr for SubscriptionId {
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

impl fmt::Display for SubscriptionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut encoder = IdEncoder::<Self>::new();
        self.push_contents_to_encoder(&mut encoder);
        fmt::Display::fmt(&encoder.finalize(), f)
    }
}

impl From<u128> for SubscriptionId {
    fn from(value: u128) -> Self {
        Self(Ulid::from(value))
    }
}

// Passthrough json schema to the string
#[cfg(feature = "serde_schema")]
impl schemars::JsonSchema for SubscriptionId {
    fn schema_name() -> String {
        <String as schemars::JsonSchema>::schema_name()
    }

    fn json_schema(g: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        <String as schemars::JsonSchema>::json_schema(g)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subscription_id_format() {
        let a = SubscriptionId::new();
        assert!(a.timestamp().as_u64() > 0);
        let a_str = a.to_string();
        assert!(a_str.starts_with("sub_"));
    }

    #[test]
    fn test_subscription_roundtrip() {
        let a = SubscriptionId::new();
        let b: SubscriptionId = a.to_string().parse().unwrap();
        assert_eq!(a, b);
        assert_eq!(a.to_string(), b.to_string());
    }
}
