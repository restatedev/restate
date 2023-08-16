// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, PickFirst, Same};
use uuid::Uuid;

/// # Uuid
///
/// This type represents a uuid. A uuid can be specified in one of the following formats:
///
/// * As string in the uuid format
/// * As Base64 encoded byte array
///
/// When serialized, the uuid will be represented as a human readable string.
#[serde_as]
#[derive(Debug, Serialize, Deserialize)]
#[serde(transparent)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schema", schemars(rename = "Uuid"))]
pub struct SerdeableUuid(
    #[serde_as(as = "UuidAsStringOrBytesOrBase64")]
    #[cfg_attr(feature = "schema", schemars(with = "String"))]
    Uuid,
);

impl From<SerdeableUuid> for Uuid {
    fn from(value: SerdeableUuid) -> Self {
        value.0
    }
}

impl From<Uuid> for SerdeableUuid {
    fn from(value: Uuid) -> Self {
        SerdeableUuid(value)
    }
}

/// This type alias can be used in combination with [`serde_with::serde_as`] annotation
/// to deserialize [`Uuid`] as one of the following formats:
///
/// * As string in the uuid format (provided by the [`uuid`] crate)
/// * As regular byte array (provided by the [`uuid`] crate)
/// * As Base64 encoded byte array
///
/// When serializing, the string representation of uuid will be used.
pub type UuidAsStringOrBytesOrBase64 = PickFirst<(Same, UuidAsBase64)>;

serde_with::serde_conv!(
    pub UuidAsBase64,
    uuid::Uuid,
    |input: &uuid::Uuid| base64::prelude::BASE64_STANDARD.encode(input.as_bytes()),
    |input: String| -> Result<_, Box<dyn std::error::Error + Sync + Send + 'static>> {
        Ok(uuid::Uuid::from_slice(&base64::prelude::BASE64_STANDARD.decode(input).map_err(Box::new)?).map_err(Box::new)?)
    }
);

#[cfg(test)]
mod tests {
    use super::*;
    use serde::de::IntoDeserializer;

    use serde_json::*;

    #[test]
    fn serialize_to_string() {
        let expected = Uuid::now_v7();
        let serialized = to_value(SerdeableUuid::from(expected)).unwrap();

        assert_eq!(serialized.as_str().unwrap(), expected.to_string());
    }

    #[test]
    fn deserialize_from_string() {
        let expected = Uuid::now_v7();
        let input = Value::String(expected.to_string());
        let actual: Uuid = SerdeableUuid::deserialize(input.into_deserializer())
            .unwrap()
            .into();

        assert_eq!(expected, actual);
    }

    #[test]
    fn deserialize_from_base64() {
        let expected = Uuid::now_v7();
        let input = Value::String(base64::prelude::BASE64_STANDARD.encode(expected));
        let actual: Uuid = SerdeableUuid::deserialize(input.into_deserializer())
            .unwrap()
            .into();

        assert_eq!(expected, actual);
    }
}
