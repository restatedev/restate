// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use http::{HeaderName, HeaderValue};
use serde::de::{MapAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;
use std::fmt;

/// Proxy type to implement HashMap<HeaderName, HeaderValue> ser/de
/// Use it directly or with `#[serde(with = "serde_with::As::<serde_with::FromInto<restate_serde_util::SerdeableHeaderMap>>")]`.
#[derive(Debug, Default, Clone)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schema", schemars(transparent))]
#[cfg_attr(feature = "utoipa-schema", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "utoipa-schema", schema(value_type = HashMap<String, String>))]
pub struct SerdeableHeaderHashMap(
    #[cfg_attr(feature = "schema", schemars(with = "HashMap<String, String>"))]
    HashMap<HeaderName, HeaderValue>,
);

impl SerdeableHeaderHashMap {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl From<SerdeableHeaderHashMap> for HashMap<HeaderName, HeaderValue> {
    fn from(value: SerdeableHeaderHashMap) -> Self {
        value.0
    }
}

impl From<HashMap<HeaderName, HeaderValue>> for SerdeableHeaderHashMap {
    fn from(value: HashMap<HeaderName, HeaderValue>) -> Self {
        Self(value)
    }
}

impl Serialize for SerdeableHeaderHashMap {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (k, v) in &self.0 {
            map.serialize_entry(k.as_str(), v.to_str().map_err(serde::ser::Error::custom)?)?;
        }
        map.end()
    }
}

struct SerdeableHeaderHashMapVisitor;

impl<'de> Visitor<'de> for SerdeableHeaderHashMapVisitor {
    type Value = SerdeableHeaderHashMap;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("header map")
    }

    fn visit_map<M>(self, mut access: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        let mut map = HashMap::with_capacity(access.size_hint().unwrap_or(0));

        while let Some((key, value)) = access.next_entry::<String, String>()? {
            map.insert(
                key.try_into().map_err(serde::de::Error::custom)?,
                value.try_into().map_err(serde::de::Error::custom)?,
            );
        }

        Ok(map.into())
    }
}

// This is the trait that informs Serde how to deserialize SerdeableHeaderHashMap.
impl<'de> Deserialize<'de> for SerdeableHeaderHashMap {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Instantiate our Visitor and ask the Deserializer to drive
        // it over the input data, resulting in an instance of SerdeableHeaderHashMap.
        deserializer.deserialize_map(SerdeableHeaderHashMapVisitor)
    }
}
