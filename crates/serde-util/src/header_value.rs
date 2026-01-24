// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use http::HeaderValue;
use serde::Deserialize;
use serde_with::{DeserializeAs, SerializeAs};

/// SerializeAs/DeserializeAs to implement ser/de trait for [HeaderValue]
/// Use it with `#[serde(with = "serde_with::As::<HeaderValueSerde>")]`.
pub struct HeaderValueSerde;

impl SerializeAs<HeaderValue> for HeaderValueSerde {
    fn serialize_as<S>(source: &HeaderValue, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(source.to_str().map_err(serde::ser::Error::custom)?)
    }
}

impl<'de> DeserializeAs<'de, HeaderValue> for HeaderValueSerde {
    fn deserialize_as<D>(deserializer: D) -> Result<HeaderValue, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let buf = String::deserialize(deserializer)?;
        HeaderValue::from_str(&buf).map_err(serde::de::Error::custom)
    }
}
