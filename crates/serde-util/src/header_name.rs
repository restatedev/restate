// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use http::HeaderName;
use serde::Deserialize;
use serde_with::{DeserializeAs, SerializeAs};
use std::str::FromStr;

/// SerializeAs/DeserializeAs to implement ser/de trait for [HeaderName]
/// Use it with `#[serde(with = "serde_with::As::<HeaderNameSerde>")]`.
pub struct HeaderNameSerde;

impl SerializeAs<HeaderName> for HeaderNameSerde {
    fn serialize_as<S>(source: &HeaderName, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(source.as_str())
    }
}

impl<'de> DeserializeAs<'de, HeaderName> for HeaderNameSerde {
    fn deserialize_as<D>(deserializer: D) -> Result<HeaderName, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let buf = String::deserialize(deserializer)?;
        HeaderName::from_str(&buf).map_err(serde::de::Error::custom)
    }
}
