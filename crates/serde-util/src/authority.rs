// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use http::uri::Authority;
use serde::Deserialize;
use serde_with::{DeserializeAs, SerializeAs};

/// SerializeAs/DeserializeAs to implement ser/de trait for [Authority]
/// Use it with `#[serde(with = "serde_with::As::<AuthoritySerde>")]`.
pub struct AuthoritySerde;

impl SerializeAs<Authority> for AuthoritySerde {
    fn serialize_as<S>(source: &Authority, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(source.as_str())
    }
}

impl<'de> DeserializeAs<'de, Authority> for AuthoritySerde {
    fn deserialize_as<D>(deserializer: D) -> Result<Authority, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let buf = String::deserialize(deserializer)?;
        Authority::try_from(buf).map_err(serde::de::Error::custom)
    }
}
