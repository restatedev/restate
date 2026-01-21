// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::Bytes;
use serde::Deserialize;
use serde::de::Error;
use serde_with::{DeserializeAs, SerializeAs};

/// SerializeAs/DeserializeAs to implement ser/de trait for prost types
/// Use it with `#[serde(with = "serde_with::As::<ProtobufEncoded>")]`.
pub struct ProtobufEncoded;

impl<T> SerializeAs<T> for ProtobufEncoded
where
    T: prost::Message,
{
    fn serialize_as<S>(source: &T, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_bytes(&source.encode_to_vec())
    }
}

impl<'de, T> DeserializeAs<'de, T> for ProtobufEncoded
where
    T: prost::Message + Default,
{
    fn deserialize_as<D>(deserializer: D) -> Result<T, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let mut buf = Bytes::deserialize(deserializer).map_err(Error::custom)?;
        T::decode(&mut buf).map_err(Error::custom)
    }
}
