// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub struct VersionSerde;

impl serde_with::SerializeAs<http::Version> for VersionSerde {
    fn serialize_as<S>(value: &http::Version, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        http_serde::version::serialize(value, serializer)
    }
}

impl<'de> serde_with::DeserializeAs<'de, http::Version> for VersionSerde {
    fn deserialize_as<D>(deserializer: D) -> Result<http::Version, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        http_serde::version::deserialize(deserializer)
    }
}
