// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_with::serde_as;

use super::*;

/// Marker that consumes any v1 schema registry payload during deserialization
/// without parsing it into the (now removed) v1 data structures.
///
/// We only need to know whether the legacy `services`/`deployments` fields are
/// present so that we can panic with a useful migration message in
/// [`From<Schema> for super::Schema`].
#[derive(Debug, Clone)]
struct V1LegacyMarker;

impl<'de> Deserialize<'de> for V1LegacyMarker {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        serde::de::IgnoredAny::deserialize(deserializer)?;
        Ok(V1LegacyMarker)
    }
}

impl Serialize for V1LegacyMarker {
    fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // We never write v1 fields back: the conversion `From<super::Schema> for Schema`
        // always sets them to `None` so this branch is unreachable in practice.
        Err(serde::ser::Error::custom(
            "V1LegacyMarker should never be serialized",
        ))
    }
}

/// The schema information.
#[serde_as]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Schema {
    // --- Removed v1 data structure fields.
    //
    // We keep them around purely to detect schema registries written by
    // versions older than v1.4 and refuse to start with a clear error.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    services: Option<V1LegacyMarker>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    deployments: Option<V1LegacyMarker>,

    // --- New data structure

    // Registered deployments
    #[serde(default, skip_serializing_if = "Option::is_none")]
    deployments_v2: Option<Vec<Deployment>>,

    // --- Same in old and new schema data structure
    /// This gets bumped on each update.
    version: Version,
    // flexbuffers only supports string-keyed maps :-( --> so we store it as vector of kv pairs
    #[serde_as(as = "serde_with::Seq<(_, _)>")]
    subscriptions: HashMap<SubscriptionId, Subscription>,

    // Kafka clusters
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    #[serde_as(as = "restate_serde_util::MapAsVec")]
    kafka_clusters: HashMap<String, KafkaCluster>,
}

impl restate_serde_util::MapAsVecItem for KafkaCluster {
    type Key = String;

    fn key(&self) -> Self::Key {
        self.name.to_string()
    }
}

impl From<super::Schema> for Schema {
    fn from(
        super::Schema {
            version,
            deployments,
            subscriptions,
            kafka_clusters,
            ..
        }: super::Schema,
    ) -> Self {
        Self {
            services: None,
            deployments: None,
            deployments_v2: Some(deployments.into_values().collect()),
            version,
            subscriptions,
            kafka_clusters,
        }
    }
}

impl From<Schema> for super::Schema {
    fn from(
        Schema {
            services,
            deployments,
            deployments_v2,
            version,
            subscriptions,
            kafka_clusters,
        }: Schema,
    ) -> Self {
        if let Some(deployments_v2) = deployments_v2 {
            Self {
                version,
                active_service_revisions: ActiveServiceRevision::create_index(&deployments_v2),
                deployments: deployments_v2
                    .into_iter()
                    .map(|deployment| (deployment.id, deployment))
                    .collect(),
                subscriptions,
                kafka_clusters,
            }
        } else if services.is_some() || deployments.is_some() {
            panic!(
                "Detected schema registry data in the legacy v1 format, which is no longer supported. \
                 To migrate, downgrade to the previous Restate minor (v1.6), perform any write to the \
                 schema registry (for example, adding a header to a deployment is enough to trigger \
                 the in-place migration), then upgrade again."
            )
        } else {
            panic!(
                "Unexpected situation where neither v1 data structure nor v2 data structure is used!"
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::storage::StorageCodec;
    use crate::{Version, schema};

    /// Mirror of the legacy v1 on-disk schema (just enough to encode a payload that
    /// looks like a pre-v1.4 registry would on disk). The contents of the
    /// `services`/`deployments` maps are intentionally opaque: the `From` impl
    /// must panic before reading them.
    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct LegacyV1Schema {
        version: Version,
        services: HashMap<String, serde_json::Value>,
        deployments: Vec<(String, serde_json::Value)>,
        #[serde(default)]
        subscriptions: Vec<(String, serde_json::Value)>,
    }

    mod storage {
        use crate::flexbuffers_storage_encode_decode;

        use super::LegacyV1Schema;

        flexbuffers_storage_encode_decode!(LegacyV1Schema);
    }

    #[test]
    #[should_panic(expected = "legacy v1 format")]
    fn detect_legacy_v1_payload_panics() {
        let legacy = LegacyV1Schema {
            version: Version::from(1),
            services: HashMap::from([(
                "Greeter".to_owned(),
                serde_json::json!({"name": "Greeter"}),
            )]),
            deployments: vec![],
            subscriptions: vec![],
        };

        let mut buf = bytes::BytesMut::default();
        StorageCodec::encode(&legacy, &mut buf).unwrap();
        let _ = StorageCodec::decode::<schema::Schema, _>(&mut buf.freeze()).unwrap();
    }
}
