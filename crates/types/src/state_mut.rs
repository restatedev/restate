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
use std::fmt::{Display, Formatter};

use base64::Engine;
use bytes::Bytes;
use serde_with::serde_as;
use sha2::{Digest, Sha256};

use crate::identifiers::{ServiceId, StateMutationId, WithPartitionKey};

#[serde_as]
/// ExternalStateMutation
///
/// represents an external request to mutate a user's state.
#[derive(
    derive_more::Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize, bilrost::Message,
)]
pub struct ExternalStateMutation {
    #[bilrost(1)]
    pub service_id: ServiceId,
    #[bilrost(2)]
    pub version: Option<String>,
    // flexbuffers only supports string-keyed maps :-( --> so we store it as vector of kv pairs
    #[serde_as(as = "serde_with::Seq<(_, _)>")]
    #[bilrost(3)]
    #[debug("<hidden>")]
    pub state: HashMap<Bytes, Bytes>,
    /// Id of the state mutation without its partition key, which comes from `service_id`. If not
    /// set, the partition processor derives the id from the position of the command in the log.
    ///
    /// *Since v1.8.0*
    #[bilrost(tag(4), encoding(plainbytes))]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    id_remainder: Option<[u8; 16]>,
}

impl ExternalStateMutation {
    /// Creates a state mutation without an id.
    pub fn new(
        service_id: ServiceId,
        version: Option<String>,
        state: HashMap<Bytes, Bytes>,
    ) -> Self {
        Self {
            service_id,
            version,
            state,
            id_remainder: None,
        }
    }

    /// Assigns a new random id to the state mutation.
    pub fn with_generated_id(mut self) -> Self {
        self.id_remainder =
            Some(StateMutationId::generate(self.service_id.partition_key()).to_remainder_bytes());
        self
    }

    /// Returns the id of the state mutation if one was assigned.
    pub fn id(&self) -> Option<StateMutationId> {
        self.id_remainder.map(|remainder| {
            StateMutationId::from_partition_key_and_bytes(
                self.service_id.partition_key(),
                remainder,
            )
        })
    }

    /// Splits the mutation into its id and the input that gets stored for it.
    pub fn into_parts(self) -> (Option<StateMutationId>, StateMutationInput) {
        let id = self.id();
        let Self {
            service_id,
            version,
            state,
            id_remainder: _,
        } = self;
        (
            id,
            StateMutationInput {
                service_id,
                version,
                state,
            },
        )
    }
}

/// A state mutation as stored in the vqueue input table. Its id is part of the entry key.
///
/// Uses the same bilrost tags as [`ExternalStateMutation`], so both types can decode each
/// other's encoding.
///
/// *Since v1.8.0*
#[derive(derive_more::Debug, Clone, Eq, PartialEq, bilrost::Message)]
pub struct StateMutationInput {
    #[bilrost(1)]
    pub service_id: ServiceId,
    #[bilrost(2)]
    pub version: Option<String>,
    #[bilrost(3)]
    #[debug("<hidden>")]
    pub state: HashMap<Bytes, Bytes>,
}

/// # StateMutationVersion
///
/// This type represents a user state version. This implementation hashes canonically the raw key-value
/// and hands out an opaque string representation of that version, to be used for exact comparisons.
#[derive(Eq, PartialEq, Debug, Ord, PartialOrd)]
pub struct StateMutationVersion(String);

impl StateMutationVersion {
    pub fn from_raw<S: Into<String>>(raw: S) -> StateMutationVersion {
        StateMutationVersion(raw.into())
    }

    pub fn from_user_state(state: &[(Bytes, Bytes)]) -> StateMutationVersion {
        let mut kvs: Vec<_> = state.iter().collect();
        kvs.sort_by_key(|(k, _)| k);

        let mut hasher = Sha256::new();
        for (i, (k, v)) in kvs.iter().enumerate() {
            hasher.update(i.to_be_bytes());
            hasher.update([0x1u8]);
            hasher.update(k);
            hasher.update([0x2u8]);
            hasher.update(v);
            hasher.update([0x3u8]);
        }
        let result = hasher.finalize();
        let str = restate_base64_util::URL_SAFE.encode(result);
        StateMutationVersion(str)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_inner(self) -> String {
        self.0
    }
}

impl Display for StateMutationVersion {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use bilrost::{Message, OwnedMessage};

    use super::*;

    /// Stored vqueue inputs must stay readable by nodes that decode them as
    /// [`ExternalStateMutation`] (e.g. after a rollback) and vice versa.
    #[test]
    fn state_mutation_input_is_wire_compatible() {
        let mutation = ExternalStateMutation::new(
            ServiceId::new(None, "MySvc", "my-key"),
            Some("v1".to_owned()),
            [(Bytes::from("key"), Bytes::from("value"))].into(),
        )
        .with_generated_id();

        // The id takes its partition key from the service id
        let (id, parts_input) = mutation.clone().into_parts();
        assert_eq!(
            id.unwrap().partition_key(),
            mutation.service_id.partition_key()
        );

        let input = StateMutationInput::decode(mutation.encode_to_bytes()).expect("decodes");
        assert_eq!(input, parts_input);

        let decoded = ExternalStateMutation::decode(input.encode_to_bytes()).expect("decodes");
        assert_eq!(
            decoded,
            ExternalStateMutation::new(mutation.service_id, mutation.version, mutation.state)
        );
    }

    #[test]
    fn example_usage() {
        let state = vec![(Bytes::from("name"), Bytes::from("bob"))];
        let version = StateMutationVersion::from_user_state(&state);

        let expected =
            StateMutationVersion::from_raw("tDqA04Lj3_qJ-PgNPTecXDGZgpwy6jm2Ni2BYqJIthM");

        assert_eq!(version, expected);
    }

    #[test]
    fn multipule_kvs() {
        let state = vec![
            (Bytes::from("b"), Bytes::from("bbb")),
            (Bytes::from("a"), Bytes::from("aaa")),
        ];
        let version = StateMutationVersion::from_user_state(&state);

        let expected =
            StateMutationVersion::from_raw("RVM17P6x18Wp-JnzQ01BzmqCvhAunTOQI_az6Px3Zyk");

        assert_eq!(version, expected);
    }

    #[test]
    fn empty_state() {
        let state = vec![];
        let version = StateMutationVersion::from_user_state(&state);
        let expected =
            StateMutationVersion::from_raw("47DEQpj8HBSa-_TImW-5JCeuQeRkm5NMpJWZG3hSuFU");

        assert_eq!(version, expected);
    }
}
