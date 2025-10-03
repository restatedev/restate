// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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

use crate::bilrost_storage_encode_decode;
use crate::identifiers::ServiceId;

#[serde_as]
/// ExternalStateMutation
///
/// represents an external request to mutate a user's state.
#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize, bilrost::Message)]
pub struct ExternalStateMutation {
    #[bilrost(1)]
    pub service_id: ServiceId,
    #[bilrost(2)]
    pub version: Option<String>,
    // flexbuffers only supports string-keyed maps :-( --> so we store it as vector of kv pairs
    #[bilrost(3)]
    #[serde_as(as = "serde_with::Seq<(_, _)>")]
    pub state: HashMap<Bytes, Bytes>,
}

bilrost_storage_encode_decode!(ExternalStateMutation);

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
    use super::*;

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
