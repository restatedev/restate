// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::storage::{
    EncodedPolyBytes, PolyBytes, StorageCodec, StorageDecode, StorageDecodeError, StorageEncode,
};
use crate::time::NanosSinceEpoch;

use super::{KeyFilter, Keys, MatchKeyQuery};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    created_at: NanosSinceEpoch,
    #[serde(with = "serde_with::As::<EncodedPolyBytes>")]
    body: PolyBytes,
    keys: Keys,
}

impl Record {
    pub fn from_parts(created_at: NanosSinceEpoch, keys: Keys, body: PolyBytes) -> Self {
        Self {
            created_at,
            keys,
            body,
        }
    }

    pub fn created_at(&self) -> NanosSinceEpoch {
        self.created_at
    }

    pub fn keys(&self) -> &Keys {
        &self.keys
    }

    pub fn estimated_encode_size(&self) -> usize {
        let body_size = match &self.body {
            PolyBytes::Bytes(slice) => slice.len(),
            PolyBytes::Typed(_) => {
                // constant, assumption based on base envelope size of ~600 bytes.
                2_048 // 2KiB
            }
        };
        size_of::<Keys>() + size_of::<NanosSinceEpoch>() + body_size
    }

    pub fn body(&self) -> &PolyBytes {
        &self.body
    }

    pub fn dissolve(self) -> (NanosSinceEpoch, PolyBytes, Keys) {
        (self.created_at, self.body, self.keys)
    }

    /// Decode the record body into an owned value T.
    ///
    /// Internally, this will clone the inner value if it's already in record cache, or will move
    /// the value from the underlying Arc delivered from the loglet. Use this approach if you need
    /// to mutate the value in-place and the cost of cloning sections is high. It's generally
    /// recommended to use `decode_arc` whenever possible for large payloads.
    pub fn decode<T: StorageDecode + StorageEncode + Clone>(self) -> Result<T, StorageDecodeError> {
        let decoded = match self.body {
            PolyBytes::Bytes(slice) => {
                let mut buf = std::io::Cursor::new(slice);
                StorageCodec::decode(&mut buf)?
            }
            PolyBytes::Typed(value) => {
                let target_arc: Arc<T> = value.downcast_arc().map_err(|_| {
                StorageDecodeError::DecodeValue(
                    anyhow::anyhow!(
                        "Type mismatch. Original value in PolyBytes::Typed does not match requested type"
                    )
                    .into(),
                )})?;
                // Attempts to move the inner value (T) if this Arc has exactly one strong
                // reference. Otherwise, it clones the inner value.
                match Arc::try_unwrap(target_arc) {
                    Ok(value) => value,
                    Err(value) => value.as_ref().clone(),
                }
            }
        };
        Ok(decoded)
    }

    /// Decode the record body into an Arc<T>. This is the most efficient way to access the entry
    /// if you need read-only access or if it's acceptable to selectively clone inner sections. If
    /// the record is in record cache, this will avoid cloning or deserialization of the value.
    pub fn decode_arc<T: StorageDecode + StorageEncode>(
        self,
    ) -> Result<Arc<T>, StorageDecodeError> {
        let decoded = match self.body {
            PolyBytes::Bytes(slice) => {
                let mut buf = std::io::Cursor::new(slice);
                Arc::new(StorageCodec::decode(&mut buf)?)
            }
            PolyBytes::Typed(value) => {
                value.downcast_arc().map_err(|_| {
                StorageDecodeError::DecodeValue(
                    anyhow::anyhow!(
                        "Type mismatch. Original value in PolyBytes::Typed does not match requested type"
                    )
                    .into(),
                )})?
            },
        };
        Ok(decoded)
    }
}

impl MatchKeyQuery for Record {
    fn matches_key_query(&self, query: &KeyFilter) -> bool {
        self.keys.matches_key_query(query)
    }
}

impl From<String> for Record {
    fn from(value: String) -> Self {
        Record {
            created_at: NanosSinceEpoch::now(),
            keys: Keys::None,
            body: PolyBytes::Typed(Arc::new(value)),
        }
    }
}

impl From<&str> for Record {
    fn from(value: &str) -> Self {
        Record {
            created_at: NanosSinceEpoch::now(),
            keys: Keys::None,
            body: PolyBytes::Typed(Arc::new(value.to_owned())),
        }
    }
}

#[cfg(any(test, feature = "test-util"))]
impl From<(&str, Keys)> for Record {
    fn from((value, keys): (&str, Keys)) -> Self {
        Record::from_parts(
            NanosSinceEpoch::now(),
            keys,
            PolyBytes::Typed(Arc::new(value.to_owned())),
        )
    }
}
