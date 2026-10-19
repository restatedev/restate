// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_storage_api::StorageError;

use super::KeyDecode;

/// A decoder positioned at the next field of an ordered key payload.
///
/// `C` is the generated key type and `FIELD` is the next field's zero-based
/// position. Generated `decode_*` methods return owned values; `take_*` methods
/// return borrowed [`super::FieldDecoder`] views. Each step consumes the decoder
/// and returns the next one, except the final step, which returns only its field
/// and rejects trailing bytes.
///
/// At position zero, [`Self::decode_all`] materializes the whole key. Generated
/// `take_all` methods instead return a view of all fields; both reject trailing
/// bytes. Borrowed views can defer value validation until a field is decoded.
pub struct KeyDecoder<'a, C, const FIELD: usize = 0> {
    pub remaining: &'a [u8],
    pub _marker: std::marker::PhantomData<C>,
}

impl<'a, C: KeyDecode> KeyDecoder<'a, C> {
    /// Starts at the first payload field, after the caller has validated and
    /// removed the fixed physical prefix. This constructor does not inspect bytes.
    pub fn from_payload(remaining: &'a [u8]) -> Self {
        Self {
            remaining,
            _marker: std::marker::PhantomData,
        }
    }

    /// Decodes the complete payload, rejecting trailing bytes.
    pub fn decode_all(mut self) -> crate::Result<C> {
        let value = C::decode(&mut self.remaining)?;
        if !self.remaining.is_empty() {
            return Err(StorageError::DataIntegrityError);
        }
        Ok(value)
    }
}
