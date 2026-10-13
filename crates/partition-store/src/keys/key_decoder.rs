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

/// A progressive and lazy key decoder.
pub struct KeyDecoder<'a, C, const FIELD: usize = 0> {
    pub remaining: &'a [u8],
    pub _marker: std::marker::PhantomData<C>,
}

impl<'a, C: KeyDecode> KeyDecoder<'a, C> {
    /// Starts at the payload after the caller has validated the physical prefix.
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
