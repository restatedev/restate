// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::{Buf, BufMut};

use restate_storage_api::StorageError;
use restate_util_string::{MemCmpStr, MemCmpString, MemCmpStringError, MemCmpTarget};

use super::{KeyDecode, KeyEncode};

impl<S: MemCmpTarget> KeyEncode for MemCmpString<S> {
    fn encode<B: BufMut>(&self, target: &mut B) {
        self.encode_to(target);
    }

    fn serialized_length(&self) -> usize {
        self.encoded_len()
    }
}

impl<S: MemCmpTarget> KeyDecode for MemCmpString<S> {
    fn decode<B: Buf>(source: &mut B) -> crate::Result<Self> {
        Self::decode_from(source).map_err(map_decode_error)
    }
}

impl KeyEncode for MemCmpStr<'_> {
    fn encode<B: BufMut>(&self, target: &mut B) {
        self.encode_to(target);
    }

    fn serialized_length(&self) -> usize {
        self.encoded_len()
    }
}

pub(super) fn map_decode_error(error: MemCmpStringError) -> StorageError {
    match error {
        MemCmpStringError::InvalidMarker(_) | MemCmpStringError::NonZeroPadding => {
            StorageError::Generic(error.into())
        }
        MemCmpStringError::Truncated
        | MemCmpStringError::InvalidUtf8
        | MemCmpStringError::OutputTooSmall => StorageError::DataIntegrityError,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_traits_delegate_to_mem_comparable_codec() {
        let value = MemCmpStr::new("hello 🦀");
        let mut encoded = Vec::with_capacity(value.serialized_length());
        value.encode(&mut encoded);

        let mut input = encoded.as_slice();
        assert_eq!(
            MemCmpString::<String>::decode(&mut input).unwrap().as_str(),
            value.as_str()
        );
        assert!(input.is_empty());

        encoded[8] = 10;
        assert!(matches!(
            MemCmpString::<String>::decode(&mut encoded.as_slice()),
            Err(StorageError::Generic(_))
        ));
    }
}
