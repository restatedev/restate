// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Ordered-field descriptions of existing table-key encodings.
//!
//! These codecs describe bytes that legacy [`KeyEncode`](super::KeyEncode)
//! implementations already write, so existing layouts can be navigated without a
//! format change. Their codec order is canonical and prefix-free, but it is not
//! the logical order of the values: a length prefix sorts `"b"` before `"aa"`.
//! Selections therefore translate only equality and membership; ranges and string
//! prefixes remain residual.

use bytes::BufMut;
use prost::encoding::{decode_varint, encode_varint, encoded_len_varint};

use restate_storage_api::StorageError;
use restate_util_string::ReString;

use super::{FieldCodec, OrderedFieldCodec, TerminalFieldCodec};

/// A length-delimited byte string: a minimal varint length, then the bytes. This
/// is the key encoding of `ServiceName`, `ReString`, `ByteString`, and `Bytes`
/// fields (`write_delimited`).
pub enum DelimitedBytesCodec {}

/// A delimited field validated by [`DelimitedBytesCodec::take_encoded`], the only
/// constructor, so decoding cannot observe a noncanonical or truncated field.
#[derive(Clone, Copy)]
pub struct DelimitedField<'a> {
    field: &'a [u8],
    payload: &'a [u8],
}

impl AsRef<[u8]> for DelimitedField<'_> {
    fn as_ref(&self) -> &[u8] {
        self.field
    }
}

impl OrderedFieldCodec for DelimitedBytesCodec {}

impl FieldCodec for DelimitedBytesCodec {
    type Value = Vec<u8>;
    type Ref<'a> = &'a [u8];
    type Encoded<'a> = DelimitedField<'a>;

    fn borrow(value: &Vec<u8>) -> &[u8] {
        value
    }

    fn encode_ref<B: BufMut>(value: &[u8], target: &mut B) {
        encode_varint(value.len() as u64, target);
        target.put_slice(value);
    }

    fn serialized_length_ref(value: &[u8]) -> usize {
        encoded_len_varint(value.len() as u64) + value.len()
    }

    fn decode_encoded(encoded: DelimitedField<'_>) -> crate::Result<Vec<u8>> {
        Ok(encoded.payload.to_vec())
    }

    fn take_encoded<'a>(source: &mut &'a [u8]) -> crate::Result<DelimitedField<'a>> {
        let original = *source;
        let mut rest = original;
        let len = decode_varint(&mut rest).map_err(|_| StorageError::DataIntegrityError)?;
        let prefix = original.len() - rest.len();
        // A non-minimal length would decode to a value that re-encodes differently.
        if prefix != encoded_len_varint(len) {
            return Err(StorageError::DataIntegrityError);
        }
        let end = usize::try_from(len)
            .ok()
            .and_then(|len| prefix.checked_add(len))
            .filter(|end| *end <= original.len())
            .ok_or(StorageError::DataIntegrityError)?;
        let (field, remaining) = original.split_at(end);
        *source = remaining;
        Ok(DelimitedField {
            field,
            payload: &field[prefix..],
        })
    }
}

/// The optional-scope key encoding (`OptionalScope`): `u` without a scope, or `s`,
/// a big-endian `u32` length, and the non-empty scope bytes.
pub enum OptionalScopeCodec {}

const UNSCOPED: u8 = b'u';
const SCOPED: u8 = b's';

/// An optional-scope field validated by [`OptionalScopeCodec::take_encoded`], the
/// only constructor. UTF-8 validity is checked when decoding.
#[derive(Clone, Copy)]
pub struct OptionalScopeField<'a> {
    field: &'a [u8],
    scope: Option<&'a [u8]>,
}

impl AsRef<[u8]> for OptionalScopeField<'_> {
    fn as_ref(&self) -> &[u8] {
        self.field
    }
}

impl OrderedFieldCodec for OptionalScopeCodec {}

impl FieldCodec for OptionalScopeCodec {
    type Value = Option<ReString>;
    type Ref<'a> = Option<&'a str>;
    type Encoded<'a> = OptionalScopeField<'a>;

    fn borrow(value: &Option<ReString>) -> Option<&str> {
        value.as_deref()
    }

    fn encode_ref<B: BufMut>(value: Option<&str>, target: &mut B) {
        match value {
            Some(scope) => {
                target.put_u8(SCOPED);
                target.put_u32(u32::try_from(scope.len()).expect("scopes fit a u32 length"));
                target.put_slice(scope.as_bytes());
            }
            None => target.put_u8(UNSCOPED),
        }
    }

    fn serialized_length_ref(value: Option<&str>) -> usize {
        value.map_or(1, |scope| 1 + size_of::<u32>() + scope.len())
    }

    fn decode_encoded(encoded: OptionalScopeField<'_>) -> crate::Result<Option<ReString>> {
        encoded
            .scope
            .map(|scope| {
                std::str::from_utf8(scope)
                    .map(ReString::from)
                    .map_err(|_| StorageError::DataIntegrityError)
            })
            .transpose()
    }

    fn take_encoded<'a>(source: &mut &'a [u8]) -> crate::Result<OptionalScopeField<'a>> {
        let original = *source;
        let (end, scope) = match original.split_first() {
            Some((&UNSCOPED, _)) => (1, None),
            Some((&SCOPED, rest)) => {
                let (len, rest) = rest
                    .split_first_chunk::<{ size_of::<u32>() }>()
                    .ok_or(StorageError::DataIntegrityError)?;
                let len = u32::from_be_bytes(*len) as usize;
                // The existing decoder rejects empty scopes.
                let scope = rest
                    .get(..len)
                    .filter(|scope| !scope.is_empty())
                    .ok_or(StorageError::DataIntegrityError)?;
                (1 + size_of::<u32>() + len, Some(scope))
            }
            _ => return Err(StorageError::DataIntegrityError),
        };
        let (field, remaining) = original.split_at(end);
        *source = remaining;
        Ok(OptionalScopeField { field, scope })
    }
}

/// The raw lock name (`service/key`) that ends a lock key (`LockName`'s
/// `KeyEncode`). It is not prefix-free (`Cart/a` is a byte prefix of `Cart/ab`),
/// so it is only usable as a terminal field. Every remainder is a valid
/// boundary; decoding checks UTF-8.
pub enum LockNameCodec {}

impl TerminalFieldCodec for LockNameCodec {}

impl FieldCodec for LockNameCodec {
    type Value = ReString;
    type Ref<'a> = &'a str;
    type Encoded<'a> = &'a [u8];

    fn borrow(value: &ReString) -> &str {
        value.as_str()
    }

    fn encode_ref<B: BufMut>(value: &str, target: &mut B) {
        target.put_slice(value.as_bytes());
    }

    fn serialized_length_ref(value: &str) -> usize {
        value.len()
    }

    fn decode_encoded(encoded: &[u8]) -> crate::Result<ReString> {
        std::str::from_utf8(encoded)
            .map(ReString::from)
            .map_err(|_| StorageError::DataIntegrityError)
    }

    fn take_encoded<'a>(source: &mut &'a [u8]) -> crate::Result<&'a [u8]> {
        Ok(std::mem::take(source))
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use restate_types::Scope;

    use crate::keys::{KeyEncode, OptionalScope};

    use super::*;

    #[test]
    fn legacy_encodings_match_and_reject_noncanonical_fields() {
        // Same bytes as the existing key encodings, with exact field boundaries.
        for value in ["", "a", &"x".repeat(200)] {
            let mut legacy = Vec::new();
            Bytes::copy_from_slice(value.as_bytes()).encode(&mut legacy);
            let mut ours = Vec::new();
            DelimitedBytesCodec::encode_field(&value.as_bytes().to_vec(), &mut ours);
            assert_eq!(ours, legacy);
            legacy.push(0xff);
            let mut source = legacy.as_slice();
            assert_eq!(
                DelimitedBytesCodec::decode_field(&mut source).unwrap(),
                value.as_bytes()
            );
            assert_eq!(source, [0xff]);
        }
        for scope in [None, Some("tenant-1")] {
            let mut legacy = Vec::new();
            OptionalScope(scope.map(|scope| Scope::try_non_interned(scope).unwrap()))
                .encode(&mut legacy);
            let mut ours = Vec::new();
            OptionalScopeCodec::encode_field(&scope.map(ReString::from), &mut ours);
            assert_eq!(ours, legacy);
            legacy.push(0xff);
            let mut source = legacy.as_slice();
            assert_eq!(
                OptionalScopeCodec::decode_field(&mut source)
                    .unwrap()
                    .as_deref(),
                scope
            );
            assert_eq!(source, [0xff]);
        }
        // Overlong lengths, truncation, empty scopes, and unknown tags are errors.
        // Decoding takes only views validated this way, so no raw bytes bypass it.
        for invalid in [&[0x81, 0x00, b'a'][..], &[2, b'a'], &[0x80]] {
            assert!(DelimitedBytesCodec::decode_field(&mut &invalid[..]).is_err());
        }
        for invalid in [
            &[b's', 0, 0, 0, 0][..],
            &[b's', 0, 0, 0, 2, b'a'],
            b"x",
            &[],
        ] {
            assert!(OptionalScopeCodec::decode_field(&mut &invalid[..]).is_err());
        }
    }
}
