// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::BufMut;

use restate_storage_api::StorageError;
use restate_types::ServiceName;
use restate_util_string::ReString;

use super::{
    EncodedMemCmpStr, IndexFieldDecode, IndexFieldEncode, IndexFieldView, KeyEncode, MemCmpStr,
};

impl IndexFieldEncode for str {
    #[inline]
    fn encode_field<B: BufMut>(&self, target: &mut B) {
        MemCmpStr::new(self).encode(target);
    }

    #[inline]
    fn serialized_length(&self) -> usize {
        MemCmpStr::new(self).encoded_len()
    }
}

impl IndexFieldDecode for str {
    type Owned = ReString;
    type Encoded<'a> = &'a EncodedMemCmpStr;

    fn decode_encoded(encoded: Self::Encoded<'_>) -> crate::Result<Self::Owned> {
        Ok(encoded.decode())
    }

    fn take_encoded<'a>(buf: &mut &'a [u8]) -> crate::Result<Self::Encoded<'a>> {
        let (encoded, remaining) = EncodedMemCmpStr::try_ref_from_prefix(buf)
            .map_err(super::mem_comparable_string::map_decode_error)?;
        *buf = remaining;
        Ok(encoded)
    }
}

impl IndexFieldView<str> for str {
    type Ref<'a> = &'a str;
}

impl IndexFieldEncode for ReString {
    #[inline]
    fn encode_field<B: BufMut>(&self, target: &mut B) {
        self.as_str().encode_field(target);
    }

    #[inline]
    fn serialized_length(&self) -> usize {
        self.as_str().serialized_length()
    }
}

impl IndexFieldDecode for ReString {
    type Owned = ReString;
    type Encoded<'a> = &'a EncodedMemCmpStr;

    fn decode_encoded(encoded: Self::Encoded<'_>) -> crate::Result<Self::Owned> {
        str::decode_encoded(encoded)
    }

    fn take_encoded<'a>(buf: &mut &'a [u8]) -> crate::Result<Self::Encoded<'a>> {
        str::take_encoded(buf)
    }
}

impl IndexFieldView<str> for ReString {
    type Ref<'a> = &'a str;
}

impl IndexFieldEncode for ServiceName {
    #[inline]
    fn encode_field<B: BufMut>(&self, target: &mut B) {
        self.as_str().encode_field(target);
    }

    #[inline]
    fn serialized_length(&self) -> usize {
        self.as_str().serialized_length()
    }
}

impl IndexFieldDecode for ServiceName {
    type Owned = ServiceName;
    type Encoded<'a> = &'a EncodedMemCmpStr;

    fn decode_encoded(encoded: Self::Encoded<'_>) -> crate::Result<Self::Owned> {
        let service_name = encoded.decode();
        if service_name.is_empty() {
            return Err(StorageError::DataIntegrityError);
        }

        Ok(ServiceName::new(service_name.as_str()))
    }

    fn take_encoded<'a>(buf: &mut &'a [u8]) -> crate::Result<Self::Encoded<'a>> {
        str::take_encoded(buf)
    }
}

impl IndexFieldView<str> for ServiceName {
    type Ref<'a> = &'a str;
}

impl<T, U> IndexFieldView<U> for Option<T>
where
    T: IndexFieldView<U>,
    U: IndexFieldEncode + ?Sized,
{
    type Ref<'a>
        = Option<T::Ref<'a>>
    where
        U: 'a;
}

impl IndexFieldDecode for u64 {
    type Owned = u64;
    type Encoded<'a> = &'a [u8; size_of::<u64>()];

    fn decode_encoded(encoded: Self::Encoded<'_>) -> crate::Result<u64> {
        Ok(u64::from_be_bytes(*encoded))
    }

    fn take_encoded<'a>(buf: &mut &'a [u8]) -> crate::Result<Self::Encoded<'a>> {
        let Some((encoded, remaining)) = buf.split_first_chunk() else {
            return Err(StorageError::DataIntegrityError);
        };
        *buf = remaining;
        Ok(encoded)
    }
}

impl IndexFieldEncode for u64 {
    #[inline]
    fn encode_field<B: BufMut>(&self, target: &mut B) {
        target.put_u64(*self);
    }

    #[inline]
    fn serialized_length(&self) -> usize {
        size_of::<u64>()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::keys::FieldDecoder;

    use super::*;

    #[test]
    fn field_decoder_defers_service_name_validation() {
        let mut empty = Vec::new();
        "".encode_field(&mut empty);

        let mut input = empty.as_slice();
        let field = FieldDecoder::<ServiceName>::take(&mut input).unwrap();
        assert_eq!(field.as_bytes(), empty);
        assert_eq!(field.encoded(), EncodedMemCmpStr::EMPTY);
        assert!(field.decode().is_err());
        assert!(input.is_empty());

        let mut optional = vec![1];
        optional.extend_from_slice(&empty);
        let mut input = optional.as_slice();
        let field = FieldDecoder::<Option<ServiceName>>::take(&mut input).unwrap();
        assert_eq!(field.as_bytes(), optional);
        assert_eq!(field.encoded().as_bytes(), optional);
        assert_eq!(field.encoded().as_option(), Some(EncodedMemCmpStr::EMPTY));
        assert!(field.decode().is_err());
        assert!(input.is_empty());
    }

    #[test]
    fn optional_fields_reuse_validated_inner_views() {
        static PARSES: AtomicUsize = AtomicUsize::new(0);

        struct CountedString;

        impl IndexFieldDecode for CountedString {
            type Owned = ReString;
            type Encoded<'a> = &'a EncodedMemCmpStr;

            fn decode_encoded(encoded: Self::Encoded<'_>) -> crate::Result<Self::Owned> {
                str::decode_encoded(encoded)
            }

            fn take_encoded<'a>(buf: &mut &'a [u8]) -> crate::Result<Self::Encoded<'a>> {
                PARSES.fetch_add(1, Ordering::Relaxed);
                str::take_encoded(buf)
            }
        }

        for value in [None, Some(None), Some(Some("multi-group UTF-8: 🦀 café"))] {
            PARSES.store(0, Ordering::Relaxed);
            let mut bytes = Vec::new();
            value.encode_field(&mut bytes);
            let field_len = bytes.len();
            bytes.extend_from_slice(b"suffix");
            let mut input = bytes.as_slice();
            let encoded = {
                let field =
                    FieldDecoder::<Option<Option<CountedString>>>::take(&mut input).unwrap();
                assert_eq!(field.as_bytes(), &bytes[..field_len]);
                assert_eq!(input, b"suffix");
                let expected = value.map(|inner| inner.map(ReString::from));
                assert_eq!(field.decode().unwrap(), expected);
                assert_eq!(field.decode().unwrap(), expected);
                field.encoded()
            };

            assert_eq!(encoded.as_bytes(), &bytes[..field_len]);
            assert_eq!(encoded.as_option().is_some(), value.is_some());
            if let Some(inner) = encoded.as_option() {
                assert_eq!(inner.as_bytes(), &bytes[1..field_len]);
                assert_eq!(inner.as_option().is_some(), value.unwrap().is_some());
                if let Some(string) = inner.as_option() {
                    assert_eq!(string.as_bytes().as_ptr(), bytes[2..].as_ptr());
                    assert_eq!(string.decode(), value.flatten().unwrap());
                }
            }
            assert_eq!(
                PARSES.load(Ordering::Relaxed),
                usize::from(value.flatten().is_some())
            );
        }
    }

    #[test]
    fn optional_fields_preserve_boundaries_and_reject_invalid_encodings() {
        for value in [None, Some(0_u64), Some(u64::MAX)] {
            let mut bytes = Vec::new();
            value.encode_field(&mut bytes);
            let field_len = bytes.len();
            bytes.extend_from_slice(b"suffix");
            let mut input = bytes.as_slice();
            let encoded = Option::<u64>::take_encoded(&mut input).unwrap();
            assert_eq!(encoded.as_bytes(), &bytes[..field_len]);
            assert_eq!(encoded.as_option().copied(), value.map(u64::to_be_bytes));
            assert_eq!(Option::<u64>::decode_encoded(encoded).unwrap(), value);
            assert_eq!(input, b"suffix");
        }

        for invalid in [
            &[][..],
            &[2],                               // Invalid presence tag.
            &[1],                               // Missing payload.
            &[1, b'a'],                         // Truncated string group.
            &[1, 0xff, 0, 0, 0, 0, 0, 0, 0, 1], // Invalid UTF-8.
            &[1, 0, 0, 0, 0, 0, 0, 0, 0, 10],   // Invalid string marker.
        ] {
            let mut input = invalid;
            assert!(Option::<ReString>::take_encoded(&mut input).is_err());
        }
        assert!(Option::<u64>::take_encoded(&mut &[1, 0, 0][..]).is_err());
    }
}
