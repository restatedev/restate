// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::marker::PhantomData;

use bytes::BufMut;

use restate_storage_api::StorageError;

/// Encodes a field so that lexicographical byte order matches the field's logical order.
///
/// Compound indexes can concatenate these encodings to preserve their field-wise ordering.
pub trait IndexFieldEncode {
    fn encode_field<B: BufMut>(&self, target: &mut B);

    fn serialized_length(&self) -> usize;
}

pub trait IndexFieldDecode {
    type Owned: IndexFieldEncode;
    /// A borrowed view of the complete field encoding, including any wrapper tags.
    ///
    /// Leaf codecs return references; compound codecs can retain their parsed inner views.
    type Encoded<'a>: AsRef<[u8]> + Copy + 'a;

    fn decode_field(buf: &mut &[u8]) -> crate::Result<Self::Owned>
    where
        Self::Owned: Sized,
    {
        let encoded = Self::take_encoded(buf)?;
        Self::decode_encoded(encoded)
    }

    fn decode_encoded(encoded: Self::Encoded<'_>) -> crate::Result<Self::Owned>
    where
        Self::Owned: Sized;

    /// Borrows exactly one encoded field and advances `buf` past it without materializing
    /// its value. The returned view retains any validation needed to decode the encoding.
    fn take_encoded<'a>(buf: &mut &'a [u8]) -> crate::Result<Self::Encoded<'a>>;
}

/// A zero-copy view over exactly one encoded index field.
pub struct FieldDecoder<'a, T: IndexFieldDecode + ?Sized> {
    encoded: T::Encoded<'a>,
    _marker: PhantomData<T>,
}

impl<'a, T: IndexFieldDecode + ?Sized> FieldDecoder<'a, T> {
    pub fn take(buf: &mut &'a [u8]) -> crate::Result<Self> {
        let encoded = T::take_encoded(buf)?;
        Ok(Self {
            encoded,
            _marker: PhantomData,
        })
    }

    /// Returns the raw encoded bytes for this field.
    pub fn as_bytes(&self) -> &[u8] {
        self.encoded.as_ref()
    }

    /// Returns the field's typed encoded representation.
    pub fn encoded(&self) -> T::Encoded<'a> {
        self.encoded
    }

    /// Fully decodes this field into its owned representation.
    pub fn decode(&self) -> crate::Result<T::Owned> {
        T::decode_encoded(self.encoded)
    }
}

/// Defines the borrowed view of an index field while preserving wrappers such as [`Option`].
pub trait IndexFieldView<T: IndexFieldEncode + ?Sized>: IndexFieldDecode {
    type Ref<'a>: IndexFieldEncode
    where
        T: 'a;
}

/// Converts a borrowed value into the representation used to encode an index field.
pub trait IntoIndexFieldRef<'a, T: IndexFieldEncode + ?Sized> {
    type Output: IndexFieldEncode;

    fn into_index_field_ref(self) -> Self::Output;
}

impl<'a, S, T> IntoIndexFieldRef<'a, T> for &'a S
where
    S: AsRef<T> + ?Sized,
    T: IndexFieldEncode + ?Sized + 'a,
{
    type Output = &'a T;

    fn into_index_field_ref(self) -> Self::Output {
        self.as_ref()
    }
}

impl<'a, S, T> IntoIndexFieldRef<'a, T> for Option<&'a S>
where
    S: AsRef<T> + ?Sized,
    T: IndexFieldEncode + ?Sized + 'a,
{
    type Output = Option<&'a T>;

    fn into_index_field_ref(self) -> Self::Output {
        self.map(AsRef::as_ref)
    }
}

impl<T: IndexFieldEncode + ?Sized> IndexFieldEncode for &T {
    #[inline]
    fn encode_field<B: BufMut>(&self, target: &mut B) {
        T::encode_field(*self, target);
    }

    #[inline]
    fn serialized_length(&self) -> usize {
        T::serialized_length(*self)
    }
}

// Option fields encode u8 NULL tag. If 0 the field is NULL
impl<T: IndexFieldEncode> IndexFieldEncode for Option<T> {
    #[inline]
    fn encode_field<B: BufMut>(&self, target: &mut B) {
        match self {
            Some(value) => {
                target.put_u8(1);
                value.encode_field(target);
            }
            None => target.put_u8(0),
        }
    }

    #[inline]
    fn serialized_length(&self) -> usize {
        match self {
            Some(value) => 1 + value.serialized_length(),
            None => 1,
        }
    }
}

/// A borrowed optional field, retaining both the presence-tagged bytes and the parsed
/// inner view so accessing or decoding it does not repeat encoding validation.
#[derive(Debug, Clone, Copy)]
pub struct EncodedOption<'a, E> {
    bytes: &'a [u8],
    value: Option<E>,
}

impl<'a, E> EncodedOption<'a, E> {
    /// Returns the complete field encoding, including the presence tag.
    pub fn as_bytes(&self) -> &'a [u8] {
        self.bytes
    }

    /// Returns the already-parsed inner view without inspecting its bytes again.
    pub fn as_option(&self) -> Option<E>
    where
        E: Copy,
    {
        self.value
    }
}

impl<E> AsRef<[u8]> for EncodedOption<'_, E> {
    fn as_ref(&self) -> &[u8] {
        self.bytes
    }
}

impl<T: IndexFieldDecode> IndexFieldDecode for Option<T> {
    type Owned = Option<T::Owned>;
    type Encoded<'a> = EncodedOption<'a, T::Encoded<'a>>;

    fn decode_encoded(encoded: Self::Encoded<'_>) -> crate::Result<Self::Owned>
    where
        Self::Owned: Sized,
    {
        encoded.as_option().map(T::decode_encoded).transpose()
    }

    fn take_encoded<'a>(buf: &mut &'a [u8]) -> crate::Result<Self::Encoded<'a>> {
        let original = *buf;
        let Some((&tag, remaining)) = original.split_first() else {
            return Err(StorageError::DataIntegrityError);
        };
        *buf = remaining;

        let value = match tag {
            0 => None,
            1 => Some(T::take_encoded(buf)?),
            _ => return Err(StorageError::DataIntegrityError),
        };

        Ok(EncodedOption {
            bytes: &original[..original.len() - buf.len()],
            value,
        })
    }
}
