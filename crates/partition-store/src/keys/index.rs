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
use zerocopy::{TryFromBytes, big_endian};

use restate_storage_api::StorageError;
use restate_types::ServiceName;
use restate_types::sharding::PartitionId;
use restate_util_string::ReString;

use crate::index::{IndexId, SecondaryIndex};

use super::{EncodedMemCmpStr, KeyDecoder, KeyEncode, KeyKind, MemCmpStr};

const KEY_KIND: KeyKind = KeyKind::SecondaryIndex;

/// Prefix of secondary indexes in one physical partition.
///
/// Its ten-byte representation matches RocksDB's configured fixed-prefix extractor:
/// `ZI (2) | partition padding (2) | partition id (2) | index id (4)`.
#[derive(
    zerocopy::ByteEq,
    zerocopy::IntoBytes,
    zerocopy::TryFromBytes,
    zerocopy::KnownLayout,
    zerocopy::Immutable,
    zerocopy::Unaligned,
)]
#[repr(C)]
pub struct IndexKeyPrefix {
    /// The key kind is always b'ZI'.
    key_kind: [u8; 2],
    /// Zero padding that widens the persisted partition identifier to four bytes.
    padding: big_endian::U16,
    /// The partition that owns the statistic.
    partition_id: big_endian::U16,
    /// The raw globally unique index identifier.
    idx_id: big_endian::U32,
}

static_assertions::const_assert_eq!(size_of::<IndexKeyPrefix>(), crate::DB_PREFIX_LENGTH);
static_assertions::assert_eq_align!(IndexKeyPrefix, u8);

impl IndexKeyPrefix {
    /// Length of the persisted prefix.
    pub const SERIALIZED_LENGTH: usize = size_of::<Self>();

    /// Constructs the prefix for statistic `S` in a physical partition.
    pub fn of<I: SecondaryIndex + ?Sized>(partition_id: PartitionId) -> Self {
        Self {
            key_kind: *KEY_KIND.as_bytes(),
            padding: big_endian::U16::ZERO,
            partition_id: big_endian::U16::new(*partition_id),
            idx_id: big_endian::U32::new(I::INDEX_ID.as_u32()),
        }
    }

    fn key_kind(&self) -> Option<KeyKind> {
        KeyKind::from_bytes(&self.key_kind)
    }

    /// Returns the partition that owns the statistic.
    pub fn partition_id(&self) -> PartitionId {
        PartitionId::from(self.partition_id.get())
    }

    /// Returns the raw persisted index identifier.
    pub fn index_id_raw(&self) -> u32 {
        self.idx_id.get()
    }

    /// Returns the typed index identifier when it is known to this binary.
    pub fn index_id(&self) -> Option<IndexId> {
        IndexId::from_u32(self.index_id_raw())
    }

    /// Decodes and validates an index prefix, returning the index key payload decoder.
    pub fn decode_prefix(key: &[u8]) -> crate::Result<(&Self, KeyDecoder<'_, IndexKeyPrefix, 1>)> {
        KeyDecoder::new_index(key).decode_prefix()
    }
}

impl<'a> KeyDecoder<'a, IndexKeyPrefix, 0> {
    pub fn new_index(remaining: &'a [u8]) -> Self {
        Self {
            remaining,
            _marker: std::marker::PhantomData,
        }
    }

    pub fn decode_prefix(
        &self,
    ) -> Result<(&'a IndexKeyPrefix, KeyDecoder<'a, IndexKeyPrefix, 1>), StorageError> {
        let Ok((prefix, payload)) = IndexKeyPrefix::try_ref_from_prefix(self.remaining) else {
            return Err(StorageError::Conversion(anyhow::anyhow!(
                "failed to decode index key prefix"
            )));
        };
        if prefix.key_kind() != Some(KEY_KIND) {
            return Err(StorageError::Conversion(anyhow::anyhow!(
                "invalid index key prefix"
            )));
        }
        if prefix.padding != big_endian::U16::ZERO {
            return Err(StorageError::Conversion(anyhow::anyhow!(
                "invalid index key partition padding"
            )));
        }

        Ok((
            prefix,
            KeyDecoder {
                remaining: payload,
                _marker: std::marker::PhantomData,
            },
        ))
    }
}

/// Encodes the ordered fields after a key's fixed physical prefix.
pub trait EncodeIndexKey {
    fn encode<B: BufMut>(&self, bytes: &mut B);
    fn encoded_len(&self) -> usize;
}

/// Decodes the ordered fields after a key's fixed physical prefix.
pub trait DecodeIndexKey: Sized {
    fn decode(bytes: &mut &[u8]) -> crate::Result<Self>;
}

impl<'a, C: DecodeIndexKey> KeyDecoder<'a, C> {
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

/// Encodes a field so that lexicographical byte order matches the field's logical order.
///
/// Compound indexes can concatenate these encodings to preserve their field-wise ordering.
pub trait IndexFieldEncode {
    fn encode_field<B: BufMut>(&self, target: &mut B);

    fn serialized_length(&self) -> usize;
}

pub trait IndexFieldDecode {
    type Owned: IndexFieldEncode;
    type Encoded: AsRef<[u8]> + ?Sized + 'static;

    fn decode_field(buf: &mut &[u8]) -> crate::Result<Self::Owned>
    where
        Self::Owned: Sized,
    {
        let encoded = Self::take_encoded(buf)?;
        Self::decode_encoded(encoded)
    }

    fn decode_encoded(encoded: &Self::Encoded) -> crate::Result<Self::Owned>
    where
        Self::Owned: Sized;

    /// Takes the encoded representation of one field without materializing its value.
    fn take_encoded<'a>(buf: &mut &'a [u8]) -> crate::Result<&'a Self::Encoded>;
}

/// A zero-copy view over exactly one encoded index field.
pub struct FieldDecoder<'a, T: IndexFieldDecode + ?Sized> {
    encoded: &'a T::Encoded,
    _marker: std::marker::PhantomData<T>,
}

impl<'a, T: IndexFieldDecode + ?Sized> FieldDecoder<'a, T> {
    pub(crate) fn take(buf: &mut &'a [u8]) -> crate::Result<Self> {
        let encoded = T::take_encoded(buf)?;
        Ok(Self {
            encoded,
            _marker: std::marker::PhantomData,
        })
    }

    /// Returns the raw encoded bytes for this field.
    pub fn as_bytes(&self) -> &[u8] {
        self.encoded.as_ref()
    }

    /// Returns the field's typed encoded representation.
    pub fn encoded(&self) -> &'a T::Encoded {
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

impl<T: IndexFieldDecode> IndexFieldDecode for Option<T> {
    type Owned = Option<T::Owned>;
    type Encoded = [u8];

    fn decode_field(buf: &mut &[u8]) -> crate::Result<Self::Owned>
    where
        Self::Owned: Sized,
    {
        let Some((&tag, remaining)) = buf.split_first() else {
            return Err(StorageError::DataIntegrityError);
        };
        *buf = remaining;

        match tag {
            0 => Ok(None),
            1 => Ok(Some(T::decode_field(buf)?)),
            _ => Err(StorageError::DataIntegrityError),
        }
    }

    fn decode_encoded(encoded: &Self::Encoded) -> crate::Result<Self::Owned>
    where
        Self::Owned: Sized,
    {
        let mut remaining = encoded;
        let decoded = Self::decode_field(&mut remaining)?;
        if !remaining.is_empty() {
            return Err(StorageError::DataIntegrityError);
        }
        Ok(decoded)
    }

    fn take_encoded<'a>(buf: &mut &'a [u8]) -> crate::Result<&'a Self::Encoded> {
        let original = *buf;
        let Some((&tag, remaining)) = original.split_first() else {
            return Err(StorageError::DataIntegrityError);
        };
        *buf = remaining;

        match tag {
            0 => {}
            1 => {
                T::take_encoded(buf)?;
            }
            _ => return Err(StorageError::DataIntegrityError),
        }

        Ok(&original[..original.len() - buf.len()])
    }
}

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
    type Encoded = EncodedMemCmpStr;

    fn decode_encoded(encoded: &Self::Encoded) -> crate::Result<Self::Owned> {
        Ok(encoded.decode())
    }

    fn take_encoded<'a>(buf: &mut &'a [u8]) -> crate::Result<&'a Self::Encoded> {
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
    type Encoded = EncodedMemCmpStr;

    fn decode_encoded(encoded: &Self::Encoded) -> crate::Result<Self::Owned> {
        str::decode_encoded(encoded)
    }

    fn take_encoded<'a>(buf: &mut &'a [u8]) -> crate::Result<&'a Self::Encoded> {
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
    type Encoded = EncodedMemCmpStr;

    fn decode_encoded(encoded: &Self::Encoded) -> crate::Result<Self::Owned> {
        let service_name = encoded.decode();
        if service_name.is_empty() {
            return Err(StorageError::DataIntegrityError);
        }

        Ok(ServiceName::new(service_name.as_str()))
    }

    fn take_encoded<'a>(buf: &mut &'a [u8]) -> crate::Result<&'a Self::Encoded> {
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
    type Encoded = [u8; size_of::<u64>()];

    fn decode_encoded(encoded: &Self::Encoded) -> crate::Result<u64> {
        Ok(u64::from_be_bytes(*encoded))
    }

    fn take_encoded<'a>(buf: &mut &'a [u8]) -> crate::Result<&'a Self::Encoded> {
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
        assert_eq!(field.encoded(), optional);
        assert!(field.decode().is_err());
        assert!(input.is_empty());
    }
}
