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

/// A field's physical representation, independent of its Rust value type.
///
/// A schema selects this codec for encoding, lengths, boundary detection, and
/// decoding. Different codecs may share `Value` while using incompatible bytes.
/// `Ref` borrows an encoding input; it is not a view of encoded bytes.
/// This trait alone does not guarantee bytewise ordering.
pub trait FieldCodec {
    /// The decoded value; it does not select a representation on its own.
    type Value;
    /// A borrowed encoding input with the same representation as `Value`.
    type Ref<'a>: Copy;
    /// The complete field bytes, including wrapper tags and parsed inner views.
    type Encoded<'a>: AsRef<[u8]> + Copy + 'a;

    /// Borrows the input used by both owned encoding and length calculation.
    fn borrow(value: &Self::Value) -> Self::Ref<'_>;

    /// Appends a borrowed input without clearing `target`.
    fn encode_ref<B: BufMut>(value: Self::Ref<'_>, target: &mut B);

    /// The exact number of bytes appended by `encode_ref`.
    fn serialized_length_ref(value: Self::Ref<'_>) -> usize;

    fn encode_field<B: BufMut>(value: &Self::Value, target: &mut B) {
        Self::encode_ref(Self::borrow(value), target);
    }

    fn serialized_length(value: &Self::Value) -> usize {
        Self::serialized_length_ref(Self::borrow(value))
    }

    /// Takes and decodes one field, leaving subsequent fields in `buf`.
    fn decode_field(buf: &mut &[u8]) -> crate::Result<Self::Value> {
        Self::decode_encoded(Self::take_encoded(buf)?)
    }

    /// Materializes a field and performs any deferred semantic checks.
    fn decode_encoded(encoded: Self::Encoded<'_>) -> crate::Result<Self::Value>;

    /// Borrows exactly one field and advances `buf`. Success establishes the
    /// boundary but need not imply semantic validity. An error may advance `buf`.
    /// Successfully decoded values must re-encode identically through this codec.
    fn take_encoded<'a>(buf: &mut &'a [u8]) -> crate::Result<Self::Encoded<'a>>;
}

/// Certifies a codec for fields of a byte-ordered composite key.
///
/// # Ordering contract
///
/// Encodings must be canonical and preserve the codec-defined field order under
/// lexicographic byte comparison. They must also be **prefix-free**: no complete
/// encoding of a valid value may be a proper prefix of another valid value's
/// encoding for the same codec. Shared leading bytes are allowed. These properties
/// let keys concatenate fields without separators: later fields cannot change the
/// order established by distinct values of an earlier field.
///
/// Every field of an ordered composite key must satisfy this contract, including
/// fields not referenced by a filter. Mixing in a legacy [`super::KeyEncode`]
/// representation does not preserve the guarantee merely because the same Rust
/// value type also has an ordered codec.
///
/// This does not require [`Ord`] or promise its ordering: enum codecs can use name
/// order, and descending codecs reverse the underlying value order. Logical
/// predicates must be translated to the physical encoding and order before byte
/// comparisons.
///
/// # Nullable fields
///
/// `Option<C>` encodes `None` (NULL) as `[0]` and `Some(value)` as `[1]` followed by
/// codec `C`'s complete encoding. Thus `None` sorts before every `Some`, including
/// an empty string, even when `C` uses descending order. Values inside `Some`
/// retain `C`'s ordering.
///
/// A presence tag does not violate prefix-freedom: `[0]` cannot prefix a `Some`
/// encoding, and encodings sharing the `[1]` tag inherit `C`'s prefix-freedom.
/// "Prefix-free" describes complete encoded values, not an absence of tag prefixes.
///
/// # Examples
///
/// Nullable strings sort NULL first. Although `"a"` is a prefix of `"aa"`, its
/// complete field encoding is not a prefix of the other's encoding:
///
/// ```
/// use std::cmp::Reverse;
///
/// use restate_partition_store::keys::{FieldCodec, OrderedFieldCodec};
/// use restate_partition_store::keys::codecs::{StringCodec, ReverseU64Codec};
///
/// fn encode<C: OrderedFieldCodec>(value: C::Ref<'_>) -> Vec<u8> {
///     let mut bytes = Vec::new();
///     C::encode_ref(value, &mut bytes);
///     bytes
/// }
///
/// let none = encode::<Option<StringCodec>>(None);
/// let empty = encode::<Option<StringCodec>>(Some(""));
/// let a = encode::<Option<StringCodec>>(Some("a"));
/// let aa = encode::<Option<StringCodec>>(Some("aa"));
/// assert_eq!(none, [0]);
/// assert_eq!(a[0], 1);
/// assert!(none < empty && empty < a && a < aa);
/// assert!(!aa.starts_with(&a));
///
/// // A later field cannot reverse the order established by the name.
/// let mut first = a;
/// let mut second = aa;
/// ReverseU64Codec::encode_field(&Reverse(0), &mut first);
/// ReverseU64Codec::encode_field(&Reverse(u64::MAX), &mut second);
/// assert!(first < second);
///
/// // Descending values sort largest first, but the outer NULL tag still sorts first.
/// assert!(encode::<ReverseU64Codec>(&Reverse(9)) < encode::<ReverseU64Codec>(&Reverse(2)));
/// assert!(encode::<Option<ReverseU64Codec>>(None)
///     < encode::<Option<ReverseU64Codec>>(Some(&Reverse(u64::MAX))));
/// ```
pub trait OrderedFieldCodec: FieldCodec {}

/// A zero-copy view over exactly one field in codec `C`'s representation.
///
/// Taking the field advances the input but keeps the field borrowed from the
/// original bytes. [`Self::decode`] materializes it on demand; it does not cache
/// the decoded value.
///
/// # Examples
///
/// ```
/// use restate_partition_store::keys::{FieldCodec, FieldDecoder};
/// use restate_partition_store::keys::codecs::{StringCodec, U64Codec};
///
/// let mut payload = Vec::new();
/// Option::<StringCodec>::encode_ref(Some("svc"), &mut payload);
/// U64Codec::encode_field(&42, &mut payload);
///
/// let mut remaining = payload.as_slice();
/// let name = FieldDecoder::<Option<StringCodec>>::take(&mut remaining)?;
/// let sequence = FieldDecoder::<U64Codec>::take(&mut remaining)?;
/// assert!(remaining.is_empty());
/// assert_eq!(name.encoded().as_option().unwrap().decode(), "svc");
/// assert_eq!(sequence.decode()?, 42);
/// # Ok::<(), restate_storage_api::StorageError>(())
/// ```
pub struct FieldDecoder<'a, C: FieldCodec> {
    encoded: C::Encoded<'a>,
    _marker: PhantomData<C>,
}

impl<'a, C: FieldCodec> FieldDecoder<'a, C> {
    /// Borrows the next field and advances `buf`, deferring owned decoding.
    pub fn take(buf: &mut &'a [u8]) -> crate::Result<Self> {
        let encoded = C::take_encoded(buf)?;
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
    pub fn encoded(&self) -> C::Encoded<'a> {
        self.encoded
    }

    /// Fully decodes this field into its owned representation.
    pub fn decode(&self) -> crate::Result<C::Value> {
        C::decode_encoded(self.encoded)
    }
}

/// Converts an input through `AsRef`, preserving `Option` wrappers.
/// Generated constructors require `Output` to match the selected codec's `Ref`;
/// this conversion does not select an encoding or certify ordering.
pub trait IntoFieldRef<'a, T: ?Sized> {
    type Output;

    fn into_field_ref(self) -> Self::Output;
}

impl<'a, S, T> IntoFieldRef<'a, T> for &'a S
where
    S: AsRef<T> + ?Sized,
    T: ?Sized + 'a,
{
    type Output = &'a T;

    fn into_field_ref(self) -> Self::Output {
        self.as_ref()
    }
}

impl<'a, S, T> IntoFieldRef<'a, T> for Option<&'a S>
where
    S: AsRef<T> + ?Sized,
    T: ?Sized + 'a,
{
    type Output = Option<&'a T>;

    fn into_field_ref(self) -> Self::Output {
        self.map(AsRef::as_ref)
    }
}

/// Encodes `None` as `[0]` and `Some(value)` as `[1]` followed by the inner encoding.
/// NULL sorts first; `Some` values retain `C`'s representation. If `C` is ordered,
/// the wrapper preserves its order and prefix-free boundaries, even for descending
/// codecs. See [`OrderedFieldCodec`] for examples.
impl<C: FieldCodec> FieldCodec for Option<C> {
    type Value = Option<C::Value>;
    type Ref<'a> = Option<C::Ref<'a>>;
    type Encoded<'a> = EncodedOption<'a, C::Encoded<'a>>;

    fn borrow(value: &Self::Value) -> Self::Ref<'_> {
        value.as_ref().map(C::borrow)
    }

    #[inline]
    fn encode_ref<B: BufMut>(value: Self::Ref<'_>, target: &mut B) {
        match value {
            Some(value) => {
                target.put_u8(1);
                C::encode_ref(value, target);
            }
            None => target.put_u8(0),
        }
    }

    #[inline]
    fn serialized_length_ref(value: Self::Ref<'_>) -> usize {
        match value {
            Some(value) => 1 + C::serialized_length_ref(value),
            None => 1,
        }
    }
    fn decode_encoded(encoded: Self::Encoded<'_>) -> crate::Result<Self::Value> {
        encoded.as_option().map(C::decode_encoded).transpose()
    }

    fn take_encoded<'a>(buf: &mut &'a [u8]) -> crate::Result<Self::Encoded<'a>> {
        let original = *buf;
        let Some((&tag, remaining)) = original.split_first() else {
            return Err(StorageError::DataIntegrityError);
        };
        *buf = remaining;
        let value = match tag {
            0 => None,
            1 => Some(C::take_encoded(buf)?),
            _ => return Err(StorageError::DataIntegrityError),
        };
        Ok(EncodedOption {
            bytes: &original[..original.len() - buf.len()],
            value,
        })
    }
}

impl<C: OrderedFieldCodec> OrderedFieldCodec for Option<C> {}

/// A borrowed optional field, retaining both the presence-tagged bytes and the parsed
/// inner view so accessing or decoding it does not repeat structural parsing.
/// Decoding the inner value may still perform semantic checks.
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
