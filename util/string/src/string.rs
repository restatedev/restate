// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::sync::Arc;

use compact_str::CompactString;

/// A compact, flexible string type optimized for common usage patterns in Restate.
///
/// `ReString` can hold string data in one of three internal representations:
/// - **Static**: a `&'static str` — zero-cost to clone, no allocation.
/// - **Ref-counted**: an `Arc<str>` — cheap to clone via reference counting.
/// - **Owned**: a [`CompactString`] — inline for short strings (up to 24 bytes on
///   64-bit), heap-allocated otherwise.
///
/// This type is 24 bytes and has niche optimization, so `Option<ReString>` is the
/// same size as `ReString`.
#[derive(Clone)]
pub struct ReString(Inner);

const _: () = {
    assert!(
        24 == size_of::<ReString>(),
        "ReString should be the same size as u64"
    );

    assert!(
        size_of::<Option<ReString>>() == size_of::<ReString>(),
        "ReString should be the same size as Option<ReString>"
    );
};

impl Default for ReString {
    fn default() -> Self {
        Self(Inner::Static(""))
    }
}

impl ReString {
    /// Creates a ref-counted `ReString` from the given string. Cloning the result
    /// only increments a reference count.
    #[inline]
    pub fn new_shared(s: impl AsRef<str>) -> Self {
        Self(Inner::RefCounted(Arc::from(s.as_ref())))
    }

    /// Creates an owned `ReString`. Short strings (up to 24 bytes) are stored inline
    /// without a heap allocation.
    pub fn new_owned(s: impl AsRef<str>) -> Self {
        Self(Inner::Owned(CompactString::new(s.as_ref())))
    }

    /// Creates a `ReString` backed by a `&'static str`. This is zero-cost to clone.
    #[inline]
    pub const fn from_static(s: &'static str) -> Self {
        Self(Inner::Static(s))
    }

    /// Creates a `ReString` from an existing `Arc<str>`.
    #[inline]
    pub fn from_shared(s: Arc<str>) -> Self {
        Self(Inner::RefCounted(s))
    }

    /// Decodes a UTF-8 string from a [`bytes::Buf`] into an owned `ReString`.
    #[cfg(feature = "bytes")]
    pub fn from_utf8_buf<B: bytes::Buf>(buf: &mut B) -> Result<Self, std::str::Utf8Error> {
        Ok(Self(Inner::Owned(CompactString::from_utf8_buf(buf)?)))
    }

    /// Decodes a UTF-8 string from a [`bytes::Buf`] without validating the contents.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the buffer contains valid UTF-8.
    #[cfg(feature = "bytes")]
    pub unsafe fn from_utf8_buf_unchecked<B: bytes::Buf>(buf: &mut B) -> Self {
        Self(Inner::Owned(unsafe {
            CompactString::from_utf8_buf_unchecked(buf)
        }))
    }

    /// Returns `true` if cloning this `ReString` is cheap (i.e. it is static,
    /// ref-counted, or stored inline without a heap allocation).
    #[inline]
    pub fn is_clone_cheap(&self) -> bool {
        match &self.0 {
            Inner::RefCounted(_) | Inner::Static(_) => true,
            Inner::Owned(s) => !s.is_heap_allocated(),
        }
    }

    /// Returns `true` if this `ReString` is backed by a `&'static str`.
    #[inline]
    pub const fn is_static(&self) -> bool {
        match self.0 {
            Inner::Static(_) => true,
            Inner::Owned(ref s) => s.as_static_str().is_some(),
            _ => false,
        }
    }

    /// Returns `true` if this `ReString` is heap-allocated (either ref-counted
    /// or an owned string that exceeded the inline capacity).
    pub fn is_heap_allocated(&self) -> bool {
        match &self.0 {
            Inner::RefCounted(_) => true,
            Inner::Owned(c) => c.is_heap_allocated(),
            _ => false,
        }
    }

    /// Converts this `ReString` into a form that is cheap to clone. If the string
    /// is already cheap to clone (static, ref-counted, or inline), it is returned
    /// as-is. Otherwise, the owned heap data is promoted to an `Arc<str>`.
    #[inline(always)]
    pub fn into_cheap_cloneable(self) -> ReString {
        match self.0 {
            Inner::RefCounted(s) => Self(Inner::RefCounted(s)),
            Inner::Static(s) => Self(Inner::Static(s)),
            Inner::Owned(s) if s.is_heap_allocated() => Self(Inner::RefCounted(Arc::from(s))),
            Inner::Owned(s) => Self(Inner::Owned(s)),
        }
    }

    /// Returns the string contents as a `&str`.
    #[inline(always)]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    /// If this `ReString` is backed by a `&'static str`, returns it. Otherwise
    /// returns `None`.
    #[inline]
    pub const fn as_static_str(&self) -> Option<&'static str> {
        match self.0 {
            Inner::Static(s) => Some(s),
            Inner::Owned(ref s) => s.as_static_str(),
            _ => None,
        }
    }
}

impl std::ops::Deref for ReString {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl AsRef<str> for ReString {
    #[inline(always)]
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

#[derive(Clone)]
enum Inner {
    Static(&'static str),
    RefCounted(Arc<str>),
    Owned(CompactString),
}

impl Inner {
    #[inline(always)]
    fn as_str(&self) -> &str {
        match self {
            Self::Static(s) => s,
            Self::RefCounted(s) => s.as_ref(),
            Self::Owned(s) => s.as_ref(),
        }
    }
}

impl PartialOrd for ReString {
    #[inline(always)]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ReString {
    #[inline(always)]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_str().cmp(other.as_str())
    }
}

impl Eq for ReString {}

impl<T: AsRef<str>> PartialEq<T> for ReString {
    #[inline(always)]
    fn eq(&self, other: &T) -> bool {
        self.as_str().eq(other.as_ref())
    }
}

impl std::hash::Hash for ReString {
    #[inline(always)]
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_str().hash(state)
    }
}

impl From<Cow<'static, str>> for ReString {
    #[inline]
    fn from(s: Cow<'static, str>) -> Self {
        match s {
            Cow::Owned(s) => Self(Inner::Owned(CompactString::new(s.into_boxed_str()))),
            Cow::Borrowed(s) => Self(Inner::Static(s)),
        }
    }
}

impl From<Arc<str>> for ReString {
    #[inline]
    fn from(s: Arc<str>) -> Self {
        Self(Inner::RefCounted(s))
    }
}

impl From<CompactString> for ReString {
    #[inline]
    fn from(s: CompactString) -> Self {
        Self(Inner::Owned(s))
    }
}

impl From<Box<str>> for ReString {
    #[inline]
    fn from(s: Box<str>) -> Self {
        Self(Inner::Owned(CompactString::new(s)))
    }
}

impl From<String> for ReString {
    #[inline]
    fn from(s: String) -> Self {
        Self(Inner::Owned(CompactString::new(s.into_boxed_str())))
    }
}

impl<'a> From<&'a str> for ReString {
    #[inline]
    fn from(s: &'a str) -> Self {
        Self(Inner::Owned(CompactString::new(s)))
    }
}

impl std::fmt::Debug for ReString {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Inner::Static(s) => std::fmt::Debug::fmt(s, f),
            Inner::RefCounted(ref s) => std::fmt::Debug::fmt(s, f),
            Inner::Owned(ref s) => std::fmt::Debug::fmt(s, f),
        }
    }
}

impl std::fmt::Display for ReString {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Inner::Static(s) => std::fmt::Display::fmt(s, f),
            Inner::RefCounted(ref s) => std::fmt::Display::fmt(s, f),
            Inner::Owned(ref s) => std::fmt::Display::fmt(s, f),
        }
    }
}

// SERDE
#[cfg(feature = "serde")]
impl serde::Serialize for ReString {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.as_str().serialize(serializer)
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for ReString {
    fn deserialize<D: serde::de::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let c = CompactString::deserialize(deserializer)?;
        Ok(Self(Inner::Owned(c)))
    }
}

// Bilrost encoding
#[cfg(feature = "bilrost")]
mod bilrost_encoding {
    use bilrost::buf::ReverseBuf;
    use bilrost::encoding::for_overwrite_via_default;
    use bilrost::encoding::{
        Capped, DecodeContext, DistinguishedValueDecoder, EmptyState, GeneralGeneric,
        RestrictedDecodeContext, ValueDecoder, ValueEncoder, WireType, Wiretyped,
        delegate_value_encoding, encode_varint, encoded_len_varint, prepend_varint,
    };
    use bilrost::{Canonicity, DecodeError};
    use bytes::{Buf, BufMut};

    use super::ReString;

    for_overwrite_via_default!(ReString);

    impl EmptyState<(), ReString> for () {
        #[inline]
        fn is_empty(val: &ReString) -> bool {
            val.is_empty()
        }

        #[inline]
        fn clear(val: &mut ReString) {
            *val = Default::default();
        }
    }

    impl<const P: u8> Wiretyped<GeneralGeneric<P>, ReString> for () {
        const WIRE_TYPE: WireType = WireType::LengthDelimited;
    }

    impl<const P: u8> ValueEncoder<GeneralGeneric<P>, ReString> for () {
        #[inline]
        fn encode_value<B: BufMut + ?Sized>(value: &ReString, buf: &mut B) {
            encode_varint(value.len() as u64, buf);
            buf.put_slice(value.as_bytes());
        }

        #[inline]
        fn prepend_value<B: ReverseBuf + ?Sized>(value: &ReString, buf: &mut B) {
            buf.prepend_slice(value.as_bytes());
            prepend_varint(value.len() as u64, buf);
        }

        #[inline]
        fn value_encoded_len(value: &ReString) -> usize {
            encoded_len_varint(value.len() as u64) + value.len()
        }
    }

    impl<const P: u8> ValueDecoder<GeneralGeneric<P>, ReString> for () {
        #[inline]
        fn decode_value<B: Buf + ?Sized>(
            value: &mut ReString,
            mut buf: Capped<B>,
            _ctx: DecodeContext,
        ) -> Result<(), DecodeError> {
            let mut string_data = buf.take_length_delimited()?.take_all();
            // SAFETY:
            // for performance reasons, we assume that the decoding bilrost implies that
            // the encoded value is valid UTF-8. We assume that because we don't ever decode
            // bilrost values from untrusted sources.
            *value = unsafe { ReString::from_utf8_buf_unchecked(&mut string_data) };
            Ok(())
        }
    }

    impl<const P: u8> DistinguishedValueDecoder<GeneralGeneric<P>, ReString> for () {
        const CHECKS_EMPTY: bool = false;

        #[inline]
        fn decode_value_distinguished<const ALLOW_EMPTY: bool>(
            value: &mut ReString,
            buf: Capped<impl Buf + ?Sized>,
            ctx: RestrictedDecodeContext,
        ) -> Result<Canonicity, DecodeError> {
            <() as ValueDecoder<GeneralGeneric<P>, _>>::decode_value(value, buf, ctx.into_inner())?;
            Ok(Canonicity::Canonical)
        }
    }

    delegate_value_encoding!(
        encoding (GeneralGeneric<P>) borrows type (ReString) as owned including distinguished
        with generics (const P: u8)
    );

    #[cfg(test)]
    mod test {
        use bilrost::{Message, OwnedMessage};

        use super::ReString;

        #[derive(bilrost::Message)]
        struct ReStringMessage {
            #[bilrost(1)]
            value: ReString,
        }

        #[derive(bilrost::Message)]
        struct StringMessage {
            #[bilrost(1)]
            value: String,
        }

        #[test]
        fn restring_roundtrip() {
            for input in ["", "hello", "a".repeat(1024).as_str()] {
                let src = ReStringMessage {
                    value: ReString::from(input),
                };
                let encoded = src.encode_to_bytes();
                let decoded = ReStringMessage::decode(encoded).unwrap();
                assert_eq!(decoded.value.as_str(), input);
            }
        }

        #[test]
        fn wire_compatible_with_string() {
            // ReString -> String
            let src = ReStringMessage {
                value: ReString::from("wire-compat"),
            };
            let encoded = src.encode_to_bytes();
            let decoded = StringMessage::decode(encoded).unwrap();
            assert_eq!(decoded.value, "wire-compat");

            // String -> ReString
            let src = StringMessage {
                value: "wire-compat".into(),
            };
            let encoded = src.encode_to_bytes();
            let decoded = ReStringMessage::decode(encoded).unwrap();
            assert_eq!(decoded.value.as_str(), "wire-compat");
        }

        #[test]
        fn empty_restring_roundtrip() {
            let src = ReStringMessage {
                value: ReString::default(),
            };
            assert!(src.value.is_empty());

            let encoded = src.encode_to_bytes();
            let decoded = ReStringMessage::decode(encoded).unwrap();
            assert!(decoded.value.is_empty());
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{OwnedStringLike, StringLike};

    use super::*;

    #[allow(clippy::assertions_on_constants)]
    fn _assert_owned_string_like<T: OwnedStringLike>(_: T) {
        assert!(true);
    }

    // compile-time check that the trait bounds are satisfied
    #[allow(clippy::assertions_on_constants)]
    fn _assert_string_like<T: StringLike>(_: T) {
        assert!(true);
    }

    #[test]
    fn owned_string_like() {
        let s: ReString = "hello".into();
        assert_eq!(s, "hello");
        _assert_owned_string_like(s.clone());
        _assert_string_like(s);
        // non-owned

        let s = "hello";
        assert_eq!(s, "hello");
        _assert_string_like(s);
    }
}
