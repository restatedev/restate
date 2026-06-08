// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::{Borrow, Cow};
use std::sync::Arc;

use compact_str::{CompactString, ToCompactString};

use crate::RestateString;

/// The maximum size of a string we can fit inline in a [`ReString`].
pub const INLINE_CAPACITY: usize = std::mem::size_of::<String>();

/// A trait for converting a value to a [`ReString`].
///
/// Automatically implemented for any type that implements
/// [`fmt::Display`][core::fmt::Display]. Don't implement this trait directly:
/// implement [`fmt::Display`][core::fmt::Display] and the [`ToReString`] impl
/// comes for free.
pub trait ToReString {
    /// Converts the given value to a [`ReString`].
    ///
    /// # Panics
    ///
    /// Panics if the system runs out of memory and cannot hold the whole string,
    /// or if [`Display::fmt()`][core::fmt::Display::fmt] returns an error.
    fn to_restring(&self) -> ReString;
}

impl<T: ToCompactString> ToReString for T {
    #[inline]
    fn to_restring(&self) -> ReString {
        ReString::from(self.to_compact_string())
    }
}

/// A compact, flexible string type optimized for common usage patterns in Restate.
///
/// `ReString` is immutable and clones in constant time with no extra allocation.
///
/// Internal representations:
/// - **Inlined**: either a short string (≤ 24 bytes, stored fully inline in the
///   value) or a `&'static str` (the static pointer is held in the inline slot,
///   zero-copy). Cloning copies 24 bytes.
/// - **Ref-counted** (`Arc<str>`): for any heap-resident content. Cloning is a
///   single atomic refcount bump.
///
/// Invariant: the inlined representation never owns a heap allocation — heap
/// content is always promoted to `Arc<str>`.
///
/// This type is 24 bytes and has niche optimization, so `Option<ReString>` is the
/// same size as `ReString`.
#[derive(Clone)]
pub struct ReString(Inner);

const _: () = {
    assert!(
        24 == size_of::<ReString>(),
        "ReString should be the same size as String"
    );

    assert!(
        size_of::<Option<ReString>>() == size_of::<ReString>(),
        "ReString should be the same size as Option<ReString>"
    );
};

impl Default for ReString {
    fn default() -> Self {
        Self(Inner::Inlined(CompactString::const_new("")))
    }
}

impl RestateString for ReString {
    type Err = std::str::Utf8Error;

    #[inline]
    unsafe fn new_unchecked(s: &str) -> Self {
        ReString::new(s)
    }

    #[inline(always)]
    unsafe fn from_restring_unchecked(i: ReString) -> Self {
        i
    }

    #[inline(always)]
    fn try_from_restring(s: ReString) -> Result<Self, Self::Err> {
        Ok(s)
    }

    fn try_from_static(s: &'static str) -> Result<Self, Self::Err> {
        Ok(Self::from_static(s))
    }

    fn try_new(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::new(s))
    }

    fn try_from_arc(s: &Arc<str>) -> Result<Self, Self::Err> {
        Ok(Self::from(s))
    }

    fn to_restring(&self) -> ReString {
        self.clone()
    }

    #[inline(always)]
    fn into_restring(self) -> ReString {
        self
    }
}

impl crate::interned::Internable for ReString {
    fn intern_pool() -> &'static std::thread::LocalKey<crate::interned::InternPool> {
        thread_local! {
            static POOL: crate::interned::InternPool =
                std::cell::RefCell::new(hashbrown::HashSet::default());
        }
        &POOL
    }
}

impl ReString {
    /// Inline if the slice fits inline (≤ `size_of::<String>()` bytes), else
    /// heap-allocate as `Arc<str>`. Upholds the `Inlined ⇒ not heap-allocated`
    /// invariant.
    #[inline(always)]
    fn dispatch_str(s: &str) -> Inner {
        if s.len() <= INLINE_CAPACITY {
            let c = CompactString::from(s);
            debug_assert!(!c.is_heap_allocated());
            Inner::Inlined(c)
        } else {
            Inner::RefCounted(Arc::from(s))
        }
    }

    /// Inline if the `CompactString` is not heap-allocated (i.e. inline or
    /// static), else promote to `Arc<str>`.
    #[inline(always)]
    fn dispatch_compact(c: CompactString) -> Inner {
        if c.is_heap_allocated() {
            Inner::RefCounted(Arc::from(c.as_str()))
        } else {
            Inner::Inlined(c)
        }
    }

    /// Inline if the input fits inline (≤ [`INLINE_CAPACITY`] bytes), otherwise
    /// heap-allocate as `Arc<str>`. Both forms clone in constant time.
    #[inline]
    pub fn new(s: impl AsRef<str>) -> Self {
        Self(Self::dispatch_str(s.as_ref()))
    }

    /// Creates a `ReString` backed by a `&'static str`. Zero-copy: the static
    /// pointer is held inside the inline slot. Cheap to clone (24-byte memcpy
    /// of the inline slot, no allocation).
    #[inline(always)]
    pub const fn from_static(s: &'static str) -> Self {
        Self(Inner::Inlined(CompactString::const_new(s)))
    }

    /// Decodes a UTF-8 string from a [`bytes::Buf`], draining `buf.remaining()` bytes.
    ///
    /// Short payloads (≤ [`INLINE_CAPACITY`]) stay inline. Longer payloads are
    /// written directly into a single `Arc<str>` allocation sized exactly to
    /// `buf.remaining()` — no intermediate buffer, no growth.
    #[cfg(feature = "bytes")]
    pub fn from_utf8_buf<B: bytes::Buf>(buf: &mut B) -> Result<Self, std::str::Utf8Error> {
        let len = buf.remaining();
        if len <= INLINE_CAPACITY {
            let c = CompactString::from_utf8_buf(buf)?;
            debug_assert!(!c.is_heap_allocated());
            Ok(Self(Inner::Inlined(c)))
        } else {
            let arc_bytes = buf_alloc::arc_bytes_from_buf(buf, len);
            std::str::from_utf8(&arc_bytes)?;
            // SAFETY: validated above; arc_bytes is dropped on the error path.
            let arc_str = unsafe { buf_alloc::arc_str_from_arc_bytes(arc_bytes) };
            Ok(Self(Inner::RefCounted(arc_str)))
        }
    }

    /// Decodes a UTF-8 string from a [`bytes::Buf`] without validating the contents.
    ///
    /// Short payloads stay inline. Longer payloads land in a single `Arc<str>`
    /// allocation; no intermediate `CompactString` buffer is built.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the buffer contains valid UTF-8.
    #[cfg(feature = "bytes")]
    pub unsafe fn from_utf8_buf_unchecked<B: bytes::Buf>(buf: &mut B) -> Self {
        let len = buf.remaining();
        if len <= INLINE_CAPACITY {
            // SAFETY: caller upholds UTF-8 invariant.
            let c = unsafe { CompactString::from_utf8_buf_unchecked(buf) };
            debug_assert!(!c.is_heap_allocated());
            Self(Inner::Inlined(c))
        } else {
            let arc_bytes = buf_alloc::arc_bytes_from_buf(buf, len);
            // SAFETY: caller upholds UTF-8 invariant.
            let arc_str = unsafe { buf_alloc::arc_str_from_arc_bytes(arc_bytes) };
            Self(Inner::RefCounted(arc_str))
        }
    }

    /// Returns `true` if this `ReString` is backed by a `&'static str`.
    #[inline(always)]
    pub const fn is_static(&self) -> bool {
        match self.0 {
            Inner::Inlined(ref s) => s.as_static_str().is_some(),
            Inner::RefCounted(_) => false,
        }
    }

    /// Returns `true` if the string is held on the heap (i.e. ref-counted).
    /// Inline and static-backed values return `false`.
    #[inline(always)]
    pub const fn is_heap_allocated(&self) -> bool {
        matches!(self.0, Inner::RefCounted(_))
    }

    /// Returns the string contents as a `&str`.
    #[inline(always)]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    /// If this `ReString` is backed by a `&'static str`, returns it. Otherwise
    /// returns `None`.
    #[inline(always)]
    pub const fn as_static_str(&self) -> Option<&'static str> {
        match self.0 {
            Inner::Inlined(ref s) => s.as_static_str(),
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

impl Borrow<str> for ReString {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl AsRef<str> for ReString {
    #[inline(always)]
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl AsRef<[u8]> for ReString {
    #[inline(always)]
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

#[cfg(feature = "bytes")]
impl From<ReString> for bytes::Bytes {
    #[inline]
    fn from(value: ReString) -> Self {
        bytes::Bytes::from_owner(value)
    }
}

#[cfg(feature = "bytes")]
impl TryFrom<bytes::Bytes> for ReString {
    type Error = std::str::Utf8Error;

    #[inline]
    fn try_from(mut value: bytes::Bytes) -> Result<Self, Self::Error> {
        ReString::from_utf8_buf(&mut value)
    }
}

#[cfg(feature = "bytestring")]
impl From<ReString> for bytestring::ByteString {
    #[inline]
    fn from(value: ReString) -> Self {
        // SAFETY: the ReString is already valid UTF-8.
        unsafe { bytestring::ByteString::from_bytes_unchecked(bytes::Bytes::from_owner(value)) }
    }
}

#[derive(Clone)]
enum Inner {
    RefCounted(Arc<str>),
    Inlined(CompactString),
}

impl Inner {
    #[inline(always)]
    fn as_str(&self) -> &str {
        match self {
            Self::RefCounted(s) => s.as_ref(),
            Self::Inlined(s) => s.as_ref(),
        }
    }
}

#[cfg(feature = "bytes")]
mod buf_alloc {
    use std::mem::MaybeUninit;
    use std::sync::Arc;

    use bytes::Buf;

    /// Drains exactly `len` bytes from `buf` into a freshly allocated
    /// `Arc<[u8]>`. One heap allocation, sized exactly; no intermediate buffer.
    ///
    /// Caller must ensure `buf.remaining() >= len` (`copy_to_slice` panics
    /// otherwise).
    pub(super) fn arc_bytes_from_buf<B: Buf>(buf: &mut B, len: usize) -> Arc<[u8]> {
        let mut arc: Arc<[MaybeUninit<u8>]> = Arc::new_uninit_slice(len);
        // Fresh Arc has refcount 1 and no weak refs — `get_mut` always succeeds.
        let dest = Arc::get_mut(&mut arc).expect("fresh Arc has unique ownership");
        // SAFETY: dest covers `len` MaybeUninit<u8>s. Treating as &mut [u8] is sound
        // because copy_to_slice writes every byte before any read, and u8 has no
        // validity requirement so `MaybeUninit<u8>` and `u8` share layout.
        let dest_bytes =
            unsafe { std::slice::from_raw_parts_mut(dest.as_mut_ptr().cast::<u8>(), len) };
        buf.copy_to_slice(dest_bytes);
        // SAFETY: copy_to_slice initialized all `len` bytes.
        unsafe { arc.assume_init() }
    }

    /// Reinterprets an `Arc<[u8]>` as `Arc<str>` without copying.
    ///
    /// # Safety
    ///
    /// The bytes must be valid UTF-8.
    pub(super) unsafe fn arc_str_from_arc_bytes(bytes: Arc<[u8]>) -> Arc<str> {
        // `Arc<[u8]>` and `Arc<str>` share the same allocation layout: ArcInner
        // header followed by the bytes, with the slice length carried in the fat
        // pointer's metadata (identical for `[u8]` and `str`).
        // SAFETY: caller upholds the UTF-8 invariant.
        unsafe { Arc::from_raw(Arc::into_raw(bytes) as *const str) }
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
            Cow::Owned(s) => Self(Self::dispatch_str(&s)),
            Cow::Borrowed(s) => Self(Inner::Inlined(CompactString::const_new(s))),
        }
    }
}

#[cfg(feature = "metrics")]
impl From<ReString> for metrics::SharedString {
    #[inline]
    fn from(s: ReString) -> Self {
        match s.0 {
            Inner::RefCounted(s) => metrics::SharedString::from_shared(s),
            Inner::Inlined(s) => {
                if let Some(s) = s.as_static_str() {
                    metrics::SharedString::const_str(s)
                } else {
                    metrics::SharedString::from_owned(s.into_string())
                }
            }
        }
    }
}

#[cfg(feature = "opentelemetry")]
impl From<ReString> for opentelemetry::Value {
    #[inline]
    fn from(s: ReString) -> Self {
        opentelemetry::Value::String(opentelemetry::StringValue::from(s))
    }
}

#[cfg(feature = "opentelemetry")]
impl From<ReString> for opentelemetry::StringValue {
    #[inline]
    fn from(s: ReString) -> Self {
        match s.0 {
            Inner::RefCounted(s) => opentelemetry::StringValue::from(s),
            Inner::Inlined(s) => {
                if let Some(s) = s.as_static_str() {
                    opentelemetry::StringValue::from(s)
                } else {
                    opentelemetry::StringValue::from(s.into_string())
                }
            }
        }
    }
}

impl From<Arc<str>> for ReString {
    #[inline]
    fn from(s: Arc<str>) -> Self {
        if s.len() <= INLINE_CAPACITY {
            Self(Inner::Inlined(CompactString::new(s)))
        } else {
            Self(Inner::RefCounted(s))
        }
    }
}

impl<'a> From<&'a Arc<str>> for ReString {
    #[inline]
    fn from(s: &'a Arc<str>) -> Self {
        if s.len() <= INLINE_CAPACITY {
            Self(Inner::Inlined(CompactString::new(s)))
        } else {
            Self(Inner::RefCounted(Arc::clone(s)))
        }
    }
}

impl From<CompactString> for ReString {
    #[inline]
    fn from(s: CompactString) -> Self {
        Self(Self::dispatch_compact(s))
    }
}

impl From<Box<str>> for ReString {
    #[inline]
    fn from(s: Box<str>) -> Self {
        if s.len() <= INLINE_CAPACITY {
            Self(Inner::Inlined(CompactString::from(s)))
        } else {
            Self(Inner::RefCounted(Arc::from(s)))
        }
    }
}

impl From<String> for ReString {
    #[inline]
    fn from(s: String) -> Self {
        Self(Self::dispatch_str(&s))
    }
}

impl<'a> From<&'a str> for ReString {
    #[inline]
    fn from(s: &'a str) -> Self {
        Self::new(s)
    }
}

impl std::fmt::Debug for ReString {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Inner::RefCounted(ref s) => std::fmt::Debug::fmt(s, f),
            Inner::Inlined(ref s) => std::fmt::Debug::fmt(s, f),
        }
    }
}

impl std::fmt::Display for ReString {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Inner::RefCounted(ref s) => std::fmt::Display::fmt(s, f),
            Inner::Inlined(ref s) => std::fmt::Display::fmt(s, f),
        }
    }
}

// SERDE
#[cfg(feature = "serde")]
impl serde_core::Serialize for ReString {
    fn serialize<S: serde_core::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.as_str().serialize(serializer)
    }
}

#[cfg(feature = "serde")]
impl<'de> serde_core::Deserialize<'de> for ReString {
    fn deserialize<D: serde_core::de::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Self, D::Error> {
        struct ReStringVisitor;

        impl<'de> serde_core::de::Visitor<'de> for ReStringVisitor {
            type Value = ReString;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a string")
            }

            #[inline]
            fn visit_str<E>(self, v: &str) -> Result<ReString, E>
            where
                E: serde_core::de::Error,
            {
                Ok(ReString::new(v))
            }

            #[inline]
            fn visit_borrowed_str<E>(self, v: &'de str) -> Result<ReString, E>
            where
                E: serde_core::de::Error,
            {
                Ok(ReString::new(v))
            }

            #[inline]
            fn visit_string<E>(self, v: String) -> Result<ReString, E>
            where
                E: serde_core::de::Error,
            {
                Ok(ReString::from(v))
            }

            fn visit_bytes<E>(self, v: &[u8]) -> Result<ReString, E>
            where
                E: serde_core::de::Error,
            {
                std::str::from_utf8(v)
                    .map(ReString::new)
                    .map_err(|_| E::invalid_value(serde_core::de::Unexpected::Bytes(v), &self))
            }

            fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<ReString, E>
            where
                E: serde_core::de::Error,
            {
                String::from_utf8(v).map(ReString::from).map_err(|e| {
                    E::invalid_value(serde_core::de::Unexpected::Bytes(&e.into_bytes()), &self)
                })
            }
        }

        deserializer.deserialize_str(ReStringVisitor)
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

    fn _assert_owned_string_like<T: OwnedStringLike>() {}
    fn _assert_string_like<T: StringLike>() {}

    #[test]
    fn trait_bounds() {
        _assert_owned_string_like::<ReString>();
        _assert_string_like::<ReString>();
        _assert_string_like::<&str>();
    }

    /// 100-byte filler — well over the 24-byte inline boundary.
    const LONG: &str = "this string is much longer than the inline boundary used by ReString \
         and so it lands on heap";

    #[test]
    fn short_input_is_inlined() {
        let r = ReString::new("short");
        assert!(!r.is_heap_allocated());
        assert!(!r.is_static());
        assert_eq!(r, "short");
    }

    #[test]
    fn long_input_is_ref_counted() {
        let r = ReString::new(LONG);
        assert!(r.is_heap_allocated());
        assert_eq!(r.as_str(), LONG);
    }

    /// Every constructor that may receive heap content must promote to
    /// `Arc<str>`, never store a heap-allocated `CompactString` in `Inlined`.
    #[test]
    fn constructors_uphold_heap_invariant() {
        // From<&str>
        assert!(ReString::from(LONG).is_heap_allocated());

        // From<String>
        assert!(ReString::from(LONG.to_owned()).is_heap_allocated());

        // From<Box<str>>
        assert!(ReString::from(LONG.to_owned().into_boxed_str()).is_heap_allocated());

        // From<CompactString> with heap-allocated content
        let c = CompactString::new(LONG);
        assert!(c.is_heap_allocated());
        assert!(ReString::from(c).is_heap_allocated());

        // From<Cow::Owned>
        let cow: Cow<'static, str> = Cow::Owned(LONG.to_owned());
        assert!(ReString::from(cow).is_heap_allocated());

        // From<Cow::Borrowed> stays static-Inlined regardless of length
        let cow: Cow<'static, str> = Cow::Borrowed(LONG);
        let r = ReString::from(cow);
        assert!(!r.is_heap_allocated());
        assert!(r.is_static());

        // ToReString::to_restring goes through CompactString::from_display
        let r: ReString = LONG.to_restring();
        assert!(r.is_heap_allocated());
    }

    #[test]
    fn from_static_is_zero_copy_and_clone_preserves_pointer() {
        const S: &str = "static string longer than the 24-byte inline boundary";
        let r = ReString::from_static(S);
        assert!(r.is_static());
        assert!(!r.is_heap_allocated());
        assert_eq!(r.as_str().as_ptr(), S.as_ptr());

        // Intentional clone to test that the clone keeps the inlined representation.
        #[allow(clippy::redundant_clone)]
        let cloned = r.clone();
        assert!(cloned.is_static());
        assert_eq!(cloned.as_str().as_ptr(), S.as_ptr());
        assert_eq!(cloned.as_static_str(), Some(S));
    }

    #[test]
    fn ref_counted_clones_share_buffer() {
        let r = ReString::new(LONG);
        let p = r.as_str().as_ptr();
        // Intentional clone to test that the clone keeps the inlined representation.
        #[allow(clippy::redundant_clone)]
        let r2 = r.clone();
        assert_eq!(r2.as_str().as_ptr(), p);
    }

    #[test]
    fn default_is_empty_and_not_heap() {
        let r = ReString::default();
        assert!(r.is_empty());
        assert!(!r.is_heap_allocated());
    }

    #[test]
    fn equality_across_representations() {
        let s = "compare-me!";
        let inline = ReString::new(s);
        let from_str = ReString::from(s);
        let from_string = ReString::from(s.to_owned());
        assert_eq!(inline, from_str);
        assert_eq!(from_str, from_string);
        assert_eq!(inline, s);
    }

    #[cfg(feature = "bytes")]
    mod bytes_buf {
        use std::collections::VecDeque;
        use std::io::Cursor;

        use bytes::Buf;

        use super::*;

        #[test]
        fn from_utf8_buf_short_stays_inline() {
            let mut buf = Cursor::new(b"short");
            let r = ReString::from_utf8_buf(&mut buf).unwrap();
            assert!(!r.is_heap_allocated());
            assert_eq!(r.as_str(), "short");
            assert_eq!(buf.remaining(), 0);
        }

        #[test]
        fn from_utf8_buf_long_is_ref_counted() {
            let mut buf = Cursor::new(LONG.as_bytes());
            let r = ReString::from_utf8_buf(&mut buf).unwrap();
            assert!(r.is_heap_allocated());
            assert_eq!(r.as_str(), LONG);
            assert_eq!(buf.remaining(), 0);
        }

        #[test]
        fn from_utf8_buf_unchecked_long_is_ref_counted() {
            let mut buf = Cursor::new(LONG.as_bytes());
            // SAFETY: LONG is valid UTF-8.
            let r = unsafe { ReString::from_utf8_buf_unchecked(&mut buf) };
            assert!(r.is_heap_allocated());
            assert_eq!(r.as_str(), LONG);
        }

        /// Non-contiguous `Buf` must be drained correctly across all chunks.
        #[test]
        fn from_utf8_buf_handles_non_contiguous_buf() {
            // Split LONG across two slices in a VecDeque ring so chunk() returns
            // them in two separate calls.
            let bytes = LONG.as_bytes();
            let (front, back) = bytes.split_at(bytes.len() / 2);
            let mut deque: VecDeque<u8> = VecDeque::with_capacity(bytes.len());
            // Push back so deque becomes non-contiguous after some pops.
            for &b in back {
                deque.push_back(b);
            }
            for &b in front.iter().rev() {
                deque.push_front(b);
            }
            // Confirm non-contiguous.
            let (a, b) = deque.as_slices();
            assert!(!a.is_empty() && !b.is_empty());
            assert_eq!(deque.remaining(), bytes.len());

            let r = ReString::from_utf8_buf(&mut deque).unwrap();
            assert!(r.is_heap_allocated());
            assert_eq!(r.as_str(), LONG);
            assert_eq!(deque.remaining(), 0);
        }

        #[test]
        fn from_utf8_buf_invalid_utf8_errors() {
            // 0x80 alone is not valid UTF-8. Pad to be > INLINE_CAPACITY so we
            // exercise the Arc-allocation path.
            let mut payload = vec![b'a'; INLINE_CAPACITY];
            payload.push(0x80);
            let mut buf = Cursor::new(payload);
            assert!(ReString::from_utf8_buf(&mut buf).is_err());
        }
    }

    #[cfg(feature = "serde")]
    #[test]
    fn deserialize_long_is_ref_counted() {
        let json = serde_json::to_string(LONG).unwrap();
        let r: ReString = serde_json::from_str(&json).unwrap();
        assert!(r.is_heap_allocated());
        assert_eq!(r.as_str(), LONG);
    }

    #[cfg(feature = "serde")]
    #[test]
    fn deserialize_short_stays_inline() {
        let json = "\"hi\"";
        let r: ReString = serde_json::from_str(json).unwrap();
        assert!(!r.is_heap_allocated());
        assert_eq!(r.as_str(), "hi");
    }
}
