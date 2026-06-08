// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Thread-local string interner for [`ReString`]-shaped types.
//!
//! [`Interned<S>`] deduplicates long-lived strings by storing one canonical
//! [`ReString`] per distinct value in a per-thread pool. Subsequent constructions of
//! the same value return a clone of the canonical entry instead of allocating a new
//! `Arc<str>`.
//!
//! Composing over `S` keeps interning orthogonal to validation:
//! - [`Interned<ReString>`] — plain interned string.
//! - [`Interned<RestrictedValue<ReString>>`](crate::RestrictedValue) — validated *and* interned.
//!
//! Interning saves an allocation only when the caller is *about to* allocate. If the
//! caller already owns one (`Arc<str>`, `ReString`, `String`, `'static str`, …) the
//! corresponding `try_from_*` constructor wraps it without consulting the interner.

use std::borrow::Borrow;
use std::cell::RefCell;
use std::sync::Arc;
use std::thread::LocalKey;

use hashbrown::HashSet;

use crate::{ReString, RestateString, StringLike};

/// Per-`S` thread-local pool type. Each [`Internable`] implementer owns its own
/// `thread_local!` static of this shape, so pools never mix entries across `S`.
pub(crate) type InternPool = RefCell<HashSet<ReString>>;

/// Grants a [`RestateString`] type its own per-thread interner pool. Every entry in
/// `Self`'s pool was inserted via `Self::try_new` and so satisfies `Self`'s
/// invariants — the hit path can re-wrap a canonical [`ReString`] via
/// [`RestateString::from_unchecked`] without re-validating.
pub(crate) trait Internable: RestateString {
    fn intern_pool() -> &'static LocalKey<InternPool>;
}

/// A deduplicated, optionally validated string backed by a thread-local interner.
///
/// `S` is the inner type — [`ReString`] for plain interned strings, or a validated
/// wrapper like [`RestrictedValue<ReString>`](crate::RestrictedValue) for validated
/// + interned strings.
///
/// Short strings (≤ `size_of::<String>()` bytes) bypass the interner — they are
/// stored inline inside the `ReString`, where deduplication has no benefit.
///
/// # When to use
///
/// Use this for long-lived, frequently-duplicated identifiers (service names, handler
/// names, deployment IDs, etc.) where deduplication saves memory. Avoid it for
/// short-lived or unique strings where the interner lookup is pure overhead.
///
/// Interned strings pin memory for the lifetime of the process — treat them as
/// effectively leaked. Only intern values that are validated, sanitized, or
/// deserialized from a trusted source (partition store, metadata store, etc.).
/// There is currently no way to clear the interner.
///
/// # Thread safety
///
/// Each thread maintains its own interner. An `Interned<S>` is `Send + Sync` via the
/// inner `Arc` and can be moved across threads, but interning the same value on a
/// different thread produces a separate canonical entry on that thread.
#[derive(Clone, Eq, PartialEq, Hash, Ord, PartialOrd, Default)]
pub struct Interned<S>(S);

impl<S> RestateString for Interned<S>
where
    S: Internable,
{
    type Err = S::Err;

    #[inline]
    unsafe fn new_unchecked(s: &str) -> Self {
        // Will intern if possible but will not run any validation for any intermediate
        // wrappers (e.g. RestrictedValue).
        unsafe { ThreadInterner::get_or_intern_unchecked(s) }
    }

    #[inline(always)]
    unsafe fn from_restring_unchecked(s: ReString) -> Self {
        unsafe { Self(S::from_restring_unchecked(s)) }
    }

    #[inline(always)]
    fn try_from_restring(s: ReString) -> Result<Self, Self::Err> {
        Ok(Self(S::try_from_restring(s)?))
    }

    #[inline(always)]
    fn try_from_static(s: &'static str) -> Result<Self, Self::Err> {
        // Already static — skip the interner.
        Ok(Interned(S::try_from_static(s)?))
    }

    #[inline(always)]
    fn try_new(s: &str) -> Result<Self, Self::Err> {
        ThreadInterner::try_get_or_intern(s)
    }

    #[inline(always)]
    fn try_from_arc(s: &Arc<str>) -> Result<Self, Self::Err> {
        // Caller already owns an Arc — skip the interner, but still validate.
        Ok(Interned(S::try_from_arc(s)?))
    }

    #[inline(always)]
    fn to_restring(&self) -> ReString {
        self.0.to_restring()
    }

    #[inline(always)]
    fn into_restring(self) -> ReString {
        self.0.into_restring()
    }
}

impl<S: Internable> Internable for Interned<S> {
    #[inline(always)]
    fn intern_pool() -> &'static LocalKey<InternPool> {
        S::intern_pool()
    }
}

impl<S: StringLike> Interned<S> {
    /// Returns the string contents as a `&str`.
    #[inline(always)]
    pub fn as_str(&self) -> &str {
        self.0.as_ref()
    }

    #[inline(always)]
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl<S: StringLike> std::fmt::Debug for Interned<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.0, f)
    }
}

impl<S: StringLike> std::fmt::Display for Interned<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl<S: StringLike> std::ops::Deref for Interned<S> {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl<S: StringLike> AsRef<[u8]> for Interned<S> {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl<S: StringLike> AsRef<str> for Interned<S> {
    fn as_ref(&self) -> &str {
        self.0.as_ref()
    }
}

impl<S> AsRef<S> for Interned<S> {
    fn as_ref(&self) -> &S {
        &self.0
    }
}

impl<S: StringLike> Borrow<str> for Interned<S> {
    fn borrow(&self) -> &str {
        self.0.borrow()
    }
}

impl<S> Borrow<ReString> for Interned<S>
where
    S: Borrow<ReString>,
{
    fn borrow(&self) -> &ReString {
        self.0.borrow()
    }
}

/// Wraps a `ReString` without consulting the interner — `ReString` is already cheap
/// to clone.
impl From<ReString> for Interned<ReString> {
    #[inline]
    fn from(value: ReString) -> Self {
        Self(value)
    }
}

impl<S> Interned<S> {
    #[inline]
    pub const fn non_interned(s: S) -> Self {
        Interned(s)
    }
}

impl Interned<ReString> {
    /// Infallible constructor for `Interned<ReString>`.
    #[inline]
    pub fn new(s: &str) -> Self {
        // Safety: creating a raw-string requires on validation.
        unsafe { ThreadInterner::get_or_intern_unchecked(s) }
    }
}

pub(crate) struct ThreadInterner;

impl ThreadInterner {
    pub(crate) fn try_get_or_intern<S: Internable>(s: &str) -> Result<Interned<S>, S::Err> {
        // Short strings stay inline inside `ReString` — no allocation, no interner
        // lookup needed.
        if s.len() <= crate::string::INLINE_CAPACITY {
            return Ok(Interned(S::try_new(s)?));
        }

        S::intern_pool().with_borrow_mut(|pool| {
            if let Some(found) = pool.get(s) {
                // SAFETY: every entry in `S`'s pool was inserted via `S::try_new`,
                // so it already satisfies `S`'s invariants.
                Ok(Interned(unsafe {
                    S::from_restring_unchecked(found.clone())
                }))
            } else {
                // Do not intern if we failed to validate/create.
                let inner = S::try_new(s)?;
                pool.insert(inner.to_restring());
                Ok(Interned(inner))
            }
        })
    }

    pub(crate) unsafe fn get_or_intern_unchecked<S: Internable>(s: &str) -> Interned<S> {
        // Short strings stay inline inside `ReString` — no allocation, no interner
        // lookup needed.
        if s.len() <= crate::string::INLINE_CAPACITY {
            return unsafe { Interned(S::new_unchecked(s)) };
        }

        S::intern_pool().with_borrow_mut(|pool| {
            if let Some(found) = pool.get(s) {
                // SAFETY: every entry in `S`'s pool was inserted via `S::try_new`,
                // so it already satisfies `S`'s invariants.
                Interned(unsafe { S::from_restring_unchecked(found.clone()) })
            } else {
                let raw = ReString::new(s);
                pool.insert(raw.clone());
                Interned(unsafe { S::from_restring_unchecked(raw) })
            }
        })
    }

    #[cfg(test)]
    pub(crate) fn len<S: Internable>() -> usize {
        S::intern_pool().with_borrow(|pool| pool.len())
    }

    #[cfg(test)]
    pub(crate) fn contains<S: Internable>(s: &str) -> bool {
        S::intern_pool().with_borrow(|pool| pool.contains(s))
    }
}

#[cfg(feature = "serde")]
impl<T: StringLike> serde_core::Serialize for Interned<T> {
    fn serialize<S: serde_core::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.as_str().serialize(serializer)
    }
}

#[cfg(feature = "serde")]
impl<'de, S> serde_core::Deserialize<'de> for Interned<S>
where
    S: Internable,
{
    fn deserialize<D: serde_core::de::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Self, D::Error> {
        use std::marker::PhantomData;

        use serde_core::de::{Error, Unexpected, Visitor};

        struct InternedVisitor<S>(PhantomData<S>);

        impl<'a, S: Internable> Visitor<'a> for InternedVisitor<S> {
            type Value = Interned<S>;

            fn expecting(&self, formatter: &mut core::fmt::Formatter) -> core::fmt::Result {
                formatter.write_str("a string")
            }

            fn visit_str<E: Error>(self, v: &str) -> Result<Self::Value, E> {
                ThreadInterner::try_get_or_intern(v).map_err(|e| Error::custom(e))
            }

            fn visit_borrowed_str<E: Error>(self, v: &'a str) -> Result<Self::Value, E> {
                ThreadInterner::try_get_or_intern(v).map_err(|e| Error::custom(e))
            }

            fn visit_string<E: Error>(self, v: String) -> Result<Self::Value, E> {
                ThreadInterner::try_get_or_intern(&v).map_err(|e| Error::custom(e))
            }

            fn visit_bytes<E: Error>(self, v: &[u8]) -> Result<Self::Value, E> {
                match core::str::from_utf8(v) {
                    Ok(s) => ThreadInterner::try_get_or_intern(s).map_err(|e| Error::custom(e)),
                    Err(_) => Err(Error::invalid_value(Unexpected::Bytes(v), &self)),
                }
            }

            fn visit_borrowed_bytes<E: Error>(self, v: &'a [u8]) -> Result<Self::Value, E> {
                match core::str::from_utf8(v) {
                    Ok(s) => ThreadInterner::try_get_or_intern(s).map_err(|e| Error::custom(e)),
                    Err(_) => Err(Error::invalid_value(Unexpected::Bytes(v), &self)),
                }
            }

            fn visit_byte_buf<E: Error>(self, v: Vec<u8>) -> Result<Self::Value, E> {
                match core::str::from_utf8(&v) {
                    Ok(s) => ThreadInterner::try_get_or_intern(s).map_err(|e| Error::custom(e)),
                    Err(_e) => Err(Error::invalid_value(Unexpected::Bytes(&v), &self)),
                }
            }
        }

        deserializer.deserialize_str(InternedVisitor(PhantomData))
    }
}

#[cfg(feature = "bilrost")]
mod bilrost_encoding {
    use bilrost::buf::ReverseBuf;
    use bilrost::encoding::{
        Capped, DecodeContext, DistinguishedValueDecoder, EmptyState, ForOverwrite, GeneralGeneric,
        RestrictedDecodeContext, ValueDecoder, ValueEncoder, WireType, Wiretyped,
        delegate_value_encoding, encode_varint, encoded_len_varint, prepend_varint,
    };
    use bilrost::{Canonicity, DecodeError, DecodeErrorKind};
    use bytes::{Buf, BufMut};

    use super::{Internable, Interned, ReString, ThreadInterner};

    impl<S: Internable> ForOverwrite<(), Interned<S>> for () {
        #[inline]
        fn for_overwrite() -> Interned<S> {
            // SAFETY: empty `ReString` is a transient placeholder used only during
            // bilrost decoding. The decoder always overwrites this value via
            // `decode_value` before it is observed, so `S`'s validation invariants
            // are not relied upon here.
            Interned(unsafe { S::from_restring_unchecked(ReString::default()) })
        }
    }

    impl<S: Internable> EmptyState<(), Interned<S>> for () {
        #[inline]
        fn is_empty(val: &Interned<S>) -> bool {
            val.as_str().is_empty()
        }

        #[inline]
        fn clear(val: &mut Interned<S>) {
            // SAFETY: see `ForOverwrite::for_overwrite` above.
            *val = Interned(unsafe { S::from_restring_unchecked(ReString::default()) });
        }
    }

    impl<const P: u8, S: Internable> Wiretyped<GeneralGeneric<P>, Interned<S>> for () {
        const WIRE_TYPE: WireType = WireType::LengthDelimited;
    }

    impl<const P: u8, S: Internable> ValueEncoder<GeneralGeneric<P>, Interned<S>> for () {
        #[inline]
        fn encode_value<B: BufMut + ?Sized>(value: &Interned<S>, buf: &mut B) {
            let s = value.as_str();
            encode_varint(s.len() as u64, buf);
            buf.put_slice(s.as_bytes());
        }

        #[inline]
        fn prepend_value<B: ReverseBuf + ?Sized>(value: &Interned<S>, buf: &mut B) {
            let s = value.as_str();
            buf.prepend_slice(s.as_bytes());
            prepend_varint(s.len() as u64, buf);
        }

        #[inline]
        fn value_encoded_len(value: &Interned<S>) -> usize {
            let s = value.as_str();
            encoded_len_varint(s.len() as u64) + s.len()
        }
    }

    impl<const P: u8, S: Internable> ValueDecoder<GeneralGeneric<P>, Interned<S>> for () {
        #[inline]
        fn decode_value<B: Buf + ?Sized>(
            value: &mut Interned<S>,
            mut buf: Capped<B>,
            _ctx: DecodeContext,
        ) -> Result<(), DecodeError> {
            // SAFETY (all paths below):
            // We assume that bilrost-encoded values are valid UTF-8 because we don't decode
            // bilrost values from untrusted sources.
            let mut string_data = buf.take_length_delimited()?;
            let len = string_data.remaining_before_cap();

            if len <= crate::string::INLINE_CAPACITY {
                // Short — store inline, skip interner. `S::try_from_restring` runs validation.
                let mut take = string_data.take_all();
                let inline = unsafe { ReString::from_utf8_buf_unchecked(&mut take) };
                let inner = S::try_from_restring(inline)
                    .map_err(|_| DecodeError::new(DecodeErrorKind::InvalidValue))?;
                *value = Interned(inner);
            } else if string_data.chunk().len() >= len {
                // Fast path: all bytes contiguous in chunk. Read directly as &str and query
                // the interner — avoids a temporary heap allocation.
                let s = unsafe { std::str::from_utf8_unchecked(&string_data.chunk()[..len]) };
                *value = ThreadInterner::try_get_or_intern::<S>(s)
                    .map_err(|_| DecodeError::new(DecodeErrorKind::InvalidValue))?;
                string_data.advance(len);
            } else {
                // Slow path: data fragmented across chunks. Materialize into a temporary
                // ReString first, then intern by &str view.
                let mut take = string_data.take_all();
                let tmp = unsafe { ReString::from_utf8_buf_unchecked(&mut take) };
                *value = ThreadInterner::try_get_or_intern::<S>(tmp.as_str())
                    .map_err(|_| DecodeError::new(DecodeErrorKind::InvalidValue))?;
            }

            Ok(())
        }
    }

    impl<const P: u8, S: Internable> DistinguishedValueDecoder<GeneralGeneric<P>, Interned<S>> for () {
        const CHECKS_EMPTY: bool = false;

        #[inline]
        fn decode_value_distinguished<const ALLOW_EMPTY: bool>(
            value: &mut Interned<S>,
            buf: Capped<impl Buf + ?Sized>,
            ctx: RestrictedDecodeContext,
        ) -> Result<Canonicity, DecodeError> {
            <() as ValueDecoder<GeneralGeneric<P>, _>>::decode_value(value, buf, ctx.into_inner())?;
            Ok(Canonicity::Canonical)
        }
    }

    delegate_value_encoding!(
        encoding (GeneralGeneric<P>) borrows type (Interned<S>) as owned including distinguished
        with generics (const P: u8, S: Internable)
    );

    #[cfg(test)]
    mod test {
        use bilrost::{Message, OwnedMessage};

        use super::super::{Interned, ReString, ThreadInterner};

        #[derive(bilrost::Message)]
        struct InternedMessage {
            #[bilrost(1)]
            value: Interned<ReString>,
        }

        #[derive(bilrost::Message)]
        struct StringMessage {
            #[bilrost(1)]
            value: String,
        }

        #[test]
        fn interned_bilrost_roundtrip() {
            // short string: should not be interned
            let src = InternedMessage {
                value: Interned::<ReString>::new("hello"),
            };
            let before = ThreadInterner::len::<ReString>();
            let encoded = src.encode_to_bytes();
            let decoded = InternedMessage::decode(encoded).unwrap();
            assert_eq!(decoded.value.as_str(), "hello");
            // short string decoded — interner should not have grown
            assert_eq!(ThreadInterner::len::<ReString>(), before);

            // long string: should go through the interner on decode
            let long = "bilrost_rt_test_".repeat(10);
            let src = InternedMessage {
                value: Interned::<ReString>::new(&long),
            };
            let before = ThreadInterner::len::<ReString>();
            let encoded = src.encode_to_bytes();
            let decoded = InternedMessage::decode(encoded).unwrap();
            assert_eq!(decoded.value.as_str(), long);
            // The string was already interned by `new` above, so the set should not grow
            assert_eq!(ThreadInterner::len::<ReString>(), before);
            assert!(ThreadInterner::contains::<ReString>(&long));
        }

        #[test]
        fn interned_wire_compatible_with_string() {
            let long = "wire_compat_test_".repeat(10);
            // Interned<ReString> -> String
            let src = InternedMessage {
                value: Interned::<ReString>::new(&long),
            };
            let encoded = src.encode_to_bytes();
            let decoded = StringMessage::decode(encoded).unwrap();
            assert_eq!(decoded.value, long);

            // String -> Interned<ReString> (decoded long string should be interned)
            let before = ThreadInterner::len::<ReString>();
            let src = StringMessage {
                value: long.clone(),
            };
            let encoded = src.encode_to_bytes();
            let decoded = InternedMessage::decode(encoded).unwrap();
            assert_eq!(decoded.value.as_str(), long);
            // Already interned above, so no growth
            assert_eq!(ThreadInterner::len::<ReString>(), before);
        }

        #[test]
        fn empty_interned_roundtrip() {
            let before = ThreadInterner::len::<ReString>();
            let src = InternedMessage {
                value: Interned::<ReString>::default(),
            };
            assert!(src.value.as_str().is_empty());

            let encoded = src.encode_to_bytes();
            let decoded = InternedMessage::decode(encoded).unwrap();
            assert!(decoded.value.as_str().is_empty());
            // Empty string should not be interned
            assert_eq!(ThreadInterner::len::<ReString>(), before);
        }

        /// Decodes from a contiguous buffer (fast path: chunk covers all bytes)
        /// and a fragmented buffer (slow path: chunk is smaller than the string).
        #[test]
        fn decode_contiguous_vs_fragmented() {
            use bytes::Buf;

            let long = "contiguous_vs_fragmented_".repeat(10);

            // Intern the string and capture the canonical data pointer
            let original = Interned::<ReString>::new(&long);
            let canonical_ptr = original.as_str().as_ptr();

            let src = InternedMessage { value: original };
            let encoded = src.encode_to_bytes();

            // --- Contiguous decode (Bytes — single chunk) ---
            let before = ThreadInterner::len::<ReString>();
            let decoded = InternedMessage::decode(encoded.clone()).unwrap();
            assert_eq!(decoded.value.as_str(), long);
            assert_eq!(ThreadInterner::len::<ReString>(), before);
            // Fast path should return the same Arc allocation
            assert_eq!(decoded.value.as_str().as_ptr(), canonical_ptr);

            // --- Fragmented decode (Chain — first chunk is 1 byte) ---
            let (head, tail) = encoded.split_at(1);
            let fragmented = head.chain(tail);
            assert!(fragmented.chunk().len() < encoded.len());

            let decoded = InternedMessage::decode(fragmented).unwrap();
            assert_eq!(decoded.value.as_str(), long);
            assert_eq!(ThreadInterner::len::<ReString>(), before);
            // Slow path should also return the same Arc allocation
            assert_eq!(decoded.value.as_str().as_ptr(), canonical_ptr);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RestrictedValue;

    #[test]
    fn short_strings_bypass_interner() {
        let before = ThreadInterner::len::<ReString>();
        let a = Interned::<ReString>::new("hi");
        let b = Interned::<ReString>::new("hi");
        // The interner should not have grown
        assert_eq!(ThreadInterner::len::<ReString>(), before);
        assert!(!ThreadInterner::contains::<ReString>("hi"));
        assert_eq!(a.as_str(), "hi");
        assert_eq!(a, b);
        // Short strings are inline — each gets its own copy, so pointers differ
        assert_ne!(a.as_str().as_ptr(), b.as_str().as_ptr());
        let inner: &ReString = a.as_ref();
        assert!(!inner.is_heap_allocated());
    }

    #[test]
    fn long_strings_are_interned_and_deduplicated() {
        let long = "interned_dedup_test_".repeat(10);
        assert!(!ThreadInterner::contains::<ReString>(&long));
        let before = ThreadInterner::len::<ReString>();

        let a = Interned::<ReString>::new(&long);
        assert_eq!(ThreadInterner::len::<ReString>(), before + 1);
        assert!(ThreadInterner::contains::<ReString>(&long));

        // Interning the same string again should not grow the set
        let b = Interned::<ReString>::new(&long);
        assert_eq!(ThreadInterner::len::<ReString>(), before + 1);

        assert_eq!(a.as_str(), long);
        assert_eq!(a, b);
        // Both point to the exact same Arc allocation
        assert_eq!(a.as_str().as_ptr(), b.as_str().as_ptr());
        let inner: &ReString = a.as_ref();
        assert!(inner.is_heap_allocated());
    }

    #[test]
    fn default_is_empty() {
        let s = Interned::<ReString>::default();
        assert_eq!(s.as_str(), "");
    }

    #[test]
    fn debug_and_display() {
        let s = Interned::<ReString>::new("hello");
        assert_eq!(format!("{s}"), "hello");
        assert_eq!(format!("{s:?}"), "\"hello\"");
    }

    #[test]
    fn borrow_and_as_ref() {
        use std::borrow::Borrow;
        let s = Interned::<ReString>::new("test");
        let s_str: &str = s.borrow();
        assert_eq!(s_str, "test");
        let s_ref: &str = s.as_ref();
        assert_eq!(s_ref, "test");
        let re_ref: &ReString = s.as_ref();
        assert_eq!(re_ref.as_str(), "test");
    }

    #[test]
    fn pools_are_disjoint_per_wrapper_and_uphold_validation() {
        // Each `S` owns its own per-thread pool, so a permissive wrapper cannot plant
        // an entry that a strict wrapper would later inherit.
        let bad = "has space and !@# bad chars long enough to escape inlining";
        assert!(bad.len() > crate::string::INLINE_CAPACITY);

        // Insert via Interned<ReString> — no validation against RestrictedValue rules.
        let _planted: Interned<ReString> = RestateString::try_new(bad).unwrap();
        assert!(ThreadInterner::contains::<ReString>(bad));
        // Strict wrapper's pool was never touched.
        assert!(!ThreadInterner::contains::<RestrictedValue<ReString>>(bad));

        // Look up via the strict wrapper — miss path runs `S::try_new`, validates,
        // rejects. Pool stays empty.
        let err = <Interned<RestrictedValue<ReString>> as RestateString>::try_new(bad).unwrap_err();
        assert!(matches!(
            err,
            crate::RestrictedValueError::Invalid | crate::RestrictedValueError::TooLong { .. }
        ));
        assert!(!ThreadInterner::contains::<RestrictedValue<ReString>>(bad));
    }

    #[test]
    fn restricted_value_interning() {
        // Long valid restricted value — should intern.
        let long = "restricted_dedup_a1234567890_xyz_abc";
        assert_eq!(long.len(), 36);
        let before = ThreadInterner::len::<RestrictedValue<ReString>>();
        let a: Interned<RestrictedValue<ReString>> = RestateString::try_new(long).unwrap();
        let b: Interned<RestrictedValue<ReString>> = RestateString::try_new(long).unwrap();
        assert_eq!(
            ThreadInterner::len::<RestrictedValue<ReString>>(),
            before + 1
        );
        assert_eq!(a.as_str(), long);
        // Same canonical Arc on hit path.
        assert_eq!(a.as_str().as_ptr(), b.as_str().as_ptr());

        // Invalid input fails validation, does not pollute the interner.
        let before = ThreadInterner::len::<RestrictedValue<ReString>>();
        let err = <Interned<RestrictedValue<ReString>> as RestateString>::try_new("bad string!")
            .unwrap_err();
        assert!(matches!(err, crate::RestrictedValueError::Invalid));
        assert_eq!(ThreadInterner::len::<RestrictedValue<ReString>>(), before);
    }
}
