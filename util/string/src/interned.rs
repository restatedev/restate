// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Thread-local string interner for [`ReString`].
//!
//! This module provides [`InternedReString`], a wrapper around [`ReString`] that
//! deduplicates long strings via a per-thread [`HashSet`]. The interning happens
//! transparently on construction ([`InternedReString::new`]) and during
//! deserialization (serde and bilrost), so callers that decode many repeated
//! string values automatically benefit from reduced heap usage.

use std::borrow::Borrow;
use std::cell::RefCell;
use std::sync::Arc;

use hashbrown::HashSet;

use crate::ReString;

thread_local! {
    static INTERNER: RefCell<ThreadInterner> = const {
        RefCell::new(ThreadInterner::new())
    };
}

/// A deduplicated [`ReString`] backed by a thread-local interner.
///
/// Strings longer than `size_of::<String>()` bytes are stored as `Arc<str>` in a
/// thread-local [`HashSet`]. Repeated intern calls for the same string value return
/// clones of the same `Arc`, so all instances share a single heap allocation.
/// Short strings are stored inline without touching the interner.
///
/// # When to use
///
/// Use this type for long-lived, frequently-duplicated identifiers (service names,
/// handler names, deployment IDs, etc.) where deduplication saves memory. Avoid it
/// for short-lived or unique strings where the interner lookup is pure overhead.
///
/// # Thread safety
///
/// Each thread maintains its own interner. An `InternedReString` created on one thread
/// can be sent to another (it is `Send + Sync` via the inner `Arc`), but the receiving
/// thread's interner will not know about it. Interning the same value on a different
/// thread produces a separate allocation.
#[derive(Clone, Eq, PartialEq, Hash, Ord, PartialOrd, Default)]
pub struct InternedReString(ReString);

impl InternedReString {
    /// Interns the given string on the current thread.
    ///
    /// Short strings (up to `size_of::<String>()` bytes) are stored inline without
    /// touching the interner. Longer strings are looked up in the thread-local set and
    /// deduplicated: if the value is already present the existing `Arc` is cloned,
    /// otherwise a new `Arc<str>` is inserted.
    pub fn new(s: &str) -> Self {
        ThreadInterner::get_or_intern(s)
    }

    #[inline]
    pub const fn from_static(s: &'static str) -> Self {
        Self(ReString::from_static(s))
    }

    /// This will not intern the string since the underlying type is already
    /// cheaply cloneable.
    #[inline]
    pub fn from_shared(s: Arc<str>) -> Self {
        Self(ReString::from_shared(s))
    }

    /// Returns the string contents as a `&str`.
    #[inline(always)]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    #[inline(always)]
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    /// Unwraps the inner [`ReString`], consuming `self`.
    #[inline(always)]
    pub fn into_restring(self) -> ReString {
        self.0
    }

    /// Returns a clone of the inner [`ReString`].
    #[inline(always)]
    pub fn to_restring(&self) -> ReString {
        self.0.clone()
    }
}

impl std::fmt::Debug for InternedReString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.0, f)
    }
}

impl std::fmt::Display for InternedReString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl std::ops::Deref for InternedReString {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl AsRef<[u8]> for InternedReString {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl AsRef<str> for InternedReString {
    fn as_ref(&self) -> &str {
        self.0.as_ref()
    }
}

impl AsRef<ReString> for InternedReString {
    fn as_ref(&self) -> &ReString {
        &self.0
    }
}

impl Borrow<str> for InternedReString {
    fn borrow(&self) -> &str {
        self.0.borrow()
    }
}

impl Borrow<ReString> for InternedReString {
    fn borrow(&self) -> &ReString {
        &self.0
    }
}

impl From<String> for InternedReString {
    #[inline]
    fn from(value: String) -> Self {
        ThreadInterner::get_or_intern(value.as_str())
    }
}

impl From<ReString> for InternedReString {
    #[inline]
    fn from(value: ReString) -> Self {
        if value.is_clone_cheap() {
            Self(value)
        } else {
            ThreadInterner::get_or_intern(value.as_str())
        }
    }
}

impl From<&str> for InternedReString {
    fn from(value: &str) -> Self {
        ThreadInterner::get_or_intern(value)
    }
}

/// Per-thread interner that holds the canonical set of long-lived [`ReString`] values.
///
/// Short strings (up to `size_of::<String>()` bytes) are never inserted — they are
/// stored inline and don't benefit from deduplication. Everything else is stored as
/// `Arc<str>` so that clones are reference-counted and cheap.
struct ThreadInterner {
    heap: HashSet<ReString, ahash::RandomState>,
}

impl ThreadInterner {
    const fn new() -> Self {
        Self {
            // arbitrary seeds, safe to change since we don't use hashes in storage
            heap: HashSet::with_hasher(ahash::RandomState::with_seeds(
                1232134512, 14, 82334, 988889,
            )),
        }
    }

    fn get_or_intern(s: &str) -> InternedReString {
        // do not intern short strings, avoid the map lookup
        if s.len() <= std::mem::size_of::<String>() {
            return InternedReString(ReString::new_owned(s));
        }

        let s = INTERNER.with_borrow_mut(|interner| {
            interner
                .heap
                .get_or_insert_with(s, |k| ReString::new_shared(k))
                .clone()
        });

        InternedReString(s)
    }

    #[cfg(test)]
    fn len() -> usize {
        INTERNER.with_borrow(|interner| interner.heap.len())
    }

    #[cfg(test)]
    fn contains(s: &str) -> bool {
        INTERNER.with_borrow(|interner| interner.heap.contains(s))
    }
}

// The trick here is to use the interner on deserialization to avoid allocating a new string

// SERDE
#[cfg(feature = "serde")]
impl serde::Serialize for InternedReString {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.as_str().serialize(serializer)
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for InternedReString {
    fn deserialize<D: serde::de::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::{Error, Unexpected, Visitor};

        struct InternedReStringVisitor;

        impl<'a> Visitor<'a> for InternedReStringVisitor {
            type Value = InternedReString;

            fn expecting(&self, formatter: &mut core::fmt::Formatter) -> core::fmt::Result {
                formatter.write_str("a string")
            }

            fn visit_str<E: Error>(self, v: &str) -> Result<Self::Value, E> {
                Ok(ThreadInterner::get_or_intern(v))
            }

            fn visit_borrowed_str<E: Error>(self, v: &'a str) -> Result<Self::Value, E> {
                Ok(ThreadInterner::get_or_intern(v))
            }

            fn visit_string<E: Error>(self, v: String) -> Result<Self::Value, E> {
                Ok(ThreadInterner::get_or_intern(&v))
            }

            fn visit_bytes<E: Error>(self, v: &[u8]) -> Result<Self::Value, E> {
                match core::str::from_utf8(v) {
                    Ok(s) => Ok(ThreadInterner::get_or_intern(s)),
                    Err(_) => Err(Error::invalid_value(Unexpected::Bytes(v), &self)),
                }
            }

            fn visit_borrowed_bytes<E: Error>(self, v: &'a [u8]) -> Result<Self::Value, E> {
                match core::str::from_utf8(v) {
                    Ok(s) => Ok(ThreadInterner::get_or_intern(s)),
                    Err(_) => Err(Error::invalid_value(Unexpected::Bytes(v), &self)),
                }
            }

            fn visit_byte_buf<E: Error>(self, v: Vec<u8>) -> Result<Self::Value, E> {
                match core::str::from_utf8(&v) {
                    Ok(s) => Ok(ThreadInterner::get_or_intern(s)),
                    Err(_e) => Err(Error::invalid_value(Unexpected::Bytes(&v), &self)),
                }
            }
        }

        deserializer.deserialize_str(InternedReStringVisitor)
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

    use super::{InternedReString, ReString, ThreadInterner};

    for_overwrite_via_default!(InternedReString);

    impl EmptyState<(), InternedReString> for () {
        #[inline]
        fn is_empty(val: &InternedReString) -> bool {
            val.0.is_empty()
        }

        #[inline]
        fn clear(val: &mut InternedReString) {
            *val = Default::default();
        }
    }

    impl<const P: u8> Wiretyped<GeneralGeneric<P>, InternedReString> for () {
        const WIRE_TYPE: WireType = WireType::LengthDelimited;
    }

    impl<const P: u8> ValueEncoder<GeneralGeneric<P>, InternedReString> for () {
        #[inline]
        fn encode_value<B: BufMut + ?Sized>(value: &InternedReString, buf: &mut B) {
            encode_varint(value.0.len() as u64, buf);
            buf.put_slice(value.0.as_bytes());
        }

        #[inline]
        fn prepend_value<B: ReverseBuf + ?Sized>(value: &InternedReString, buf: &mut B) {
            buf.prepend_slice(value.0.as_bytes());
            prepend_varint(value.0.len() as u64, buf);
        }

        #[inline]
        fn value_encoded_len(value: &InternedReString) -> usize {
            encoded_len_varint(value.0.len() as u64) + value.0.len()
        }
    }

    impl<const P: u8> ValueDecoder<GeneralGeneric<P>, InternedReString> for () {
        #[inline]
        fn decode_value<B: Buf + ?Sized>(
            value: &mut InternedReString,
            mut buf: Capped<B>,
            _ctx: DecodeContext,
        ) -> Result<(), DecodeError> {
            // SAFETY (all paths below):
            // We assume that bilrost-encoded values are valid UTF-8 because we don't decode
            // bilrost values from untrusted sources.
            let mut string_data = buf.take_length_delimited()?;
            let len = string_data.remaining_before_cap();

            if len <= std::mem::size_of::<String>() {
                // Short string — will be stored inline, no need for interner lookup.
                let mut take = string_data.take_all();
                *value = InternedReString(unsafe { ReString::from_utf8_buf_unchecked(&mut take) });
            } else if string_data.chunk().len() >= len {
                // Fast path: all bytes are contiguous in the chunk. Read directly as &str
                // and query the interner, avoiding a temporary heap allocation.
                let s = unsafe { std::str::from_utf8_unchecked(&string_data.chunk()[..len]) };
                *value = ThreadInterner::get_or_intern(s);
                string_data.advance(len);
            } else {
                // Slow path: data is fragmented across chunks, must copy into a temporary
                // buffer before interning.
                let mut take = string_data.take_all();
                let tmp = unsafe { ReString::from_utf8_buf_unchecked(&mut take) };
                *value = ThreadInterner::get_or_intern(&tmp);
            }

            Ok(())
        }
    }

    impl<const P: u8> DistinguishedValueDecoder<GeneralGeneric<P>, InternedReString> for () {
        const CHECKS_EMPTY: bool = false;

        #[inline]
        fn decode_value_distinguished<const ALLOW_EMPTY: bool>(
            value: &mut InternedReString,
            buf: Capped<impl Buf + ?Sized>,
            ctx: RestrictedDecodeContext,
        ) -> Result<Canonicity, DecodeError> {
            <() as ValueDecoder<GeneralGeneric<P>, _>>::decode_value(value, buf, ctx.into_inner())?;
            Ok(Canonicity::Canonical)
        }
    }

    delegate_value_encoding!(
        encoding (GeneralGeneric<P>) borrows type (InternedReString) as owned including distinguished
        with generics (const P: u8)
    );

    #[cfg(test)]
    mod test {
        use bilrost::{Message, OwnedMessage};

        use super::{InternedReString, ThreadInterner};

        #[derive(bilrost::Message)]
        struct InternedMessage {
            #[bilrost(1)]
            value: InternedReString,
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
                value: InternedReString::new("hello"),
            };
            let before = ThreadInterner::len();
            let encoded = src.encode_to_bytes();
            let decoded = InternedMessage::decode(encoded).unwrap();
            assert_eq!(decoded.value.as_str(), "hello");
            // short string decoded — interner should not have grown
            assert_eq!(ThreadInterner::len(), before);

            // long string: should go through the interner on decode
            let long = "bilrost_rt_test_".repeat(10);
            let src = InternedMessage {
                value: InternedReString::new(&long),
            };
            let before = ThreadInterner::len();
            let encoded = src.encode_to_bytes();
            let decoded = InternedMessage::decode(encoded).unwrap();
            assert_eq!(decoded.value.as_str(), long);
            // The string was already interned by `new` above, so the set should not grow
            assert_eq!(ThreadInterner::len(), before);
            assert!(ThreadInterner::contains(&long));
        }

        #[test]
        fn interned_wire_compatible_with_string() {
            let long = "wire_compat_test_".repeat(10);
            // InternedReString -> String
            let src = InternedMessage {
                value: InternedReString::new(&long),
            };
            let encoded = src.encode_to_bytes();
            let decoded = StringMessage::decode(encoded).unwrap();
            assert_eq!(decoded.value, long);

            // String -> InternedReString (decoded long string should be interned)
            let before = ThreadInterner::len();
            let src = StringMessage {
                value: long.clone(),
            };
            let encoded = src.encode_to_bytes();
            let decoded = InternedMessage::decode(encoded).unwrap();
            assert_eq!(decoded.value.as_str(), long);
            // Already interned above, so no growth
            assert_eq!(ThreadInterner::len(), before);
        }

        #[test]
        fn empty_interned_roundtrip() {
            let before = ThreadInterner::len();
            let src = InternedMessage {
                value: InternedReString::default(),
            };
            assert!(src.value.as_str().is_empty());

            let encoded = src.encode_to_bytes();
            let decoded = InternedMessage::decode(encoded).unwrap();
            assert!(decoded.value.as_str().is_empty());
            // Empty string should not be interned
            assert_eq!(ThreadInterner::len(), before);
        }

        /// Decodes from a contiguous buffer (fast path: chunk covers all bytes)
        /// and a fragmented buffer (slow path: chunk is smaller than the string).
        #[test]
        fn decode_contiguous_vs_fragmented() {
            use bytes::Buf;

            let long = "contiguous_vs_fragmented_".repeat(10);

            // Intern the string and capture the canonical data pointer
            let original = InternedReString::new(&long);
            let canonical_ptr = original.as_str().as_ptr();

            let src = InternedMessage { value: original };
            let encoded = src.encode_to_bytes();

            // --- Contiguous decode (Bytes — single chunk) ---
            let before = ThreadInterner::len();
            let decoded = InternedMessage::decode(encoded.clone()).unwrap();
            assert_eq!(decoded.value.as_str(), long);
            assert_eq!(ThreadInterner::len(), before);
            // Fast path should return the same Arc allocation
            assert_eq!(decoded.value.as_str().as_ptr(), canonical_ptr);

            // --- Fragmented decode (Chain — first chunk is 1 byte) ---
            let (head, tail) = encoded.split_at(1);
            let fragmented = head.chain(tail);
            assert!(fragmented.chunk().len() < encoded.len());

            let decoded = InternedMessage::decode(fragmented).unwrap();
            assert_eq!(decoded.value.as_str(), long);
            assert_eq!(ThreadInterner::len(), before);
            // Slow path should also return the same Arc allocation
            assert_eq!(decoded.value.as_str().as_ptr(), canonical_ptr);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_strings_bypass_interner() {
        let before = ThreadInterner::len();
        let a = InternedReString::new("hi");
        let b = InternedReString::new("hi");
        // The interner should not have grown
        assert_eq!(ThreadInterner::len(), before);
        assert!(!ThreadInterner::contains("hi"));
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
        assert!(!ThreadInterner::contains(&long));
        let before = ThreadInterner::len();

        let a = InternedReString::new(&long);
        assert_eq!(ThreadInterner::len(), before + 1);
        assert!(ThreadInterner::contains(&long));

        // Interning the same string again should not grow the set
        let b = InternedReString::new(&long);
        assert_eq!(ThreadInterner::len(), before + 1);

        assert_eq!(a.as_str(), long);
        assert_eq!(a, b);
        // Both point to the exact same Arc allocation
        assert_eq!(a.as_str().as_ptr(), b.as_str().as_ptr());
        let inner: &ReString = a.as_ref();
        assert!(inner.is_heap_allocated());
        assert!(inner.is_clone_cheap());
    }

    #[test]
    fn default_is_empty() {
        let s = InternedReString::default();
        assert_eq!(s.as_str(), "");
    }

    #[test]
    fn debug_and_display() {
        let s = InternedReString::new("hello");
        assert_eq!(format!("{s}"), "hello");
        assert_eq!(format!("{s:?}"), "\"hello\"");
    }

    #[test]
    fn borrow_and_as_ref() {
        use std::borrow::Borrow;
        let s = InternedReString::new("test");
        let s_str: &str = s.borrow();
        assert_eq!(s_str, "test");
        let s_ref: &str = s.as_ref();
        assert_eq!(s_ref, "test");
        let re_ref: &ReString = s.as_ref();
        assert_eq!(re_ref.as_str(), "test");
    }
}
