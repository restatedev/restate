// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Borrow;
use std::fmt;
use std::ops::Deref;

use bytes::{Buf, BufMut};
#[cfg(feature = "bytestring")]
use bytestring::ByteString;
use zerocopy::{ByteEq, Immutable, IntoBytes, KnownLayout};

use crate::{OwnedStringLike, ReString};

const GROUP_SIZE: usize = 8;
const GROUP_MARKER_SIZE: usize = 1;
const ENCODED_GROUP_SIZE: usize = GROUP_SIZE + GROUP_MARKER_SIZE;
const CONTINUATION_MARKER: u8 = ENCODED_GROUP_SIZE as u8;

/// An invalid mem-comparable string encoding.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum MemCmpStringError {
    /// The input ended before the terminal group.
    Truncated,
    /// A group marker was outside the supported range.
    InvalidMarker(u8),
    /// The unused bytes in the terminal group were not zero-filled.
    NonZeroPadding,
    /// The decoded payload was not valid UTF-8.
    InvalidUtf8,
    /// The destination buffer was too small for the decoded payload.
    OutputTooSmall,
}

impl fmt::Display for MemCmpStringError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Truncated => f.write_str("truncated mem-comparable string"),
            Self::InvalidMarker(marker) => {
                write!(f, "invalid mem-comparable string marker: {marker:#04x}")
            }
            Self::NonZeroPadding => f.write_str("non-zero padding in mem-comparable string"),
            Self::InvalidUtf8 => f.write_str("mem-comparable string payload is not valid UTF-8"),
            Self::OutputTooSmall => {
                f.write_str("destination is too small for the mem-comparable string")
            }
        }
    }
}

impl std::error::Error for MemCmpStringError {}

/// Result returned by mem-comparable string decoding operations.
pub type MemCmpResult<T> = std::result::Result<T, MemCmpStringError>;

/// Maximum payload length staged on the stack by the generic decode paths.
const INLINE_DECODE_LIMIT: usize = 64;

/// Masks selecting the padding bytes of a group loaded as a big-endian `u64`, indexed by the
/// number of used bytes in the group.
const PADDING_MASKS: [u64; GROUP_SIZE + 1] = {
    let mut masks = [0u64; GROUP_SIZE + 1];
    let mut used = 0;
    while used < GROUP_SIZE {
        masks[used] = u64::MAX >> (used * 8);
        used += 1;
    }
    masks
};

/// A string type that a decoded mem-comparable string can materialize into.
///
/// [`OwnedStringLike`] supplies construction from a borrowed `str` and all the string-shaped
/// bounds; `From<String>` lets allocation-reusing types take ownership of the decode buffer.
/// Implementations choose how much decoded data to stage on the stack and may specialize the
/// contiguous decode path.
pub trait MemCmpTarget: OwnedStringLike + From<String> {
    /// Strings up to this length are decoded onto the stack and built via `From<&str>`, skipping
    /// the intermediate heap buffer. This must not exceed the codec's 64-byte stack limit.
    const STACK_DECODE_LIMIT: usize;

    #[doc(hidden)]
    fn decode_contiguous(encoded: &[u8], decoded_len: usize) -> MemCmpResult<Self> {
        if decoded_len <= Self::STACK_DECODE_LIMIT.min(INLINE_DECODE_LIMIT) {
            let mut stack = [0u8; INLINE_DECODE_LIMIT];
            let decoded = &mut stack[..decoded_len];
            let is_ascii = copy_groups::<true>(encoded, decoded);
            Ok(Self::from(validate_str(decoded, is_ascii)?))
        } else {
            let (decoded, is_ascii) = decode_contiguous_bytes::<true>(encoded, decoded_len);
            Ok(Self::from(validate_string(decoded, is_ascii)?))
        }
    }

    #[doc(hidden)]
    unsafe fn decode_contiguous_unchecked(
        encoded: &[u8],
        decoded_len: usize,
    ) -> MemCmpResult<Self> {
        if decoded_len <= Self::STACK_DECODE_LIMIT.min(INLINE_DECODE_LIMIT) {
            let mut stack = [0u8; INLINE_DECODE_LIMIT];
            let decoded = &mut stack[..decoded_len];
            copy_groups::<false>(encoded, decoded);
            // SAFETY: upheld by the caller.
            Ok(Self::from(unsafe { str::from_utf8_unchecked(decoded) }))
        } else {
            let (decoded, _) = decode_contiguous_bytes::<false>(encoded, decoded_len);
            // SAFETY: upheld by the caller.
            Ok(Self::from(unsafe { String::from_utf8_unchecked(decoded) }))
        }
    }
}

impl MemCmpTarget for String {
    const STACK_DECODE_LIMIT: usize = 0;
}

// `ReString::from(String)` copies either way, so borrowed construction is never worse and
// inlines short strings without any allocation.
impl MemCmpTarget for ReString {
    const STACK_DECODE_LIMIT: usize = INLINE_DECODE_LIMIT;

    fn decode_contiguous(encoded: &[u8], decoded_len: usize) -> MemCmpResult<Self> {
        if decoded_len <= INLINE_DECODE_LIMIT {
            let mut stack = [0u8; INLINE_DECODE_LIMIT];
            let decoded = &mut stack[..decoded_len];
            let is_ascii = copy_groups::<true>(encoded, decoded);
            return Ok(Self::from(validate_str(decoded, is_ascii)?));
        }

        let mut decoded = DecodedMemCmpBuf::new(encoded, decoded_len);
        ReString::from_utf8_buf(&mut decoded).map_err(|_| MemCmpStringError::InvalidUtf8)
    }

    unsafe fn decode_contiguous_unchecked(
        encoded: &[u8],
        decoded_len: usize,
    ) -> MemCmpResult<Self> {
        if decoded_len <= INLINE_DECODE_LIMIT {
            let mut stack = [0u8; INLINE_DECODE_LIMIT];
            let decoded = &mut stack[..decoded_len];
            copy_groups::<false>(encoded, decoded);
            // SAFETY: upheld by the caller.
            return Ok(Self::from(unsafe { str::from_utf8_unchecked(decoded) }));
        }

        let mut decoded = DecodedMemCmpBuf::new(encoded, decoded_len);
        // SAFETY: upheld by the caller.
        Ok(unsafe { ReString::from_utf8_buf_unchecked(&mut decoded) })
    }
}

// `ByteString::from(String)` wraps the allocation zero-copy.
#[cfg(feature = "bytestring")]
impl MemCmpTarget for ByteString {
    const STACK_DECODE_LIMIT: usize = 0;
}

/// A UTF-8 string encoded for byte-wise comparisons in ordered byte keys.
///
/// Uses the MyRocks variable-length mem-comparable format: every group stores 8 data bytes plus
/// a flag byte, with `9` marking a continuation group and `0..=8` marking the number of bytes
/// used in the final group.
///
/// The key encoding is self-delimiting and preserves the string's UTF-8 byte lexicographical
/// ordering, making it suitable for variable-length string fields that are followed by more key
/// fields.
///
/// The backing store is chosen via the type parameter: `MemCmpString` defaults to `String`;
/// `MemCmpString<ReString>` decodes short strings without allocating; `MemCmpString<ByteString>`
/// yields cheaply-cloneable fields. Note the default only applies in type positions, so
/// inference-ambiguous calls need an annotation (e.g. `MemCmpString::<String>::decode_from(..)`).
#[derive(Debug, Clone, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct MemCmpString<S = String>(S);

/// A borrowed UTF-8 string encoded for byte-wise comparisons in RocksDB keys.
///
/// This is the encode-only borrowed counterpart of [`MemCmpString`].
#[derive(Debug, Copy, Clone, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct MemCmpStr<'a>(&'a str);

/// A mem-comparable UTF-8 string encoded in a key.
///
/// This dynamically sized view borrows the encoded bytes directly. For values produced by this
/// crate's encoders, comparing encoded representations has the same ordering as comparing the
/// decoded UTF-8 strings.
#[repr(transparent)]
#[derive(Debug, ByteEq, Ord, PartialOrd, Immutable, IntoBytes)]
pub struct EncodedMemCmpStr([u8]);

impl std::hash::Hash for EncodedMemCmpStr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        state.write(self.as_bytes());
    }
}

impl EncodedMemCmpStr {
    /// Encoded representation of an empty string.
    pub const EMPTY: &'static EncodedMemCmpStr = encoded_mem_cmp_str!("");

    /// Returns the encoded bytes.
    #[inline]
    pub const fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Returns the length of the encoded representation.
    #[inline]
    pub const fn encoded_len(&self) -> usize {
        self.0.len()
    }

    /// Decodes this string into an owned [`ReString`].
    #[inline]
    pub fn decode(&self) -> ReString {
        let groups = self.encoded_len() / ENCODED_GROUP_SIZE;
        let decoded_len =
            (groups - 1) * GROUP_SIZE + usize::from(self.as_bytes()[self.encoded_len() - 1]);
        // SAFETY: `EncodedMemCmpStr` guarantees an exact, structurally valid encoding with a
        // UTF-8 payload.
        unsafe {
            <ReString as MemCmpTarget>::decode_contiguous_unchecked(self.as_bytes(), decoded_len)
        }
        .expect("EncodedMemCmpStr must contain a valid mem-comparable string")
    }

    /// Takes one mem-comparable UTF-8 string from the start of `input`.
    #[inline]
    pub fn try_ref_from_prefix(input: &[u8]) -> MemCmpResult<(&Self, &[u8])> {
        let encoded_len = next_encoded_str::<true>(input)?;
        let (encoded, remaining) = input.split_at(encoded_len);
        // SAFETY: the encoded payload was structurally and UTF-8 validated above.
        Ok((
            unsafe { Self::from_encoded_bytes_unchecked(encoded) },
            remaining,
        ))
    }

    /// Takes one structurally valid mem-comparable string without validating its UTF-8 payload.
    ///
    /// # Safety
    ///
    /// The decoded payload at the start of `input` must be valid UTF-8.
    #[inline]
    pub unsafe fn try_ref_from_prefix_unchecked(input: &[u8]) -> MemCmpResult<(&Self, &[u8])> {
        let encoded_len = next_encoded_str::<false>(input)?;
        let (encoded, remaining) = input.split_at(encoded_len);
        // SAFETY: upheld by the caller.
        Ok((
            unsafe { Self::from_encoded_bytes_unchecked(encoded) },
            remaining,
        ))
    }

    /// Reinterprets an encoded byte slice without validating it.
    ///
    /// # Safety
    ///
    /// `encoded` must contain exactly one structurally valid mem-comparable string whose decoded
    /// payload is valid UTF-8.
    #[inline]
    pub(super) const unsafe fn from_encoded_bytes_unchecked(encoded: &[u8]) -> &Self {
        // SAFETY: `EncodedMemCmpStr` is transparent over `[u8]`, preserving the slice metadata.
        unsafe { &*(encoded as *const [u8] as *const Self) }
    }

    #[doc(hidden)]
    pub const fn encoded_len_for(value: &str) -> usize {
        serialized_length(value.len())
    }

    #[doc(hidden)]
    pub const fn encode_static<const N: usize>(value: &str) -> [u8; N] {
        let source = value.as_bytes();
        assert!(N == serialized_length(source.len()));

        let mut encoded = [0; N];
        let mut source_offset = 0;
        let mut encoded_offset = 0;

        while source.len() - source_offset > GROUP_SIZE {
            let mut i = 0;
            while i < GROUP_SIZE {
                encoded[encoded_offset + i] = source[source_offset + i];
                i += 1;
            }
            encoded[encoded_offset + GROUP_SIZE] = CONTINUATION_MARKER;
            source_offset += GROUP_SIZE;
            encoded_offset += ENCODED_GROUP_SIZE;
        }

        let remaining = source.len() - source_offset;
        let mut i = 0;
        while i < remaining {
            encoded[encoded_offset + i] = source[source_offset + i];
            i += 1;
        }
        encoded[encoded_offset + GROUP_SIZE] = remaining as u8;
        encoded
    }

    #[doc(hidden)]
    /// Reinterprets a static encoded byte slice without validating it.
    ///
    /// # Safety
    ///
    /// `encoded` must contain exactly one structurally valid mem-comparable string whose decoded
    /// payload is valid UTF-8.
    pub const unsafe fn from_static_encoded_unchecked(encoded: &'static [u8]) -> &'static Self {
        // SAFETY: upheld by the caller.
        unsafe { Self::from_encoded_bytes_unchecked(encoded) }
    }
}

impl AsRef<[u8]> for EncodedMemCmpStr {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

/// Encodes a static string into a canonical [`EncodedMemCmpStr`] at compile time
/// suitable for const promotion.
#[macro_export]
macro_rules! encoded_mem_cmp_str {
    ($value:expr) => {
        // SAFETY: `ENCODED` was produced from the valid UTF-8 string `VALUE` above.
        unsafe {
            $crate::EncodedMemCmpStr::from_static_encoded_unchecked(
                &const {
                    $crate::EncodedMemCmpStr::encode_static::<
                        { $crate::EncodedMemCmpStr::encoded_len_for($value) },
                    >($value)
                },
            )
        }
    };
}

use encoded_mem_cmp_str;

impl<S: MemCmpTarget> MemCmpString<S> {
    #[inline]
    pub fn new(value: impl Into<S>) -> Self {
        Self(value.into())
    }

    #[inline]
    pub fn as_str(&self) -> &str {
        self.0.as_ref()
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        self.as_str().as_bytes()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.as_str().len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.as_str().is_empty()
    }

    #[inline]
    pub fn into_inner(self) -> S {
        self.0
    }

    #[inline]
    pub fn encoded_len(&self) -> usize {
        serialized_length(self.len())
    }

    #[inline]
    pub fn encode_to<B: BufMut>(&self, target: &mut B) {
        write_bytes(self.as_bytes(), target);
    }

    #[inline]
    pub fn decode_from<B: Buf>(source: &mut B) -> MemCmpResult<Self> {
        decode_target(source).map(Self)
    }

    /// Decodes without validating that the decoded payload is UTF-8.
    ///
    /// # Safety
    ///
    /// The decoded payload at the start of `source` must be valid UTF-8.
    #[inline]
    pub unsafe fn decode_from_unchecked<B: Buf>(source: &mut B) -> MemCmpResult<Self> {
        // SAFETY: upheld by the caller.
        unsafe { decode_target_unchecked(source) }.map(Self)
    }
}

/// Decodes the next mem-comparable string and passes it to `f` as a borrowed `str`, for one-off
/// conversions without committing to a [`MemCmpTarget`]. Strings up to `INLINE_DECODE_LIMIT`
/// bytes are staged on the stack, including for fragmented inputs.
pub fn decode_str_with<B: Buf, R>(source: &mut B, f: impl FnOnce(&str) -> R) -> MemCmpResult<R> {
    let chunk = source.chunk();
    match scan_terminal(chunk)? {
        Some((encoded_len, decoded_len)) if decoded_len <= INLINE_DECODE_LIMIT => {
            let mut stack = [0u8; INLINE_DECODE_LIMIT];
            let decoded = &mut stack[..decoded_len];
            let is_ascii = copy_groups::<true>(&chunk[..encoded_len], decoded);
            let decoded = validate_str(decoded, is_ascii)?;
            source.advance(encoded_len);
            Ok(f(decoded))
        }
        Some((encoded_len, decoded_len)) => {
            let (decoded, is_ascii) =
                decode_contiguous_bytes::<true>(&chunk[..encoded_len], decoded_len);
            let decoded = validate_str(&decoded, is_ascii)?;
            source.advance(encoded_len);
            Ok(f(decoded))
        }
        None if chunk.len() == source.remaining() => Err(MemCmpStringError::Truncated),
        None => match read_decoded_fragmented::<_, true>(source, INLINE_DECODE_LIMIT)? {
            DecodedBytes::Inline {
                bytes,
                len,
                is_ascii,
            } => Ok(f(validate_str(&bytes[..len], is_ascii)?)),
            DecodedBytes::Heap { bytes, is_ascii } => Ok(f(validate_str(&bytes, is_ascii)?)),
        },
    }
}

/// Decodes the next mem-comparable string without validating its UTF-8 payload and passes it to
/// `f` as a borrowed `str`.
///
/// # Safety
///
/// The decoded payload at the start of `source` must be valid UTF-8.
pub unsafe fn decode_str_with_unchecked<B: Buf, R>(
    source: &mut B,
    f: impl FnOnce(&str) -> R,
) -> MemCmpResult<R> {
    let chunk = source.chunk();
    match scan_terminal(chunk)? {
        Some((encoded_len, decoded_len)) if decoded_len <= INLINE_DECODE_LIMIT => {
            let mut stack = [0u8; INLINE_DECODE_LIMIT];
            let decoded = &mut stack[..decoded_len];
            copy_groups::<false>(&chunk[..encoded_len], decoded);
            source.advance(encoded_len);
            // SAFETY: upheld by the caller.
            Ok(f(unsafe { str::from_utf8_unchecked(decoded) }))
        }
        Some((encoded_len, decoded_len)) => {
            let (decoded, _) = decode_contiguous_bytes::<false>(&chunk[..encoded_len], decoded_len);
            source.advance(encoded_len);
            // SAFETY: upheld by the caller.
            Ok(f(unsafe { str::from_utf8_unchecked(&decoded) }))
        }
        None if chunk.len() == source.remaining() => Err(MemCmpStringError::Truncated),
        None => match read_decoded_fragmented::<_, false>(source, INLINE_DECODE_LIMIT)? {
            DecodedBytes::Inline { bytes, len, .. } => {
                // SAFETY: upheld by the caller.
                Ok(f(unsafe { str::from_utf8_unchecked(&bytes[..len]) }))
            }
            DecodedBytes::Heap { bytes, .. } => {
                // SAFETY: upheld by the caller.
                Ok(f(unsafe { str::from_utf8_unchecked(&bytes) }))
            }
        },
    }
}

impl<S: MemCmpTarget> From<String> for MemCmpString<S> {
    #[inline]
    fn from(value: String) -> Self {
        Self(S::from(value))
    }
}

impl<S: MemCmpTarget> From<&str> for MemCmpString<S> {
    #[inline]
    fn from(value: &str) -> Self {
        Self(S::from(value))
    }
}

impl<S: MemCmpTarget> AsRef<str> for MemCmpString<S> {
    #[inline]
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl<S: MemCmpTarget> Borrow<str> for MemCmpString<S> {
    #[inline]
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl<S: MemCmpTarget> Deref for MemCmpString<S> {
    type Target = str;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl<S: MemCmpTarget> std::fmt::Display for MemCmpString<S> {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl<'a> MemCmpStr<'a> {
    #[inline]
    pub fn new(value: &'a str) -> Self {
        Self(value)
    }

    #[inline]
    pub fn as_str(&self) -> &'a str {
        self.0
    }

    #[inline]
    pub fn as_bytes(&self) -> &'a [u8] {
        self.0.as_bytes()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[inline]
    pub fn encoded_len(&self) -> usize {
        serialized_length(self.len())
    }

    #[inline]
    pub fn encode_to<B: BufMut>(&self, target: &mut B) {
        write_bytes(self.as_bytes(), target);
    }
}

impl<'a> From<&'a str> for MemCmpStr<'a> {
    #[inline]
    fn from(value: &'a str) -> Self {
        Self(value)
    }
}

impl AsRef<str> for MemCmpStr<'_> {
    #[inline]
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Borrow<str> for MemCmpStr<'_> {
    #[inline]
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl Deref for MemCmpStr<'_> {
    type Target = str;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl std::fmt::Display for MemCmpStr<'_> {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[inline]
const fn serialized_length(source_len: usize) -> usize {
    (source_len.saturating_sub(1) / GROUP_SIZE + 1) * ENCODED_GROUP_SIZE
}

/// Decodes the next mem-comparable string from the front of `input` into `dst`, returning
/// `(encoded_len, decoded_len)`. Since decoding always shrinks, a `dst` of `input.len()` bytes
/// always fits; a too-short `dst` yields [`MemCmpStringError::OutputTooSmall`].
///
/// Unlike the exact-allocation decode paths, this copies in a single fused pass — with a
/// caller-provided buffer there is nothing to size upfront, so a scan pass would be pure
/// overhead.
///
/// This allocation-free primitive is useful when the caller already owns decode scratch space.
#[doc(hidden)]
pub fn decode_str_into(input: &[u8], dst: &mut [u8]) -> MemCmpResult<(usize, usize)> {
    let mut dst_off = 0;
    let mut utf8 = Utf8Validator::default();
    let mut is_ascii = true;

    for (index, group) in input.chunks_exact(ENCODED_GROUP_SIZE).enumerate() {
        if group[GROUP_SIZE] == CONTINUATION_MARKER {
            let data = &group[..GROUP_SIZE];
            let out = dst
                .get_mut(dst_off..dst_off + GROUP_SIZE)
                .ok_or(MemCmpStringError::OutputTooSmall)?;
            out.copy_from_slice(data);
            dst_off += GROUP_SIZE;
            if is_ascii {
                is_ascii = data.is_ascii();
            }
            if !is_ascii {
                utf8.push(data)?;
            }
            continue;
        }

        let used = terminal_group_len(group)?;
        let data = &group[..used];
        let out = dst
            .get_mut(dst_off..dst_off + used)
            .ok_or(MemCmpStringError::OutputTooSmall)?;
        out.copy_from_slice(data);
        if is_ascii {
            is_ascii = data.is_ascii();
        }
        if !is_ascii {
            utf8.push(data)?;
            utf8.finish()?;
        }
        return Ok(((index + 1) * ENCODED_GROUP_SIZE, dst_off + used));
    }

    Err(MemCmpStringError::Truncated)
}

#[repr(C)]
#[derive(Immutable, IntoBytes, KnownLayout)]
struct EncodedGroup {
    data: [u8; GROUP_SIZE],
    marker: u8,
}

/// Builds the zero-padded final group; `tail` holds the last `0..=GROUP_SIZE` source bytes.
#[inline]
fn final_group(tail: &[u8]) -> EncodedGroup {
    let mut data = [0; GROUP_SIZE];
    data[..tail.len()].copy_from_slice(tail);
    EncodedGroup {
        data,
        marker: tail.len() as u8,
    }
}

#[inline]
fn write_bytes<B: BufMut>(source: &[u8], target: &mut B) {
    let continuation_len = source.len().saturating_sub(1) / GROUP_SIZE * GROUP_SIZE;
    let encoded_len = serialized_length(source.len());

    let chunk = target.chunk_mut();
    if chunk.len() >= encoded_len {
        let mut pos = 0;
        for group in source[..continuation_len].chunks_exact(GROUP_SIZE) {
            let group = EncodedGroup {
                data: group.try_into().expect("group of 8 bytes"),
                marker: CONTINUATION_MARKER,
            };
            chunk[pos..pos + ENCODED_GROUP_SIZE].copy_from_slice(group.as_bytes());
            pos += ENCODED_GROUP_SIZE;
        }
        chunk[pos..pos + ENCODED_GROUP_SIZE]
            .copy_from_slice(final_group(&source[continuation_len..]).as_bytes());
        // SAFETY: all `encoded_len` bytes of the chunk were initialized above.
        unsafe { target.advance_mut(encoded_len) };
        return;
    }

    write_bytes_fallback(source, continuation_len, target);
}

/// Encodes one group at a time through the `BufMut` API; used when the target has no contiguous
/// room for the whole encoding.
#[cold]
fn write_bytes_fallback<B: BufMut>(source: &[u8], continuation_len: usize, target: &mut B) {
    for group in source[..continuation_len].chunks_exact(GROUP_SIZE) {
        target.put_slice(
            EncodedGroup {
                data: group.try_into().expect("group of 8 bytes"),
                marker: CONTINUATION_MARKER,
            }
            .as_bytes(),
        );
    }
    target.put_slice(final_group(&source[continuation_len..]).as_bytes());
}

/// Validates a final (non-continuation) group and returns the number of source bytes it holds.
#[inline]
fn terminal_group_len(group: &[u8]) -> MemCmpResult<usize> {
    let bits = u64::from_be_bytes(group[..GROUP_SIZE].try_into().expect("group of 8 bytes"));
    terminal_group_len_from(bits, group[GROUP_SIZE])
}

#[inline]
fn terminal_group_len_from(bits: u64, marker: u8) -> MemCmpResult<usize> {
    if marker > GROUP_SIZE as u8 {
        return Err(MemCmpStringError::InvalidMarker(marker));
    }
    let used = usize::from(marker);
    if bits & PADDING_MASKS[used] != 0 {
        return Err(MemCmpStringError::NonZeroPadding);
    }
    Ok(used)
}

#[inline]
fn next_encoded_str<const CHECK_UTF8: bool>(input: &[u8]) -> MemCmpResult<usize> {
    let mut offset = 0;
    let mut utf8 = Utf8Validator::default();
    let mut is_ascii = true;

    loop {
        let Some(group) = input.get(offset..offset + ENCODED_GROUP_SIZE) else {
            return Err(MemCmpStringError::Truncated);
        };
        offset += ENCODED_GROUP_SIZE;

        let terminal = group[GROUP_SIZE] != CONTINUATION_MARKER;
        let used = if terminal {
            terminal_group_len(group)?
        } else {
            GROUP_SIZE
        };

        if CHECK_UTF8 {
            let data = &group[..used];
            if is_ascii {
                is_ascii = data.is_ascii();
            }
            if !is_ascii {
                utf8.push(data)?;
            }
        }

        if terminal {
            if CHECK_UTF8 && !is_ascii {
                utf8.finish()?;
            }
            return Ok(offset);
        }
    }
}

#[derive(Default)]
struct Utf8Validator {
    incomplete: [u8; 4],
    incomplete_len: usize,
}

impl Utf8Validator {
    fn push(&mut self, mut data: &[u8]) -> MemCmpResult<()> {
        while self.incomplete_len > 0 && !data.is_empty() {
            self.incomplete[self.incomplete_len] = data[0];
            self.incomplete_len += 1;
            data = &data[1..];

            match str::from_utf8(&self.incomplete[..self.incomplete_len]) {
                Ok(_) => self.incomplete_len = 0,
                Err(error) if error.error_len().is_some() => {
                    return Err(MemCmpStringError::InvalidUtf8);
                }
                Err(_) => {}
            }
        }

        if self.incomplete_len > 0 {
            return Ok(());
        }

        if let Err(error) = str::from_utf8(data) {
            if error.error_len().is_some() {
                return Err(MemCmpStringError::InvalidUtf8);
            }
            let tail = &data[error.valid_up_to()..];
            self.incomplete[..tail.len()].copy_from_slice(tail);
            self.incomplete_len = tail.len();
        }

        Ok(())
    }

    fn finish(self) -> MemCmpResult<()> {
        if self.incomplete_len == 0 {
            Ok(())
        } else {
            Err(MemCmpStringError::InvalidUtf8)
        }
    }
}

/// Finds the final group of an encoded string within `chunk`.
///
/// Returns `Some((encoded_len, decoded_len))` when the string terminates within `chunk`, `None`
/// when the encoding continues past it.
#[inline]
fn scan_terminal(chunk: &[u8]) -> MemCmpResult<Option<(usize, usize)>> {
    let mut offset = 0;
    while chunk.len() - offset >= ENCODED_GROUP_SIZE {
        let group = &chunk[offset..offset + ENCODED_GROUP_SIZE];
        if group[GROUP_SIZE] == CONTINUATION_MARKER {
            offset += ENCODED_GROUP_SIZE;
            continue;
        }
        let used = terminal_group_len(group)?;
        return Ok(Some((
            offset + ENCODED_GROUP_SIZE,
            offset / ENCODED_GROUP_SIZE * GROUP_SIZE + used,
        )));
    }
    Ok(None)
}

/// Copies the source bytes out of `encoded` groups into `decoded`, whose length must be the
/// decoded length reported by [`scan_terminal`].
#[inline]
fn copy_groups<const CHECK_ASCII: bool>(encoded: &[u8], decoded: &mut [u8]) -> bool {
    let mut groups = encoded.chunks_exact(ENCODED_GROUP_SIZE);
    let mut is_ascii = true;
    // full groups copy with a compile-time-known length
    let mut full = decoded.chunks_exact_mut(GROUP_SIZE);
    for out in &mut full {
        let group = groups.next().expect("enough encoded groups");
        out.copy_from_slice(&group[..GROUP_SIZE]);
        if CHECK_ASCII && is_ascii {
            is_ascii = out.is_ascii();
        }
    }

    let tail = full.into_remainder();
    if !tail.is_empty() {
        let group = groups.next().expect("a final encoded group");
        tail.copy_from_slice(&group[..tail.len()]);
        if CHECK_ASCII && is_ascii {
            is_ascii = tail.is_ascii();
        }
    }
    is_ascii
}

/// Copies the decoded bytes out of an exact encoded field into a fresh exactly-sized `Vec`.
#[inline]
fn decode_contiguous_bytes<const CHECK_ASCII: bool>(
    encoded: &[u8],
    decoded_len: usize,
) -> (Vec<u8>, bool) {
    if encoded.len() == ENCODED_GROUP_SIZE {
        // single group: a plain allocation plus one append beats zero-filling
        let mut decoded = Vec::with_capacity(decoded_len);
        decoded.extend_from_slice(&encoded[..decoded_len]);
        let is_ascii = !CHECK_ASCII || decoded.is_ascii();
        (decoded, is_ascii)
    } else {
        let mut decoded = vec![0u8; decoded_len];
        let is_ascii = copy_groups::<CHECK_ASCII>(encoded, &mut decoded);
        (decoded, is_ascii)
    }
}

#[inline(always)]
fn decode_target<S: MemCmpTarget, B: Buf>(source: &mut B) -> MemCmpResult<S> {
    let chunk = source.chunk();
    match scan_terminal(chunk)? {
        Some((encoded_len, decoded_len)) => {
            let decoded = S::decode_contiguous(&chunk[..encoded_len], decoded_len)?;
            source.advance(encoded_len);
            Ok(decoded)
        }
        None if chunk.len() == source.remaining() => Err(MemCmpStringError::Truncated),
        None => match read_decoded_fragmented::<_, true>(
            source,
            S::STACK_DECODE_LIMIT.min(INLINE_DECODE_LIMIT),
        )? {
            DecodedBytes::Inline {
                bytes,
                len,
                is_ascii,
            } => Ok(S::from(validate_str(&bytes[..len], is_ascii)?)),
            DecodedBytes::Heap { bytes, is_ascii } => {
                Ok(S::from(validate_string(bytes, is_ascii)?))
            }
        },
    }
}

#[inline(always)]
unsafe fn decode_target_unchecked<S: MemCmpTarget, B: Buf>(source: &mut B) -> MemCmpResult<S> {
    let chunk = source.chunk();
    match scan_terminal(chunk)? {
        Some((encoded_len, decoded_len)) => {
            // SAFETY: upheld by the caller.
            let decoded =
                unsafe { S::decode_contiguous_unchecked(&chunk[..encoded_len], decoded_len)? };
            source.advance(encoded_len);
            Ok(decoded)
        }
        None if chunk.len() == source.remaining() => Err(MemCmpStringError::Truncated),
        None => match read_decoded_fragmented::<_, false>(
            source,
            S::STACK_DECODE_LIMIT.min(INLINE_DECODE_LIMIT),
        )? {
            DecodedBytes::Inline { bytes, len, .. } => {
                // SAFETY: upheld by the caller.
                Ok(S::from(unsafe { str::from_utf8_unchecked(&bytes[..len]) }))
            }
            DecodedBytes::Heap { bytes, .. } => {
                // SAFETY: upheld by the caller.
                Ok(S::from(unsafe { String::from_utf8_unchecked(bytes) }))
            }
        },
    }
}

fn validate_str(decoded: &[u8], is_ascii: bool) -> MemCmpResult<&str> {
    if is_ascii {
        // SAFETY: ASCII is valid UTF-8.
        Ok(unsafe { str::from_utf8_unchecked(decoded) })
    } else {
        str::from_utf8(decoded).map_err(|_| MemCmpStringError::InvalidUtf8)
    }
}

fn validate_string(decoded: Vec<u8>, is_ascii: bool) -> MemCmpResult<String> {
    if is_ascii {
        // SAFETY: ASCII is valid UTF-8.
        Ok(unsafe { String::from_utf8_unchecked(decoded) })
    } else {
        String::from_utf8(decoded).map_err(|_| MemCmpStringError::InvalidUtf8)
    }
}

enum DecodedBytes {
    Inline {
        bytes: [u8; INLINE_DECODE_LIMIT],
        len: usize,
        is_ascii: bool,
    },
    Heap {
        bytes: Vec<u8>,
        is_ascii: bool,
    },
}

impl DecodedBytes {
    fn new() -> Self {
        Self::Inline {
            bytes: [0; INLINE_DECODE_LIMIT],
            len: 0,
            is_ascii: true,
        }
    }

    fn extend<const CHECK_ASCII: bool>(
        &mut self,
        data: &[u8],
        stack_decode_limit: usize,
        heap_capacity_hint: usize,
    ) {
        match self {
            Self::Inline {
                bytes,
                len,
                is_ascii,
            } if *len + data.len() <= stack_decode_limit => {
                bytes[*len..*len + data.len()].copy_from_slice(data);
                *len += data.len();
                if CHECK_ASCII && *is_ascii {
                    *is_ascii = data.is_ascii();
                }
            }
            Self::Inline {
                bytes,
                len,
                is_ascii,
            } => {
                let capacity = (stack_decode_limit * 2)
                    .max(heap_capacity_hint)
                    .max(*len + data.len());
                let mut heap = Vec::with_capacity(capacity);
                heap.extend_from_slice(&bytes[..*len]);
                heap.extend_from_slice(data);
                if CHECK_ASCII && *is_ascii {
                    *is_ascii = data.is_ascii();
                }
                *self = Self::Heap {
                    bytes: heap,
                    is_ascii: *is_ascii,
                };
            }
            Self::Heap { bytes, is_ascii } => {
                bytes.extend_from_slice(data);
                if CHECK_ASCII && *is_ascii {
                    *is_ascii = data.is_ascii();
                }
            }
        }
    }
}

/// Decodes one group at a time through the `Buf` API; used when the string does not terminate
/// within the source's first chunk. Values up to `stack_decode_limit` remain on the stack.
#[cold]
fn read_decoded_fragmented<B: Buf, const CHECK_ASCII: bool>(
    source: &mut B,
    stack_decode_limit: usize,
) -> MemCmpResult<DecodedBytes> {
    let mut decoded = DecodedBytes::new();
    let mut group = [0u8; ENCODED_GROUP_SIZE];
    let heap_capacity_hint = source.remaining().min(INLINE_DECODE_LIMIT);

    loop {
        if source.remaining() < ENCODED_GROUP_SIZE {
            return Err(MemCmpStringError::Truncated);
        }

        source.copy_to_slice(&mut group);
        if group[GROUP_SIZE] == CONTINUATION_MARKER {
            decoded.extend::<CHECK_ASCII>(
                &group[..GROUP_SIZE],
                stack_decode_limit,
                heap_capacity_hint,
            );
            continue;
        }

        let used = terminal_group_len(&group)?;
        decoded.extend::<CHECK_ASCII>(&group[..used], stack_decode_limit, heap_capacity_hint);
        return Ok(decoded);
    }
}

/// A decoded payload view over an exact contiguous encoded field.
struct DecodedMemCmpBuf<'a> {
    encoded: &'a [u8],
    remaining: usize,
    group_remaining: usize,
}

impl<'a> DecodedMemCmpBuf<'a> {
    fn new(encoded: &'a [u8], decoded_len: usize) -> Self {
        Self {
            encoded,
            remaining: decoded_len,
            group_remaining: decoded_len.min(GROUP_SIZE),
        }
    }
}

impl Buf for DecodedMemCmpBuf<'_> {
    fn remaining(&self) -> usize {
        self.remaining
    }

    fn chunk(&self) -> &[u8] {
        &self.encoded[..self.group_remaining]
    }

    fn advance(&mut self, mut count: usize) {
        assert!(
            count <= self.remaining,
            "cannot advance past decoded payload"
        );

        while count > 0 {
            let step = count.min(self.group_remaining);
            self.encoded = &self.encoded[step..];
            self.group_remaining -= step;
            self.remaining -= step;
            count -= step;

            if self.group_remaining == 0 && self.remaining > 0 {
                self.encoded = &self.encoded[GROUP_MARKER_SIZE..];
                self.group_remaining = self.remaining.min(GROUP_SIZE);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::{Buf, BufMut, Bytes, BytesMut};

    use super::*;

    const STATIC_VALUES: &[(&str, &EncodedMemCmpStr)] = &[
        ("", encoded_mem_cmp_str!("")),
        ("a", encoded_mem_cmp_str!("a")),
        ("abcdefgh", encoded_mem_cmp_str!("abcdefgh")),
        ("abcdefghi", encoded_mem_cmp_str!("abcdefghi")),
        ("abcdefghijklmno", encoded_mem_cmp_str!("abcdefghijklmno")),
        ("abcdefghijklmnop", encoded_mem_cmp_str!("abcdefghijklmnop")),
        (
            "abcdefghijklmnopq",
            encoded_mem_cmp_str!("abcdefghijklmnopq"),
        ),
        ("🦀", encoded_mem_cmp_str!("🦀")),
        ("hello 🦀", encoded_mem_cmp_str!("hello 🦀")),
    ];

    fn encode_mem_comparable(value: &str) -> Bytes {
        let value: MemCmpString = MemCmpString::from(value);
        let mut buf = BytesMut::with_capacity(value.encoded_len());
        value.encode_to(&mut buf);
        assert_eq!(buf.len(), value.encoded_len());
        buf.freeze()
    }

    #[test]
    fn encoding_examples() {
        assert_eq!(
            encode_mem_comparable("").as_ref(),
            &[0, 0, 0, 0, 0, 0, 0, 0, 0]
        );
        assert_eq!(
            encode_mem_comparable("a").as_ref(),
            &[b'a', 0, 0, 0, 0, 0, 0, 0, 1]
        );
        assert_eq!(
            encode_mem_comparable("abcdefg").as_ref(),
            &[b'a', b'b', b'c', b'd', b'e', b'f', b'g', 0, 7]
        );
        assert_eq!(
            encode_mem_comparable("abcdefgh").as_ref(),
            &[b'a', b'b', b'c', b'd', b'e', b'f', b'g', b'h', 8]
        );
        assert_eq!(
            encode_mem_comparable("abcdefghi").as_ref(),
            &[
                b'a', b'b', b'c', b'd', b'e', b'f', b'g', b'h', 9, b'i', 0, 0, 0, 0, 0, 0, 0, 1,
            ]
        );
    }

    #[test]
    fn static_encoded_views_match_runtime_encoding_and_take_one_field() {
        for &(value, encoded) in STATIC_VALUES {
            let expected = encode_mem_comparable(value);
            assert_eq!(encoded.as_bytes(), expected.as_ref());
            assert_eq!(encoded.encoded_len(), expected.len());
            assert_eq!(encoded.decode(), ReString::from(value));

            let mut suffixed = expected.to_vec();
            suffixed.extend_from_slice(b"suffix");
            let (parsed, remaining) = EncodedMemCmpStr::try_ref_from_prefix(&suffixed).unwrap();
            assert_eq!(parsed, encoded);
            assert_eq!(remaining, b"suffix");

            // SAFETY: `suffixed` starts with an encoding produced from `value` above.
            let (parsed, remaining) =
                unsafe { EncodedMemCmpStr::try_ref_from_prefix_unchecked(&suffixed).unwrap() };
            assert_eq!(parsed, encoded);
            assert_eq!(remaining, b"suffix");
        }
    }

    #[test]
    fn keeps_utf8_lexicographical_sorting() {
        let samples = [
            "",
            "\0",
            "\0a",
            "a",
            "a\0",
            "a\0a",
            "aa",
            "abcdefg",
            "abcdefg\0",
            "abcdefgh",
            "abcdefghi",
            "b",
            "z",
            "å",
            "Ω",
            "🦀",
            "\u{10ffff}",
        ];

        let mut expected = samples.to_vec();
        expected.sort_unstable();

        let mut by_encoded = samples
            .iter()
            .map(|value| (*value, encode_mem_comparable(value)))
            .collect::<Vec<_>>();
        by_encoded.sort_by(|(_, lhs), (_, rhs)| lhs.as_ref().cmp(rhs.as_ref()));

        let actual = by_encoded
            .into_iter()
            .map(|(value, _)| value)
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }

    #[test]
    fn roundtrip_and_stops_at_boundary() {
        for sample in [
            "",
            "a",
            "a\0",
            "abcdefg",
            "abcdefgh",
            "abcdefghi",
            "🦀",
            "hello 🦀",
        ] {
            let value: MemCmpString = MemCmpString::from(sample);
            let mut buf = BytesMut::with_capacity(value.encoded_len() + 4);
            value.encode_to(&mut buf);
            buf.put_u32(42);

            let mut got = buf.freeze();
            let decoded = MemCmpString::<String>::decode_from(&mut got).unwrap();
            assert_eq!(decoded.as_str(), sample);
            assert_eq!(decoded.encoded_len(), value.encoded_len());
            assert_eq!(got.get_u32(), 42);
            assert!(!got.has_remaining());
        }
    }

    #[test]
    fn rejects_malformed_encoding() {
        let truncated = [0u8; GROUP_SIZE];
        assert!(MemCmpString::<String>::decode_from(&mut truncated.as_slice()).is_err());

        // a continuation group followed by a truncated group
        let mut truncated_continuation = encode_mem_comparable("abcdefghi").to_vec();
        truncated_continuation.truncate(ENCODED_GROUP_SIZE + GROUP_SIZE);
        assert!(
            MemCmpString::<String>::decode_from(&mut truncated_continuation.as_slice()).is_err()
        );

        let mut bad_marker = [0; ENCODED_GROUP_SIZE];
        bad_marker[GROUP_SIZE] = CONTINUATION_MARKER + 1;
        assert!(MemCmpString::<String>::decode_from(&mut bad_marker.as_slice()).is_err());

        let mut bad_padding = encode_mem_comparable("a").to_vec();
        bad_padding[1] = b'x';
        assert!(MemCmpString::<String>::decode_from(&mut bad_padding.as_slice()).is_err());

        let mut invalid_utf8 = [0; ENCODED_GROUP_SIZE];
        invalid_utf8[0] = 0xff;
        invalid_utf8[GROUP_SIZE] = 1;
        assert!(MemCmpString::<String>::decode_from(&mut invalid_utf8.as_slice()).is_err());
        assert!(decode_str_with(&mut invalid_utf8.as_slice(), str::to_owned).is_err());
        assert!(EncodedMemCmpStr::try_ref_from_prefix(&invalid_utf8).is_err());
        assert!(decode_str_into(&invalid_utf8, &mut [0; GROUP_SIZE]).is_err());
        let (front, back) = invalid_utf8.split_at(1);
        assert!(MemCmpString::<String>::decode_from(&mut front.chain(back)).is_err());
        assert!(decode_str_with(&mut front.chain(back), str::to_owned).is_err());

        // a multi-byte code point crossing an encoded group boundary
        let mut invalid_boundary = encode_mem_comparable("abcdefg🦀").to_vec();
        invalid_boundary[ENCODED_GROUP_SIZE] = b'x';
        assert_eq!(
            EncodedMemCmpStr::try_ref_from_prefix(&invalid_boundary).unwrap_err(),
            MemCmpStringError::InvalidUtf8
        );

        // truncation must also be detected on the fragmented and decode_with paths
        let encoded = encode_mem_comparable("abcdefghi");
        let (a, b) = encoded.as_ref().split_at(5);
        let truncated_b = &b[..b.len() - 1];
        assert!(MemCmpString::<String>::decode_from(&mut a.chain(truncated_b)).is_err());
        assert!(decode_str_with(&mut a.chain(truncated_b), str::to_owned).is_err());
        assert!(decode_str_with(&mut &encoded.as_ref()[..GROUP_SIZE], str::to_owned).is_err());
    }

    #[test]
    fn decode_with_and_fragmented_sources() {
        // stack-staged decode into ReString and ByteString targets, with trailing key fields
        // left intact
        let value: MemCmpString = MemCmpString::from("hello 🦀");
        let mut buf = BytesMut::with_capacity(2 * (value.encoded_len() + 4));
        value.encode_to(&mut buf);
        buf.put_u32(42);
        value.encode_to(&mut buf);
        buf.put_u32(43);
        let mut input = buf.freeze();
        let decoded = MemCmpString::<ReString>::decode_from(&mut input).unwrap();
        assert_eq!(decoded.as_str(), "hello 🦀");
        assert_eq!(input.get_u32(), 42);
        #[cfg(feature = "bytestring")]
        {
            let decoded = MemCmpString::<ByteString>::decode_from(&mut input).unwrap();
            assert_eq!(decoded.as_str(), "hello 🦀");
            assert_eq!(input.get_u32(), 43);
        }
        #[cfg(not(feature = "bytestring"))]
        {
            let decoded = MemCmpString::<ReString>::decode_from(&mut input).unwrap();
            assert_eq!(decoded.as_str(), "hello 🦀");
            assert_eq!(input.get_u32(), 43);
        }
        assert!(!input.has_remaining());

        // heap-spill path (> INLINE_DECODE_LIMIT)
        let long = "x".repeat(INLINE_DECODE_LIMIT + 36);
        let encoded = encode_mem_comparable(&long);
        let mut input = encoded.as_ref();
        let decoded = decode_str_with(&mut input, str::to_owned).unwrap();
        assert_eq!(decoded, long);
        assert!(input.is_empty());
        let mut input = encoded.as_ref();
        let decoded = MemCmpString::<ReString>::decode_from(&mut input).unwrap();
        assert_eq!(decoded.as_str(), long);
        assert!(input.is_empty());

        // fragmented source: the terminal group lies beyond the first chunk
        let (front, back) = encoded.as_ref().split_at(5 * ENCODED_GROUP_SIZE);
        let mut chained = front.chain(back);
        let decoded = MemCmpString::<String>::decode_from(&mut chained).unwrap();
        assert_eq!(decoded.as_str(), long);
        assert!(!chained.has_remaining());

        let medium = "x".repeat(32);
        let encoded_medium = encode_mem_comparable(&medium);
        let (front, back) = encoded_medium.as_ref().split_at(1);
        let DecodedBytes::Heap { bytes, .. } =
            read_decoded_fragmented::<_, true>(&mut front.chain(back), 0).unwrap()
        else {
            panic!("a zero stack limit must use the heap");
        };
        assert!(bytes.capacity() >= encoded_medium.len());
        assert!(matches!(
            read_decoded_fragmented::<_, true>(&mut front.chain(back), 24).unwrap(),
            DecodedBytes::Heap { .. }
        ));
        assert!(matches!(
            read_decoded_fragmented::<_, true>(&mut front.chain(back), INLINE_DECODE_LIMIT)
                .unwrap(),
            DecodedBytes::Inline { .. }
        ));

        let unicode = "🦀".repeat(INLINE_DECODE_LIMIT);
        let encoded_unicode = encode_mem_comparable(&unicode);
        let (front, back) = encoded_unicode.as_ref().split_at(5 * ENCODED_GROUP_SIZE);
        // SAFETY: `encoded_unicode` was produced from `unicode` above.
        let decoded = unsafe {
            MemCmpString::<String>::decode_from_unchecked(&mut front.chain(back)).unwrap()
        };
        assert_eq!(decoded.as_str(), unicode);
        // SAFETY: `encoded_unicode` was produced from `unicode` above.
        let decoded =
            unsafe { decode_str_with_unchecked(&mut front.chain(back), str::to_owned).unwrap() };
        assert_eq!(decoded, unicode);

        // fused decode into a caller buffer, with trailing key bytes present
        let mut with_suffix = encoded.to_vec();
        with_suffix.extend_from_slice(&[7; 4]);
        let mut arena = [0u8; 256];
        let (enc_len, dec_len) = decode_str_into(&with_suffix, &mut arena).unwrap();
        assert_eq!((enc_len, dec_len), (encoded.len(), long.len()));
        assert_eq!(&arena[..dec_len], long.as_bytes());
        assert!(decode_str_into(&encoded[..encoded.len() - 1], &mut arena).is_err());
        assert!(decode_str_into(&encoded, &mut arena[..long.len() - 1]).is_err());
    }

    /// Sweeps every length through every encode and decode path: group boundaries (7/8/9,
    /// 15/16/17 — including exact-multiple-of-8 terminal groups), the
    /// `INLINE_DECODE_LIMIT` edge (64/65), fast and fallback encoding, contiguous, fragmented,
    /// and fused decoding, plus order preservation between consecutive samples.
    #[test]
    fn roundtrip_all_lengths_through_all_paths() {
        let mut arena = [0u8; 128];
        let mut previous_encoded = None::<Bytes>;

        for len in 0..=80usize {
            let sample: String = ('a'..='z').cycle().take(len).collect();
            let encoded = encode_mem_comparable(&sample);

            // fallback encoding (no contiguous room) produces identical bytes
            let mut front = [0xffu8; 5];
            let mut back = [0xffu8; 128];
            {
                let mut target = front.as_mut_slice().chain_mut(back.as_mut_slice());
                MemCmpStr::from(sample.as_str()).encode_to(&mut target);
            }
            let mut fallback = front.to_vec();
            fallback.extend_from_slice(&back);
            assert_eq!(&fallback[..encoded.len()], encoded.as_ref(), "len {len}");

            // contiguous decode, both entry points
            let mut input = encoded.clone();
            assert_eq!(
                MemCmpString::<String>::decode_from(&mut input)
                    .unwrap()
                    .as_str(),
                sample,
                "len {len}"
            );
            assert!(!input.has_remaining());
            let mut input = encoded.as_ref();
            assert_eq!(
                decode_str_with(&mut input, str::to_owned).unwrap(),
                sample,
                "len {len}"
            );

            let mut input = encoded.as_ref();
            // SAFETY: `encoded` was produced from `sample` above.
            assert_eq!(
                unsafe { MemCmpString::<String>::decode_from_unchecked(&mut input) }
                    .unwrap()
                    .as_str(),
                sample,
                "len {len}"
            );
            let mut input = encoded.as_ref();
            // SAFETY: `encoded` was produced from `sample` above.
            assert_eq!(
                unsafe { decode_str_with_unchecked(&mut input, str::to_owned) }.unwrap(),
                sample,
                "len {len}"
            );

            // fragmented decode, both entry points
            let (a, b) = encoded.as_ref().split_at(encoded.len() / 2);
            assert_eq!(
                MemCmpString::<String>::decode_from(&mut a.chain(b))
                    .unwrap()
                    .as_str(),
                sample,
                "len {len}"
            );
            assert_eq!(
                MemCmpString::<ReString>::decode_from(&mut a.chain(b))
                    .unwrap()
                    .as_str(),
                sample,
                "len {len}"
            );
            assert_eq!(
                decode_str_with(&mut a.chain(b), str::to_owned).unwrap(),
                sample,
                "len {len}"
            );
            // SAFETY: `encoded` was produced from `sample` above.
            assert_eq!(
                unsafe { MemCmpString::<String>::decode_from_unchecked(&mut a.chain(b)) }
                    .unwrap()
                    .as_str(),
                sample,
                "len {len}"
            );
            // SAFETY: `encoded` was produced from `sample` above.
            assert_eq!(
                unsafe { decode_str_with_unchecked(&mut a.chain(b), str::to_owned) }.unwrap(),
                sample,
                "len {len}"
            );

            // fused decode into a caller buffer
            let (enc_len, dec_len) = decode_str_into(encoded.as_ref(), &mut arena).unwrap();
            assert_eq!((enc_len, dec_len), (encoded.len(), len), "len {len}");
            assert_eq!(&arena[..dec_len], sample.as_bytes(), "len {len}");

            // each sample is a strict prefix of the next, so encodings must sort the same way
            if let Some(previous) = previous_encoded.replace(encoded.clone()) {
                assert!(previous < encoded, "len {len}");
            }
        }
    }

    #[test]
    fn encode_falls_back_without_contiguous_room() {
        let expected = encode_mem_comparable("abcdefghi");

        let mut front = [0xffu8; 10];
        let mut back = [0xffu8; 10];
        {
            let mut target = front.as_mut_slice().chain_mut(back.as_mut_slice());
            MemCmpStr::from("abcdefghi").encode_to(&mut target);
        }

        let mut written = front.to_vec();
        written.extend_from_slice(&back);
        assert_eq!(&written[..expected.len()], expected.as_ref());
    }

    #[test]
    fn static_borrowed_and_owned_encoders_match() {
        for &(sample, static_encoded) in STATIC_VALUES {
            let owned: MemCmpString = MemCmpString::from(sample);
            let borrowed = MemCmpStr::from(sample);

            let mut owned_buf = BytesMut::with_capacity(owned.encoded_len());
            let mut borrowed_buf = BytesMut::with_capacity(borrowed.encoded_len());
            owned.encode_to(&mut owned_buf);
            borrowed.encode_to(&mut borrowed_buf);

            assert_eq!(borrowed.encoded_len(), owned.encoded_len());
            assert_eq!(static_encoded.encoded_len(), owned.encoded_len());
            assert_eq!(static_encoded.as_bytes(), owned_buf.as_ref());
            assert_eq!(static_encoded.as_bytes(), borrowed_buf.as_ref());
        }
    }
}
