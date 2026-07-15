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
use std::ops::Deref;

use anyhow::anyhow;
use bytes::{Buf, BufMut};
use bytestring::ByteString;

use restate_storage_api::StorageError;
use restate_util_string::{OwnedStringLike, ReString};

use super::{KeyDecode, KeyEncode};

const GROUP_SIZE: usize = 8;
const GROUP_MARKER_SIZE: usize = 1;
const ENCODED_GROUP_SIZE: usize = GROUP_SIZE + GROUP_MARKER_SIZE;
const CONTINUATION_MARKER: u8 = ENCODED_GROUP_SIZE as u8;

/// Decoded strings up to this length are staged on the stack in [`decode_str_with`] and in
/// [`MemCmpString::decode_from`] for targets with [`MemCmpTarget::PREFERS_BORROWED`].
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
/// Implementations only choose the decode staging via
/// [`PREFERS_BORROWED`](Self::PREFERS_BORROWED).
pub trait MemCmpTarget: OwnedStringLike + From<String> {
    /// When true, strings up to [`INLINE_DECODE_LIMIT`] bytes are decoded onto the stack and
    /// built via `From<&str>`, skipping the intermediate heap buffer. Set this if building from
    /// a borrowed slice is at least as cheap as taking ownership of a `String` (e.g. types with
    /// inline small-string storage).
    const PREFERS_BORROWED: bool;
}

impl MemCmpTarget for String {
    const PREFERS_BORROWED: bool = false;
}

// `ReString::from(String)` copies either way, so borrowed construction is never worse and
// inlines short strings without any allocation.
impl MemCmpTarget for ReString {
    const PREFERS_BORROWED: bool = true;
}

// `ByteString::from(String)` wraps the allocation zero-copy.
impl MemCmpTarget for ByteString {
    const PREFERS_BORROWED: bool = false;
}

/// A UTF-8 string encoded for byte-wise comparisons in RocksDB keys.
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
/// inference-ambiguous calls need an annotation (e.g. `MemCmpString::<String>::decode(..)`).
#[derive(Debug, Clone, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct MemCmpString<S = String>(S);

/// A borrowed UTF-8 string encoded for byte-wise comparisons in RocksDB keys.
///
/// This is the encode-only borrowed counterpart of [`MemCmpString`].
#[derive(Debug, Copy, Clone, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct MemCmpStr<'a>(&'a str);

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
    pub fn decode_from<B: Buf>(source: &mut B) -> crate::Result<Self> {
        if S::PREFERS_BORROWED {
            decode_str_with(source, |s| Self(S::from(s)))
        } else {
            Self::decode_owned(source)
        }
    }

    /// `inline(always)`: this is the decode hot path; left to its own devices the inliner
    /// outlines it, costing ~40% on single-group strings.
    #[inline(always)]
    fn decode_owned<B: Buf>(source: &mut B) -> crate::Result<Self> {
        let chunk = source.chunk();
        let decoded = match scan_terminal(chunk)? {
            Some((encoded_len, decoded_len)) => {
                let decoded = decode_contiguous(chunk, encoded_len, decoded_len);
                source.advance(encoded_len);
                decoded
            }
            None if chunk.len() == source.remaining() => {
                return Err(StorageError::DataIntegrityError);
            }
            None => read_decoded_fragmented(source)?,
        };
        // SAFETY: the groups were encoded from a valid `str` by `write_bytes`.
        let decoded = unsafe { String::from_utf8_unchecked(decoded) };
        Ok(Self(S::from(decoded)))
    }
}

/// Decodes the next mem-comparable string and passes it to `f` as a borrowed `str`, for one-off
/// conversions without committing to a [`MemCmpTarget`]. Strings up to [`INLINE_DECODE_LIMIT`]
/// bytes are staged on the stack.
pub fn decode_str_with<B: Buf, R>(source: &mut B, f: impl FnOnce(&str) -> R) -> crate::Result<R> {
    let chunk = source.chunk();
    match scan_terminal(chunk)? {
        Some((encoded_len, decoded_len)) if decoded_len <= INLINE_DECODE_LIMIT => {
            let mut stack = [0u8; INLINE_DECODE_LIMIT];
            let decoded = &mut stack[..decoded_len];
            copy_groups(&chunk[..encoded_len], decoded);
            source.advance(encoded_len);
            // SAFETY: the groups were encoded from a valid `str` by `write_bytes`.
            Ok(f(unsafe { str::from_utf8_unchecked(decoded) }))
        }
        Some((encoded_len, decoded_len)) => {
            let decoded = decode_contiguous(chunk, encoded_len, decoded_len);
            source.advance(encoded_len);
            // SAFETY: the groups were encoded from a valid `str` by `write_bytes`.
            Ok(f(unsafe { str::from_utf8_unchecked(&decoded) }))
        }
        None if chunk.len() == source.remaining() => Err(StorageError::DataIntegrityError),
        None => {
            let decoded = read_decoded_fragmented(source)?;
            // SAFETY: the groups were encoded from a valid `str` by `write_bytes`.
            Ok(f(unsafe { str::from_utf8_unchecked(&decoded) }))
        }
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

impl<S: MemCmpTarget> KeyEncode for MemCmpString<S> {
    fn encode<B: BufMut>(&self, target: &mut B) {
        self.encode_to(target);
    }

    fn serialized_length(&self) -> usize {
        self.encoded_len()
    }
}

impl<S: MemCmpTarget> KeyDecode for MemCmpString<S> {
    fn decode<B: Buf>(source: &mut B) -> crate::Result<Self> {
        Self::decode_from(source)
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

impl KeyEncode for MemCmpStr<'_> {
    fn encode<B: BufMut>(&self, target: &mut B) {
        self.encode_to(target);
    }

    fn serialized_length(&self) -> usize {
        self.encoded_len()
    }
}

#[inline]
fn serialized_length(source_len: usize) -> usize {
    (source_len.saturating_sub(1) / GROUP_SIZE + 1) * ENCODED_GROUP_SIZE
}

/// Decodes the next mem-comparable string from the front of `input` into `dst`, returning
/// `(encoded_len, decoded_len)`. Since decoding always shrinks, a `dst` of `input.len()` bytes
/// always fits; a too-short `dst` yields `DataIntegrityError`.
///
/// Unlike the exact-allocation decode paths, this copies in a single fused pass — with a
/// caller-provided buffer there is nothing to size upfront, so a scan pass would be pure
/// overhead.
///
/// This is the allocation-free primitive that secondary-index scratch decoding builds on;
/// hidden until that API stabilizes.
#[doc(hidden)]
pub fn decode_str_into(input: &[u8], dst: &mut [u8]) -> crate::Result<(usize, usize)> {
    let mut src_off = 0;
    let mut dst_off = 0;

    loop {
        let Some(group) = input.get(src_off..src_off + ENCODED_GROUP_SIZE) else {
            return Err(StorageError::DataIntegrityError);
        };
        src_off += ENCODED_GROUP_SIZE;

        if group[GROUP_SIZE] == CONTINUATION_MARKER {
            let out = dst
                .get_mut(dst_off..dst_off + GROUP_SIZE)
                .ok_or(StorageError::DataIntegrityError)?;
            out.copy_from_slice(&group[..GROUP_SIZE]);
            dst_off += GROUP_SIZE;
            continue;
        }

        let used = terminal_group_len(group)?;
        let out = dst
            .get_mut(dst_off..dst_off + used)
            .ok_or(StorageError::DataIntegrityError)?;
        out.copy_from_slice(&group[..used]);
        return Ok((src_off, dst_off + used));
    }
}

/// Builds the zero-padded final group; `tail` holds the last `0..=GROUP_SIZE` source bytes.
#[inline]
fn final_group(tail: &[u8]) -> [u8; ENCODED_GROUP_SIZE] {
    let mut group = [0u8; ENCODED_GROUP_SIZE];
    group[..tail.len()].copy_from_slice(tail);
    group[GROUP_SIZE] = tail.len() as u8;
    group
}

#[inline]
fn write_bytes<B: BufMut>(source: &[u8], target: &mut B) {
    if source.len() <= GROUP_SIZE {
        target.put_slice(&final_group(source));
        return;
    }

    let continuation_len = (source.len() - 1) / GROUP_SIZE * GROUP_SIZE;
    let encoded_len = serialized_length(source.len());

    let chunk = target.chunk_mut();
    if chunk.len() >= encoded_len {
        let mut pos = 0;
        for group in source[..continuation_len].chunks_exact(GROUP_SIZE) {
            chunk[pos..pos + GROUP_SIZE].copy_from_slice(group);
            chunk.write_byte(pos + GROUP_SIZE, CONTINUATION_MARKER);
            pos += ENCODED_GROUP_SIZE;
        }
        chunk[pos..pos + ENCODED_GROUP_SIZE]
            .copy_from_slice(&final_group(&source[continuation_len..]));
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
        target.put_slice(group);
        target.put_u8(CONTINUATION_MARKER);
    }
    target.put_slice(&final_group(&source[continuation_len..]));
}

/// Validates a final (non-continuation) group and returns the number of source bytes it holds.
#[inline]
fn terminal_group_len(group: &[u8]) -> crate::Result<usize> {
    let bits = u64::from_be_bytes(group[..GROUP_SIZE].try_into().expect("group of 8 bytes"));
    terminal_group_len_from(bits, group[GROUP_SIZE])
}

/// Validates a final group given its data bytes as a big-endian word and its marker byte.
///
/// A marker-0 group after continuations (the MyRocks legacy form for exact multiples of 8) is
/// deliberately not rejected: we never encode it, and rejecting it would cost an extra check.
#[inline]
fn terminal_group_len_from(bits: u64, marker: u8) -> crate::Result<usize> {
    if marker > GROUP_SIZE as u8 {
        return Err(StorageError::Generic(anyhow!(
            "invalid mem-comparable string marker: {marker:#04x}"
        )));
    }
    let used = usize::from(marker);
    if bits & PADDING_MASKS[used] != 0 {
        return Err(StorageError::Generic(anyhow!(
            "non-zero padding in mem-comparable string"
        )));
    }
    Ok(used)
}

/// Returns the encoded length of the next string field without copying it out, validating the
/// terminal group. Backs zero-copy field skipping and raw-bytes predicates in secondary
/// indexes.
#[allow(dead_code)]
#[inline]
pub(super) fn next_encoded_str_len(input: &[u8]) -> crate::Result<usize> {
    let mut offset = 0;
    loop {
        let Some(group) = input.get(offset..offset + ENCODED_GROUP_SIZE) else {
            return Err(StorageError::DataIntegrityError);
        };
        offset += ENCODED_GROUP_SIZE;

        if group[GROUP_SIZE] == CONTINUATION_MARKER {
            continue;
        }
        terminal_group_len(group)?;
        return Ok(offset);
    }
}

/// Finds the final group of an encoded string within `chunk`.
///
/// Returns `Some((encoded_len, decoded_len))` when the string terminates within `chunk`, `None`
/// when the encoding continues past it.
#[inline]
fn scan_terminal(chunk: &[u8]) -> crate::Result<Option<(usize, usize)>> {
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
fn copy_groups(encoded: &[u8], decoded: &mut [u8]) {
    let mut groups = encoded.chunks_exact(ENCODED_GROUP_SIZE);
    // full groups copy with a compile-time-known length
    let mut full = decoded.chunks_exact_mut(GROUP_SIZE);
    for out in &mut full {
        let group = groups.next().expect("enough encoded groups");
        out.copy_from_slice(&group[..GROUP_SIZE]);
    }

    let tail = full.into_remainder();
    if !tail.is_empty() {
        let group = groups.next().expect("a final encoded group");
        tail.copy_from_slice(&group[..tail.len()]);
    }
}

/// Copies the decoded bytes out of the first `encoded_len` bytes of `chunk`, as reported by
/// [`scan_terminal`], into a fresh exactly-sized `Vec`.
#[inline]
fn decode_contiguous(chunk: &[u8], encoded_len: usize, decoded_len: usize) -> Vec<u8> {
    if encoded_len == ENCODED_GROUP_SIZE {
        // single group: a plain allocation plus one append beats zero-filling
        let mut decoded = Vec::with_capacity(decoded_len);
        decoded.extend_from_slice(&chunk[..decoded_len]);
        decoded
    } else {
        let mut decoded = vec![0u8; decoded_len];
        copy_groups(&chunk[..encoded_len], &mut decoded);
        decoded
    }
}

/// Decodes one group at a time through the `Buf` API; used when the string does not terminate
/// within the source's first chunk.
#[cold]
fn read_decoded_fragmented<B: Buf>(source: &mut B) -> crate::Result<Vec<u8>> {
    let mut decoded = Vec::new();
    let mut group = [0u8; ENCODED_GROUP_SIZE];

    loop {
        if source.remaining() < ENCODED_GROUP_SIZE {
            return Err(StorageError::DataIntegrityError);
        }

        source.copy_to_slice(&mut group);
        if group[GROUP_SIZE] == CONTINUATION_MARKER {
            decoded.extend_from_slice(&group[..GROUP_SIZE]);
            continue;
        }

        let used = terminal_group_len(&group)?;
        decoded.extend_from_slice(&group[..used]);
        return Ok(decoded);
    }
}

#[cfg(test)]
mod tests {
    use bytes::{Buf, BufMut, Bytes, BytesMut};

    use crate::keys::{KeyDecode, KeyEncode};

    use super::*;

    fn encode_mem_comparable(value: &str) -> Bytes {
        let value: MemCmpString = MemCmpString::from(value);
        let mut buf = BytesMut::with_capacity(value.serialized_length());
        value.encode(&mut buf);
        assert_eq!(buf.len(), value.serialized_length());
        buf.freeze()
    }

    #[test]
    fn matches_myrocks_variable_format_examples() {
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
            "hello 🦀",
        ] {
            let value: MemCmpString = MemCmpString::from(sample);
            let mut buf = BytesMut::with_capacity(value.serialized_length() + 4);
            value.encode(&mut buf);
            42u32.encode(&mut buf);

            let mut got = buf.freeze();
            let decoded = MemCmpString::<String>::decode(&mut got).unwrap();
            assert_eq!(decoded.as_str(), sample);
            assert_eq!(decoded.serialized_length(), value.serialized_length());
            assert_eq!(u32::decode(&mut got).unwrap(), 42);
            assert!(!got.has_remaining());
        }
    }

    #[test]
    fn rejects_malformed_encoding() {
        let truncated = [0u8; GROUP_SIZE];
        assert!(MemCmpString::<String>::decode(&mut truncated.as_slice()).is_err());

        // a continuation group followed by a truncated group
        let mut truncated_continuation = encode_mem_comparable("abcdefghi").to_vec();
        truncated_continuation.truncate(ENCODED_GROUP_SIZE + GROUP_SIZE);
        assert!(MemCmpString::<String>::decode(&mut truncated_continuation.as_slice()).is_err());

        let mut bad_marker = [0; ENCODED_GROUP_SIZE];
        bad_marker[GROUP_SIZE] = CONTINUATION_MARKER + 1;
        assert!(MemCmpString::<String>::decode(&mut bad_marker.as_slice()).is_err());

        let mut bad_padding = encode_mem_comparable("a").to_vec();
        bad_padding[1] = b'x';
        assert!(MemCmpString::<String>::decode(&mut bad_padding.as_slice()).is_err());

        // truncation must also be detected on the fragmented and decode_with paths
        let encoded = encode_mem_comparable("abcdefghi");
        let (a, b) = encoded.as_ref().split_at(5);
        let truncated_b = &b[..b.len() - 1];
        assert!(MemCmpString::<String>::decode(&mut a.chain(truncated_b)).is_err());
        assert!(decode_str_with(&mut a.chain(truncated_b), str::to_owned).is_err());
        assert!(decode_str_with(&mut &encoded.as_ref()[..GROUP_SIZE], str::to_owned).is_err());
    }

    #[test]
    fn decode_with_and_fragmented_sources() {
        // stack-staged decode into ReString and ByteString targets, with trailing key fields
        // left intact
        let value: MemCmpString = MemCmpString::from("hello 🦀");
        let mut buf = BytesMut::with_capacity(2 * (value.serialized_length() + 4));
        value.encode(&mut buf);
        42u32.encode(&mut buf);
        value.encode(&mut buf);
        43u32.encode(&mut buf);
        let mut input = buf.freeze();
        let decoded = MemCmpString::<ReString>::decode_from(&mut input).unwrap();
        assert_eq!(decoded.as_str(), "hello 🦀");
        assert_eq!(u32::decode(&mut input).unwrap(), 42);
        let decoded = MemCmpString::<ByteString>::decode_from(&mut input).unwrap();
        assert_eq!(decoded.as_str(), "hello 🦀");
        assert_eq!(u32::decode(&mut input).unwrap(), 43);
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
        let decoded = MemCmpString::<String>::decode(&mut chained).unwrap();
        assert_eq!(decoded.as_str(), long);
        assert!(!chained.has_remaining());

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
    /// 15/16/17 — including the exact-multiple-of-8 empty-tail copies), the
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
                MemCmpString::<String>::decode(&mut input).unwrap().as_str(),
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

            // fragmented decode, both entry points
            let (a, b) = encoded.as_ref().split_at(encoded.len() / 2);
            assert_eq!(
                MemCmpString::<String>::decode(&mut a.chain(b))
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
    fn borrowed_str_encodes_like_owned_string() {
        for sample in ["", "a", "abcdefgh", "abcdefghi", "hello 🦀"] {
            let owned: MemCmpString = MemCmpString::from(sample);
            let borrowed = MemCmpStr::from(sample);

            let mut owned_buf = BytesMut::with_capacity(owned.encoded_len());
            let mut borrowed_buf = BytesMut::with_capacity(borrowed.encoded_len());
            owned.encode(&mut owned_buf);
            borrowed.encode(&mut borrowed_buf);

            assert_eq!(borrowed.encoded_len(), owned.encoded_len());
            assert_eq!(borrowed_buf, owned_buf);
        }
    }
}
