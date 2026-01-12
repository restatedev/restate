// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bitflags::bitflags;
use bytes::{Buf, BufMut, Bytes, BytesMut};

use restate_clock::UniqueTimestamp;
use restate_types::logs::{KeyFilter, Keys, MatchKeyQuery, Record};
use restate_types::storage::PolyBytes;
use restate_types::time::NanosSinceEpoch;

#[derive(Debug, derive_more::TryFrom, Eq, PartialEq, Ord, PartialOrd)]
#[try_from(repr)]
#[repr(u8)]
pub(super) enum RecordFormat {
    CustomV1 = 0x01,
}

#[derive(Debug, Clone, Default)]
#[repr(transparent)]
pub struct RecordFlags(u16);
bitflags! {
    impl RecordFlags: u16 {
        /// Record timestamp is in HLC format (UniqueTimestamp)
        /// If unset, the timestamp is in NanosSinceEpoch format.
        const HlcTimestamp = 0b00000000_00000001;
        /// This flag indicates that instead of reading the payload directly from its position
        /// in the message buffer, we should expect a u8 field that contains the number of extra
        /// bytes the header occupies before the payload.
        ///
        /// This allows future versions to add extra header fields without breaking backwards
        /// compatibility.
        const ExtendedHeader = 0b00000000_00000010;
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Record decode error: {0}")]
pub enum RecordDecodeError {
    UnsupportedFormatVersion(u8),
    InvalidRecordTimestamp(#[from] restate_clock::Error),
    UnsupportedKeyStyle(u8),
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, derive_more::TryFrom)]
#[try_from(repr)]
#[repr(u8)]
enum KeyStyle {
    None = 0,
    Single = 1,
    Pair = 2,
    RangeInclusive = 3,
}

/// Helpers to encode/decode the record payload as rocksdb value
pub(super) struct DataRecordDecoder<'a> {
    keys: Keys,
    buffer: &'a [u8],
}

impl<'a> DataRecordDecoder<'a> {
    pub fn new(mut buffer: &'a [u8]) -> Result<Self, RecordDecodeError> {
        debug_assert!(buffer.len() > 1);
        // read format byte
        let record_format = read_format(&mut buffer)?;
        debug_assert_eq!(RecordFormat::CustomV1, record_format);
        // read keys
        let keys = read_keys(&mut buffer)?;
        Ok(Self { keys, buffer })
    }

    pub fn matches_key_query(&self, filter: &KeyFilter) -> bool {
        self.keys.matches_key_query(filter)
    }

    pub fn size(&self) -> usize {
        self.buffer.len()
    }

    pub fn decode(mut self) -> Result<Record, RecordDecodeError> {
        // record flags
        let flags = RecordFlags::from_bits_retain(self.buffer.get_u16_le());

        let timestamp = self.buffer.get_u64_le();
        let created_at = if flags.contains(RecordFlags::HlcTimestamp) {
            NanosSinceEpoch::from(UniqueTimestamp::try_from(timestamp)?)
        } else {
            NanosSinceEpoch::from(timestamp)
        };

        if flags.contains(RecordFlags::ExtendedHeader) {
            let extra_header_bytes = self.buffer.get_u8();
            // We skip the extra header bytes since we don't know how to decode them.
            self.buffer.advance(extra_header_bytes as usize);
        }

        let body = PolyBytes::Bytes(Bytes::copy_from_slice(self.buffer.chunk()));

        Ok(Record::from_parts(created_at, self.keys, body))
    }
}

#[derive(derive_more::From)]
pub(super) struct DataRecordEncoder<'a>(&'a Record);

impl DataRecordEncoder<'_> {
    /// On-disk record layout.
    /// For the record header, byte-order is little-endian.
    ///
    /// The record layout follow this structure:
    ///    [1 byte]        Format version.
    ///    [1 byte]        KeyStyle (see `KeyStyle` enum)
    ///      * [8 bytes]   First Key (if KeyStyle is != 0)
    ///      * [8 bytes]   Second Key (if KeyStyle is > 1)
    ///    [2 bytes]       Flags (see `RecordFlags` enum)
    ///    [8 bytes]       `created_at` timestamp
    ///    [1 byte]        [If Flags::ExtendedHeader] The number of extra bytes occupied by future header fields.
    ///    [...]           Additional header fields
    ///    [remaining]     Serialized Payload
    #[tracing::instrument(skip_all)]
    pub fn encode_to_disk_format(self, scratch: &mut BytesMut) -> bytes::buf::Chain<Bytes, Bytes> {
        // header is 1 + 1 + 8 + 8 + 2 + 8 = 28 bytes at most
        scratch.reserve(self.header_size());
        let (created_at, body, keys) = (self.0.created_at(), self.0.body(), self.0.keys());

        // Write the format version
        scratch.put_u8(RecordFormat::CustomV1 as u8);
        // key style and keys
        match keys {
            Keys::None => scratch.put_u8(KeyStyle::None as u8),
            Keys::Single(key) => {
                scratch.put_u8(KeyStyle::Single as u8);
                scratch.put_u64_le(*key);
            }
            Keys::Pair(key1, key2) => {
                scratch.put_u8(KeyStyle::Pair as u8);
                scratch.put_u64_le(*key1);
                scratch.put_u64_le(*key2);
            }
            Keys::RangeInclusive(range) => {
                scratch.put_u8(KeyStyle::RangeInclusive as u8);
                scratch.put_u64_le(*range.start());
                scratch.put_u64_le(*range.end());
            }
        }
        // flags
        let flags = RecordFlags::default();
        scratch.put_u16_le(flags.bits());

        // created_at
        scratch.put_u64_le(created_at.as_u64());
        let header_bytes = scratch.split().freeze();
        // body
        let body_bytes = body
            .encode_to_bytes(scratch)
            .expect("Encoding is infallible");

        header_bytes.chain(body_bytes)
    }

    pub const fn header_size(&self) -> usize {
        let keys = self.0.keys();

        // key style and keys
        let keys_size = match keys {
            Keys::None => size_of::<u8>(),
            Keys::Single(_) => size_of::<u8>() + size_of::<u64>(),
            Keys::Pair(..) => size_of::<u8>() + size_of::<u64>() + size_of::<u64>(),
            Keys::RangeInclusive(_) => size_of::<u8>() + size_of::<u64>() + size_of::<u64>(),
        };

        // version
        size_of::<u8>() +
        // key style and keys
        keys_size +
        // flags
        size_of::<u16>() +
        // created_at
        size_of::<u64>()
    }
}

// Reads KeyStyle and extract the keys from the buffer
fn read_keys<B: Buf>(buf: &mut B) -> Result<Keys, RecordDecodeError> {
    let key_style = buf.get_u8();
    let key_style = KeyStyle::try_from(key_style)
        .map_err(|_| RecordDecodeError::UnsupportedKeyStyle(key_style))?;
    match key_style {
        KeyStyle::None => Ok(Keys::None),
        KeyStyle::Single => {
            let key = buf.get_u64_le();
            Ok(Keys::Single(key))
        }
        KeyStyle::Pair => {
            let key1 = buf.get_u64_le();
            let key2 = buf.get_u64_le();
            Ok(Keys::Pair(key1, key2))
        }
        KeyStyle::RangeInclusive => {
            let key1 = buf.get_u64_le();
            let key2 = buf.get_u64_le();
            Ok(Keys::RangeInclusive(key1..=key2))
        }
    }
}

// also validates that record's format version is supported
fn read_format<B: Buf>(buf: &mut B) -> Result<RecordFormat, RecordDecodeError> {
    let format = buf.get_u8();
    RecordFormat::try_from(format).map_err(|_| RecordDecodeError::UnsupportedFormatVersion(format))
}

#[cfg(test)]
mod tests {
    use super::*;

    use googletest::prelude::*;
    use restate_types::logs::Keys;
    use restate_types::time::MillisSinceEpoch;

    /// Tests backward compatibility for decoding records with different flag combinations.
    /// This ensures the decoder can handle:
    /// 1. Records without HLC timestamps (plain NanosSinceEpoch)
    /// 2. Records with HLC timestamps (UniqueTimestamp encoded as u64)
    /// 3. Records with extended headers (extra bytes after timestamp)
    #[test]
    fn test_decode_with_record_flags() -> googletest::Result<()> {
        let body = b"test payload";

        // Test 1: Decode record with no flags set (plain NanosSinceEpoch timestamp)
        {
            let mut buf = BytesMut::new();
            buf.put_u8(RecordFormat::CustomV1 as u8); // format
            buf.put_u8(KeyStyle::Single as u8); // key style
            buf.put_u64_le(42); // key
            buf.put_u16_le(0); // flags = 0 (no HLC, no extended header)
            let timestamp_nanos: u64 = 1_000_000_000;
            buf.put_u64_le(timestamp_nanos); // created_at as NanosSinceEpoch
            buf.put_slice(body);

            let decoder = DataRecordDecoder::new(&buf)?;
            let decoded = decoder.decode()?;
            assert_that!(decoded.keys(), eq(&Keys::Single(42)));
            assert_that!(
                decoded.created_at(),
                eq(NanosSinceEpoch::from(timestamp_nanos))
            );
        }

        // Test 2: Decode record with HlcTimestamp flag set
        {
            let mut buf = BytesMut::new();
            buf.put_u8(RecordFormat::CustomV1 as u8); // format
            buf.put_u8(KeyStyle::Single as u8); // key style
            buf.put_u64_le(42); // key
            buf.put_u16_le(RecordFlags::HlcTimestamp.bits()); // flags = HlcTimestamp
            // Use a timestamp after RESTATE_EPOCH (2022-01-01): 1_700_000_000_000 ms = Nov 2023
            let hlc_timestamp =
                UniqueTimestamp::try_from_unix_millis(MillisSinceEpoch::new(1_700_000_000_000))
                    .unwrap();
            buf.put_u64_le(hlc_timestamp.as_u64()); // created_at as HLC
            buf.put_slice(body);

            let decoder = DataRecordDecoder::new(&buf)?;
            let decoded = decoder.decode()?;
            assert_that!(decoded.keys(), eq(&Keys::Single(42)));
            // HLC timestamp gets converted to NanosSinceEpoch (millisecond precision)
            assert_that!(
                decoded.created_at(),
                eq(NanosSinceEpoch::from(hlc_timestamp))
            );
        }

        // Test 3: Decode record with ExtendedHeader flag set
        {
            let mut buf = BytesMut::new();
            buf.put_u8(RecordFormat::CustomV1 as u8); // format
            buf.put_u8(KeyStyle::None as u8); // key style
            buf.put_u16_le(RecordFlags::ExtendedHeader.bits()); // flags = ExtendedHeader
            let timestamp_nanos: u64 = 2_000_000_000;
            buf.put_u64_le(timestamp_nanos); // created_at
            buf.put_u8(5); // extra header length = 5 bytes
            buf.put_slice(&[0xDE, 0xAD, 0xBE, 0xEF, 0x00]); // extra header bytes (unknown future fields)
            buf.put_slice(body);

            let decoder = DataRecordDecoder::new(&buf)?;
            let decoded = decoder.decode()?;
            assert_that!(decoded.keys(), eq(&Keys::None));
            assert_that!(
                decoded.created_at(),
                eq(NanosSinceEpoch::from(timestamp_nanos))
            );
            // Verify the body is correctly extracted after skipping extended header
            let body_bytes = decoded
                .body()
                .encode_to_bytes(&mut BytesMut::new())
                .unwrap();
            assert_that!(body_bytes.as_ref(), eq(body.as_slice()));
        }

        // Test 4: Decode record with both HlcTimestamp and ExtendedHeader flags
        {
            let mut buf = BytesMut::new();
            buf.put_u8(RecordFormat::CustomV1 as u8); // format
            buf.put_u8(KeyStyle::Pair as u8); // key style
            buf.put_u64_le(100); // key1
            buf.put_u64_le(200); // key2
            let flags = RecordFlags::HlcTimestamp | RecordFlags::ExtendedHeader;
            buf.put_u16_le(flags.bits());
            // Use a timestamp after RESTATE_EPOCH (2022-01-01): 1_750_000_000_000 ms = Dec 2025
            let hlc_timestamp =
                UniqueTimestamp::try_from_unix_millis(MillisSinceEpoch::new(1_750_000_000_000))
                    .unwrap();
            buf.put_u64_le(hlc_timestamp.as_u64()); // created_at as HLC
            buf.put_u8(3); // extra header length = 3 bytes
            buf.put_slice(&[0x01, 0x02, 0x03]); // extra header bytes
            buf.put_slice(body);

            let decoder = DataRecordDecoder::new(&buf)?;
            let decoded = decoder.decode()?;
            assert_that!(decoded.keys(), eq(&Keys::Pair(100, 200)));
            assert_that!(
                decoded.created_at(),
                eq(NanosSinceEpoch::from(hlc_timestamp))
            );
            let body_bytes = decoded
                .body()
                .encode_to_bytes(&mut BytesMut::new())
                .unwrap();
            assert_that!(body_bytes.as_ref(), eq(body.as_slice()));
        }

        Ok(())
    }
}
