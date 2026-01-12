// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::Deref;

use bitflags::bitflags;
use bytes::{Buf, BufMut, Bytes, BytesMut};

use restate_clock::UniqueTimestamp;
use restate_types::flexbuffers_storage_encode_decode;
use restate_types::logs::{KeyFilter, Keys, MatchKeyQuery, Record};
use restate_types::storage::{PolyBytes, StorageCodec, StorageCodecKind, StorageDecodeError};
use restate_types::time::NanosSinceEpoch;

// use legacy for new appends until enough minor/major versions are released after current (1.0.x)
// to allow for backwards compatibility.
//
// CustomEncoding became the default in v1.6.0
pub(super) const FORMAT_FOR_NEW_APPENDS: RecordFormat = RecordFormat::CustomEncoding;

#[derive(Debug, derive_more::TryFrom, Eq, PartialEq, Ord, PartialOrd)]
#[try_from(repr)]
#[repr(u8)]
pub(super) enum RecordFormat {
    Legacy = 0x02, // matches  StorageCodecKind::FlexBufferSerde
    CustomEncoding = 0x03,
}

static_assertions::const_assert!(
    RecordFormat::Legacy as u8 == StorageCodecKind::FlexbuffersSerde as u8
);

#[derive(Debug, thiserror::Error)]
#[error("Record decode error: {0}")]
pub(super) enum RecordDecodeError {
    UnsupportedFormatVersion(u8),
    UnsupportedKeyStyle(u8),
    InvalidRecordTimestamp(#[from] restate_clock::Error),
    DecodeError(#[from] StorageDecodeError),
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

/// Deprecated. This is the header for format-version 0x02.
#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub(super) struct LegacyHeader {
    pub created_at: NanosSinceEpoch,
}

impl Default for LegacyHeader {
    fn default() -> Self {
        Self {
            created_at: NanosSinceEpoch::now(),
        }
    }
}

/// Deprecated. This is the payload for format-version 0x02.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub(super) struct LegacyPayload {
    pub header: LegacyHeader,
    pub body: Bytes,
    // default is Keys::None which means that records written prior to this field will not be
    // filtered out. Partition processors will continue to filter correctly using the extracted
    // keys from Envelope, but will not take advantage of push-down filtering.
    #[serde(default)]
    pub keys: Keys,
}

flexbuffers_storage_encode_decode!(LegacyPayload);

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, derive_more::TryFrom)]
#[try_from(repr)]
#[repr(u8)]
enum KeyStyle {
    None = 0,
    Single = 1,
    Pair = 2,
    RangeInclusive = 3,
}

pub(super) fn encode_record_and_split(
    format_version: RecordFormat,
    record: &Record,
    serde_buffer: &mut BytesMut,
) -> BytesMut {
    match format_version {
        RecordFormat::Legacy => write_legacy_payload(record, serde_buffer),
        RecordFormat::CustomEncoding => write_record(record, serde_buffer),
    }
}

pub(super) fn decode_and_filter_record(
    mut buffer: &[u8],
    filter: &KeyFilter,
) -> Result<Option<Record>, RecordDecodeError> {
    debug_assert!(buffer.len() > 1);
    // only peek the format version, don't advance the cursor.
    let record_format = peek_format(buffer[0])?;

    match record_format {
        RecordFormat::Legacy => {
            // legacy payload is decoded via flexbuffers
            let internal_payload: LegacyPayload = StorageCodec::decode(&mut buffer)?;
            let record = if internal_payload.keys.matches_key_query(filter) {
                Some(Record::from_parts(
                    internal_payload.header.created_at,
                    internal_payload.keys,
                    PolyBytes::Bytes(internal_payload.body),
                ))
            } else {
                None
            };

            Ok(record)
        }
        RecordFormat::CustomEncoding => decode_custom_encoded_record(buffer, filter),
    }
}

fn write_legacy_payload(record: &Record, serde_buffer: &mut BytesMut) -> BytesMut {
    // encoding the user payload.
    let body = match record.body() {
        PolyBytes::Bytes(raw_bytes) => raw_bytes.clone(),
        PolyBytes::Both(_, raw_bytes) => raw_bytes.clone(),
        PolyBytes::Typed(encodeable) => {
            StorageCodec::encode_and_split(encodeable.deref(), serde_buffer)
                .expect("record serde is infallible")
                .freeze()
        }
    };

    let final_payload = LegacyPayload {
        header: LegacyHeader {
            created_at: record.created_at(),
        },
        keys: record.keys().clone(),
        body,
    };
    // encoding the wrapper
    StorageCodec::encode_and_split(&final_payload, serde_buffer)
        .expect("record serde is infallible")
}

/// On-disk record layout. This format only applies on format versions higher than 0x02.
/// For the record header, byte order is little-endian.
///
/// The record layout follow this structure:
///    [1 byte]        Format version (0x02 is old flexbuffers). current is in `CURRENT_FORMAT_VERSION`
///    [1 byte]        KeyStyle (see `KeyStyle` enum)
///      * [8 bytes]   First Key (if KeyStyle is != 0)
///      * [8 bytes]   Second Key (if KeyStyle is > 1)
///    [2 bytes]       Flags (see `RecordFlags` enum)
///    [8 bytes]       `created_at` timestamp
///    [1 byte]        [If Flags::ExtendedHeader] The number of extra bytes occupied by future header fields.
///    [...]           Additional header fields
///    [remaining]     Serialized Payload
fn write_record(record: &Record, buf: &mut BytesMut) -> BytesMut {
    // Write the format version
    buf.put_u8(RecordFormat::CustomEncoding as u8);
    // key style and keys
    match record.keys() {
        Keys::None => buf.put_u8(KeyStyle::None as u8),
        Keys::Single(key) => {
            buf.put_u8(KeyStyle::Single as u8);
            buf.put_u64_le(*key);
        }
        Keys::Pair(key1, key2) => {
            buf.put_u8(KeyStyle::Pair as u8);
            buf.put_u64_le(*key1);
            buf.put_u64_le(*key2);
        }
        Keys::RangeInclusive(range) => {
            buf.put_u8(KeyStyle::RangeInclusive as u8);
            buf.put_u64_le(*range.start());
            buf.put_u64_le(*range.end());
        }
    }
    // flags
    let flags = RecordFlags::default();
    buf.put_u16_le(flags.bits());
    // created_at
    buf.put_u64_le(record.created_at().as_u64());

    if flags.contains(RecordFlags::ExtendedHeader) {
        let extra_header_bytes = buf.get_u8();
        // We skip the extra header bytes since we don't know how to decode them.
        buf.advance(extra_header_bytes as usize);
    }

    // serialize payload
    match record.body() {
        PolyBytes::Bytes(raw_bytes) => buf.put_slice(raw_bytes),
        PolyBytes::Both(_, raw_bytes) => buf.put_slice(raw_bytes),
        PolyBytes::Typed(encodeable) => {
            StorageCodec::encode(encodeable.deref(), buf).expect("record serde is infallible")
        }
    }
    buf.split()
}

fn decode_custom_encoded_record(
    mut buffer: &[u8],
    filter: &KeyFilter,
) -> Result<Option<Record>, RecordDecodeError> {
    // read format byte
    read_format(&mut buffer)?;
    // read keys
    let keys = read_keys(&mut buffer)?;
    // flags
    let flags = RecordFlags::from_bits_retain(buffer.get_u16_le());

    let timestamp = buffer.get_u64_le();
    let created_at = if flags.contains(RecordFlags::HlcTimestamp) {
        NanosSinceEpoch::from(UniqueTimestamp::try_from(timestamp)?)
    } else {
        NanosSinceEpoch::from(timestamp)
    };

    if flags.contains(RecordFlags::ExtendedHeader) {
        let extra_header_bytes = buffer.get_u8();
        // We skip the extra header bytes since we don't know how to decode them.
        buffer.advance(extra_header_bytes as usize);
    }

    if !keys.matches_key_query(filter) {
        return Ok(None);
    }

    let body = PolyBytes::Bytes(Bytes::copy_from_slice(buffer.chunk()));

    Ok(Some(Record::from_parts(created_at, keys, body)))
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
    peek_format(format)
}

fn peek_format(raw_value: u8) -> Result<RecordFormat, RecordDecodeError> {
    RecordFormat::try_from(raw_value)
        .map_err(|_| RecordDecodeError::UnsupportedFormatVersion(raw_value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use restate_types::time::MillisSinceEpoch;
    use std::sync::Arc;

    use bytes::BytesMut;
    use googletest::prelude::*;

    use restate_types::logs::Keys;

    #[test]
    fn test_record_format() {
        use super::RecordFormat;
        use std::convert::TryFrom;
        assert_eq!(RecordFormat::try_from(0x02).unwrap(), RecordFormat::Legacy);
        assert_eq!(
            RecordFormat::try_from(0x03).unwrap(),
            RecordFormat::CustomEncoding
        );
        assert!(RecordFormat::try_from(0x04).is_err());
    }

    #[test]
    fn test_codec_compatibility() -> googletest::Result<()> {
        // ensure that we can encode and decode both the old and new formats
        let record = Record::from_parts(
            NanosSinceEpoch::from(100),
            Keys::Single(14),
            PolyBytes::Typed(Arc::new("hello".to_owned())),
        );

        let mut buffer = BytesMut::new();

        // encode with the old format, make sure we can decode and check filter.
        let encoded = encode_record_and_split(RecordFormat::Legacy, &record, &mut buffer);

        // no match
        let filter = KeyFilter::Include(15);
        let decoded = decode_and_filter_record(&encoded, &filter)?;
        assert!(decoded.is_none());

        // should match
        let filter = KeyFilter::Any;
        let decoded = decode_and_filter_record(&encoded, &filter)?;

        assert!(decoded.is_some());

        let decoded_unwrapped = decoded.unwrap();
        assert_that!(decoded_unwrapped.keys(), eq(&Keys::Single(14)));
        assert_that!(
            decoded_unwrapped.created_at(),
            eq(NanosSinceEpoch::from(100))
        );
        assert_that!(decoded_unwrapped.decode::<String>().unwrap(), eq("hello"));

        // do the same but encode with new format
        // encode with the old format, make sure we can decode and check filter.
        let encoded = encode_record_and_split(RecordFormat::CustomEncoding, &record, &mut buffer);

        // no match
        let filter = KeyFilter::Include(15);
        let decoded = decode_and_filter_record(&encoded, &filter)?;
        assert!(decoded.is_none());

        // should match
        let filter = KeyFilter::Any;
        let decoded = decode_and_filter_record(&encoded, &filter)?;

        assert!(decoded.is_some());

        let decoded_unwrapped = decoded.unwrap();
        assert_that!(decoded_unwrapped.keys(), eq(&Keys::Single(14)));
        assert_that!(
            decoded_unwrapped.created_at(),
            eq(NanosSinceEpoch::from(100))
        );
        assert_that!(decoded_unwrapped.decode::<String>().unwrap(), eq("hello"));

        Ok(())
    }

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
            buf.put_u8(RecordFormat::CustomEncoding as u8); // format
            buf.put_u8(KeyStyle::Single as u8); // key style
            buf.put_u64_le(42); // key
            buf.put_u16_le(0); // flags = 0 (no HLC, no extended header)
            let timestamp_nanos: u64 = 1_000_000_000;
            buf.put_u64_le(timestamp_nanos); // created_at as NanosSinceEpoch
            buf.put_slice(body);

            let decoded = decode_and_filter_record(&buf, &KeyFilter::Any)?.unwrap();
            assert_that!(decoded.keys(), eq(&Keys::Single(42)));
            assert_that!(
                decoded.created_at(),
                eq(NanosSinceEpoch::from(timestamp_nanos))
            );
        }

        // Test 2: Decode record with HlcTimestamp flag set
        {
            let mut buf = BytesMut::new();
            buf.put_u8(RecordFormat::CustomEncoding as u8); // format
            buf.put_u8(KeyStyle::Single as u8); // key style
            buf.put_u64_le(42); // key
            buf.put_u16_le(RecordFlags::HlcTimestamp.bits()); // flags = HlcTimestamp
            // Use a timestamp after RESTATE_EPOCH (2022-01-01): 1_700_000_000_000 ms = Nov 2023
            let hlc_timestamp =
                UniqueTimestamp::try_from_unix_millis(MillisSinceEpoch::new(1_700_000_000_000))
                    .unwrap();
            buf.put_u64_le(hlc_timestamp.as_u64()); // created_at as HLC
            buf.put_slice(body);

            let decoded = decode_and_filter_record(&buf, &KeyFilter::Any)?.unwrap();
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
            buf.put_u8(RecordFormat::CustomEncoding as u8); // format
            buf.put_u8(KeyStyle::None as u8); // key style
            buf.put_u16_le(RecordFlags::ExtendedHeader.bits()); // flags = ExtendedHeader
            let timestamp_nanos: u64 = 2_000_000_000;
            buf.put_u64_le(timestamp_nanos); // created_at
            buf.put_u8(5); // extra header length = 5 bytes
            buf.put_slice(&[0xDE, 0xAD, 0xBE, 0xEF, 0x00]); // extra header bytes (unknown future fields)
            buf.put_slice(body);

            let decoded = decode_and_filter_record(&buf, &KeyFilter::Any)?.unwrap();
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
            buf.put_u8(RecordFormat::CustomEncoding as u8); // format
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

            let decoded = decode_and_filter_record(&buf, &KeyFilter::Any)?.unwrap();
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
