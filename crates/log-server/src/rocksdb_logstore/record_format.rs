// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use restate_types::logs::{KeyFilter, Keys, MatchKeyQuery, Record};
use restate_types::storage::{PolyBytes, StorageEncode};
use restate_types::time::NanosSinceEpoch;

#[derive(Debug, derive_more::TryFrom, Eq, PartialEq, Ord, PartialOrd)]
#[try_from(repr)]
#[repr(u8)]
pub(super) enum RecordFormat {
    CustomV1 = 0x01,
}

#[derive(Debug, thiserror::Error)]
#[error("Record decode error: {0}")]
pub enum RecordDecodeError {
    UnsupportedFormatVersion(u8),
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
        // unused flags
        let _flags = read_flags(&mut self.buffer);

        let created_at = NanosSinceEpoch::from(read_created_at(&mut self.buffer));
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
    ///    [2 bytes]       Flags (reserved for future use)
    ///    [8 bytes]       `created_at` timestamp
    ///    [remaining]     Serialized Payload
    pub fn encode_to_disk_format(self, buf: &mut BytesMut) -> BytesMut {
        let (created_at, body, keys) = (self.0.created_at(), self.0.body(), self.0.keys());

        // Write the format version
        buf.put_u8(RecordFormat::CustomV1 as u8);
        // key style and keys
        match keys {
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
        // flags (unused)
        buf.put_u16_le(0);
        // created_at
        buf.put_u64_le(created_at.as_u64());
        // body
        body.encode(buf).expect("Encoding is infallible");

        buf.split()
    }

    pub fn estimated_encode_size(&self) -> usize {
        let (body, keys) = (self.0.body(), self.0.keys());

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
        size_of::<u64>() + body.estimated_encode_size()
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

fn read_flags<B: Buf>(buf: &mut B) -> u16 {
    buf.get_u16_le()
}

fn read_created_at<B: Buf>(buf: &mut B) -> u64 {
    buf.get_u64_le()
}
