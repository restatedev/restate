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
use restate_types::storage::PolyBytes;
use restate_types::time::NanosSinceEpoch;

use crate::rocksdb_logstore::record_format::RecordFormat;

use super::{KeyStyle, RecordDecodeError};

/// Helpers to encode/decode the record payload as rocksdb value
pub struct DataRecordDecoder<B> {
    keys: Keys,
    buffer: B,
}

impl<B: Buf> DataRecordDecoder<B> {
    pub fn new(mut buffer: B) -> Result<Self, RecordDecodeError> {
        debug_assert!(buffer.remaining() > 1);
        // read keys
        let keys = read_keys(&mut buffer)?;
        Ok(Self { keys, buffer })
    }

    pub fn matches_key_query(&self, filter: &KeyFilter) -> bool {
        self.keys.matches_key_query(filter)
    }

    pub fn size(&self) -> usize {
        self.buffer.remaining()
    }

    pub fn decode(mut self) -> Result<Record, RecordDecodeError> {
        // unused flags
        let _flags = self.buffer.get_u16_le();

        let created_at = NanosSinceEpoch::from(self.buffer.get_u64_le());
        let remaining = self.buffer.remaining();
        let body = PolyBytes::Bytes(self.buffer.copy_to_bytes(remaining));

        Ok(Record::from_parts(created_at, self.keys, body))
    }
}

#[derive(derive_more::From)]
pub struct DataRecordEncoder<'a>(&'a Record);

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
    ///    [8 bytes]       `created_at` timestamp (NanosSinceEpoch)
    ///    [remaining]     Serialized Payload
    #[tracing::instrument(skip_all)]
    pub fn encode(self, scratch: &mut BytesMut) -> bytes::buf::Chain<Bytes, Bytes> {
        scratch.reserve(self.header_size() + size_of::<RecordFormat>());
        // Write the format version
        scratch.put_u8(RecordFormat::V1 as u8);

        let (created_at, body, keys) = (self.0.created_at(), self.0.body(), self.0.keys());

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
        // flags (unused)
        scratch.put_u16_le(0);
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
