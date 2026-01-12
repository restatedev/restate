// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod v1;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use restate_types::logs::{KeyFilter, Record};

#[derive(Debug, Default, derive_more::TryFrom, Eq, PartialEq, Ord, PartialOrd)]
#[try_from(repr)]
#[repr(u8)]
pub(super) enum RecordFormat {
    // The default is what we select for new writes
    #[default]
    V1 = 0x01,
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

enum Decoder<B> {
    V1(v1::DataRecordDecoder<B>),
}

/// Helpers to encode/decode the record payload as rocksdb value
pub(super) struct DataRecordDecoder<B> {
    inner: Decoder<B>,
}

impl<B: Buf> DataRecordDecoder<B> {
    pub fn new(mut buffer: B) -> Result<Self, RecordDecodeError> {
        let format = buffer.get_u8();
        let format = RecordFormat::try_from(format)
            .map_err(|_| RecordDecodeError::UnsupportedFormatVersion(format))?;

        let inner = match format {
            RecordFormat::V1 => Decoder::V1(v1::DataRecordDecoder::new(buffer)?),
        };

        Ok(Self { inner })
    }

    pub fn matches_key_query(&self, filter: &KeyFilter) -> bool {
        match self.inner {
            Decoder::V1(ref inner) => inner.matches_key_query(filter),
        }
    }

    pub fn size(&self) -> usize {
        match self.inner {
            Decoder::V1(ref inner) => inner.size(),
        }
    }

    pub fn decode(self) -> Result<Record, RecordDecodeError> {
        match self.inner {
            Decoder::V1(inner) => inner.decode(),
        }
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
    ///    ...             Depending on the format version
    pub fn encode_to_disk_format(
        self,
        format_version: RecordFormat,
        scratch: &mut BytesMut,
    ) -> bytes::buf::Chain<Bytes, Bytes> {
        match format_version {
            RecordFormat::V1 => v1::DataRecordEncoder::from(self.0).encode(scratch),
        }
    }
}
