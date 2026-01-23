// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::mem::size_of;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use restate_types::logs::{LogletOffset, SequenceNumber};

pub(crate) const DATA_KEY_PREFIX_LENGTH: usize = size_of::<u8>() + size_of::<u64>();

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordKey {
    pub loglet_id: u64,
    pub offset: LogletOffset,
}

impl RecordKey {
    pub fn new(loglet_id: u64, offset: LogletOffset) -> Self {
        Self { loglet_id, offset }
    }

    pub fn upper_bound(loglet_id: u64) -> Self {
        Self {
            loglet_id,
            offset: LogletOffset::MAX,
        }
    }

    pub const fn serialized_size() -> usize {
        size_of::<Self>() + 1
    }

    pub fn encode_and_split(self, buf: &mut BytesMut) -> BytesMut {
        self.encode(buf);
        buf.split()
    }

    pub fn encode(self, buf: &mut BytesMut) {
        buf.reserve(Self::serialized_size());
        buf.put_u8(b'd');
        buf.put_u64(self.loglet_id);
        // despite offset being 32bit, for backward compatibility and future-proofing, we keep the
        // storage layer as u64 but we assert that the actual value fits u32 on read.
        buf.put_u64(self.offset.into());
    }

    pub fn from_slice(data: &[u8]) -> Self {
        let mut data = data;
        let c = data.get_u8();
        debug_assert_eq!(c, b'd');
        let loglet_id = data.get_u64();
        let offset =
            LogletOffset::new(u32::try_from(data.get_u64()).expect("offset must fit within u32"));
        Self { loglet_id, offset }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default, derive_more::TryFrom)]
#[try_from(repr)]
#[repr(u8)]
pub enum MetadataKind {
    #[default]
    Unknown = 0,
    LogState = 1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetadataKey {
    pub loglet_id: u64,
    pub kind: MetadataKind,
}

impl MetadataKey {
    pub fn new(loglet_id: u64, kind: MetadataKind) -> Self {
        Self { loglet_id, kind }
    }

    pub fn to_bytes(self) -> BytesMut {
        let mut buf = BytesMut::with_capacity(Self::serialized_size());
        self.encode(&mut buf);
        buf
    }

    pub const fn serialized_size() -> usize {
        size_of::<Self>() + 1
    }

    #[allow(unused)]
    pub fn encode_and_split(self, buf: &mut BytesMut) -> BytesMut {
        self.encode(buf);
        buf.split()
    }

    pub fn encode(self, buf: &mut BytesMut) {
        buf.reserve(Self::serialized_size());
        // m for metadata
        buf.put_u8(b'm');
        buf.put_u64(self.loglet_id);
        buf.put_u8(self.kind as u8);
    }

    pub fn from_slice(data: &[u8]) -> Self {
        let mut data = Bytes::copy_from_slice(data);
        let c = data.get_u8();
        debug_assert_eq!(c, b'm');
        let loglet_id = data.get_u64();
        let kind = MetadataKind::try_from(data.get_u8()).unwrap_or_default();

        Self { loglet_id, kind }
    }
}

#[cfg(test)]
mod tests {
    // test RecordKey
    use super::*;

    #[test]
    fn test_record_key() {
        let key = RecordKey::new(1, LogletOffset::new(2));
        let mut buf = BytesMut::new();
        let bytes = key.encode_and_split(&mut buf);
        let key2 = RecordKey::from_slice(&bytes);
        assert_eq!(key, key2);
    }

    #[test]
    fn test_metadata_key() {
        let key = MetadataKey::new(1, MetadataKind::LogState);
        assert_eq!(key.loglet_id, 1);
        assert_eq!(key.kind, MetadataKind::LogState);
        let mut buf = BytesMut::new();
        let bytes = key.encode_and_split(&mut buf);
        let key2 = MetadataKey::from_slice(&bytes);
        assert_eq!(key, key2);
        assert_eq!(key2.loglet_id, 1);
        assert_eq!(key2.kind, MetadataKind::LogState);
    }
}
