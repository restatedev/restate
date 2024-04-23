// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Write;
use std::mem::size_of;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use restate_types::logs::SequenceNumber;

use crate::loglet::LogletOffset;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordKey {
    pub log_id: u64,
    pub offset: LogletOffset,
}

impl RecordKey {
    pub fn new(log_id: u64, offset: LogletOffset) -> Self {
        Self { log_id, offset }
    }

    pub fn upper_bound(log_id: u64) -> Self {
        Self {
            log_id,
            offset: LogletOffset::MAX,
        }
    }

    pub fn to_bytes(self) -> Bytes {
        let mut buf = BytesMut::with_capacity(size_of::<Self>() + 1);
        buf.write_char('d').expect("enough key buffer");
        buf.put_u64(self.log_id);
        buf.put_u64(self.offset.into());
        buf.freeze()
    }

    pub fn from_slice(data: &[u8]) -> Self {
        let mut data = data;
        let c = data.get_u8();
        debug_assert_eq!(c, b'd');
        let log_id = data.get_u64();
        let offset = LogletOffset::from(data.get_u64());
        Self { log_id, offset }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default, strum_macros::FromRepr)]
#[repr(u8)]
pub enum MetadataKind {
    #[default]
    Unknown = 0,
    LogState = 1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetadataKey {
    pub log_id: u64,
    pub kind: MetadataKind,
}

impl MetadataKey {
    pub fn new(log_id: u64, kind: MetadataKind) -> Self {
        Self { log_id, kind }
    }

    pub fn to_bytes(self) -> Bytes {
        let mut buf = BytesMut::with_capacity(size_of::<Self>() + 1);
        // m for metadata
        buf.write_char('m').expect("enough key buffer");
        buf.put_u64(self.log_id);
        buf.put_u8(self.kind as u8);
        buf.freeze()
    }

    pub fn from_slice(data: &[u8]) -> Self {
        let mut data = Bytes::copy_from_slice(data);
        let c = data.get_u8();
        debug_assert_eq!(c, b'm');
        let log_id = data.get_u64();
        let kind = MetadataKind::from_repr(data.get_u8());
        let kind = kind.unwrap_or_default();

        Self { log_id, kind }
    }
}

#[cfg(test)]
mod tests {
    // test RecordKey
    use super::*;
    use crate::loglet::LogletOffset;

    #[test]
    fn test_record_key() {
        let key = RecordKey::new(1, LogletOffset(2));
        let bytes = key.to_bytes();
        let key2 = RecordKey::from_slice(&bytes);
        assert_eq!(key, key2);
    }

    #[test]
    fn test_metadata_key() {
        let key = MetadataKey::new(1, MetadataKind::LogState);
        assert_eq!(key.log_id, 1);
        assert_eq!(key.kind, MetadataKind::LogState);
        let bytes = key.to_bytes();
        let key2 = MetadataKey::from_slice(&bytes);
        assert_eq!(key, key2);
        assert_eq!(key2.log_id, 1);
        assert_eq!(key2.kind, MetadataKind::LogState);
    }
}
