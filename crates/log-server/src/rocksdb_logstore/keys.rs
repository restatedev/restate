// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::mem::size_of;

use bytes::{Buf, BufMut};

use restate_types::logs::{LogletId, LogletOffset};

// log-store marker
pub(super) const MARKER_KEY: &[u8] = b"storage-marker";

// makes sure that it doesn't go unnoticed if this changed by mistake.
static_assertions::const_assert_eq!(9, KeyPrefix::size());

/// Key prefixes for different types of keys in the storage layer.
/// Keep those variants sorted by their byte value to avoid confusion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, derive_more::TryFrom)]
#[try_from(repr)]
#[repr(u8)]
pub(super) enum KeyPrefixKind {
    // data column family
    DataRecord = b'd',
    // metadata column family
    Sequencer = b's',
    TrimPoint = b't',
    Seal = b'Z',
    // Do not use u8::MAX
    Invalid = 0xFF,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct KeyPrefix {
    kind: KeyPrefixKind,
    loglet_id: LogletId,
}

impl KeyPrefix {
    pub const fn new(kind: KeyPrefixKind, loglet_id: LogletId) -> Self {
        Self { kind, loglet_id }
    }

    pub fn encode_exclusive_upper_bound<B: BufMut>(&self, buf: &mut B) {
        // key is [prefix][loglet_id]
        // the upper exclusive prefix is:
        // if loglet_id == loglet_id::MAX. Then it's:
        //   [prefix + 1][0..]
        // if loglet_id < loglet_id::MAX. Then it's:
        //   [prefix][loglet_id+1]
        debug_assert_ne!(self.kind, KeyPrefixKind::Invalid);
        if *self.loglet_id == u64::MAX {
            buf.put_u8(self.kind as u8 + 1);
            buf.put_u64(0);
        } else {
            buf.put_u8(self.kind as u8);
            buf.put_u64(*self.loglet_id + 1);
        }
    }

    /// Encodes this value into its binary representation on the stack
    fn to_binary_array(self) -> [u8; Self::size()] {
        let mut buf = [0u8; Self::size()];
        self.encode(&mut &mut buf[..]);
        buf
    }

    /// Encodes this value into its binary representation and advances the underlying buffer
    fn encode<B: BufMut>(self, buf: &mut B) {
        buf.put_u8(self.kind as u8);
        // note: this is big-endian and must remain like this
        buf.put_u64(*self.loglet_id);
    }

    fn decode<B: Buf>(buf: &mut B) -> KeyPrefix {
        let kind = KeyPrefixKind::try_from(buf.get_u8()).expect("recognized key kind");
        let loglet_id = LogletId::from(buf.get_u64());
        Self { kind, loglet_id }
    }

    /// The number of bytes required for the binary representation of this value
    pub(super) const fn size() -> usize {
        size_of::<KeyPrefixKind>() + size_of::<LogletId>()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct DataRecordKey {
    prefix: KeyPrefix,
    offset: LogletOffset,
}

impl DataRecordKey {
    pub fn new(loglet_id: LogletId, offset: LogletOffset) -> Self {
        Self {
            prefix: KeyPrefix::new(KeyPrefixKind::DataRecord, loglet_id),
            offset,
        }
    }

    pub fn loglet_id(&self) -> LogletId {
        self.prefix.loglet_id
    }

    pub fn offset(&self) -> LogletOffset {
        self.offset
    }

    pub fn exclusive_upper_bound(loglet_id: LogletId) -> [u8; Self::size()] {
        let mut buf = [0u8; Self::size()];
        let mut b = &mut buf[..];
        KeyPrefix::new(KeyPrefixKind::DataRecord, loglet_id).encode_exclusive_upper_bound(&mut b);
        (&mut b).put_u64(0);
        buf
    }

    /// Encodes this value into its binary representation on the stack
    pub fn to_binary_array(self) -> [u8; Self::size()] {
        let mut buf = [0u8; Self::size()];
        self.encode(&mut &mut buf[..]);
        buf
    }

    /// Encodes this value into its binary representation and advances the underlying buffer
    pub fn encode<B: BufMut>(self, buf: &mut B) {
        self.prefix.encode(buf);
        // despite offset being 32bit, for backward compatibility and future-proofing, we keep the
        // storage layer as u64 but we assert that the actual value fits u32 on read.
        buf.put_u64(u64::from(*self.offset));
    }

    /// panics if the prefix is not a data record
    pub fn from_slice<B: Buf>(mut data: B) -> Self {
        let prefix = KeyPrefix::decode(&mut data);
        debug_assert_eq!(prefix.kind, KeyPrefixKind::DataRecord);
        let offset =
            LogletOffset::new(u32::try_from(data.get_u64()).expect("offset must fit within u32"));
        Self { prefix, offset }
    }

    /// The number of bytes required for the binary representation of this value
    pub const fn size() -> usize {
        // We store the offset as u64 rather than the actual u32
        KeyPrefix::size() + size_of::<u64>()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct MetadataKey {
    prefix: KeyPrefix,
}

impl MetadataKey {
    pub fn new(kind: KeyPrefixKind, loglet_id: LogletId) -> Self {
        // Just a sanity check
        debug_assert_ne!(kind, KeyPrefixKind::DataRecord);
        Self {
            prefix: KeyPrefix::new(kind, loglet_id),
        }
    }

    #[allow(unused)]
    pub fn loglet_id(&self) -> LogletId {
        self.prefix.loglet_id
    }

    pub fn kind(&self) -> KeyPrefixKind {
        self.prefix.kind
    }

    /// The number of bytes required for the binary representation of this value
    pub const fn size() -> usize {
        KeyPrefix::size()
    }

    /// Encodes this value into its binary representation on the stack
    pub fn to_binary_array(self) -> [u8; Self::size()] {
        self.prefix.to_binary_array()
    }

    pub fn from_slice<B: Buf>(data: &mut B) -> Self {
        let prefix = KeyPrefix::decode(data);
        debug_assert_ne!(prefix.kind, KeyPrefixKind::DataRecord);
        Self { prefix }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_record_key() {
        let key = DataRecordKey::new(1.into(), LogletOffset::new(2));
        let bytes = key.to_binary_array();
        let key2 = DataRecordKey::from_slice(&mut bytes.as_slice());
        assert_eq!(key, key2);
    }

    #[test]
    fn test_metadata_key() {
        let key = MetadataKey::new(KeyPrefixKind::Seal, 1.into());
        assert_eq!(*key.loglet_id(), 1);
        assert_eq!(key.kind(), KeyPrefixKind::Seal);
        let bytes = key.to_binary_array();
        let key2 = MetadataKey::from_slice(&mut bytes.as_slice());
        assert_eq!(key, key2);
    }

    #[test]
    fn test_upper_bound() {
        // loglet is within bounds
        let my_key = DataRecordKey::new(10.into(), 10.into());
        let upper_bound_bytes = DataRecordKey::exclusive_upper_bound(10.into());
        // byte-wise comparator
        assert!(upper_bound_bytes > my_key.to_binary_array());
        let my_key = DataRecordKey::new(u64::MAX.into(), 10.into());
        let last_legal_loglet_id = DataRecordKey::exclusive_upper_bound(u64::MAX.into());
        // byte-wise comparator
        assert!(last_legal_loglet_id > my_key.to_binary_array());
    }
}
