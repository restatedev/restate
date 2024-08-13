// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use core::str;
use std::marker::PhantomData;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use restate_types::logs::{KeyFilter, MatchKeyQuery};

use restate_types::logs::{BodyWithKeys, HasRecordKeys, Keys, Lsn, SequenceNumber};
use restate_types::storage::{
    PolyBytes, StorageCodec, StorageDecode, StorageDecodeError, StorageEncode,
};
use restate_types::time::NanosSinceEpoch;

use crate::LsnExt;

/// An entry in the log.
///
/// The entry might represent a data record or a placeholder for a trim gap if the log is trimmed
/// at this position.
#[derive(Debug)]
pub struct LogEntry<S: SequenceNumber = Lsn> {
    offset: S,
    record: MaybeRecord<S>,
}

impl<S: SequenceNumber> LogEntry<S> {
    pub(crate) fn new_data(offset: S, record: Record) -> Self {
        Self {
            offset,
            record: MaybeRecord::Data(record),
        }
    }

    /// `to` is inclusive
    pub(crate) fn new_trim_gap(offset: S, to: S) -> Self {
        LogEntry {
            offset,
            record: MaybeRecord::TrimGap(TrimGap { to }),
        }
    }

    pub(crate) fn with_base_lsn(self, base_lsn: Lsn) -> LogEntry<Lsn> {
        let record = match self.record {
            MaybeRecord::TrimGap(TrimGap { to }) => MaybeRecord::TrimGap(TrimGap {
                to: base_lsn.offset_by(to),
            }),
            MaybeRecord::Data(record) => MaybeRecord::Data(record),
        };

        LogEntry {
            offset: base_lsn.offset_by(self.offset),
            record,
        }
    }

    pub fn is_trim_gap(&self) -> bool {
        self.record.is_trim_gap()
    }

    pub fn is_data_record(&self) -> bool {
        self.record.is_data()
    }

    #[inline]
    pub fn sequence_number(&self) -> S {
        self.offset
    }

    #[inline]
    pub fn trim_gap_to_sequence_number(&self) -> Option<S> {
        match &self.record {
            MaybeRecord::TrimGap(gap) => Some(gap.to),
            _ => None,
        }
    }

    pub fn into_record(self) -> Option<Record> {
        match self.record {
            MaybeRecord::Data(record) => Some(record),
            _ => None,
        }
    }

    pub fn as_record(&self) -> Option<&Record> {
        match &self.record {
            MaybeRecord::Data(record) => Some(record),
            _ => None,
        }
    }

    pub fn try_decode_arc<T: StorageDecode + StorageEncode>(
        self,
    ) -> Option<Result<Arc<T>, StorageDecodeError>> {
        self.into_record().map(|record| record.decode_arc())
    }

    pub fn try_decode<T: StorageDecode + StorageEncode + Clone>(
        self,
    ) -> Option<Result<T, StorageDecodeError>> {
        self.into_record().map(|record| record.decode())
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn decode_unchecked<T: StorageDecode + StorageEncode + Clone>(self) -> T {
        self.into_record().unwrap().decode().unwrap()
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct Header {
    pub created_at: NanosSinceEpoch,
}

impl Default for Header {
    fn default() -> Self {
        Self {
            created_at: NanosSinceEpoch::now(),
        }
    }
}

#[derive(Debug)]
pub struct Record {
    header: Header,
    body: PolyBytes,
    keys: Keys,
}

impl Record {
    pub(crate) fn from_parts(header: Header, keys: Keys, body: PolyBytes) -> Self {
        Self { header, keys, body }
    }

    pub fn created_at(&self) -> NanosSinceEpoch {
        self.header.created_at
    }

    pub fn keys(&self) -> &Keys {
        &self.keys
    }

    /// Decode the record body into an owned value T.
    ///
    /// Internally, this will clone the inner value if it's already in record cache, or will move
    /// the value from the underlying Arc delivered from the loglet. Use this approach if you need
    /// to mutate the value in-place and the cost of cloning sections is high. It's generally
    /// recommended to use `decode_arc` whenever possible for large payloads.
    pub fn decode<T: StorageDecode + StorageEncode + Clone>(self) -> Result<T, StorageDecodeError> {
        let decoded = match self.body {
            PolyBytes::Bytes(slice) => {
                let mut buf = std::io::Cursor::new(slice);
                StorageCodec::decode(&mut buf)?
            }
            PolyBytes::Typed(value) => {
                let target_arc: Arc<T> = value.downcast_arc().map_err(|_| {
                StorageDecodeError::DecodeValue(
                    anyhow::anyhow!(
                        "Type mismatch. Original value in PolyBytes::Typed does not match requested type"
                    )
                    .into(),
                )})?;
                // Attempts to move the inner value (T) if this Arc has exactly one strong
                // reference. Otherwise, it clones the inner value.
                match Arc::try_unwrap(target_arc) {
                    Ok(value) => value,
                    Err(value) => value.as_ref().clone(),
                }
            }
        };
        Ok(decoded)
    }

    /// Decode the record body into an Arc<T>. This is the most efficient way to access the entry
    /// if you need read-only access or if it's acceptable to selectively clone inner sections. If
    /// the record is in record cache, this will avoid cloning or deserialization of the value.
    pub fn decode_arc<T: StorageDecode + StorageEncode>(
        self,
    ) -> Result<Arc<T>, StorageDecodeError> {
        let decoded = match self.body {
            PolyBytes::Bytes(slice) => {
                let mut buf = std::io::Cursor::new(slice);
                Arc::new(StorageCodec::decode(&mut buf)?)
            }
            PolyBytes::Typed(value) => {
                value.downcast_arc().map_err(|_| {
                StorageDecodeError::DecodeValue(
                    anyhow::anyhow!(
                        "Type mismatch. Original value in PolyBytes::Typed does not match requested type"
                    )
                    .into(),
                )})?
            },
        };
        Ok(decoded)
    }
}

impl MatchKeyQuery for Record {
    fn matches_key_query(&self, query: &KeyFilter) -> bool {
        self.keys.matches_key_query(query)
    }
}

#[derive(Debug, derive_more::IsVariant)]
enum MaybeRecord<S: SequenceNumber = Lsn> {
    TrimGap(TrimGap<S>),
    Data(Record),
}

#[derive(Debug, Clone, PartialEq)]
struct TrimGap<S: SequenceNumber> {
    /// to is inclusive
    pub to: S,
}

/// Type-erased input record for bifrost.
///
/// Used by loglet implementations.
#[derive(Clone, derive_more::Debug)]
pub struct ErasedInputRecord {
    pub(crate) header: Header,
    pub(crate) keys: Keys,
    #[debug(skip)]
    pub(crate) body: Arc<dyn StorageEncode>,
}

impl From<String> for ErasedInputRecord {
    fn from(value: String) -> Self {
        ErasedInputRecord {
            header: Header::default(),
            keys: Keys::None,
            body: Arc::new(value),
        }
    }
}

impl From<&str> for ErasedInputRecord {
    fn from(value: &str) -> Self {
        ErasedInputRecord {
            header: Header::default(),
            keys: Keys::None,
            body: Arc::new(value.to_owned()),
        }
    }
}

pub struct InputRecord<T> {
    header: Header,
    keys: Keys,
    body: Arc<dyn StorageEncode>,
    _phantom: PhantomData<T>,
}

impl<T> Clone for InputRecord<T> {
    fn clone(&self) -> Self {
        Self {
            header: self.header.clone(),
            keys: self.keys.clone(),
            body: Arc::clone(&self.body),
            _phantom: self._phantom,
        }
    }
}

// This is a zero-cost transformation. The type is erased at runtime, but the underlying
// layout is identical.
impl<T: StorageEncode> InputRecord<T> {
    pub(crate) fn into_erased(self) -> ErasedInputRecord {
        ErasedInputRecord {
            header: self.header,
            keys: self.keys,
            body: self.body,
        }
    }
}

impl<T: StorageEncode> InputRecord<T> {
    pub fn from_parts(header: Header, keys: Keys, body: Arc<T>) -> Self {
        Self {
            header,
            keys,
            body,
            _phantom: PhantomData,
        }
    }

    pub fn created_at(&self) -> NanosSinceEpoch {
        self.header.created_at
    }
}

impl<T: StorageEncode + HasRecordKeys> From<Arc<T>> for InputRecord<T> {
    fn from(val: Arc<T>) -> Self {
        InputRecord {
            header: Header::default(),
            keys: val.record_keys(),
            body: val,
            _phantom: PhantomData,
        }
    }
}

impl From<String> for InputRecord<String> {
    fn from(val: String) -> Self {
        InputRecord {
            header: Header::default(),
            keys: Keys::None,
            body: Arc::new(val),
            _phantom: PhantomData,
        }
    }
}

impl From<&str> for InputRecord<String> {
    fn from(val: &str) -> Self {
        InputRecord {
            header: Header::default(),
            keys: Keys::None,
            body: Arc::new(String::from(val)),
            _phantom: PhantomData,
        }
    }
}

impl<T: StorageEncode> From<BodyWithKeys<T>> for InputRecord<T> {
    fn from(val: BodyWithKeys<T>) -> Self {
        InputRecord {
            header: Header::default(),
            keys: val.record_keys(),
            body: Arc::new(val.into_inner()),
            _phantom: PhantomData,
        }
    }
}
