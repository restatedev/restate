// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::marker::PhantomData;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use restate_types::logs::WithKeys;

use restate_types::logs::{BodyWithKeys, HasRecordKeys, Keys, Lsn, SequenceNumber};
use restate_types::storage::{StorageCodec, StorageDecode, StorageDecodeError, StorageEncode};
use restate_types::time::NanosSinceEpoch;

use crate::LsnExt;

pub trait AppendableRecord: HasRecordKeys + StorageEncode + std::fmt::Debug + 'static {}
impl<T> AppendableRecord for T where T: HasRecordKeys + StorageEncode + std::fmt::Debug + 'static {}

/// A single entry in the log.
#[derive(Debug, PartialEq)]
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

    pub fn to_record(self) -> Option<Record> {
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

    pub fn try_decode<T: StorageDecode>(self) -> Option<Result<T, StorageDecodeError>> {
        self.to_record().map(|record| record.decode())
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn decode_unchecked<T: StorageDecode>(self) -> T {
        self.to_record().unwrap().decode().unwrap()
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

#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct Record {
    header: Header,
    body: bytes::Bytes,
}

impl Record {
    pub(crate) fn from_parts(header: Header, body: impl Into<bytes::Bytes>) -> Self {
        Self {
            header,
            body: body.into(),
        }
    }

    pub fn created_at(&self) -> NanosSinceEpoch {
        self.header.created_at
    }

    pub fn decode<T: StorageDecode>(mut self) -> Result<T, StorageDecodeError> {
        StorageCodec::decode(&mut self.body)
    }
}

#[derive(Debug, Clone, PartialEq, strum_macros::EnumIs, strum_macros::EnumTryAs)]
enum MaybeRecord<S: SequenceNumber = Lsn> {
    TrimGap(TrimGap<S>),
    Data(Record),
}

#[derive(Debug, Clone, PartialEq)]
struct TrimGap<S: SequenceNumber> {
    /// to is inclusive
    pub to: S,
}

/// Type-erased input record for bifrost. It holds the header and a payload value
/// with its key extractor.
#[derive(Clone, Debug)]
pub struct ErasedInputRecord {
    pub(crate) header: Header,
    pub(crate) body: Arc<dyn AppendableRecord>,
}

impl From<String> for ErasedInputRecord {
    fn from(value: String) -> Self {
        ErasedInputRecord {
            header: Header::default(),
            body: Arc::new(value.with_no_keys()),
        }
    }
}

impl From<&str> for ErasedInputRecord {
    fn from(value: &str) -> Self {
        ErasedInputRecord {
            header: Header::default(),
            body: Arc::new(String::from(value).with_no_keys()),
        }
    }
}

pub struct InputRecord<T> {
    header: Header,
    body: Arc<dyn AppendableRecord>,
    _phantom: PhantomData<T>,
}

impl<T> Clone for InputRecord<T> {
    fn clone(&self) -> Self {
        Self {
            header: self.header.clone(),
            body: Arc::clone(&self.body),
            _phantom: self._phantom,
        }
    }
}

impl<T> InputRecord<T> {
    pub(crate) fn into_erased(self) -> ErasedInputRecord {
        ErasedInputRecord {
            header: self.header,
            body: self.body,
        }
    }
}

impl<T: AppendableRecord> InputRecord<T> {
    pub fn from_parts(header: Header, body: Arc<T>) -> Self {
        Self {
            header,
            body,
            _phantom: PhantomData,
        }
    }

    pub fn created_at(&self) -> NanosSinceEpoch {
        self.header.created_at
    }
}

impl<T: AppendableRecord> From<Arc<T>> for InputRecord<T> {
    fn from(val: Arc<T>) -> Self {
        InputRecord {
            header: Header::default(),
            body: val,
            _phantom: PhantomData,
        }
    }
}

impl From<String> for InputRecord<String> {
    fn from(val: String) -> Self {
        InputRecord {
            header: Header::default(),
            body: Arc::new(val.with_no_keys()),
            _phantom: PhantomData,
        }
    }
}

impl From<&str> for InputRecord<String> {
    fn from(val: &str) -> Self {
        InputRecord {
            header: Header::default(),
            body: Arc::new(String::from(val).with_no_keys()),
            _phantom: PhantomData,
        }
    }
}

impl<T> From<BodyWithKeys<T>> for InputRecord<T>
where
    T: StorageEncode + std::fmt::Debug + Send + Sync + 'static,
{
    fn from(val: BodyWithKeys<T>) -> Self {
        InputRecord {
            header: Header::default(),
            body: Arc::new(val),
            _phantom: PhantomData,
        }
    }
}

impl<T> HasRecordKeys for InputRecord<T>
where
    T: HasRecordKeys,
{
    fn record_keys(&self) -> Keys {
        self.body.record_keys()
    }
}
