// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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

use bytes::Bytes;
use restate_types::logs::{BodyWithKeys, HasRecordKeys, Keys, Lsn, Record};
use restate_types::logs::{LogletOffset, SequenceNumber};
use restate_types::storage::{PolyBytes, StorageDecode, StorageDecodeError, StorageEncode};
use restate_types::time::NanosSinceEpoch;

use crate::LsnExt;

#[derive(Debug, Clone, Copy, Eq, PartialEq, derive_more::Display)]
pub enum RecordKind {
    Data,
    TrimGap,
    DataLoss,
    Filtered,
}

/// An entry in the log.
///
/// The entry might represent a data record or a placeholder for a trim gap if the log is trimmed
/// at this position.
#[derive(Debug, Clone)]
pub struct LogEntry<S = Lsn> {
    offset: S,
    record: MaybeRecord<S>,
}

impl LogEntry<LogletOffset> {
    pub(crate) fn with_base_lsn(self, base_lsn: Lsn) -> LogEntry<Lsn> {
        let record = match self.record {
            MaybeRecord::TrimGap(Gap { to }) => MaybeRecord::TrimGap(Gap {
                to: base_lsn.offset_by(to),
            }),
            MaybeRecord::DataLoss(Gap { to }) => MaybeRecord::DataLoss(Gap {
                to: base_lsn.offset_by(to),
            }),
            MaybeRecord::Filtered(Gap { to }) => MaybeRecord::Filtered(Gap {
                to: base_lsn.offset_by(to),
            }),
            MaybeRecord::Data(record) => MaybeRecord::Data(record),
        };

        LogEntry {
            offset: base_lsn.offset_by(self.offset),
            record,
        }
    }
}

impl<S: Copy> LogEntry<S> {
    // Only used if any of the feature-gated providers is activated
    #[allow(dead_code)]
    pub(crate) fn new_data(offset: S, record: Record) -> Self {
        Self {
            offset,
            record: MaybeRecord::Data(record),
        }
    }

    pub fn dissolve(self) -> (S, MaybeRecord<S>) {
        (self.offset, self.record)
    }

    /// `to` is inclusive
    pub(crate) fn new_trim_gap(offset: S, to: S) -> Self {
        LogEntry {
            offset,
            record: MaybeRecord::TrimGap(Gap { to }),
        }
    }

    /// `to` is inclusive
    #[allow(dead_code)]
    pub(crate) fn new_data_loss_gap(offset: S, to: S) -> Self {
        LogEntry {
            offset,
            record: MaybeRecord::DataLoss(Gap { to }),
        }
    }

    /// `to` is inclusive
    // Only used if the feature-gated replicated loglet provider is activated
    #[allow(dead_code)]
    pub(crate) fn new_filtered_gap(offset: S, to: S) -> Self {
        LogEntry {
            offset,
            record: MaybeRecord::Filtered(Gap { to }),
        }
    }

    pub fn kind(&self) -> RecordKind {
        match &self.record {
            MaybeRecord::Data(_) => RecordKind::Data,
            MaybeRecord::TrimGap(_) => RecordKind::TrimGap,
            MaybeRecord::DataLoss(_) => RecordKind::DataLoss,
            MaybeRecord::Filtered(_) => RecordKind::Filtered,
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
    pub fn next_sequence_number(&self) -> S
    where
        S: SequenceNumber,
    {
        match &self.record {
            MaybeRecord::TrimGap(gap) => gap.to.next(),
            MaybeRecord::DataLoss(gap) => gap.to.next(),
            MaybeRecord::Filtered(gap) => gap.to.next(),
            MaybeRecord::Data(_) => self.offset.next(),
        }
    }

    #[inline]
    pub fn trim_gap_to_sequence_number(&self) -> Option<S> {
        match &self.record {
            MaybeRecord::TrimGap(gap) => Some(gap.to),
            _ => None,
        }
    }

    /// The last sequence number covered by this record/gap (inclusive)
    pub fn to_sequence_number(&self) -> S {
        match &self.record {
            MaybeRecord::TrimGap(gap) => gap.to,
            MaybeRecord::DataLoss(gap) => gap.to,
            MaybeRecord::Filtered(gap) => gap.to,
            MaybeRecord::Data(_) => self.offset,
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

#[derive(Debug, Clone, derive_more::IsVariant)]
pub enum MaybeRecord<S = Lsn> {
    TrimGap(Gap<S>),
    Filtered(Gap<S>),
    DataLoss(Gap<S>),
    // Only used if any of the feature-gated providers is activated
    Data(Record),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Gap<S> {
    /// to is inclusive
    pub to: S,
}

#[derive(Clone)]
pub struct InputRecord<T> {
    created_at: NanosSinceEpoch,
    keys: Keys,
    body: PolyBytes,
    _phantom: PhantomData<T>,
}

// This is a zero-cost transformation. The type is erased at runtime, but the underlying
// layout is identical.
impl<T: StorageEncode> InputRecord<T> {
    pub fn into_record(self) -> Record {
        Record::from_parts(self.created_at, self.keys, self.body)
    }
}

impl<T: StorageEncode> InputRecord<T> {
    pub fn from_parts(created_at: NanosSinceEpoch, keys: Keys, body: Arc<T>) -> Self {
        Self {
            created_at,
            keys,
            body: PolyBytes::Typed(body),
            _phantom: PhantomData,
        }
    }

    /// Builds an [`InputRecord<T>`] directly from raw bytes without validating the payload.
    ///
    /// # Safety
    /// Caller must guarantee the bytes are a correctly storage-encoded `T`.
    pub unsafe fn from_bytes_unchecked(
        created_at: NanosSinceEpoch,
        keys: Keys,
        body: Bytes,
    ) -> Self {
        Self {
            created_at,
            keys,
            body: PolyBytes::Bytes(body),
            _phantom: PhantomData,
        }
    }

    pub fn created_at(&self) -> NanosSinceEpoch {
        self.created_at
    }
}

impl<T: StorageEncode + HasRecordKeys> From<Arc<T>> for InputRecord<T> {
    fn from(val: Arc<T>) -> Self {
        InputRecord {
            created_at: NanosSinceEpoch::now(),
            keys: val.record_keys(),
            body: PolyBytes::Typed(val),
            _phantom: PhantomData,
        }
    }
}

impl From<String> for InputRecord<String> {
    fn from(val: String) -> Self {
        InputRecord {
            created_at: NanosSinceEpoch::now(),
            keys: Keys::None,
            body: PolyBytes::Typed(Arc::new(val)),
            _phantom: PhantomData,
        }
    }
}

impl From<&str> for InputRecord<String> {
    fn from(val: &str) -> Self {
        InputRecord {
            created_at: NanosSinceEpoch::now(),
            keys: Keys::None,
            body: PolyBytes::Typed(Arc::new(String::from(val))),
            _phantom: PhantomData,
        }
    }
}

impl<T: StorageEncode> From<BodyWithKeys<T>> for InputRecord<T> {
    fn from(val: BodyWithKeys<T>) -> Self {
        InputRecord {
            created_at: NanosSinceEpoch::now(),
            keys: val.record_keys(),
            body: PolyBytes::Typed(Arc::new(val.into_inner())),
            _phantom: PhantomData,
        }
    }
}
