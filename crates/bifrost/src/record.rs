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

use restate_types::logs::LogletOffset;
use restate_types::logs::{BodyWithKeys, HasRecordKeys, Keys, Lsn, Record};
use restate_types::storage::{PolyBytes, StorageDecode, StorageDecodeError, StorageEncode};
use restate_types::time::NanosSinceEpoch;

use crate::LsnExt;

/// An entry in the log.
///
/// The entry might represent a data record or a placeholder for a trim gap if the log is trimmed
/// at this position.
#[derive(Debug)]
pub struct LogEntry<S = Lsn> {
    offset: S,
    record: MaybeRecord<S>,
}

impl LogEntry<LogletOffset> {
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
}

impl<S: Copy> LogEntry<S> {
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

#[derive(Debug, derive_more::IsVariant)]
enum MaybeRecord<S = Lsn> {
    TrimGap(TrimGap<S>),
    Data(Record),
}

#[derive(Debug, Clone, PartialEq)]
struct TrimGap<S> {
    /// to is inclusive
    pub to: S,
}

pub struct InputRecord<T> {
    created_at: NanosSinceEpoch,
    keys: Keys,
    body: Arc<dyn StorageEncode>,
    _phantom: PhantomData<T>,
}

impl<T> Clone for InputRecord<T> {
    fn clone(&self) -> Self {
        Self {
            created_at: self.created_at,
            keys: self.keys.clone(),
            body: Arc::clone(&self.body),
            _phantom: self._phantom,
        }
    }
}

// This is a zero-cost transformation. The type is erased at runtime, but the underlying
// layout is identical.
impl<T: StorageEncode> InputRecord<T> {
    pub(crate) fn into_record(self) -> Record {
        Record::from_parts(self.created_at, self.keys, PolyBytes::Typed(self.body))
    }
}

impl<T: StorageEncode> InputRecord<T> {
    pub fn from_parts(created_at: NanosSinceEpoch, keys: Keys, body: Arc<T>) -> Self {
        Self {
            created_at,
            keys,
            body,
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
            body: val,
            _phantom: PhantomData,
        }
    }
}

impl From<String> for InputRecord<String> {
    fn from(val: String) -> Self {
        InputRecord {
            created_at: NanosSinceEpoch::now(),
            keys: Keys::None,
            body: Arc::new(val),
            _phantom: PhantomData,
        }
    }
}

impl From<&str> for InputRecord<String> {
    fn from(val: &str) -> Self {
        InputRecord {
            created_at: NanosSinceEpoch::now(),
            keys: Keys::None,
            body: Arc::new(String::from(val)),
            _phantom: PhantomData,
        }
    }
}

impl<T: StorageEncode> From<BodyWithKeys<T>> for InputRecord<T> {
    fn from(val: BodyWithKeys<T>) -> Self {
        InputRecord {
            created_at: NanosSinceEpoch::now(),
            keys: val.record_keys(),
            body: Arc::new(val.into_inner()),
            _phantom: PhantomData,
        }
    }
}
