// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::Bytes;
use restate_types::logs::{Lsn, Payload, SequenceNumber};
use restate_types::storage::{StorageCodec, StorageDecodeError};

use crate::LsnExt;

/// A single entry in the log.
#[derive(Debug, Clone, PartialEq)]
pub struct LogRecord<S: SequenceNumber = Lsn, D = Payload> {
    pub offset: S,
    pub record: Record<S, D>,
}

impl<S: SequenceNumber, D> LogRecord<S, D> {
    pub(crate) fn new_data(offset: S, payload: D) -> Self {
        Self {
            offset,
            record: Record::Data(payload),
        }
    }

    /// `to` is inclusive
    pub(crate) fn new_trim_gap(offset: S, to: S) -> Self {
        LogRecord {
            offset,
            record: Record::TrimGap(TrimGap { to }),
        }
    }

    pub(crate) fn with_base_lsn(self, base_lsn: Lsn) -> LogRecord<Lsn, D> {
        let record = match self.record {
            Record::TrimGap(TrimGap { to }) => Record::TrimGap(TrimGap {
                to: base_lsn.offset_by(to),
            }),
            Record::Data(payload) => Record::Data(payload),
        };

        LogRecord {
            offset: base_lsn.offset_by(self.offset),
            record,
        }
    }
}

impl<S: SequenceNumber> LogRecord<S, Bytes> {
    pub(crate) fn decode(self) -> Result<LogRecord<S, Payload>, StorageDecodeError> {
        let record = match self.record {
            Record::Data(mut payload) => Record::Data(StorageCodec::decode(&mut payload)?),
            Record::TrimGap(t) => Record::TrimGap(t),
        };
        Ok(LogRecord {
            offset: self.offset,
            record,
        })
    }
}

#[derive(Debug, Clone, PartialEq, strum_macros::EnumIs, strum_macros::EnumTryAs)]
pub enum Record<S: SequenceNumber = Lsn, D = Payload> {
    TrimGap(TrimGap<S>),
    Data(D),
}

impl<S: SequenceNumber, D> Record<S, D> {
    pub fn payload(&self) -> Option<&D> {
        match self {
            Record::Data(payload) => Some(payload),
            _ => None,
        }
    }
}

#[cfg(any(test, feature = "test-util"))]
impl<S: SequenceNumber> Record<S, Payload> {
    pub fn into_payload_unchecked(self) -> Payload {
        match self {
            Record::Data(payload) => payload,
            _ => panic!("not a data record"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TrimGap<S: SequenceNumber> {
    /// to is inclusive
    pub to: S,
}
