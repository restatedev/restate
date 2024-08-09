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

use restate_types::logs::{Lsn, SequenceNumber};
use restate_types::storage::{StorageCodec, StorageDecodeError};
use restate_types::time::NanosSinceEpoch;
use serde::{Deserialize, Serialize};

use crate::payload::Payload;
use crate::LsnExt;

/// A single entry in the log.
#[derive(Debug, Clone, PartialEq)]
pub struct LogEntry<S: SequenceNumber = Lsn, D = Payload> {
    pub offset: S,
    pub record: MaybeRecord<S, D>,
}

impl<S: SequenceNumber, D> LogEntry<S, D> {
    pub(crate) fn new_data(offset: S, payload: D) -> Self {
        Self {
            offset,
            record: MaybeRecord::Data(payload),
        }
    }

    /// `to` is inclusive
    pub(crate) fn new_trim_gap(offset: S, to: S) -> Self {
        LogEntry {
            offset,
            record: MaybeRecord::TrimGap(TrimGap { to }),
        }
    }

    pub(crate) fn with_base_lsn(self, base_lsn: Lsn) -> LogEntry<Lsn, D> {
        let record = match self.record {
            MaybeRecord::TrimGap(TrimGap { to }) => MaybeRecord::TrimGap(TrimGap {
                to: base_lsn.offset_by(to),
            }),
            MaybeRecord::Data(payload) => MaybeRecord::Data(payload),
        };

        LogEntry {
            offset: base_lsn.offset_by(self.offset),
            record,
        }
    }
}

impl<S: SequenceNumber> LogEntry<S, Bytes> {
    pub(crate) fn decode(self) -> Result<LogEntry<S, Payload>, StorageDecodeError> {
        let record = match self.record {
            MaybeRecord::Data(mut payload) => {
                MaybeRecord::Data(StorageCodec::decode(&mut payload)?)
            }
            MaybeRecord::TrimGap(t) => MaybeRecord::TrimGap(t),
        };
        Ok(LogEntry {
            offset: self.offset,
            record,
        })
    }
}

#[derive(Debug, Clone, PartialEq, strum_macros::EnumIs, strum_macros::EnumTryAs)]
pub enum MaybeRecord<S: SequenceNumber = Lsn, D = Payload> {
    TrimGap(TrimGap<S>),
    Data(D),
}

impl<S: SequenceNumber, D> MaybeRecord<S, D> {
    pub fn payload(&self) -> Option<&D> {
        match self {
            MaybeRecord::Data(payload) => Some(payload),
            _ => None,
        }
    }
}

#[cfg(any(test, feature = "test-util"))]
impl<S: SequenceNumber> MaybeRecord<S, Payload> {
    pub fn into_payload_unchecked(self) -> Payload {
        match self {
            MaybeRecord::Data(payload) => payload,
            _ => panic!("not a data record"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TrimGap<S: SequenceNumber> {
    /// to is inclusive
    pub to: S,
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
