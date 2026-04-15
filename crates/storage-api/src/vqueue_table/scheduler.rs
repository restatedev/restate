// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::{Buf, BufMut, Bytes};

use restate_clock::RoughTimestamp;
use restate_types::vqueues::VQueueId;

use super::EntryKey;
use super::stats::WaitStats;

#[derive(Debug)]
pub enum YieldReason {
    /// The entry is in "run" queue and needs to be placed back onto inbox
    PartitionLeaderChanged,
    // Only used if we'd like to update the next run time
    // Entry is in "inbox" and needs to be re-placed back onto inbox but at different
    // run_at time.
    RetryLater {
        run_at: RoughTimestamp,
    },
}

#[derive(Debug, bilrost::Oneof, bilrost::Message, derive_more::From)]
pub enum SchedulerAction {
    #[bilrost(empty)]
    Unknown,
    /// Items are in inbox, move them to the run queue.
    #[bilrost(tag(1))]
    Run(RunAction),
    /// Items moving from run queue (or in inbox) back to inbox.
    #[bilrost(tag(2))]
    Yield(YieldAction),
}

#[derive(Debug, bilrost::Message)]
pub struct RunAction {
    #[bilrost(tag(1))]
    pub key: EntryKey,
    #[bilrost(tag(2))]
    pub wait_stats: WaitStats,
}

#[derive(Debug, bilrost::Message)]
pub struct YieldAction {
    #[bilrost(tag(1))]
    pub key: EntryKey,
    #[bilrost(tag(2))]
    pub next_run_at: Option<RoughTimestamp>,
}

#[derive(Debug, bilrost::Message)]
pub struct SchedulerDecisions {
    #[bilrost(tag(1))]
    pub qids: Vec<(VQueueId, Vec<SchedulerAction>)>,
}

impl SchedulerDecisions {
    pub fn bilrost_encode<B: BufMut>(&self, b: &mut B) -> Result<(), bilrost::EncodeError> {
        bilrost::Message::encode(self, b)
    }

    pub fn encoded_len(&self) -> usize {
        bilrost::Message::encoded_len(self)
    }

    pub fn bilrost_encode_to_bytes(&self) -> Bytes {
        bilrost::Message::encode_to_bytes(self)
    }

    pub fn bilrost_decode<B: Buf>(buf: B) -> Result<Self, bilrost::DecodeError> {
        bilrost::OwnedMessage::decode(buf)
    }
}
