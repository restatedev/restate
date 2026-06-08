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
use restate_memory::NonZeroByteCount;
use restate_types::bilrost_storage_encode_decode;
use restate_types::vqueues::VQueueId;

use super::EntryKey;
use super::stats::WaitStats;

/// Why the invoker yielded the invocation back to the scheduler.
///
/// New reasons can be added without a version barrier — nodes that don't
/// recognize a reason will deserialize it as [`Unknown`](Self::Unknown) and
/// apply the default re-scheduling strategy (immediate re-invoke).
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    bilrost::Message,
    bilrost::Oneof,
)]
#[serde(tag = "reason")]
pub enum YieldReason {
    /// The entry is in "run" queue and needs to be placed back onto inbox because
    /// the partition leader has changed.
    #[bilrost(tag(1), message)]
    PartitionLeaderChange,
    /// The invocation exhausted its outbound memory budget.
    #[bilrost(tag(2), message)]
    ExhaustedMemoryBudget {
        #[bilrost(encoding(fixed))]
        needed_memory: NonZeroByteCount,
    },
    /// The invocation has been yielded due to an error during execution.
    #[bilrost(tag(3), message)]
    TransientError {
        /// Controls the service retry policy related retries. This is the the
        /// value that should be used to initialize the retry policy after resuming.
        #[bilrost(encoding(fixed))]
        retry_attempts: u32,
        /// For sdk-controlled retries. This defines the retry-count value that will be
        /// sent downstream to the SDK to be used for its ctx.run() retries on the next
        /// start message.
        #[bilrost(encoding(fixed))]
        retry_count_since_last_stored_command: u32,
    },
    /// The invocation has been yielded due to an error or cooperatively yielded
    /// due to capacity constraints.
    #[bilrost(tag(4), message)]
    InvokerLoadShedding,
    /// A yield reason not recognized by this node version or a yield that triggered
    /// through a path that doesn't expect yields (harmless).
    #[serde(other)]
    #[bilrost(empty)]
    Unknown,
}

impl std::fmt::Display for YieldReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            YieldReason::ExhaustedMemoryBudget { needed_memory } => {
                write!(
                    f,
                    "exhausted invocation memory budget ({needed_memory} needed)",
                )
            }
            YieldReason::TransientError { .. } => write!(f, "transient error"),
            YieldReason::InvokerLoadShedding => write!(f, "invoker load shedding"),
            YieldReason::PartitionLeaderChange => write!(f, "partition leader change"),
            YieldReason::Unknown => write!(f, "unknown"),
        }
    }
}

#[derive(Debug, Clone, bilrost::Oneof, bilrost::Message, derive_more::From)]
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

#[derive(Debug, Clone, bilrost::Message)]
pub struct RunAction {
    #[bilrost(tag(1))]
    pub key: EntryKey,
    #[bilrost(tag(2))]
    pub wait_stats: WaitStats,
}

#[derive(Debug, Clone, bilrost::Message)]
pub struct YieldAction {
    #[bilrost(tag(1))]
    pub key: EntryKey,
    #[bilrost(tag(2))]
    pub next_run_at: Option<RoughTimestamp>,
    #[bilrost(tag(3))]
    pub reason: YieldReason,
}

#[derive(Debug, Clone, bilrost::Message)]
pub struct SchedulerDecisionsCommand {
    #[bilrost(tag(1))]
    pub qids: Vec<(VQueueId, Vec<SchedulerAction>)>,
}

bilrost_storage_encode_decode!(SchedulerDecisionsCommand);

impl SchedulerDecisionsCommand {
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
