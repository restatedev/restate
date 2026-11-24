// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! What the invoker and the partition processor exchange for invocations on the explicit
//! recovery protocol family. Kept apart from the durable execution [`super::Effect`].

use std::future::Future;
use std::num::NonZeroU32;

use bytes::Bytes;
use bytestring::ByteString;

use restate_clock::RoughTimestamp;
use restate_storage_api::invocation_status_table::ExplicitRecoveryStatus;
use restate_types::errors::InvocationError;
use restate_types::identifiers::{DeploymentId, EntryIndex, InvocationId, ServiceId};
use restate_types::invocation::{
    FencingToken, Header, InvocationQuery, ResponseResult, ServiceInvocationSpanContext,
};
use restate_types::journal_events::raw::RawEvent;
use restate_types::journal_v2::{CallRequest, SignalResult};
use restate_types::schema::invocation_target::StatePreloadPolicy;
use restate_types::time::MillisSinceEpoch;

use super::invocation_reader::{InvocationReader, InvocationReaderError};

/// An effect of one attempt of an explicit recovery invocation, proposed to the log.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ErEffect {
    pub invocation_id: InvocationId,
    /// The partition processor drops an effect whose attempt is not the status's.
    pub attempt: u32,
    /// Per attempt, from 1. Acked back with [`ErToInvoker::Applied`] once applied.
    pub seq: u64,
    pub kind: ErEffectKind,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ErEffectKind {
    /// The attempt picked its deployment and is about to open the stream.
    AttemptStarted {
        deployment_id: DeploymentId,
        protocol_version: u16,
    },
    /// An action to apply. `highest_result_id` is the highest result id it carries.
    Action {
        action: ErAction,
        highest_result_id: Option<NonZeroU32>,
    },
    // -- Terminal
    /// The handler returned: complete the invocation with this output.
    End(ResponseResult),
    /// The attempt failed.
    Failed {
        error_event: Option<RawEvent>,
        outcome: ErFailureOutcome,
    },
    /// The attempt was shut down and the invocation must start a new attempt.
    Restart {
        rule: RestartRule,
        /// What a failure would do, applied when the restart is idle once too often (D63).
        idle_fallback: ErFailureOutcome,
    },
    /// The attempt was shut down while waiting: suspend, unless a notification is missing (D34).
    Suspend {
        received_notifications: u32,
        idle_fallback: ErFailureOutcome,
    },
}

impl ErEffectKind {
    /// After a terminal effect the attempt proposes nothing more.
    pub fn is_terminal(&self) -> bool {
        match self {
            ErEffectKind::AttemptStarted { .. } | ErEffectKind::Action { .. } => false,
            ErEffectKind::End(_)
            | ErEffectKind::Failed { .. }
            | ErEffectKind::Restart { .. }
            | ErEffectKind::Suspend { .. } => true,
        }
    }
}

/// What to do with an invocation whose attempt failed.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ErFailureOutcome {
    /// Start a new attempt, not before `resume_at`, with this many consecutive failures.
    Retry {
        resume_at: Option<RoughTimestamp>,
        retry_attempts: u32,
    },
    Pause,
    Fail(InvocationError),
}

/// The rule of the restart-or-suspend decision that asked for a restart (10 §10.5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum RestartRule {
    /// 1: not silent for the inactivity timeout, or an unasked `ShutdownMessage`.
    OwnReason = 1,
    /// 2: no last message after the shutdown.
    NoAnswer = 2,
    /// 3: fewer notifications processed than the attempt had.
    Unprocessed = 3,
    /// 4: a message crossed the shutdown.
    Crossed = 4,
    /// 5: no external result outstanding.
    NothingOutstanding = 5,
}

impl RestartRule {
    /// Restarts of rules 2 to 5 of an attempt with no action applied are idle (D63).
    pub fn may_be_idle(self) -> bool {
        self != RestartRule::OwnReason
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ErStateScope {
    Object,
    Invocation,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ErAction {
    SetState {
        scope: ErStateScope,
        key: Bytes,
        value: Bytes,
    },
    ClearState {
        scope: ErStateScope,
        key: Bytes,
    },
    ClearAllState {
        scope: ErStateScope,
    },
    /// A call, or a send when `result_id` is `None`. When `uninvokable` is set the target cannot
    /// be invoked: nothing is enqueued, and the attempt answers the call with that failure.
    Call {
        request: Box<CallRequest>,
        execution_time: Option<MillisSinceEpoch>,
        result_id: Option<NonZeroU32>,
        uninvokable: Option<InvocationError>,
    },
    Attach {
        query: InvocationQuery,
        result_id: NonZeroU32,
    },
    CancelInvocation {
        target: InvocationId,
    },
    SendSignal {
        target: InvocationId,
        name: ByteString,
        result: SignalResult,
    },
    /// Applied all or none, in order.
    Group(Vec<ErAction>),
}

/// An [`ErEffect`] with the fencing token of the invoker task that produced it. The token never
/// reaches the log.
#[derive(Debug)]
pub struct FencedErEffect {
    pub fencing_token: FencingToken,
    pub effect: Box<ErEffect>,
}

/// What the partition processor sends a running explicit recovery attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErToInvoker {
    /// Every effect of the attempt up to `seq` is applied.
    Applied { attempt: u32, seq: u64 },
    /// The result of an action of an attempt: a callee's answer, or an attach's.
    Result {
        attempt: u32,
        result_id: u32,
        result: ResponseResult,
    },
    /// The invocation received a signal by name.
    Signal {
        attempt: u32,
        name: ByteString,
        index: u32,
        result: SignalResult,
    },
    /// The invocation received the cancel signal.
    Cancel { attempt: u32 },
}

impl ErToInvoker {
    pub fn attempt(&self) -> u32 {
        match self {
            ErToInvoker::Applied { attempt, .. }
            | ErToInvoker::Result { attempt, .. }
            | ErToInvoker::Signal { attempt, .. }
            | ErToInvoker::Cancel { attempt } => *attempt,
        }
    }
}

/// Preloaded state of one scope: the entries, and whether other keys may exist.
pub type PreloadedState = (Vec<(Bytes, Bytes)>, bool);

/// What an attempt reads of the status when it starts.
#[derive(Debug, Clone)]
pub struct ErAttemptMetadata {
    pub status: ExplicitRecoveryStatus,
    pub span_context: ServiceInvocationSpanContext,
    pub journal_length: EntryIndex,
}

/// Reads made in the snapshot an attempt starts from.
pub trait ExplicitRecoveryReaderTransaction {
    type Error: InvocationReaderError;

    /// `None` unless the invocation is `Invoked` and on the explicit recovery family.
    fn read_er_metadata(
        &mut self,
        invocation_id: &InvocationId,
    ) -> impl Future<Output = Result<Option<ErAttemptMetadata>, Self::Error>> + Send;

    /// The headers and the input of the invocation: its journal entry 0.
    fn read_input(
        &mut self,
        invocation_id: &InvocationId,
    ) -> impl Future<Output = Result<Option<(Vec<Header>, Bytes)>, Self::Error>> + Send;

    /// The object state to preload, as `policy` asks and as long as it fits `size_limit`
    /// bytes. The flag is false only when every key with a value is in the result.
    fn read_object_state_preload(
        &mut self,
        service_id: &ServiceId,
        policy: &StatePreloadPolicy,
        size_limit: usize,
    ) -> impl Future<Output = Result<PreloadedState, Self::Error>> + Send;

    /// The invocation state to preload, as `policy` asks and as long as it fits `size_limit`
    /// bytes. The flag is false only when every key with a value is in the result.
    fn read_invocation_state_preload(
        &mut self,
        invocation_id: &InvocationId,
        policy: &StatePreloadPolicy,
        size_limit: usize,
    ) -> impl Future<Output = Result<PreloadedState, Self::Error>> + Send;
}

/// Reads of the latest committed state, made while an attempt runs, and the snapshot an attempt
/// starts from.
pub trait ExplicitRecoveryReader: InvocationReader {
    type ErTransaction<'a>: ExplicitRecoveryReaderTransaction<Error = Self::Error> + Send
    where
        Self: 'a;

    /// A consistent snapshot to build the start of an attempt from.
    fn er_transaction(&mut self) -> Result<Self::ErTransaction<'_>, Self::Error>;

    fn read_object_state(
        &mut self,
        service_id: &ServiceId,
        key: &Bytes,
    ) -> impl Future<Output = Result<Option<Bytes>, Self::Error>> + Send;

    fn read_object_state_keys(
        &mut self,
        service_id: &ServiceId,
    ) -> impl Future<Output = Result<Vec<Bytes>, Self::Error>> + Send;

    fn read_invocation_state(
        &mut self,
        invocation_id: &InvocationId,
        key: &Bytes,
    ) -> impl Future<Output = Result<Option<Bytes>, Self::Error>> + Send;

    fn read_invocation_state_keys(
        &mut self,
        invocation_id: &InvocationId,
    ) -> impl Future<Output = Result<Vec<Bytes>, Self::Error>> + Send;

    fn read_signal(
        &mut self,
        invocation_id: &InvocationId,
        name: &str,
        index: u32,
    ) -> impl Future<Output = Result<Option<SignalResult>, Self::Error>> + Send;
}
