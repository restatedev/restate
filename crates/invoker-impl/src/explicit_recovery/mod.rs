// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The invoker side of the explicit recovery protocol family: a task and a state machine per
//! attempt, kept apart from the durable execution ones.

mod actions;
mod book;
mod decision;
mod runner;
mod task;

pub(crate) use task::ErTask;

use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::AbortHandle;

use restate_service_protocol_explicit_recovery::codec::Message;
use restate_types::clock::RoughTimestamp;
use restate_types::errors::InvocationError;
use restate_types::identifiers::InvocationId;
use restate_types::invocation::{FencingToken, InvocationTarget, ResponseResult};
use restate_types::retries::RetryIter;
use restate_types::schema::invocation_target::OnMaxAttempts;
use restate_types::time::MillisSinceEpoch;
use restate_worker_api::invoker::explicit_recovery::{
    ErEffectKind, ErFailureOutcome, ErToInvoker, RestartRule,
};
use restate_worker_api::resources::ReservedResources;

use crate::error::{InvokerError, RequestedErrorBehavior};

/// What the SDK's side of the stream yields.
#[derive(Debug)]
pub(crate) enum InputItem {
    Headers(http::response::Parts),
    Message(Message),
    /// Bytes of a message that is not complete yet.
    Partial,
}

/// How an attempt ended, as the task sees it.
#[derive(Debug)]
pub(crate) enum ErTaskOutput {
    End(ResponseResult),
    Restart(RestartRule),
    Suspend { received_notifications: u32 },
    Failed(InvokerError),
}

#[derive(Debug)]
pub(crate) enum ErTaskEvent {
    /// A non-terminal effect, with its seq.
    Effect {
        seq: u64,
        kind: ErEffectKind,
    },
    Terminal(ErTaskOutput),
}

/// Task -> main loop.
#[derive(Debug)]
pub(crate) struct ErTaskMessage {
    pub invocation_id: InvocationId,
    pub fencing_token: FencingToken,
    pub event: ErTaskEvent,
}

/// Sends the task's events to the invoker's main loop.
#[derive(Clone)]
pub(crate) struct EffectSink {
    pub tx: mpsc::UnboundedSender<ErTaskMessage>,
    pub invocation_id: InvocationId,
    pub fencing_token: FencingToken,
}

impl EffectSink {
    pub(crate) fn effect(&self, seq: u64, kind: ErEffectKind) -> bool {
        self.send(ErTaskEvent::Effect { seq, kind })
    }

    pub(crate) fn terminal(&self, output: ErTaskOutput) {
        self.send(ErTaskEvent::Terminal(output));
    }

    fn send(&self, event: ErTaskEvent) -> bool {
        self.tx
            .send(ErTaskMessage {
                invocation_id: self.invocation_id,
                fencing_token: self.fencing_token,
                event,
            })
            .is_ok()
    }
}

/// The invoker's state of a running attempt.
pub(crate) struct ErInvocationStateMachine {
    pub _permit: ReservedResources,
    pub fencing_token: FencingToken,
    pub invocation_target: InvocationTarget,
    pub attempt: u32,
    pub retry_iter: RetryIter<'static>,
    pub on_max_attempts: OnMaxAttempts,
    pub notifications_tx: mpsc::UnboundedSender<ErToInvoker>,
    pub abort_handle: AbortHandle,
}

impl ErInvocationStateMachine {
    /// What to do with the invocation after the attempt failed with `error`. Every retry goes
    /// through the scheduler, with any delay.
    pub(crate) fn on_task_error(&mut self, error: &InvokerError) -> ErFailureOutcome {
        Self::outcome(
            &mut self.retry_iter,
            self.on_max_attempts,
            error.is_transient(),
            error.requested_error_behavior(),
            || InvocationError::from_invoker(error),
        )
    }

    /// What a failure would do now, computed on a copy of the retry iterator (D63).
    pub(crate) fn idle_fallback(&self) -> ErFailureOutcome {
        Self::outcome(
            &mut self.retry_iter.clone(),
            self.on_max_attempts,
            true,
            RequestedErrorBehavior::Retry,
            || {
                InvocationError::internal(
                    "the handler was restarted too many times in a row without applying any action",
                )
            },
        )
    }

    fn outcome(
        retry_iter: &mut RetryIter<'static>,
        on_max_attempts: OnMaxAttempts,
        transient: bool,
        behavior: RequestedErrorBehavior,
        error: impl FnOnce() -> InvocationError,
    ) -> ErFailureOutcome {
        let override_delay = match behavior {
            RequestedErrorBehavior::Pause => return ErFailureOutcome::Pause,
            RequestedErrorBehavior::Fail => return ErFailureOutcome::Fail(error()),
            RequestedErrorBehavior::Retry => None,
            RequestedErrorBehavior::RetryWithIntervalOverride(delay) => Some(delay),
        };
        if transient && let Some(delay) = override_delay.or_else(|| retry_iter.next()) {
            return ErFailureOutcome::Retry {
                resume_at: resume_at(delay),
                retry_attempts: u32::try_from(retry_iter.attempts()).unwrap_or(u32::MAX),
            };
        }
        match on_max_attempts {
            OnMaxAttempts::Pause => ErFailureOutcome::Pause,
            OnMaxAttempts::Kill => ErFailureOutcome::Fail(error()),
        }
    }
}

fn resume_at(delay: Duration) -> Option<RoughTimestamp> {
    (!delay.is_zero())
        .then(|| RoughTimestamp::from_unix_millis_ceil(MillisSinceEpoch::now() + delay))
}

trait FromInvoker {
    fn from_invoker(error: &InvokerError) -> Self;
}

impl FromInvoker for InvocationError {
    fn from_invoker(error: &InvokerError) -> Self {
        match error {
            InvokerError::SdkV2(sdk) => (*sdk.error).clone(),
            other => InvocationError::internal(other.to_string()),
        }
    }
}
