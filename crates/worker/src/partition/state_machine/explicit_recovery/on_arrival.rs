// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! What arrives for an invocation of the family: a result, a signal. While an attempt runs it is
//! counted and forwarded to it (D34); a suspended invocation wakes on anything (D29).

use tracing::debug;

use restate_storage_api::invocation_status_table::InvocationStatus;
use restate_types::identifiers::InvocationId;
use restate_types::invocation::ResponseResult;
use restate_types::journal_v2::{Signal, SignalId};
use restate_worker_api::invoker::explicit_recovery::ErToInvoker;

use super::{ErStorage, er_of, forward};
use crate::partition::processor::ProcessorContext;
use crate::partition::state_machine::{CommandHandler, Error, StateMachineApplyContext};

/// The result of an action of some attempt: what a callee or an attached invocation returned.
pub struct OnResponse {
    pub invocation_id: InvocationId,
    pub status: InvocationStatus,
    pub result_id: u32,
    pub result: ResponseResult,
}

impl<'ctx, 's: 'ctx, S, P> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S, P>>
    for OnResponse
where
    S: ErStorage,
    P: ProcessorContext,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S, P>) -> Result<(), Error> {
        let OnResponse {
            invocation_id,
            status,
            result_id,
            result,
        } = self;
        match status {
            InvocationStatus::Invoked(mut metadata) => {
                let er = er_of(&mut metadata);
                er.forwarded_notifications += 1;
                let attempt = er.attempt;
                ctx.storage
                    .put_invocation_status(&invocation_id, &InvocationStatus::Invoked(metadata))?;
                forward(
                    ctx,
                    invocation_id,
                    ErToInvoker::Result {
                        attempt,
                        result_id,
                        result,
                    },
                );
            }
            InvocationStatus::Suspended { metadata, .. } => {
                // What woke the invocation is not delivered: the new attempt asks again.
                ctx.do_resume_service(invocation_id, metadata).await?;
            }
            _ => {
                debug!(
                    restate.invocation.id = %invocation_id,
                    "Dropping result {result_id}: no attempt can receive it"
                );
            }
        }
        Ok(())
    }
}

/// A signal sent to the invocation. Signals by name stay, numbered per name from 0, for every
/// attempt to read. A signal by index other than the cancel signal is dropped (D61).
pub struct OnSignal {
    pub invocation_id: InvocationId,
    pub status: InvocationStatus,
    pub signal: Signal,
}

impl<'ctx, 's: 'ctx, S, P> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S, P>> for OnSignal
where
    S: ErStorage,
    P: ProcessorContext,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S, P>) -> Result<(), Error> {
        let OnSignal {
            invocation_id,
            status,
            signal,
        } = self;
        let SignalId::Name(name) = signal.id else {
            debug!(
                restate.invocation.id = %invocation_id,
                "Dropping signal {}: only signals by name are kept", signal.id
            );
            return Ok(());
        };
        if matches!(
            status,
            InvocationStatus::Completed(_) | InvocationStatus::Free
        ) {
            debug!(
                restate.invocation.id = %invocation_id,
                "Dropping signal {name}: the invocation ended"
            );
            return Ok(());
        }

        let index = ctx.storage.next_signal_index(&invocation_id, &name).await?;
        ctx.storage
            .put_signal(&invocation_id, &name, index, &signal.result)?;

        match status {
            InvocationStatus::Invoked(mut metadata) => {
                let er = er_of(&mut metadata);
                er.forwarded_notifications += 1;
                let attempt = er.attempt;
                ctx.storage
                    .put_invocation_status(&invocation_id, &InvocationStatus::Invoked(metadata))?;
                forward(
                    ctx,
                    invocation_id,
                    ErToInvoker::Signal {
                        attempt,
                        name,
                        index,
                        result: signal.result,
                    },
                );
            }
            InvocationStatus::Suspended { metadata, .. } => {
                ctx.do_resume_service(invocation_id, metadata).await?;
            }
            // Kept for the first attempt, or for the attempt after a resume.
            _ => {}
        }
        Ok(())
    }
}
