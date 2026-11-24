// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use tracing::{debug, trace};

use restate_storage_api::invocation_status_table::InvocationStatus;
use restate_types::identifiers::InvocationId;
use restate_types::invocation::client::CancelInvocationResponse;
use restate_types::invocation::{InvocationMutationResponseSink, TerminationFlavor};
use restate_worker_api::invoker::explicit_recovery::ErToInvoker;

use super::{ErEndStorage, er_of, forward};
use crate::partition::processor::ProcessorContext;
use crate::partition::state_machine::{CommandHandler, Error, StateMachineApplyContext};

/// The cancel signal. The invocation is cancelled from then on, and the handler decides what
/// that means: the running attempt gets the signal, a waiting invocation gets a new attempt once.
pub struct OnCancel {
    pub invocation_id: InvocationId,
    pub status: InvocationStatus,
    pub response_sink: Option<InvocationMutationResponseSink>,
}

impl<'ctx, 's: 'ctx, S, P> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S, P>> for OnCancel
where
    S: ErEndStorage,
    P: ProcessorContext,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S, P>) -> Result<(), Error> {
        let OnCancel {
            invocation_id,
            status,
            response_sink,
        } = self;
        match status {
            // Cancelled before its first attempt: it never starts.
            InvocationStatus::Scheduled(scheduled) => {
                ctx.terminate_scheduled_invocation(
                    TerminationFlavor::Cancel,
                    invocation_id,
                    scheduled,
                )
                .await?;
                ctx.reply_to_cancel(response_sink, CancelInvocationResponse::Done);
            }
            InvocationStatus::Inboxed(inboxed) => {
                ctx.terminate_inboxed_invocation(TerminationFlavor::Cancel, invocation_id, inboxed)
                    .await?;
                ctx.reply_to_cancel(response_sink, CancelInvocationResponse::Done);
            }
            InvocationStatus::Invoked(mut metadata) => {
                let er = er_of(&mut metadata);
                if er.cancelled {
                    already_cancelled(&invocation_id);
                } else {
                    // Counted, and delivered to the running attempt (D34, D60).
                    er.cancelled = true;
                    er.forwarded_notifications += 1;
                    let attempt = er.attempt;
                    ctx.storage.put_invocation_status(
                        &invocation_id,
                        &InvocationStatus::Invoked(metadata),
                    )?;
                    forward(ctx, invocation_id, ErToInvoker::Cancel { attempt });
                }
                ctx.reply_to_cancel(response_sink, CancelInvocationResponse::Appended);
            }
            InvocationStatus::Suspended { mut metadata, .. }
            | InvocationStatus::Paused(mut metadata) => {
                let er = er_of(&mut metadata);
                if er.cancelled {
                    already_cancelled(&invocation_id);
                } else {
                    // A waiting invocation gets a new attempt, once, which starts cancelled.
                    er.cancelled = true;
                    ctx.do_resume_service(invocation_id, metadata).await?;
                }
                ctx.reply_to_cancel(response_sink, CancelInvocationResponse::Appended);
            }
            InvocationStatus::Completed(_) => {
                debug!(
                    "Received cancel command for completed invocation '{invocation_id}'. To cleanup the invocation after it's been completed, use the purge invocation command."
                );
                ctx.reply_to_cancel(response_sink, CancelInvocationResponse::AlreadyCompleted);
            }
            InvocationStatus::Free => {
                ctx.send_abort_invocation_to_invoker(invocation_id);
                ctx.reply_to_cancel(response_sink, CancelInvocationResponse::NotFound);
            }
        }
        Ok(())
    }
}

fn already_cancelled(invocation_id: &InvocationId) {
    // A second cancel signal changes nothing, and wakes nothing.
    trace!(
        restate.invocation.id = %invocation_id,
        "The invocation is already cancelled"
    );
}
