// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use tracing::{debug, info};

use restate_clock::{RoughTimestamp, UniqueTimestamp};
use restate_storage_api::lock_table::WriteLockTable;
use restate_storage_api::vqueue_table::scheduler::YieldReason;
use restate_storage_api::vqueue_table::{
    EntryStatusHeader, ReadVQueueTable, Stage, WriteVQueueTable,
};
use restate_types::identifiers::InvocationId;
use restate_types::sharding::WithPartitionKey;
use restate_types::vqueues::EntryId;
use restate_vqueues::VQueue;
use restate_vqueues::context::HasVQueuesMut;

use crate::debug_if_leader;
use crate::partition::processor::Processor;
use crate::partition::state_machine::{CommandHandler, Error, StateMachineApplyContext};

/// Yields an invocation from the running stage back to the inbox stage the vqueue it belongs to.
///
/// NOTE: this requires vqueues to be enabled or that the invocation was driven by the vqueue
/// scheduler
pub struct YieldInvocationCommand<'e> {
    pub invocation_id: &'e InvocationId,
    pub yield_reason: YieldReason,
    pub resume_at: Option<RoughTimestamp>,
}

impl<'e, 'ctx: 'e, 's: 'ctx, S, P> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S, P>>
    for YieldInvocationCommand<'e>
where
    S: WriteVQueueTable + ReadVQueueTable + WriteLockTable + ReadVQueueTable,
    P: Processor + HasVQueuesMut,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S, P>) -> Result<(), Error> {
        debug_if_leader!(ctx.is_leader, "Effect: Yield");

        // From scheduler's yield
        let at = UniqueTimestamp::from_unix_millis_unchecked(ctx.record_created_at);
        let entry_id = EntryId::from(self.invocation_id);

        let Some(header) = ctx
            .storage
            .get_vqueue_entry_status(self.invocation_id.partition_key(), &entry_id)
            .await?
        else {
            // It could be that the invocation was killed and purged before the invoker yielded it.
            info!(
                "Not yielding {} because it has no vqueue state, yield_reason: {}",
                self.invocation_id, self.yield_reason
            );
            return Ok(());
        };

        if !matches!(header.stage(), Stage::Running) {
            debug!(
                "Not yielding {} because it is not in running stage. Current stage is {}",
                self.invocation_id,
                header.stage()
            );
            return Ok(());
        }

        VQueue::get(
            header.vqueue_id(),
            ctx.storage,
            ctx.processor.vqueues_mut(),
            ctx.is_leader.then_some(ctx.action_collector),
        )
        .await?
        .expect("yielding in a non-existent vqueue")
        .yield_entry(at, &header, self.resume_at, self.yield_reason);

        Ok(())
    }
}
