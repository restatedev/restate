// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_storage_api::fsm_table::FsmTable;
use restate_wal_protocol::control::VersionBarrier;

use crate::debug_if_leader;
use crate::partition::state_machine::{CommandHandler, Error, StateMachineApplyContext};

pub struct OnVersionBarrierCommand {
    pub barrier: VersionBarrier,
}

impl<'ctx, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for OnVersionBarrierCommand
where
    S: FsmTable,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        if matches!(
            self.barrier.version.cmp_precedence(ctx.min_restate_version),
            std::cmp::Ordering::Greater,
        ) {
            ctx.storage
                .put_min_restate_version(&self.barrier.version)
                .await?;
            *ctx.min_restate_version = self.barrier.version;
            debug_if_leader!(
                ctx.is_leader,
                "Update a new minimum restate-server version barrier to {}",
                ctx.min_restate_version
            );
        }
        Ok(())
    }
}
