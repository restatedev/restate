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

use restate_bifrost::DataRecord;
use restate_core::network::TransportConnect;
use restate_partition_store::PartitionStoreTransaction;
use restate_types::Versioned;
use restate_wal_protocol::control::UpsertRuleBookCommand;
use restate_wal_protocol::v2::{CommandScope, Envelope};

use super::{ApplyPartitionCommand, NextStep};
use crate::partition::leadership::LeadershipState;
use crate::partition::processor::{FsmAccess, FsmMut, HasFsmMut, Processor};
use crate::partition::state_machine::ActionCollector;
use crate::partition::{NodeContext, ProcessorError, state_machine};

pub struct UpsertRuleBookContext<'a, 'b, P, T> {
    pub txn: &'a mut PartitionStoreTransaction<'b>,
    pub node_ctx: &'a NodeContext,
    pub processor: P,
    pub leadership: &'a mut LeadershipState<T>,
    pub action_collector: &'a mut ActionCollector,
}

impl<P: Processor + HasFsmMut, T: TransportConnect> ApplyPartitionCommand<UpsertRuleBookCommand>
    for UpsertRuleBookContext<'_, '_, P, T>
{
    async fn apply(
        &mut self,
        command: DataRecord<Envelope<UpsertRuleBookCommand>>,
    ) -> Result<NextStep, ProcessorError> {
        let lsn = command.seq();
        let (header, upsert) = command.into_inner().split()?;
        let new_book = upsert.rule_book;

        let current_version = self.processor.fsm().rule_book_version();

        if new_book.version() <= current_version {
            trace!(
                "Skipping UpsertRuleBook to version {} (current: {})",
                new_book.version(),
                current_version,
            );
            return Ok(NextStep::AdvanceLastAppliedLsn {
                lsn,
                dedup: header.into_dedup(),
                scope: CommandScope::PartitionScoped,
            });
        }

        debug!(
            "Rule book updated from version {} to {}",
            current_version,
            new_book.version(),
        );

        // only leaders need to update the rules in the scheduler
        if self.leadership.is_leader() {
            let diff = new_book.diff(self.processor.fsm().rule_book());
            // Emit action for the leader to forward to UserLimiter
            // (followers ignore — no live limiter to notify).
            if !diff.is_empty() {
                self.action_collector
                    .push(state_machine::Action::RulesUpdated(diff));
            }
        }

        // Push the freshly-applied book into the node-level
        // cache so other PP-leaders on this node see the new
        // version on their next watch tick — without waiting
        // for the metadata-store poll cadence.
        self.node_ctx.rule_book_cache.notify_observed(&new_book);

        // Persist within the apply transaction.
        self.processor.fsm_mut().set_rule_book(self.txn, new_book);

        Ok(NextStep::AdvanceLastAppliedLsn {
            lsn,
            dedup: header.into_dedup(),
            scope: CommandScope::PartitionScoped,
        })
    }
}
