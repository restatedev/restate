// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use tracing::trace;

use restate_bifrost::DataRecord;
use restate_partition_store::PartitionStoreTransaction;
use restate_storage_api::fsm_table::PartitionDurability;
use restate_wal_protocol::control::UpdatePartitionDurabilityCommand;
use restate_wal_protocol::v2::{CommandScope, Envelope};

use super::{ApplyPartitionCommand, NextStep};
use crate::partition::ProcessorError;
use crate::partition::leadership::trim_queue::HasTrimQueue;
use crate::partition::processor::{FsmMut, HasFsmMut, Processor};

pub struct UpdateDurabilityContext<'a, 'b, P> {
    pub txn: &'a mut PartitionStoreTransaction<'b>,
    pub processor: P,
}

impl<P: Processor + HasFsmMut + HasTrimQueue>
    ApplyPartitionCommand<UpdatePartitionDurabilityCommand> for UpdateDurabilityContext<'_, '_, P>
{
    async fn apply(
        &mut self,
        command: DataRecord<Envelope<UpdatePartitionDurabilityCommand>>,
    ) -> Result<NextStep, ProcessorError> {
        let lsn = command.seq();
        let (header, partition_durability) = command.into_inner().split()?;

        if partition_durability.partition_id != self.processor.partition_id() {
            trace!(
                "Ignore update-partition-durability message which is not targeted to me. Message is for {} but I'm {}",
                partition_durability.partition_id,
                self.processor.partition_id()
            );
            return Ok(NextStep::AdvanceLastAppliedLsn {
                lsn,
                dedup: header.into_dedup(),
                scope: CommandScope::PartitionScoped,
            });
        }

        let partition_durability = PartitionDurability {
            modification_time: partition_durability.modification_time,
            durable_point: partition_durability.durable_point,
        };

        if self.processor.trim_queue().push(&partition_durability) {
            self.processor
                .fsm_mut()
                .set_durable_point(self.txn, partition_durability);
        }

        Ok(NextStep::AdvanceLastAppliedLsn {
            lsn,
            dedup: header.into_dedup(),
            scope: CommandScope::PartitionScoped,
        })
    }
}
