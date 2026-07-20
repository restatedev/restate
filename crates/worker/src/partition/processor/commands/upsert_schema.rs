// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use tracing::{debug, trace};

use restate_bifrost::DataRecord;
use restate_partition_store::PartitionStoreTransaction;
use restate_types::Versioned;
use restate_wal_protocol::control::UpsertSchemaCommand;
use restate_wal_protocol::v2::{CommandScope, Envelope};

use crate::partition::ProcessorError;
use crate::partition::processor::{FsmAccess, FsmMut, HasFsmMut, Processor};

use super::{ApplyPartitionCommand, NextStep};

pub struct UpsertSchemaContext<'a, 'b, P> {
    pub txn: &'a mut PartitionStoreTransaction<'b>,
    pub processor: P,
}

impl<P: Processor + HasFsmMut> ApplyPartitionCommand<UpsertSchemaCommand>
    for UpsertSchemaContext<'_, '_, P>
{
    async fn apply(
        &mut self,
        command: DataRecord<Envelope<UpsertSchemaCommand>>,
    ) -> Result<NextStep, ProcessorError> {
        let lsn = command.seq();
        let (header, upsert) = command.into_inner().split()?;

        trace!(
            "Upsert schema record to version '{}'",
            upsert.schema.version()
        );
        if self.processor.fsm().schema_version() < upsert.schema.version() {
            // only update if schema is none or has a smaller version
            debug!("Schema updated to version '{}'", upsert.schema.version());
            self.processor
                .fsm_mut()
                .set_schema(self.txn, Arc::new(upsert.schema));
        }

        Ok(NextStep::AdvanceLastAppliedLsn {
            lsn,
            dedup: header.into_dedup(),
            scope: CommandScope::PartitionScoped,
        })
    }
}
