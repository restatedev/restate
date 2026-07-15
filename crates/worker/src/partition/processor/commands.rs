// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod announce_leader;
mod truncate_outbox;
mod update_durability;
mod upsert_rule_book;
mod upsert_schema;
mod version_barrier;

// Re-exports
pub use announce_leader::AnnounceLeaderContext;
pub use truncate_outbox::TruncateOutboxContext;
pub use update_durability::UpdateDurabilityContext;
pub use upsert_rule_book::UpsertRuleBookContext;
pub use upsert_schema::UpsertSchemaContext;
pub use version_barrier::VersionBarrierContext;

use restate_bifrost::DataRecord;
use restate_types::logs::Lsn;
use restate_wal_protocol::v2::{self, CommandScope, Envelope};

use crate::partition::ProcessorError;

#[derive(Debug)]
pub enum NextStep {
    /// The record covers a range of LSNs (e.g. a filter gap or duplicate). Applying this record
    /// advances the processor's applied LSN to the supplied value.
    SkipUntil(Lsn),

    AdvanceLastAppliedLsn {
        lsn: Lsn,
        dedup: v2::Dedup,
        scope: CommandScope,
    },
}

/// Applies a single partition-scoped record to the processor.
pub trait ApplyPartitionCommand<M> {
    async fn apply(&mut self, record: DataRecord<Envelope<M>>) -> Result<NextStep, ProcessorError>;
}
