// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! `sys_journal_events`, preserving V1's row conversion.

use std::ops::ControlFlow;
use std::sync::Arc;

use datafusion::catalog::TableProvider;

use restate_partition_store::journal_events::decode_event;
use restate_partition_store::journal_scan::JournalEventsTable;
use restate_storage_api::Result;
use restate_storage_api::journal_events::EventView;
use restate_storage_query_api::schema::RecordProgress;
use restate_storage_query_api::selection::Selection;
use restate_storage_query_api::tables::journal_events::{
    JournalEvents, JournalEventsColumn, JournalEventsRowWriter, JournalEventsView,
};
use restate_storage_query_api::{PreparedRead, RowSink, SchemaBinding, Table};
use restate_types::identifiers::{InvocationId, WithPartitionKey};

use crate::context::SelectPartitions;
use crate::schema_scan::SchemaScanner;
use crate::sharding::PartitionSource;
use crate::source_registry::SourceRegistry;
use crate::table_providers::{PartitionedTableProvider, ScanPartition};

#[cfg(test)]
mod tests;

crate::tables::table_adapter!(pub(crate) SysJournalEventsTable, JournalEvents);

impl PartitionSource for SysJournalEventsTable {
    const ORDERS: &'static [restate_storage_query_api::ordering::NativeOrder] = <<JournalEventsBinding as restate_storage_query_api::SchemaBinding>::Table as restate_storage_query_api::Table>::NATIVE_ORDERS;
    const ID_COLUMN: Option<JournalEventsColumn> = Some(JournalEventsColumn::Id);
}

impl SysJournalEventsTable {
    pub(crate) fn create_provider(
        partitions: impl SelectPartitions,
        sources: &SourceRegistry,
    ) -> Arc<dyn TableProvider> {
        Arc::new(PartitionedTableProvider::declared::<Self>(
            partitions, sources,
        ))
    }

    pub(crate) fn create_local_scanner() -> impl ScanPartition {
        SchemaScanner::<JournalEventsBinding>::new()
    }
}

pub(crate) enum JournalEventsBinding {}

impl SchemaBinding for JournalEventsBinding {
    type Schema = JournalEvents;
    type Table = JournalEventsTable;
    type Materializer = ();

    fn materialize_next(
        record: <Self::Table as Table>::Record<'_>,
        _: &mut (),
        emit: &mut impl RowSink<JournalEvents>,
    ) -> Result<RecordProgress> {
        if let ControlFlow::Break(result) = Self::materialize_projected(record, emit) {
            result?;
        }
        Ok(RecordProgress::Done)
    }

    fn prepare(selection: &Selection<JournalEvents>) -> Result<PreparedRead<JournalEventsTable>> {
        JournalEventsTable::prepare(selection)
    }

    fn materialize(
        (key, mut value): <Self::Table as Table>::Record<'_>,
        emit: &mut impl for<'a> FnMut(JournalEventsView<'a>) -> ControlFlow<Result<()>>,
    ) -> ControlFlow<Result<()>> {
        match key
            .decode_all()
            .and_then(|key| decode_event(key, &mut value))
        {
            Ok((id, event)) => emit(JournalEventsView(&|row| write(row, id, &event))),
            Err(error) => ControlFlow::Break(Err(error)),
        }
    }
}

fn write(row: &mut JournalEventsRowWriter<'_>, id: InvocationId, event: &EventView) {
    row.partition_key(id.partition_key());
    if row.is_id_defined() {
        row.fmt_id(id);
    }
    row.appended_at(event.append_time.as_u64() as i64);
    row.after_journal_entry_index(event.after_journal_entry_index);
    row.fmt_event_type(event.event.ty());
    if row.is_event_json_defined()
        && let Ok(json) = serde_json::to_string(&event.event.clone().into_event_or_unknown())
    {
        row.event_json(json);
    }
}
