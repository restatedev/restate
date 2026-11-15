// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! `sys_journal` over both stored journal layouts, as two sources of one schema.

use std::ops::ControlFlow;
use std::sync::Arc;

use datafusion::catalog::TableProvider;

use restate_partition_store::journal_scan::{JournalTable, JournalV2Table};
use restate_storage_api::Result;
use restate_storage_api::journal_table::JournalEntry;
use restate_storage_api::journal_table_v2::StoredEntry;
use restate_storage_api::protobuf_types::PartitionStoreProtobufValue;
use restate_storage_query_api::schema::RecordProgress;
use restate_storage_query_api::selection::Selection;
use restate_storage_query_api::tables::journal::{Journal, JournalColumn, JournalView};
use restate_storage_query_api::{PreparedRead, RowSink, SchemaBinding, Table};
use restate_types::identifiers::{InvocationId, InvocationUuid, JournalEntryId, PartitionKey};
use restate_util_string::ReString;

use crate::QuerySource;
use crate::context::SelectPartitions;
use crate::schema_scan::SchemaScanner;
use crate::sharding::PartitionSource;
use crate::source_registry::SourceRegistry;
use crate::table_providers::PartitionedTableProvider;

mod row;
#[cfg(test)]
mod tests;

crate::tables::table_adapter!(pub(crate) SysJournalTable, Journal);

/// Entries of service protocol versions below 4.
#[derive(Debug, Clone)]
pub(crate) struct JournalV1Source;
/// Entries of service protocol version 4 and later.
#[derive(Debug, Clone)]
pub(crate) struct JournalV2Source;

impl QuerySource for JournalV1Source {
    type Schema = Journal;

    fn identity() -> ReString {
        "sys_journal.v1".into()
    }
}
impl QuerySource for JournalV2Source {
    type Schema = Journal;

    fn identity() -> ReString {
        "sys_journal.v2".into()
    }
}
impl PartitionSource for JournalV1Source {
    const ORDERING: &'static [JournalColumn] = &[JournalColumn::PartitionKey];
}
impl PartitionSource for JournalV2Source {
    const ORDERING: &'static [JournalColumn] = &[JournalColumn::PartitionKey];
}

impl SysJournalTable {
    pub(crate) fn create_provider(
        partitions: impl SelectPartitions,
        sources: &SourceRegistry,
    ) -> Arc<dyn TableProvider> {
        Arc::new(
            PartitionedTableProvider::declared::<JournalV1Source>(partitions, sources)
                .with_source::<JournalV2Source>(sources),
        )
    }

    pub(crate) fn register_scanners(sources: &SourceRegistry) {
        sources.register_declared_scanner::<JournalV1Source>(Arc::new(SchemaScanner::<
            JournalV1Binding,
        >::new()));
        sources.register_declared_scanner::<JournalV2Source>(Arc::new(SchemaScanner::<
            JournalV2Binding,
        >::new()));
    }
}

fn entry_id(partition_key: PartitionKey, uuid: InvocationUuid, index: u32) -> JournalEntryId {
    JournalEntryId::from_parts(InvocationId::from_parts(partition_key, uuid), index)
}

macro_rules! journal_binding {
    ($binding:ident, $table:ty, $decode:expr, $write:path) => {
        pub(crate) enum $binding {}

        impl SchemaBinding for $binding {
            type Schema = Journal;
            type Table = $table;
            type Materializer = ();

            fn materialize_next(
                record: <Self::Table as Table>::Record<'_>,
                _: &mut (),
                emit: &mut impl RowSink<Journal>,
            ) -> Result<RecordProgress> {
                if let ControlFlow::Break(result) = Self::materialize_projected(record, emit) {
                    result?;
                }
                Ok(RecordProgress::Done)
            }

            fn prepare(selection: &Selection<Journal>) -> Result<PreparedRead<$table>> {
                <$table>::prepare(selection)
            }

            fn materialize(
                (key, mut value): <Self::Table as Table>::Record<'_>,
                emit: &mut impl for<'a> FnMut(JournalView<'a>) -> ControlFlow<Result<()>>,
            ) -> ControlFlow<Result<()>> {
                let decoded = key.decode_all().and_then(|key| {
                    let id = entry_id(key.partition_key, key.invocation_uuid, key.journal_index);
                    Ok((id, ($decode)(&mut value)?))
                });
                match decoded {
                    Ok((id, entry)) => emit(JournalView(&|row| $write(row, id, &entry))),
                    Err(error) => ControlFlow::Break(Err(error)),
                }
            }
        }
    };
}

journal_binding!(
    JournalV1Binding,
    JournalTable,
    |value: &mut &[u8]| JournalEntry::decode(value),
    row::write_v1
);
journal_binding!(
    JournalV2Binding,
    JournalV2Table,
    |value: &mut &[u8]| StoredEntry::decode(value).map(|entry| entry.0),
    row::write_v2
);
