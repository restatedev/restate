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
use restate_partition_store::lookup::LookupBinding;
use restate_partition_store::table::WithSnapshot;
use restate_storage_api::Result;
use restate_storage_query_api::schema::RecordProgress;
use restate_storage_query_api::selection::Selection;
use restate_storage_query_api::selection::domain::Domain;
use restate_storage_query_api::tables::journal::{
    Journal, JournalColumn, JournalDomains, JournalView, named_journal_entries,
};
use restate_storage_query_api::{PreparedRead, RowSink, SchemaBinding, Table};
use restate_types::identifiers::{
    InvocationId, InvocationUuid, JournalEntryId, PartitionKey, WithInvocationId,
};
use restate_util_string::ReString;

use crate::QuerySource;
use crate::context::SelectPartitions;
use crate::schema_scan::LookupScanner;
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
    /// A finite set of invocation IDs and indexes is looked up instead of read.
    const PRIMARY_ACCESS: Option<crate::access::SelectPrimary> =
        Some(crate::access::select_lookup::<JournalV1Binding>);
}
impl PartitionSource for JournalV2Source {
    const ORDERING: &'static [JournalColumn] = &[JournalColumn::PartitionKey];
    /// A finite set of invocation IDs and indexes is looked up instead of read.
    const PRIMARY_ACCESS: Option<crate::access::SelectPrimary> =
        Some(crate::access::select_lookup::<JournalV2Binding>);
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
        sources.register_declared_scanner::<JournalV1Source>(Arc::new(LookupScanner::<
            JournalV1Binding,
        >::new()));
        sources.register_declared_scanner::<JournalV2Source>(Arc::new(LookupScanner::<
            JournalV2Binding,
        >::new()));
    }
}

fn entry_id(partition_key: PartitionKey, uuid: InvocationUuid, index: u32) -> JournalEntryId {
    JournalEntryId::from_parts(InvocationId::from_parts(partition_key, uuid), index)
}

/// Binds a journal layout to `sys_journal`. `$write` writes one stored entry
/// through the projected row writer; its errors fail the query. Records come with
/// their snapshot, from which V2 reads the content blobs of large entries.
macro_rules! journal_binding {
    ($binding:ident, $table:ty, $write:expr) => {
        pub(crate) enum $binding {}

        impl SchemaBinding for $binding {
            type Schema = Journal;
            type Table = WithSnapshot<$table>;
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

            fn prepare(selection: &Selection<Journal>) -> Result<PreparedRead<Self::Table>> {
                Ok(<$table>::prepare(selection)?.cast())
            }

            fn materialize_projected(
                ((key, value), snapshot): <Self::Table as Table>::Record<'_>,
                emit: &mut impl RowSink<Journal>,
            ) -> ControlFlow<Result<()>> {
                let id = key
                    .decode_all()
                    .map(|key| entry_id(key.partition_key, key.invocation_uuid, key.journal_index));
                match id {
                    Ok(id) => emit
                        .append_with(|builder| ($write)(&mut builder.row(), id, value, snapshot)),
                    Err(error) => ControlFlow::Break(Err(error)),
                }
            }

            /// Unlike [`Self::materialize_projected`], a row can't fail here: a
            /// stored entry that can't be written is logged and its row left partial.
            fn materialize(
                ((key, value), snapshot): <Self::Table as Table>::Record<'_>,
                emit: &mut impl for<'a> FnMut(JournalView<'a>) -> ControlFlow<Result<()>>,
            ) -> ControlFlow<Result<()>> {
                let id = key
                    .decode_all()
                    .map(|key| entry_id(key.partition_key, key.invocation_uuid, key.journal_index));
                match id {
                    Ok(id) => emit(JournalView(&|row| {
                        if let Err(error) = ($write)(row, id, value, snapshot) {
                            row::log_corruption(&id.invocation_id(), "entry", error);
                        }
                    })),
                    Err(error) => ControlFlow::Break(Err(error)),
                }
            }
        }

        /// Every combination of finitely many invocation IDs and indexes, up to
        /// `MAX_LOOKUP_ENTRIES`, is looked up instead of read.
        impl LookupBinding for $binding {
            fn lookup_ids(domains: &JournalDomains) -> Domain<(InvocationId, u32)> {
                named_journal_entries(domains)
            }
        }
    };
}

journal_binding!(JournalV1Binding, JournalTable, |row, id, value, _| {
    row::write_v1(row, id, value)
});
journal_binding!(JournalV2Binding, JournalV2Table, row::write_v2);
