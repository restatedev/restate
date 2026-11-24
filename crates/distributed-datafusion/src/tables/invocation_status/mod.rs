// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroUsize;
use std::ops::{ControlFlow, RangeBounds};
use std::sync::Arc;

use bilrost::{Message, OwnedMessage};
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use datafusion::physical_plan::{PhysicalExpr, SendableRecordBatchStream};

use restate_partition_store::index::RegisteredIndex;
use restate_partition_store::invocation_status_table::{
    EntryIndexExecution, EntryIndexPath, EntryIndexRead, InvocationEntry,
    InvocationStatusEntryRecords, InvocationStatusKey, InvocationStatusKeys,
    InvocationStatusRecords, InvocationStatusScan, ScanReason,
};
use restate_partition_store::keys::selection::KeySelection;
use restate_partition_store::vqueue_table::{
    EntryByCreated as StoredEntryIndex, EntryByStatusCreated as StoredStatusIndex,
    EntryByStatusCreatedKey,
};
use restate_storage_api::protobuf_types::v1::lazy::InvocationStatusV2Lazy;
use restate_storage_query_api::ordering::NativeOrder;
use restate_storage_query_api::schema::index::index_restriction;
use restate_storage_query_api::selection::domain::Domain;
use restate_storage_query_api::selection::{Selection, ValuePredicate};
use restate_storage_query_api::tables::index::{
    EntryByServiceCreated, EntryByServiceCreatedDomains, EntryByStatusCreated,
    EntryByStatusCreatedDomains,
};
use restate_storage_query_api::tables::invocation_status::{
    InvocationStatus, InvocationStatusColumn,
};
use restate_storage_query_api::{LogicalSchema, RowSink, SelectionSchema};
use restate_types::SemanticRestateVersion;
use restate_types::config::Configuration;
use restate_types::identifiers::{InvocationId, InvocationUuid, PartitionKey, WithPartitionKey};
use restate_types::sharding::KeyRange;
use restate_worker_api::PartitionCaptureRequirements;

use crate::access::{
    CompleteIndex, IndexAccess, IndexOrder, PrimaryKeys, PrimaryRead, PrimarySelection,
};
use crate::batches::{BatchSink, Batches, scan_rows_into};
use crate::context::SelectPartitions;
use crate::schema_scan::compile_selection;
use crate::sharding::PartitionSource;
use crate::source_registry::SourceRegistry;
use crate::statistics::{
    DEPLOYMENT_ROW_ESTIMATE, RowEstimate, SERVICE_ROW_ESTIMATE, TableStatisticsBuilder,
};
use crate::table_providers::{
    InOrder, PartitionReadContext, PartitionedTableProvider, PreparedPartitionRead, ReadMetrics,
    ScanPartition,
};

mod row;
#[cfg(test)]
mod tests;

crate::tables::table_adapter!(pub(crate) SysInvocationStatusTable, InvocationStatus);

impl PartitionSource for SysInvocationStatusTable {
    const ORDERS: &'static [NativeOrder] =
        <InvocationStatusRecords as restate_storage_query_api::Table>::NATIVE_ORDERS;
    const ID_COLUMN: Option<InvocationStatusColumn> = Some(InvocationStatusColumn::Id);
    const PRIMARY_ACCESS: Option<crate::access::SelectPrimary> = Some(select_primary);
    // A query whose statuses narrow the status index's lanes reads it; any
    // other, the creation index, which checks statuses on each candidate.
    const INDEXES: &'static [IndexAccess<InvocationStatus>] = &[
        IndexAccess::of::<StatusIndex>(),
        IndexAccess::of::<EntryIndex>(),
    ];
}

/// The entry index `entry_by_created/1`, read for the invocations it finds. It
/// holds every invocation once its partition has fully migrated to vqueues and
/// the index is Complete; until then, each partition's read scans instead,
/// before returning anything. Its policy: the query asks for the newest
/// invocations, or the oldest, of all services or a few, so the read merges one
/// creation hour's runs at a time, a run per service, and stops early:
///
/// ```sql
/// SELECT id FROM sys_invocation_status
/// [WHERE target_service_name = 'Promises' AND target_service_key IS NOT NULL]
/// ORDER BY created_at DESC [NULLS FIRST | NULLS LAST]
/// LIMIT 51
/// ```
///
/// Oldest first (`ORDER BY created_at ASC … LIMIT 250`) reads each run from
/// its oldest end. Another order scans instead. Other conditions, such as
/// `status = 'scheduled'`, are checked on each candidate
/// ([`CompleteIndex::NEEDS_ORDERED_DEMAND`]).
struct EntryIndex;

impl CompleteIndex for EntryIndex {
    type Index = EntryByServiceCreated;
    const INTRODUCED: SemanticRestateVersion = <StoredEntryIndex as RegisteredIndex>::INTRODUCED;

    /// The read merges its runs by creation time, newest first. The index keeps
    /// each entry's full creation timestamp, whose millisecond is the
    /// invocation's `created_at`: the read checks that for every row.
    ///
    /// `created_at` is never NULL: every stored invocation status has a
    /// creation time, and the row writer always sets it (`row.rs`).
    const ORDERING: &'static [IndexOrder<InvocationStatusColumn>] = &[IndexOrder {
        column: InvocationStatusColumn::CreatedAt,
        options: SortOptions {
            descending: true,
            nulls_first: false,
        },
        never_null: true,
    }];

    /// The read merges one creation hour's runs at a time, one per service
    /// with an invocation in that hour, so pages of all services qualify, as
    /// do pages of any number of named ones. Where the index isn't maintained, every
    /// partition would scan and sort instead, so the planner then scans.
    fn useful(restriction: &EntryByServiceCreatedDomains) -> bool {
        if !Configuration::pinned()
            .common
            .experimental
            .is_track_entry_indexes_enabled()
        {
            return false;
        }
        // An empty list of services selects nothing.
        !matches!(
            restriction.target_service_name.to_predicate(),
            Some(ValuePredicate::In(names)) if names.is_empty()
        )
    }

    const NEEDS_ORDERED_DEMAND: bool = true;

    /// A LIMIT without an ORDER BY, such as the UI's
    /// `WHERE target_service_name IN ('Doss') LIMIT 250`, reads the newest
    /// matches and stops, where the query names its services or bounds the
    /// creation time: the read visits only their runs or hours. Without either,
    /// it would visit every service's runs, and the scan, which stops at the
    /// LIMIT too, reads instead.
    fn useful_unordered(restriction: &EntryByServiceCreatedDomains) -> bool {
        (restriction.target_service_name.to_predicate().is_some()
            || restriction.created_at.to_predicate().is_some())
            && Self::useful(restriction)
    }
}

/// The entry index `entry_by_status_created/1`, read for the invocations of
/// some statuses or outcomes. The public `status` and `completion_result`
/// select the stages and stored statuses their entries can be in, the
/// index's **lanes**: each status the pairs whose effective status tells it
/// (`invoked` is the inbox's `ready`, `yielded` and `backing-off` and all of
/// Running), failed invocations Finished in any stored status but Succeeded
/// (see `EntryByStatusCreatedKey::lanes`). Each partition
/// reads the lanes with keys, like `entry_by_created/1`, and merges them into
/// creation order; until the index is Complete it scans instead. Its policy:
/// the newest or oldest invocations of some statuses, of all services or a
/// few, under the creation index's other conditions:
///
/// ```sql
/// SELECT id FROM sys_invocation_status
/// WHERE status = 'scheduled' [AND target_service_name IN ('Counter')]
/// ORDER BY created_at DESC NULLS LAST
/// LIMIT 250
/// ```
///
/// A status condition that doesn't narrow the lanes, such as one naming
/// every status, leaves it unused.
struct StatusIndex;

impl CompleteIndex for StatusIndex {
    type Index = EntryByStatusCreated;
    const INTRODUCED: SemanticRestateVersion = <StoredStatusIndex as RegisteredIndex>::INTRODUCED;
    /// As [`EntryIndex::ORDERING`]: the lanes merge by creation time.
    const ORDERING: &'static [IndexOrder<InvocationStatusColumn>] = EntryIndex::ORDERING;

    fn useful(restriction: &EntryByStatusCreatedDomains) -> bool {
        if !Configuration::pinned()
            .common
            .experimental
            .is_track_entry_indexes_enabled()
        {
            return false;
        }
        // An empty list of services selects nothing.
        if let Some(ValuePredicate::In(names)) = restriction.target_service_name.to_predicate()
            && names.is_empty()
        {
            return false;
        }
        let all = EntryByStatusCreatedKey::lanes(&Selection::All).count();
        EntryByStatusCreatedKey::lanes(&restriction.to_selection()).count() < all
    }

    const NEEDS_ORDERED_DEMAND: bool = true;

    /// The statuses always narrow the read to fewer lanes ([`Self::useful`]).
    fn useful_unordered(restriction: &EntryByStatusCreatedDomains) -> bool {
        Self::useful(restriction)
    }
}

impl SysInvocationStatusTable {
    pub(crate) fn create_provider(
        partitions: impl SelectPartitions,
        sources: &SourceRegistry,
    ) -> Arc<dyn TableProvider> {
        let statistics = TableStatisticsBuilder::<InvocationStatus>::new()
            .with_num_rows_estimate(RowEstimate::Large)
            .with_partition_key(InvocationStatusColumn::PartitionKey)
            .with_primary_key(InvocationStatusColumn::Id)
            .with_foreign_key(
                InvocationStatusColumn::PinnedDeploymentId,
                DEPLOYMENT_ROW_ESTIMATE,
            )
            .with_foreign_key(
                InvocationStatusColumn::TargetServiceName,
                SERVICE_ROW_ESTIMATE,
            );
        Arc::new(
            PartitionedTableProvider::declared::<Self>(partitions, sources)
                .with_statistics(statistics.build()),
        )
    }

    pub(crate) fn create_local_scanner() -> impl ScanPartition {
        StatusScanner
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, bilrost::Message)]
struct InvocationKey {
    #[bilrost(1)]
    partition_key: PartitionKey,
    #[bilrost(tag(2), encoding(plainbytes))]
    uuid: [u8; 16],
}

#[derive(bilrost::Message)]
struct InvocationKeys {
    #[bilrost(1)]
    keys: Vec<InvocationKey>,
}

#[derive(Debug)]
struct InvocationSelection(Vec<InvocationId>);

fn select_primary(
    filters: &[Arc<dyn PhysicalExpr>],
) -> anyhow::Result<Option<Box<dyn PrimarySelection>>> {
    let keys = InvocationStatus::analyze(filters, NonZeroUsize::new(16).unwrap())?
        .into_iter()
        .map(|domains| domains.id)
        .fold(Domain::empty(), Domain::union)
        .into_values();
    Ok(keys.map(|keys| Box::new(InvocationSelection(keys)) as Box<dyn PrimarySelection>))
}

impl PrimarySelection for InvocationSelection {
    fn within(&self, ownership: KeyRange) -> Option<PrimaryKeys> {
        let first = self
            .0
            .partition_point(|id| id.partition_key() < ownership.start());
        let end = self
            .0
            .partition_point(|id| id.partition_key() <= ownership.end());
        (first < end).then(|| PrimaryKeys {
            count: (end - first) as u64,
            encoded: InvocationKeys {
                keys: self.0[first..end]
                    .iter()
                    .map(|id| InvocationKey {
                        partition_key: id.partition_key(),
                        uuid: id.invocation_uuid().to_bytes(),
                    })
                    .collect(),
            }
            .encode_to_vec(),
        })
    }
}

#[derive(Debug)]
struct StatusScanner;

impl ScanPartition for StatusScanner {
    fn supports_primary_lookup(&self) -> bool {
        true
    }

    fn supports_index(&self, index: &str) -> bool {
        index == EntryByServiceCreated::NAME || index == EntryByStatusCreated::NAME
    }

    fn prepare_read(
        self: Arc<Self>,
        access: PrimaryRead,
        projection: SchemaRef,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        limit: Option<usize>,
    ) -> anyhow::Result<Arc<dyn PreparedPartitionRead>> {
        // The partition keys the predicate allows, such as one virtual object's.
        let partition_keys = InvocationStatus::candidate_shard_keys(predicate.as_slice())?;
        let scan = || -> anyhow::Result<_> {
            Ok(Arc::new(InvocationStatusKeys::prepare(
                &compile_selection::<InvocationStatus>(predicate.as_ref())?,
                &partition_keys,
            )?))
        };
        let access = match access {
            PrimaryRead::Scan => StatusAccess::Scan(scan()?),
            PrimaryRead::Index(read) => {
                anyhow::ensure!(
                    self.supports_index(read.name()),
                    "unsupported index '{}'",
                    read.name()
                );
                // The planner's choice is fixed, its order too; the restriction is
                // derived again from the shipped predicate, and the predicate stays
                // residual.
                let index = if read.name() == EntryByStatusCreated::NAME {
                    let selection =
                        index_restriction::<EntryByStatusCreated>(predicate.as_slice())?
                            .map_or(Selection::Empty, |domains| domains.to_selection());
                    EntryIndexRead::prepare_by_status(&selection, read.direction::<StatusIndex>()?)?
                } else {
                    let selection =
                        index_restriction::<EntryByServiceCreated>(predicate.as_slice())?
                            .map_or(Selection::Empty, |domains| domains.to_selection());
                    EntryIndexRead::prepare(&selection, read.direction::<EntryIndex>()?)?
                };
                // An entry of another partition key is skipped before its status
                // is fetched.
                let index = index
                    .with_partition_keys(crate::selection::partition_key_filter(&partition_keys));
                StatusAccess::EntryIndex {
                    index: Arc::new(index),
                    fallback: scan()?,
                }
            }
            PrimaryRead::MultiGet(keys) => {
                keys.validate()?;
                let decoded = InvocationKeys::decode(keys.encoded.as_slice())?;
                anyhow::ensure!(
                    decoded.keys.len() == keys.len()
                        && decoded.keys.windows(2).all(|pair| pair[0] < pair[1]),
                    "invalid invocation primary key set"
                );
                StatusAccess::Keys(
                    decoded
                        .keys
                        .into_iter()
                        .map(|key| {
                            InvocationId::from_parts(
                                key.partition_key,
                                InvocationUuid::from_bytes(key.uuid),
                            )
                        })
                        .collect(),
                )
            }
        };
        Ok(Arc::new(StatusRead {
            access,
            projection,
            limit,
        }))
    }
}

#[derive(derive_more::Debug, Clone)]
enum StatusAccess {
    Scan(#[debug(skip)] Arc<KeySelection<InvocationStatusKey>>),
    Keys(Arc<[InvocationId]>),
    EntryIndex {
        #[debug(skip)]
        index: Arc<EntryIndexRead>,
        #[debug(skip)]
        fallback: Arc<KeySelection<InvocationStatusKey>>,
    },
}

#[derive(Debug)]
struct StatusRead {
    access: StatusAccess,
    projection: SchemaRef,
    limit: Option<usize>,
}

impl PreparedPartitionRead for StatusRead {
    /// An entry-index read that scans instead loses its creation order. The read
    /// tells which path it took as it opens, before its first row.
    fn execute_in_order(
        &self,
        context: PartitionReadContext,
        batch_size: usize,
        metrics: ReadMetrics,
    ) -> anyhow::Result<(SendableRecordBatchStream, InOrder)> {
        if !matches!(self.access, StatusAccess::EntryIndex { .. }) {
            let rows = self.read(context, batch_size, metrics, None)?;
            return Ok((rows, Box::pin(std::future::ready(true))));
        }
        let (chosen, path) = tokio::sync::oneshot::channel();
        let rows = self.read(context, batch_size, metrics, Some(chosen))?;
        // No answer means the read failed, or stopped before opening, so it
        // returns no rows to order.
        Ok((
            rows,
            Box::pin(async move {
                match path.await {
                    Ok(path) => matches!(path, EntryIndexPath::Index { .. }),
                    Err(_) => true,
                }
            }),
        ))
    }

    fn capture_requirements(&self) -> PartitionCaptureRequirements {
        PartitionCaptureRequirements {
            storage: true,
            ..Default::default()
        }
    }

    fn execute(
        &self,
        context: PartitionReadContext,
        batch_size: usize,
        metrics: ReadMetrics,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        self.read(context, batch_size, metrics, None)
    }

    /// An entry-index read can't where the index isn't Complete on the
    /// snapshot, or the partition hasn't migrated to vqueues.
    fn usable(&self, context: &PartitionReadContext) -> anyhow::Result<bool> {
        Ok(match &self.access {
            StatusAccess::EntryIndex { index, .. } => index.unusable(context.storage()?)?.is_none(),
            _ => true,
        })
    }
}

impl StatusRead {
    /// An entry-index read tells `chosen` which path it takes.
    fn read(
        &self,
        context: PartitionReadContext,
        batch_size: usize,
        metrics: ReadMetrics,
        chosen: Option<tokio::sync::oneshot::Sender<EntryIndexPath>>,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        anyhow::ensure!(batch_size > 0, "batch size must be positive");
        let memory = context.retained_memory("entry index read of sys_invocation_status");
        // Which path each partition's entry-index read took, and how many
        // statuses the index read fetched, for EXPLAIN ANALYZE.
        let (index_reads, index_scans, index_statuses) = (
            metrics.counter("entry_index_reads"),
            metrics.counter("entry_index_scans"),
            metrics.counter("entry_index_statuses"),
        );
        let (store, executor) = context.storage_read()?;
        let (store, executor) = (store.clone(), executor.clone());
        let projection = Arc::clone(&self.projection);
        let access = self.access.clone();
        let limit = self.limit;
        let mut stream = RecordBatchReceiverStream::builder(Arc::clone(&projection), 1);
        let tx = stream.tx();
        stream.spawn(async move {
            if limit == Some(0) {
                return Ok(());
            }
            let filter = match access {
                // The cursor intersects the selection with the snapshot's ownership.
                StatusAccess::Scan(request) => InvocationStatusScan::Keys(request),
                StatusAccess::Keys(keys) => {
                    let ownership = store.partition_key_range();
                    if keys
                        .iter()
                        .any(|id| !ownership.contains(&id.partition_key()))
                    {
                        return datafusion::common::exec_err!(
                            "primary keys are outside their partition"
                        );
                    }
                    InvocationStatusScan::Ids(keys.iter().copied().collect())
                }
                StatusAccess::EntryIndex { index, fallback } => {
                    // Why a read scanned, and how much of the index a read
                    // examined: the runs it merged and the keys it went through.
                    let (not_migrated, not_complete, too_many_runs) = (
                        metrics.counter("entry_index_scans_not_migrated"),
                        metrics.counter("entry_index_scans_not_complete"),
                        metrics.counter("entry_index_scans_too_many_runs"),
                    );
                    let runs = metrics.counter("entry_index_runs");
                    let examined = metrics.counter("entry_index_keys_examined");
                    InvocationStatusScan::EntryIndex {
                        index,
                        fallback,
                        execution: Arc::new(EntryIndexExecution::new(
                            memory,
                            move |path| {
                                match path {
                                    EntryIndexPath::Index { runs: merged } => {
                                        index_reads.add(1);
                                        runs.add(merged);
                                    }
                                    EntryIndexPath::Scan(reason) => {
                                        index_scans.add(1);
                                        match reason {
                                            ScanReason::NotMigrated => not_migrated.add(1),
                                            ScanReason::NotComplete => not_complete.add(1),
                                            ScanReason::TooManyRuns => too_many_runs.add(1),
                                        }
                                    }
                                }
                                if let Some(chosen) = chosen {
                                    let _ = chosen.send(path);
                                }
                            },
                            move |statuses, keys| {
                                index_statuses.add(statuses);
                                examined.add(usize::try_from(keys).unwrap_or(usize::MAX));
                            },
                        )),
                    }
                }
            };
            let entries = row::reads_entries(&projection);
            let sink = BatchSink::new(
                Batches::<InvocationStatus>::new(
                    projection,
                    batch_size,
                    limit,
                    metrics.elapsed_compute,
                ),
                tx.clone(),
            );
            // Only a read that shows or filters on an entry's column reads the
            // entries, in step with the statuses.
            if entries {
                scan_rows_into::<InvocationStatusEntryRecords, InvocationStatus, _>(
                    store,
                    &executor,
                    Arc::new(filter),
                    sink,
                    |(id, status, entry), batches: &mut Batches<InvocationStatus>| {
                        append_status(batches, id, &status, entry)
                    },
                )
                .await
            } else {
                scan_rows_into::<InvocationStatusRecords, InvocationStatus, _>(
                    store,
                    &executor,
                    Arc::new(filter),
                    sink,
                    |(id, status), batches: &mut Batches<InvocationStatus>| {
                        append_status(batches, id, &status, None)
                    },
                )
                .await
            }
        });
        Ok(stream.build())
    }
}

fn append_status(
    batches: &mut Batches<InvocationStatus>,
    id: InvocationId,
    status: &InvocationStatusV2Lazy<'_>,
    entry: Option<InvocationEntry>,
) -> restate_storage_api::Result<()> {
    batches.set_record(&id);
    if let ControlFlow::Break(result) = batches.append_with(|builder| {
        row::append_row(builder, id, status, entry)
            .map_err(|error| restate_storage_api::StorageError::Conversion(error.into()))
    }) {
        result?;
    }
    Ok(())
}
