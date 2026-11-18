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
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::PhysicalExpr;
use datafusion::physical_plan::metrics::Count;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;

use restate_partition_store::ReadSnapshot;
use restate_partition_store::keys::selection::KeySelection;
use restate_partition_store::vqueue_table::{
    EntryStatusCursor, EntryStatusKey, EntryStatusKeys, EntryStatusRecords, EntryStatusScan,
    StageReads,
};
use restate_storage_api::vqueue_table::Stage;
use restate_storage_api::vqueue_table::filters::ScanEntryIdFilter;
use restate_storage_query_api::scan::{CursorScan, CursorStep, ReadCursor, Table};
use restate_storage_query_api::schema::index::index_restriction;
use restate_storage_query_api::selection::domain::Domain;
use restate_storage_query_api::selection::{Selection, ValuePredicate};
use restate_storage_query_api::tables::index::{EntryByQueue, EntryByQueueDomains};
use restate_storage_query_api::tables::vqueue_entry_status::VqueueEntryStatus;
use restate_storage_query_api::{LogicalSchema, RowSink, SelectionSchema};
use restate_types::SemanticRestateVersion;
use restate_types::identifiers::{BaseEntryId, PartitionKey};
use restate_types::sharding::KeyRange;
use restate_types::vqueues::{EntryId, EntryKind};

use crate::access::{CompleteIndex, PrimaryKeys, PrimaryRead, PrimarySelection};
use crate::batches::{BatchSink, Batches, scan_rows_into};
use crate::schema_scan::compile_selection;
use crate::table_providers::{
    PartitionReadContext, PreparedPartitionRead, ReadMetrics, ScanPartition,
};

use super::row::append_vqueue_entry_status_row;

/// Complete physical locators, independently of the SQL string representation.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, bilrost::Message)]
struct EntryKey {
    #[bilrost(1)]
    partition_key: PartitionKey,
    #[bilrost(2)]
    kind: EntryKind,
    #[bilrost(tag(3), encoding(plainbytes))]
    remainder: [u8; 16],
}

#[derive(bilrost::Message)]
struct EntryKeys {
    #[bilrost(1)]
    keys: Vec<EntryKey>,
}

#[derive(Debug)]
pub(crate) struct EntrySelection(pub(crate) Vec<BaseEntryId>);

/// The scheduler's stage tables, read as `_idx_entry_by_queue`. They hold exactly
/// one key per current entry status, maintained unconditionally by the scheduler.
pub(crate) struct StageIndex;

impl CompleteIndex for StageIndex {
    type Index = EntryByQueue;
    /// The stage tables are always maintained; V2 reads them as an index from
    /// v1.8.2, its first release.
    const INTRODUCED: SemanticRestateVersion = SemanticRestateVersion::new(1, 8, 2);

    /// A conservative first policy: a finite set of queues, or a finite set of
    /// stages without `finished`. The finished stage retains most entries, so
    /// reading it through the index would fetch most statuses by random lookups.
    fn useful(restriction: &EntryByQueueDomains) -> bool {
        matches!(
            restriction.vqueue_id.to_predicate(),
            Some(ValuePredicate::In(_))
        ) || matches!(
            restriction.stage.to_predicate(),
            Some(ValuePredicate::In(stages)) if !stages.is_empty() && !stages.contains(&Stage::Finished)
        )
    }
}

pub(crate) fn select_primary(
    filters: &[Arc<dyn PhysicalExpr>],
) -> anyhow::Result<Option<Box<dyn PrimarySelection>>> {
    let branches = VqueueEntryStatus::analyze(filters, NonZeroUsize::new(16).unwrap())?;
    // Derive lookup keys inside each Boolean alternative before unioning. The
    // original canonical-ID predicate remains residual to check the incarnation;
    // non-point domains (including exclusions) cannot narrow base-key lookups.
    let keys = branches
        .into_iter()
        .map(|domains| {
            domains
                .entry_id
                .intersect(domains.canonical_id.map_values(|id| id.to_base_entry_id()))
        })
        .fold(Domain::empty(), Domain::union)
        .into_values();
    Ok(keys.map(|keys| Box::new(EntrySelection(keys)) as Box<dyn PrimarySelection>))
}

impl PrimarySelection for EntrySelection {
    fn within(&self, ownership: KeyRange) -> Option<PrimaryKeys> {
        let first = self
            .0
            .partition_point(|id| id.partition_key() < ownership.start());
        let end = self
            .0
            .partition_point(|id| id.partition_key() <= ownership.end());
        (first < end).then(|| encode_keys(&self.0[first..end]))
    }
}

pub(crate) fn encode_keys(ids: &[BaseEntryId]) -> PrimaryKeys {
    let keys = ids
        .iter()
        .map(|id| EntryKey {
            partition_key: id.partition_key(),
            kind: id.kind(),
            remainder: *id.as_entry_id().remainder_bytes(),
        })
        .collect();
    PrimaryKeys {
        count: ids.len() as u64,
        encoded: EntryKeys { keys }.encode_to_vec(),
    }
}

pub(crate) fn decode_keys(keys: &PrimaryKeys) -> anyhow::Result<Vec<BaseEntryId>> {
    keys.validate()?;
    let decoded = EntryKeys::decode(keys.encoded.as_slice())?;
    anyhow::ensure!(
        decoded.keys.len() == keys.len()
            && decoded.keys.windows(2).all(|pair| pair[0] < pair[1])
            && decoded
                .keys
                .iter()
                .all(|key| matches!(key.kind, EntryKind::Invocation | EntryKind::StateMutation)),
        "invalid entry primary key set"
    );
    Ok(decoded
        .keys
        .into_iter()
        .map(|key| BaseEntryId::new(key.partition_key, EntryId::new(key.kind, key.remainder)))
        .collect())
}

#[derive(derive_more::Debug)]
pub(crate) struct EntryStatusScanner;

impl ScanPartition for EntryStatusScanner {
    fn supports_primary_lookup(&self) -> bool {
        true
    }

    fn supports_index(&self, index: &str) -> bool {
        index == EntryByQueue::NAME
    }

    fn prepare_read(
        self: Arc<Self>,
        access: PrimaryRead,
        projection: SchemaRef,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        limit: Option<usize>,
    ) -> anyhow::Result<Arc<dyn PreparedPartitionRead>> {
        let access = match access {
            PrimaryRead::Scan => EntryStatusAccess::Scan(Arc::new(EntryStatusKeys::prepare(
                &compile_selection::<VqueueEntryStatus>(predicate.as_ref())?,
                crate::selection::key_range(&VqueueEntryStatus::candidate_shard_keys(
                    predicate.as_slice(),
                )?),
            )?)),
            PrimaryRead::Index(index) => {
                anyhow::ensure!(self.supports_index(&index), "unsupported index '{index}'");
                // The planner's choice is fixed; the restriction is derived again
                // from the shipped predicate, and the predicate stays residual.
                let selection = index_restriction::<EntryByQueue>(predicate.as_slice())?
                    .map_or(Selection::Empty, |domains| domains.to_selection());
                EntryStatusAccess::Stages(Arc::new(StageReads::prepare(&selection)?))
            }
            PrimaryRead::MultiGet(keys) => EntryStatusAccess::MultiGet(decode_keys(&keys)?.into()),
        };
        Ok(Arc::new(EntryStatusRead {
            projection,
            access,
            limit,
        }))
    }
}

#[derive(derive_more::Debug)]
struct EntryStatusRead {
    projection: SchemaRef,
    access: EntryStatusAccess,
    limit: Option<usize>,
}

#[derive(derive_more::Debug, Clone)]
enum EntryStatusAccess {
    Scan(#[debug(skip)] Arc<KeySelection<EntryStatusKey>>),
    MultiGet(Arc<[BaseEntryId]>),
    Stages(#[debug(skip)] Arc<StageReads>),
}

impl PreparedPartitionRead for EntryStatusRead {
    fn capture_requirements(&self) -> restate_worker_api::PartitionCaptureRequirements {
        restate_worker_api::PartitionCaptureRequirements {
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
        anyhow::ensure!(batch_size > 0, "batch size must be positive");
        let index_metrics = matches!(self.access, EntryStatusAccess::Stages(_))
            .then(|| IndexMetrics::new(&metrics));
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
                EntryStatusAccess::MultiGet(keys) => {
                    let ownership = store.partition_key_range();
                    if keys
                        .iter()
                        .any(|id| !ownership.contains(&id.partition_key()))
                    {
                        return datafusion::common::exec_err!(
                            "primary keys are outside their partition"
                        );
                    }
                    EntryStatusScan::Filter(ScanEntryIdFilter::EntryIdSet(
                        keys.iter().copied().collect(),
                    ))
                }
                // The cursor intersects the selection with the snapshot's ownership.
                EntryStatusAccess::Scan(request) => EntryStatusScan::Keys(request),
                EntryStatusAccess::Stages(reads) => EntryStatusScan::Stages(reads),
            };
            scan_rows_into::<CountedEntryStatusRecords, VqueueEntryStatus, _>(
                store,
                &executor,
                Arc::new(CountedEntryStatusScan {
                    scan: filter,
                    metrics: index_metrics,
                }),
                BatchSink::new(
                    Batches::<VqueueEntryStatus>::new(
                        projection,
                        batch_size,
                        limit,
                        metrics.elapsed_compute,
                    ),
                    tx.clone(),
                ),
                move |(id, header), batches: &mut Batches<VqueueEntryStatus>| {
                    if let ControlFlow::Break(result) = batches.append_with(|builder| {
                        append_vqueue_entry_status_row(builder, &id, &header);
                        Ok(())
                    }) {
                        result?;
                    }
                    Ok(())
                },
            )
            .await
        });
        Ok(stream.build())
    }
}

/// Leaf metrics of an index read. Rows produced are the leaf's `output_rows`.
#[derive(Clone)]
pub(crate) struct IndexMetrics {
    /// Index keys evaluated by navigation, including rejected keys.
    keys_visited: Count,
    /// Index keys accepted by navigation: references to primary records.
    references: Count,
    /// Distinct primary keys requested by lookups.
    primary_fetched: Count,
    /// References without a primary record.
    primary_missing: Count,
    /// References whose primary record names another tuple or incarnation.
    primary_mismatched: Count,
}

impl IndexMetrics {
    fn new(metrics: &ReadMetrics) -> Self {
        Self {
            keys_visited: metrics.counter("index_keys_visited"),
            references: metrics.counter("index_references"),
            primary_fetched: metrics.counter("primary_keys_fetched"),
            primary_missing: metrics.counter("primary_missing"),
            primary_mismatched: metrics.counter("primary_mismatched"),
        }
    }
}

/// Records an index read's work when the cursor is dropped, which also covers
/// reads stopped early by a limit, a closed consumer or cancellation.
pub(crate) struct CountedCursor {
    cursor: EntryStatusCursor,
    metrics: Option<IndexMetrics>,
}

impl Drop for CountedCursor {
    fn drop(&mut self) {
        if let (Some(metrics), Some(counters)) = (&self.metrics, self.cursor.stage_counters()) {
            metrics.keys_visited.add(counters.visited as usize);
            metrics.references.add(counters.references as usize);
            metrics.primary_fetched.add(counters.fetched as usize);
            metrics.primary_missing.add(counters.missing as usize);
            metrics.primary_mismatched.add(counters.mismatched as usize);
        }
    }
}

impl ReadCursor<CountedEntryStatusRecords> for CountedCursor {
    fn advance(&mut self) -> restate_storage_api::Result<CursorStep> {
        self.cursor.advance()
    }

    fn current(&self) -> restate_storage_api::Result<<EntryStatusRecords as Table>::Record<'_>> {
        self.cursor.current()
    }
}

/// Entry statuses read through a [`CountedCursor`], so that the query IO pool's
/// worker, which opens and drives the cursor, still records an index read's
/// counters when it drops it.
pub(crate) struct CountedEntryStatusRecords;

impl Table for CountedEntryStatusRecords {
    type Record<'a> = <EntryStatusRecords as Table>::Record<'a>;
    type Scan = CountedEntryStatusScan;
}

/// An entry-status request, with the metrics its cursor records.
#[derive(Clone)]
pub(crate) struct CountedEntryStatusScan {
    scan: EntryStatusScan,
    metrics: Option<IndexMetrics>,
}

impl CursorScan<CountedEntryStatusRecords> for ReadSnapshot {
    type Cursor = CountedCursor;

    fn scan_cursor(
        &self,
        request: Arc<CountedEntryStatusScan>,
    ) -> restate_storage_api::Result<CountedCursor> {
        let CountedEntryStatusScan { scan, metrics } = Arc::unwrap_or_clone(request);
        Ok(CountedCursor {
            cursor: self.entry_status_cursor(scan)?,
            metrics,
        })
    }
}
