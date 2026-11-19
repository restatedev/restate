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
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::conjunction_opt;
use datafusion::physical_expr::expressions::{BinaryExpr, Column, InListExpr, Literal};
use datafusion::physical_expr::utils::{collect_columns, split_conjunction};
use datafusion::physical_plan::PhysicalExpr;
use datafusion::physical_plan::metrics::Count;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use strum::VariantArray;

use restate_partition_store::ReadSnapshot;
use restate_partition_store::index::RegisteredIndex;
use restate_partition_store::invocation_status_table::{
    EntryIndexExecution, EntryIndexPath, ScanReason,
};
use restate_partition_store::keys::selection::KeySelection;
use restate_partition_store::vqueue_table::{
    ActiveEntryByStage as StoredActiveIndex, ActiveRead, EntryStatusCursor, EntryStatusKey,
    EntryStatusKeys, EntryStatusRecords, EntryStatusScan,
    FinishedEntryByOutcome as StoredFinishedIndex, FinishedRead, StagePrefilter, StageReads,
    StageRowRecords, StageRowScan, StageRowsCursor,
};
use restate_storage_api::vqueue_table::Stage;
use restate_storage_api::vqueue_table::filters::ScanEntryIdFilter;
use restate_storage_query_api::scan::{CursorScan, CursorStep, ReadCursor, Table};
use restate_storage_query_api::schema::index::index_restriction;
use restate_storage_query_api::selection::domain::Domain;
use restate_storage_query_api::selection::{Selection, ValuePredicate};
use restate_storage_query_api::tables::index::{
    ActiveEntryByStage, ActiveEntryByStageDomains, EntryByQueue, EntryByQueueDomains,
    FinishedEntryByOutcome, FinishedEntryByOutcomeDomains,
};
use restate_storage_query_api::tables::vqueue_entry_status::{
    VqueueEntryStatus, VqueueEntryStatusColumn, VqueueEntryStatusDomains,
};
use restate_storage_query_api::{LogicalSchema, RowSink, SelectionSchema};
use restate_types::SemanticRestateVersion;
use restate_types::config::Configuration;
use restate_types::identifiers::{BaseEntryId, PartitionKey};
use restate_types::sharding::KeyRange;
use restate_types::vqueues::{EntryId, EntryKind};

use crate::access::{
    CompleteIndex, IndexOrder, PrimaryKeys, PrimaryRead, PrimarySelection, covers,
};
use crate::batches::{BatchSink, Batches, scan_rows_into};
use crate::distributed::emission::emission_filter;
use crate::schema_scan::compile_selection;
use crate::table_providers::{
    InOrder, PartitionReadContext, PreparedPartitionRead, ReadMetrics, ScanPartition,
};

use super::row::{
    EntryPlace, EntryState, append_entry_row, append_vqueue_entry_status_row, covered_row,
};

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

/// The columns a stage record holds exactly as its entry status does: its
/// key's, and its value's status and statistics, which the scheduler writes
/// with every stage change. Not the metadata (deployment, needed memory, retry
/// counts): the scheduler doesn't rewrite the record when only that changes.
const STAGE_COVERS: &[VqueueEntryStatusColumn] = &[
    VqueueEntryStatusColumn::PartitionKey,
    VqueueEntryStatusColumn::EntryId,
    VqueueEntryStatusColumn::VqueueId,
    VqueueEntryStatusColumn::Stage,
    VqueueEntryStatusColumn::Status,
    VqueueEntryStatusColumn::HasLock,
    VqueueEntryStatusColumn::NextAt,
    VqueueEntryStatusColumn::SequenceNumber,
    VqueueEntryStatusColumn::EntryKind,
    VqueueEntryStatusColumn::CreatedAt,
    VqueueEntryStatusColumn::TransitionedAt,
    VqueueEntryStatusColumn::NumAttempts,
    VqueueEntryStatusColumn::NumErrors,
    VqueueEntryStatusColumn::NumPauses,
    VqueueEntryStatusColumn::NumSuspensions,
    VqueueEntryStatusColumn::NumYields,
    VqueueEntryStatusColumn::FirstAttemptAt,
    VqueueEntryStatusColumn::LatestAttemptAt,
    VqueueEntryStatusColumn::FirstRunnableAt,
    VqueueEntryStatusColumn::LatestAttemptBlockedOnInvokerConcurrency,
    VqueueEntryStatusColumn::LatestAttemptBlockedOnThrottlingRules,
    VqueueEntryStatusColumn::LatestAttemptBlockedOnInvokerThrottling,
    VqueueEntryStatusColumn::LatestAttemptBlockedOnInvokerMemory,
    VqueueEntryStatusColumn::LatestAttemptBlockedOnConcurrencyRules,
    VqueueEntryStatusColumn::LatestAttemptBlockedOnLock,
    VqueueEntryStatusColumn::LatestAttemptBlockedOnDeploymentConcurrency,
    VqueueEntryStatusColumn::TotalBlockedOnInvokerConcurrency,
    VqueueEntryStatusColumn::TotalBlockedOnThrottlingRules,
    VqueueEntryStatusColumn::TotalBlockedOnInvokerThrottling,
    VqueueEntryStatusColumn::TotalBlockedOnInvokerMemory,
    VqueueEntryStatusColumn::TotalBlockedOnConcurrencyRules,
    VqueueEntryStatusColumn::TotalBlockedOnLock,
    VqueueEntryStatusColumn::TotalBlockedOnDeploymentConcurrency,
    VqueueEntryStatusColumn::CanonicalId,
];

/// Of [`STAGE_COVERS`], the columns of the stage key: a covered read of only
/// these decodes no stage value.
const STAGE_KEY_COLUMNS: &[VqueueEntryStatusColumn] = &[
    VqueueEntryStatusColumn::PartitionKey,
    VqueueEntryStatusColumn::EntryId,
    VqueueEntryStatusColumn::VqueueId,
    VqueueEntryStatusColumn::Stage,
    VqueueEntryStatusColumn::HasLock,
    VqueueEntryStatusColumn::NextAt,
    VqueueEntryStatusColumn::SequenceNumber,
    VqueueEntryStatusColumn::EntryKind,
    VqueueEntryStatusColumn::CanonicalId,
];

/// The query's conditions on columns a stage record holds exactly
/// ([`STAGE_COVERS`]), as a check of the record before its status is fetched:
/// only the conjuncts that read no other column are checked, which every row
/// the query keeps passes. `None` without such conditions, or if any part of
/// the predicate, over `schema`, could fail: a rejected record must still reach
/// a failing condition, as it does when every status is fetched (see
/// [`emission`](crate::distributed::emission)). `None` too if none of them
/// reads the record's value: navigation applies most conditions on the key
/// (queue, stage, lock, run time, sequence, IDs), so such a check would decode
/// every value to reject little more. One navigation doesn't apply, such as on
/// the entry kind, is then left to the fetched statuses.
pub(super) fn covered_prefilter(
    predicate: Option<&Arc<dyn PhysicalExpr>>,
    schema: &Schema,
) -> anyhow::Result<Option<StagePrefilter>> {
    let Some(predicate) =
        predicate.filter(|predicate| emission_filter(predicate, schema).is_some())
    else {
        return Ok(None);
    };
    let names = |columns: &[VqueueEntryStatusColumn]| -> Vec<&str> {
        columns
            .iter()
            .filter_map(|&column| VqueueEntryStatus::sql_name(column))
            .collect()
    };
    let (covered, keys) = (names(STAGE_COVERS), names(STAGE_KEY_COLUMNS));
    let conjuncts: Vec<_> = split_conjunction(predicate)
        .into_iter()
        .filter(|conjunct| {
            collect_columns(conjunct)
                .iter()
                .all(|column| covered.contains(&column.name()))
        })
        .cloned()
        .collect();
    let reads_values = conjuncts.iter().any(|conjunct| {
        collect_columns(conjunct)
            .iter()
            .any(|column| !keys.contains(&column.name()))
    });
    let Some(conjuncts) = conjunction_opt(conjuncts).filter(|_| reads_values) else {
        return Ok(None);
    };
    let selection = compile_selection::<VqueueEntryStatus>(Some(&conjuncts))?;
    if matches!(selection, Selection::All) {
        return Ok(None);
    }
    Ok(Some(Arc::new(move |id, row| {
        // Prefiltered reads always decode the value.
        let Some(value) = &row.value else {
            return true;
        };
        let place = EntryPlace {
            qid: &row.qid,
            stage: row.stage,
            has_lock: row.has_lock,
            next_run_at: row.run_at,
            seq: row.seq,
        };
        let state = EntryState {
            status: value.status,
            metadata: &value.metadata,
            stats: &value.stats,
        };
        covered_row(id, &place, &state).matches(&selection)
    })))
}

/// The scheduler's stage tables, read as `_idx_entry_by_queue` for a finite set
/// of queues. They hold exactly one key per current entry status, maintained
/// unconditionally by the scheduler. A conservative policy: a queue's entries
/// are taken to be few, which is not a measurement.
pub(crate) struct QueueIndex;

impl CompleteIndex for QueueIndex {
    type Index = EntryByQueue;
    /// The stage tables are always maintained; V2 reads them as an index from
    /// v1.8.2, its first release.
    const INTRODUCED: SemanticRestateVersion = SemanticRestateVersion::new(1, 8, 2);

    const COVERS: &'static [VqueueEntryStatusColumn] = STAGE_COVERS;

    fn useful(restriction: &EntryByQueueDomains) -> bool {
        matches!(
            restriction.vqueue_id.to_predicate(),
            Some(ValuePredicate::In(_))
        )
    }
}

/// The stage tables again, for a finite set of stages, when no other index fits
/// ([`ActiveIndex`] reads active stages in creation order). A read that fetches
/// statuses avoids `finished`, which retains most entries and would be fetched
/// by random lookups; a covered read fetches none, so it reads any stages. A
/// query without a stage or queue restriction scans the statuses, as before:
/// the stage tables hold the same entries, but only under the scheduler's
/// stage/status correspondence, which every stage-table read relies on.
pub(crate) struct StageIndex;

impl CompleteIndex for StageIndex {
    type Index = EntryByQueue;
    const INTRODUCED: SemanticRestateVersion = QueueIndex::INTRODUCED;

    const COVERS: &'static [VqueueEntryStatusColumn] = STAGE_COVERS;

    fn useful(restriction: &EntryByQueueDomains) -> bool {
        matches!(
            restriction.stage.to_predicate(),
            Some(ValuePredicate::In(stages)) if !stages.is_empty() && !stages.contains(&Stage::Finished)
        )
    }

    fn useful_covered(restriction: &EntryByQueueDomains) -> bool {
        // An unrestricted stage is every value the column can take.
        matches!(
            restriction.stage.to_predicate(),
            Some(ValuePredicate::In(stages))
                if !stages.is_empty() && stages.len() < Stage::VARIANTS.len()
        )
    }
}

/// Finished entries through `finished_entry_by_outcome/1`, newest finished
/// first, or oldest finished first, for a query that asks for exactly that
/// order under a LIMIT:
///
/// ```sql
/// SELECT … FROM sys_vqueue_entry_status
/// WHERE stage = 'finished' [AND status IN ('failed', 'killed')]
/// ORDER BY transitioned_at DESC [NULLS FIRST | NULLS LAST]
/// LIMIT 50
/// ```
///
/// The index holds only Finished entries, so every alternative of the query
/// must fix the stage to `finished` ([`Self::admits`]); a status condition
/// narrows the runs it reads but proves nothing alone. Ascending, the index is
/// read backwards. Another order, or a condition outside the allowlist below,
/// reads as before.
pub(crate) struct FinishedIndex;

impl CompleteIndex for FinishedIndex {
    type Index = FinishedEntryByOutcome;
    const INTRODUCED: SemanticRestateVersion = <StoredFinishedIndex as RegisteredIndex>::INTRODUCED;

    /// The read merges its runs by the time each entry entered Finished, newest
    /// first. The index keeps the full timestamp, whose millisecond is the
    /// entry's `transitioned_at`: the read checks that for every row.
    ///
    /// `transitioned_at` is never NULL: every entry has a transition time from
    /// its creation on, and the row writer always sets it (`row.rs`).
    const ORDERING: &'static [IndexOrder<VqueueEntryStatusColumn>] = &[IndexOrder {
        column: VqueueEntryStatusColumn::TransitionedAt,
        options: SortOptions {
            descending: true,
            nulls_first: false,
        },
        never_null: true,
    }];

    /// Where the index isn't maintained, every partition would scan and sort
    /// instead, so the planner then scans.
    fn useful(_: &FinishedEntryByOutcomeDomains) -> bool {
        Configuration::pinned()
            .common
            .experimental
            .is_track_entry_indexes_enabled()
    }

    /// Every alternative fixes the stage to `finished` and nothing else.
    fn admits(alternatives: &[VqueueEntryStatusDomains]) -> bool {
        !alternatives.is_empty()
            && alternatives
                .iter()
                .all(|alternative| match alternative.stage.to_predicate() {
                    Some(ValuePredicate::Equal(stage)) => stage == Stage::Finished,
                    Some(ValuePredicate::In(stages)) => {
                        !stages.is_empty() && stages.iter().all(|stage| *stage == Stage::Finished)
                    }
                    _ => false,
                })
    }

    const NEEDS_ORDERED_DEMAND: bool = true;

    /// The allowlist: the stage, the status, and the transition time. A
    /// syntactic choice, not a selectivity proof: a status condition can still
    /// reject every candidate.
    fn residual_allowed(conjunct: &Arc<dyn PhysicalExpr>) -> bool {
        ordered_condition(conjunct, VqueueEntryStatusColumn::TransitionedAt, true)
    }
}

/// Active entries, those in Inbox, Running, Suspended or Paused, through
/// `active_entry_by_stage/1`, newest created first, or oldest created first,
/// for a query that asks for exactly that order under a LIMIT:
///
/// ```sql
/// SELECT … FROM sys_vqueue_entry_status
/// WHERE stage IN ('inbox', 'paused') [AND status = 'backing-off']
/// ORDER BY created_at DESC [NULLS FIRST | NULLS LAST]
/// LIMIT 50
/// ```
///
/// The index holds only active entries, so every alternative of the query must
/// fix the stage to some of those stages ([`Self::admits`]); a status condition
/// is checked on each row and proves nothing. Ascending, the index is read
/// backwards. Another order, or a condition outside the allowlist below, reads
/// as before.
pub(crate) struct ActiveIndex;

impl CompleteIndex for ActiveIndex {
    type Index = ActiveEntryByStage;
    const INTRODUCED: SemanticRestateVersion = <StoredActiveIndex as RegisteredIndex>::INTRODUCED;

    /// The read merges its runs by creation time, newest first. The index
    /// keeps the full timestamp, whose millisecond is the entry's `created_at`:
    /// the read checks that for every row.
    ///
    /// `created_at` is never NULL: every entry has a creation time, and the row
    /// writer always sets it (`row.rs`).
    const ORDERING: &'static [IndexOrder<VqueueEntryStatusColumn>] = &[IndexOrder {
        column: VqueueEntryStatusColumn::CreatedAt,
        options: SortOptions {
            descending: true,
            nulls_first: false,
        },
        never_null: true,
    }];

    /// Where the index isn't maintained, every partition would scan and sort
    /// instead, so the planner then scans.
    fn useful(_: &ActiveEntryByStageDomains) -> bool {
        Configuration::pinned()
            .common
            .experimental
            .is_track_entry_indexes_enabled()
    }

    /// Every alternative fixes the stage to a non-empty set of the stages the
    /// index holds, as the partition store defines them. A domain of points,
    /// even one, is an `IN` set.
    fn admits(alternatives: &[VqueueEntryStatusDomains]) -> bool {
        !alternatives.is_empty()
            && alternatives.iter().all(|alternative| {
                matches!(
                    alternative.stage.to_predicate(),
                    Some(ValuePredicate::In(stages))
                        if !stages.is_empty()
                            && stages.iter().all(|stage| StoredActiveIndex::holds(*stage))
                )
            })
    }

    const NEEDS_ORDERED_DEMAND: bool = true;

    /// The allowlist: the stage, the status, and the creation time.
    fn residual_allowed(conjunct: &Arc<dyn PhysicalExpr>) -> bool {
        ordered_condition(conjunct, VqueueEntryStatusColumn::CreatedAt, true)
    }
}

/// Whether `condition` is on the stage or status by `=` or `IN`, or compares
/// `time`, the column an ordered entry index sorts by. `times` admits the time
/// comparisons; not inside alternatives, which would leave gaps to visit. See
/// [`FinishedIndex::residual_allowed`].
fn ordered_condition(
    condition: &Arc<dyn PhysicalExpr>,
    time: VqueueEntryStatusColumn,
    times: bool,
) -> bool {
    use VqueueEntryStatusColumn::{Stage as StageColumn, Status};
    let column = |expr: &Arc<dyn PhysicalExpr>| {
        let name = expr.downcast_ref::<Column>()?.name();
        [StageColumn, Status, time]
            .into_iter()
            .find(|column| VqueueEntryStatus::sql_name(*column) == Some(name))
    };
    let literal = |expr: &Arc<dyn PhysicalExpr>| expr.downcast_ref::<Literal>().is_some();
    if let Some(binary) = condition.downcast_ref::<BinaryExpr>() {
        if *binary.op() == Operator::Or {
            // A small IN list arrives as alternatives.
            return ordered_condition(binary.left(), time, false)
                && ordered_condition(binary.right(), time, false);
        }
        let (column, op) = if literal(binary.right()) {
            (column(binary.left()), Some(*binary.op()))
        } else if literal(binary.left()) {
            (column(binary.right()), binary.op().swap())
        } else {
            return false;
        };
        return match (column, op) {
            (Some(StageColumn | Status), Some(op)) => op == Operator::Eq,
            (Some(_), Some(op)) => {
                times
                    && matches!(
                        op,
                        Operator::Eq
                            | Operator::Lt
                            | Operator::LtEq
                            | Operator::Gt
                            | Operator::GtEq
                    )
            }
            _ => false,
        };
    }
    if let Some(list) = condition.downcast_ref::<InListExpr>() {
        return !list.negated()
            && matches!(column(list.expr()), Some(StageColumn | Status))
            && list.list().iter().all(literal);
    }
    false
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
            || index == FinishedEntryByOutcome::NAME
            || index == ActiveEntryByStage::NAME
    }

    fn prepare_read(
        self: Arc<Self>,
        access: PrimaryRead,
        projection: SchemaRef,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        limit: Option<usize>,
    ) -> anyhow::Result<Arc<dyn PreparedPartitionRead>> {
        // The scan of the predicate's keys: a scan's access, and an index read's
        // fallback, which reads the same rows.
        let scan = || -> anyhow::Result<_> {
            Ok(Arc::new(EntryStatusKeys::prepare(
                &compile_selection::<VqueueEntryStatus>(predicate.as_ref())?,
                crate::selection::key_range(&VqueueEntryStatus::candidate_shard_keys(
                    predicate.as_slice(),
                )?),
            )?))
        };
        let access = match access {
            PrimaryRead::Scan => EntryStatusAccess::Scan(scan()?),
            PrimaryRead::Index(index) if index.name() == FinishedEntryByOutcome::NAME => {
                // The planner's choice is fixed; the restriction is derived again
                // from the shipped predicate, and the predicate stays residual. A
                // partition that can't read the index scans with the same one.
                let selection = index_restriction::<FinishedEntryByOutcome>(predicate.as_slice())?
                    .map_or(Selection::Empty, |domains| domains.to_selection());
                EntryStatusAccess::Finished {
                    read: Arc::new(FinishedRead::prepare(
                        &selection,
                        index.direction::<FinishedIndex>()?,
                    )?),
                    fallback: scan()?,
                }
            }
            PrimaryRead::Index(index) if index.name() == ActiveEntryByStage::NAME => {
                // As for finished entries.
                let selection = index_restriction::<ActiveEntryByStage>(predicate.as_slice())?
                    .map_or(Selection::Empty, |domains| domains.to_selection());
                EntryStatusAccess::Active {
                    read: Arc::new(ActiveRead::prepare(
                        &selection,
                        index.direction::<ActiveIndex>()?,
                    )?),
                    fallback: scan()?,
                }
            }
            PrimaryRead::Index(index) => {
                anyhow::ensure!(
                    self.supports_index(index.name()),
                    "unsupported index '{}'",
                    index.name()
                );
                // The planner's choice is fixed; the restriction is derived again
                // from the shipped predicate, and the predicate stays residual.
                let selection = index_restriction::<EntryByQueue>(predicate.as_slice())?
                    .map_or(Selection::Empty, |domains| domains.to_selection());
                let reads = StageReads::prepare(&selection)?;
                if covers::<StageIndex>(&projection) {
                    // The stage records alone, values only for columns beyond
                    // their keys.
                    let values = projection.fields().iter().any(|field| {
                        !STAGE_KEY_COLUMNS.iter().any(|&column| {
                            VqueueEntryStatus::sql_name(column) == Some(field.name())
                        })
                    });
                    EntryStatusAccess::Covered(StageRowScan {
                        reads: Arc::new(reads),
                        values,
                    })
                } else {
                    // Fetched, after a check of the conditions the records
                    // answer themselves.
                    let reads = match covered_prefilter(predicate.as_ref(), &projection)? {
                        Some(prefilter) => reads.with_prefilter(prefilter),
                        None => reads,
                    };
                    EntryStatusAccess::Stages(Arc::new(reads))
                }
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
    /// Statuses referenced by the stage tables, fetched.
    Stages(#[debug(skip)] Arc<StageReads>),
    /// The stage tables alone, for a query they cover ([`StageIndex::COVERS`]).
    Covered(#[debug(skip)] StageRowScan),
    /// Finished entries through their index, or the fallback scan where a
    /// partition can't read it.
    Finished {
        #[debug(skip)]
        read: Arc<FinishedRead>,
        #[debug(skip)]
        fallback: Arc<KeySelection<EntryStatusKey>>,
    },
    /// Active entries through their index, or the fallback scan.
    Active {
        #[debug(skip)]
        read: Arc<ActiveRead>,
        #[debug(skip)]
        fallback: Arc<KeySelection<EntryStatusKey>>,
    },
}

impl EntryStatusAccess {
    /// An entry index read in index order, which a partition may scan instead.
    fn ordered(&self) -> bool {
        matches!(self, Self::Finished { .. } | Self::Active { .. })
    }
}

impl PreparedPartitionRead for EntryStatusRead {
    fn capture_requirements(&self) -> restate_worker_api::PartitionCaptureRequirements {
        restate_worker_api::PartitionCaptureRequirements {
            storage: true,
            ..Default::default()
        }
    }

    /// An entry index read that scans instead loses its order. The read tells
    /// which path it took as it opens, before its first row.
    fn execute_in_order(
        &self,
        context: PartitionReadContext,
        batch_size: usize,
        metrics: ReadMetrics,
    ) -> anyhow::Result<(SendableRecordBatchStream, InOrder)> {
        if !self.access.ordered() {
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

    fn execute(
        &self,
        context: PartitionReadContext,
        batch_size: usize,
        metrics: ReadMetrics,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        self.read(context, batch_size, metrics, None)
    }
}

impl EntryStatusRead {
    /// An entry index read tells `chosen` which path it takes.
    fn read(
        &self,
        context: PartitionReadContext,
        batch_size: usize,
        metrics: ReadMetrics,
        chosen: Option<tokio::sync::oneshot::Sender<EntryIndexPath>>,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        anyhow::ensure!(batch_size > 0, "batch size must be positive");
        let index_metrics = match &self.access {
            EntryStatusAccess::Stages(reads) if reads.prefiltered() => {
                Some(IndexMetrics::new(&metrics).with_rejections(&metrics))
            }
            EntryStatusAccess::Stages(_) => Some(IndexMetrics::new(&metrics)),
            _ => None,
        };
        let stage_metrics = matches!(self.access, EntryStatusAccess::Covered(_))
            .then(|| StageRowMetrics::new(&metrics));
        // What an entry index read did: its path and why, the runs it merged,
        // the index keys it went through, the statuses it fetched and the stale
        // references it skipped.
        let ordered = self.access.ordered().then(|| {
            let memory = context.retained_memory("entry index read");
            let reads = metrics.counter("entry_index_reads");
            let scans = metrics.counter("entry_index_scans");
            let not_complete = metrics.counter("entry_index_scans_not_complete");
            let too_many_runs = metrics.counter("entry_index_scans_too_many_runs");
            let runs = metrics.counter("entry_index_runs");
            let examined = metrics.counter("entry_index_keys_examined");
            let statuses = metrics.counter("entry_index_statuses");
            let skipped = metrics.counter("entry_index_skipped");
            Arc::new(
                EntryIndexExecution::new(
                    memory,
                    move |path| {
                        match path {
                            EntryIndexPath::Index { runs: merged } => {
                                reads.add(1);
                                runs.add(merged);
                            }
                            EntryIndexPath::Scan(reason) => {
                                scans.add(1);
                                match reason {
                                    ScanReason::NotComplete | ScanReason::NotMigrated => {
                                        not_complete.add(1)
                                    }
                                    ScanReason::TooManyRuns => too_many_runs.add(1),
                                }
                            }
                        }
                        if let Some(chosen) = chosen {
                            let _ = chosen.send(path);
                        }
                    },
                    move |fetched, keys| {
                        statuses.add(fetched);
                        examined.add(usize::try_from(keys).unwrap_or(usize::MAX));
                    },
                )
                .with_skipped(move |references| {
                    skipped.add(usize::try_from(references).unwrap_or(usize::MAX))
                }),
            )
        });
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
                EntryStatusAccess::Covered(scan) => {
                    let Some(counters) = stage_metrics else {
                        return datafusion::common::internal_err!("a covered read has no metrics");
                    };
                    return scan_rows_into::<CountedStageRowRecords, VqueueEntryStatus, _>(
                        store,
                        &executor,
                        Arc::new(CountedStageRowScan {
                            scan,
                            metrics: counters,
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
                        move |(id, row), batches: &mut Batches<VqueueEntryStatus>| {
                            let state = row.value.as_ref().map(|value| EntryState {
                                status: value.status,
                                metadata: &value.metadata,
                                stats: &value.stats,
                            });
                            let place = EntryPlace {
                                qid: &row.qid,
                                stage: row.stage,
                                has_lock: row.has_lock,
                                next_run_at: row.run_at,
                                seq: row.seq,
                            };
                            if let ControlFlow::Break(result) = batches.append_with(|builder| {
                                append_entry_row(builder, &id, &place, state.as_ref());
                                Ok(())
                            }) {
                                result?;
                            }
                            Ok(())
                        },
                    )
                    .await;
                }
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
                EntryStatusAccess::Finished { read, fallback } => {
                    let Some(execution) = ordered else {
                        return datafusion::common::internal_err!(
                            "an entry index read has no execution"
                        );
                    };
                    EntryStatusScan::Finished {
                        read,
                        fallback,
                        execution,
                    }
                }
                EntryStatusAccess::Active { read, fallback } => {
                    let Some(execution) = ordered else {
                        return datafusion::common::internal_err!(
                            "an entry index read has no execution"
                        );
                    };
                    EntryStatusScan::Active {
                        read,
                        fallback,
                        execution,
                    }
                }
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
    /// Index keys a prefilter rejected before their primary records were
    /// fetched, for reads with one.
    rejected: Option<Count>,
}

impl IndexMetrics {
    fn new(metrics: &ReadMetrics) -> Self {
        Self {
            keys_visited: metrics.counter("index_keys_visited"),
            references: metrics.counter("index_references"),
            primary_fetched: metrics.counter("primary_keys_fetched"),
            primary_missing: metrics.counter("primary_missing"),
            primary_mismatched: metrics.counter("primary_mismatched"),
            rejected: None,
        }
    }

    /// Also counts the keys a prefilter rejects.
    fn with_rejections(self, metrics: &ReadMetrics) -> Self {
        Self {
            rejected: Some(metrics.counter("index_keys_rejected")),
            ..self
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
            if let Some(rejected) = &metrics.rejected {
                rejected.add(counters.rejected as usize);
            }
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

/// Leaf metrics of a covered read of the stage tables. Rows produced are the
/// leaf's `output_rows`.
#[derive(Clone)]
pub(crate) struct StageRowMetrics {
    /// Stage keys evaluated by navigation, including rejected keys.
    keys_visited: Count,
    /// Stage values decoded.
    values_decoded: Count,
}

impl StageRowMetrics {
    fn new(metrics: &ReadMetrics) -> Self {
        Self {
            keys_visited: metrics.counter("stage_keys_visited"),
            values_decoded: metrics.counter("stage_values_decoded"),
        }
    }
}

/// Records a covered read's work when the cursor is dropped, which also covers
/// reads stopped early by a limit, a closed consumer or cancellation.
pub(crate) struct CountedStageRows {
    cursor: StageRowsCursor,
    metrics: StageRowMetrics,
}

impl Drop for CountedStageRows {
    fn drop(&mut self) {
        let counters = self.cursor.counters();
        let count = |value: u64| usize::try_from(value).unwrap_or(usize::MAX);
        self.metrics.keys_visited.add(count(counters.visited));
        self.metrics.values_decoded.add(count(counters.values));
    }
}

impl ReadCursor<CountedStageRowRecords> for CountedStageRows {
    fn advance(&mut self) -> restate_storage_api::Result<CursorStep> {
        self.cursor.advance()
    }

    fn current(&self) -> restate_storage_api::Result<<StageRowRecords as Table>::Record<'_>> {
        self.cursor.current()
    }
}

/// Stage records read through [`CountedStageRows`], so that the query IO pool's
/// worker, which opens and drives the cursor, records the read's counters when
/// it drops it.
pub(crate) struct CountedStageRowRecords;

impl Table for CountedStageRowRecords {
    type Record<'a> = <StageRowRecords as Table>::Record<'a>;
    type Scan = CountedStageRowScan;
}

/// A covered stage-record request, with the metrics its cursor records.
pub(crate) struct CountedStageRowScan {
    scan: StageRowScan,
    metrics: StageRowMetrics,
}

impl CursorScan<CountedStageRowRecords> for ReadSnapshot {
    type Cursor = CountedStageRows;

    fn scan_cursor(
        &self,
        request: Arc<CountedStageRowScan>,
    ) -> restate_storage_api::Result<CountedStageRows> {
        Ok(CountedStageRows {
            cursor: <Self as CursorScan<StageRowRecords>>::scan_cursor(
                self,
                Arc::new(request.scan.clone()),
            )?,
            metrics: request.metrics.clone(),
        })
    }
}
