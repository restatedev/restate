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
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{
    BinaryExpr, Column, InListExpr, IsNotNullExpr, IsNullExpr, Literal,
};
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use datafusion::physical_plan::{PhysicalExpr, SendableRecordBatchStream};

use restate_partition_store::index::RegisteredIndex;
use restate_partition_store::invocation_status_table::{
    EntryIndexExecution, EntryIndexPath, EntryIndexRead, InvocationStatusKey, InvocationStatusKeys,
    InvocationStatusRecords, InvocationStatusScan,
};
use restate_partition_store::keys::selection::KeySelection;
use restate_partition_store::vqueue_table::EntryByServiceCreated as StoredEntryIndex;
use restate_storage_query_api::schema::index::index_restriction;
use restate_storage_query_api::selection::domain::Domain;
use restate_storage_query_api::selection::{Selection, ValuePredicate};
use restate_storage_query_api::tables::index::{
    EntryByServiceCreated, EntryByServiceCreatedDomains,
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
    const ORDERING: &'static [InvocationStatusColumn] = &[InvocationStatusColumn::PartitionKey];
    const PRIMARY_ACCESS: Option<crate::access::SelectPrimary> = Some(select_primary);
    const INDEXES: &'static [IndexAccess<InvocationStatus>] = &[IndexAccess::of::<EntryIndex>()];
}

/// The most services a query may name to read them through the entry index.
const MAX_INDEXED_SERVICES: usize = 64;

/// The entry index, read for the invocations it finds. It holds every
/// invocation once its partition has fully migrated to vqueues and the index is
/// Ready; until then, each partition's read scans instead, before returning
/// anything. Its policy: the query names its services, so the read merges only
/// their runs, and asks for the newest of them, so the read stops early:
///
/// ```sql
/// SELECT id FROM sys_invocation_status
/// WHERE target_service_name = 'Promises' AND target_service_key IS NOT NULL
/// ORDER BY created_at DESC NULLS LAST
/// LIMIT 51
/// ```
///
/// Oldest first, another order, or a condition outside the allowlist below,
/// such as `status = 'scheduled'`, scans instead.
struct EntryIndex;

impl CompleteIndex for EntryIndex {
    type Index = EntryByServiceCreated;
    const INTRODUCED: SemanticRestateVersion = <StoredEntryIndex as RegisteredIndex>::INTRODUCED;

    /// The read merges its runs by creation time, newest first. The index keeps
    /// each entry's full creation timestamp, whose millisecond is the
    /// invocation's `created_at`: the read checks that for every row.
    const ORDERING: &'static [IndexOrder<InvocationStatusColumn>] = &[IndexOrder {
        column: InvocationStatusColumn::CreatedAt,
        options: SortOptions {
            descending: true,
            nulls_first: false,
        },
    }];

    /// A few services, each with a few handlers, stay within the runs one read
    /// merges. Where the index isn't maintained, every partition would scan
    /// and sort instead, so the planner then scans.
    fn useful(restriction: &EntryByServiceCreatedDomains) -> bool {
        if !Configuration::pinned()
            .common
            .experimental
            .is_track_entry_indexes_enabled()
        {
            return false;
        }
        match restriction.target_service_name.to_predicate() {
            Some(ValuePredicate::Equal(_)) => true,
            Some(ValuePredicate::In(names)) => (1..=MAX_INDEXED_SERVICES).contains(&names.len()),
            _ => false,
        }
    }

    const NEEDS_ORDERED_DEMAND: bool = true;

    /// The allowlist: conditions on what the read navigates (service, handler,
    /// creation time), on the service's type and on the key's presence.
    /// Alternatives of these qualify too, except for creation times: the read
    /// would visit the gaps between them. A service has one type and keys or
    /// not, so a type or key condition keeps all of a named service's
    /// invocations or none of them. A syntactic choice, not a selectivity
    /// proof: such a condition can still reject every candidate.
    fn residual_allowed(conjunct: &Arc<dyn PhysicalExpr>) -> bool {
        allowed_condition(conjunct, true)
    }
}

/// See [`EntryIndex::residual_allowed`]. `times` admits creation-time
/// comparisons.
fn allowed_condition(condition: &Arc<dyn PhysicalExpr>, times: bool) -> bool {
    use InvocationStatusColumn::{
        CreatedAt, TargetHandlerName, TargetServiceKey, TargetServiceName, TargetServiceTy,
    };
    let column = |expr: &Arc<dyn PhysicalExpr>| {
        let name = expr.downcast_ref::<Column>()?.name();
        [
            CreatedAt,
            TargetHandlerName,
            TargetServiceKey,
            TargetServiceName,
            TargetServiceTy,
        ]
        .into_iter()
        .find(|column| InvocationStatus::sql_name(*column) == Some(name))
    };
    let literal = |expr: &Arc<dyn PhysicalExpr>| expr.downcast_ref::<Literal>().is_some();
    if let Some(binary) = condition.downcast_ref::<BinaryExpr>() {
        if *binary.op() == Operator::Or {
            // A small IN list arrives as alternatives.
            return allowed_condition(binary.left(), false)
                && allowed_condition(binary.right(), false);
        }
        let (column, op) = if literal(binary.right()) {
            (column(binary.left()), Some(*binary.op()))
        } else if literal(binary.left()) {
            (column(binary.right()), binary.op().swap())
        } else {
            return false;
        };
        return match (column, op) {
            (Some(TargetServiceName | TargetHandlerName | TargetServiceTy), Some(op)) => {
                op == Operator::Eq
            }
            (Some(CreatedAt), Some(op)) => {
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
            && matches!(
                column(list.expr()),
                Some(TargetServiceName | TargetHandlerName)
            )
            && list.list().iter().all(literal);
    }
    let presence = condition
        .downcast_ref::<IsNullExpr>()
        .map(IsNullExpr::arg)
        .or_else(|| {
            condition
                .downcast_ref::<IsNotNullExpr>()
                .map(IsNotNullExpr::arg)
        });
    presence.is_some_and(|arg| column(arg) == Some(TargetServiceKey))
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
        index == EntryByServiceCreated::NAME
    }

    fn prepare_read(
        self: Arc<Self>,
        access: PrimaryRead,
        projection: SchemaRef,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        limit: Option<usize>,
    ) -> anyhow::Result<Arc<dyn PreparedPartitionRead>> {
        let scan = || -> anyhow::Result<_> {
            Ok(Arc::new(InvocationStatusKeys::prepare(
                &compile_selection::<InvocationStatus>(predicate.as_ref())?,
                crate::selection::key_range(&InvocationStatus::candidate_shard_keys(
                    predicate.as_slice(),
                )?),
            )?))
        };
        let access = match access {
            PrimaryRead::Scan => StatusAccess::Scan(scan()?),
            PrimaryRead::Index(index) => {
                anyhow::ensure!(self.supports_index(&index), "unsupported index '{index}'");
                // The planner's choice is fixed; the restriction is derived again
                // from the shipped predicate, and the predicate stays residual.
                let selection = index_restriction::<EntryByServiceCreated>(predicate.as_slice())?
                    .map_or(Selection::Empty, |domains| domains.to_selection());
                StatusAccess::EntryIndex {
                    index: Arc::new(EntryIndexRead::prepare(&selection)?),
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
                    Ok(path) => path == EntryIndexPath::Index,
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
                StatusAccess::EntryIndex { index, fallback } => InvocationStatusScan::EntryIndex {
                    index,
                    fallback,
                    execution: Arc::new(EntryIndexExecution::new(
                        memory,
                        move |path| {
                            match path {
                                EntryIndexPath::Index => index_reads.add(1),
                                EntryIndexPath::Scan => index_scans.add(1),
                            }
                            if let Some(chosen) = chosen {
                                let _ = chosen.send(path);
                            }
                        },
                        move |statuses| index_statuses.add(statuses),
                    )),
                },
            };
            scan_rows_into::<InvocationStatusRecords, InvocationStatus, _>(
                store,
                &executor,
                Arc::new(filter),
                BatchSink::new(
                    Batches::<InvocationStatus>::new(
                        projection,
                        batch_size,
                        limit,
                        metrics.elapsed_compute,
                    ),
                    tx.clone(),
                ),
                |(id, status), batches: &mut Batches<InvocationStatus>| {
                    if let ControlFlow::Break(result) = batches.append_with(|builder| {
                        row::append_row(builder, id, &status).map_err(|error| {
                            restate_storage_api::StorageError::Conversion(error.into())
                        })
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
