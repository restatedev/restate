// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The cluster tables `registry` and `migration_jobs`: what each partition replica
//! on a worker node records about its indexes, its one-time migrations, and the
//! background jobs running them.
//!
//! Each worker answers for the replicas it has open, from a report its partition
//! store reads on the query IO pool (see [`PartitionStoreManager::migration_report`]).
//! The two tables read separate reports: a query joining them sees each replica
//! at two moments.
//!
//! A node whose report finds an unreadable record still returns its other
//! replicas' rows, then ends its stream with an error naming each unreadable
//! replica and record. A scan consumed to its end turns that into the node's
//! warning; a query that stops early, for example at a `LIMIT`, may never read
//! it. The remote path delivers it the same way by inspection of the scanner
//! service, not by a test.

mod migration_jobs_schema;
mod registry_schema;
#[cfg(test)]
mod tests;

use std::fmt::Debug;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::{StreamExt, stream};
use tokio::time::Instant;

use restate_clock::WallClock;
use restate_core::Metadata;
use restate_partition_store::job::{MigrationJob, MigrationJobKind, MigrationJobStatus};
use restate_partition_store::registry::{
    DropReason, Index, IndexKind, IndexStatus, Migration, MigrationState, fill_capability,
};
use restate_partition_store::{
    IndexSizes, MigrationReport, PartitionStoreManager, QueryIoExecutor, ReplicaMigrations,
};
use restate_storage_query_api::QueryEngineTable;
use restate_types::GenerationalNodeId;
use restate_types::nodes_config::Role;

use crate::node_fan_out::{NodeFanOutTableProvider, RoleBasedNodeLocator};
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::table_providers::Scan;
use crate::table_util::Builder;

pub use migration_jobs_schema::{MigrationJobsBuilder, MigrationJobsTable};
pub use registry_schema::{RegistryBuilder, RegistryTable};

impl RegistryTable {
    /// The `registry` fan-out table over the worker nodes. Each node's scanner is
    /// registered separately (see [`create_local_scanners`]).
    pub(crate) fn create_provider(
        metadata: Metadata,
        remote_scanner_manager: RemoteScannerManager,
    ) -> Arc<dyn TableProvider> {
        Arc::new(NodeFanOutTableProvider::new(
            RegistryBuilder::schema(),
            Arc::new(RoleBasedNodeLocator::new(Role::Worker, metadata)),
            remote_scanner_manager,
            None,
            Self::identity(),
        ))
    }
}

impl MigrationJobsTable {
    /// The `migration_jobs` fan-out table over the worker nodes. Each node's
    /// scanner is registered separately (see [`create_local_scanners`]).
    pub(crate) fn create_provider(
        metadata: Metadata,
        remote_scanner_manager: RemoteScannerManager,
    ) -> Arc<dyn TableProvider> {
        Arc::new(NodeFanOutTableProvider::new(
            MigrationJobsBuilder::schema(),
            Arc::new(RoleBasedNodeLocator::new(Role::Worker, metadata)),
            remote_scanner_manager,
            None,
            Self::identity(),
        ))
    }
}

/// This node's scanners of `registry` and `migration_jobs`, in that order. They
/// read the replicas `partition_store_manager` has open, on `io_executor`.
pub fn create_local_scanners(
    partition_store_manager: Arc<PartitionStoreManager>,
    metadata: Metadata,
    io_executor: QueryIoExecutor,
) -> (Arc<dyn Scan>, Arc<dyn Scan>) {
    let scanner = |table| -> Arc<dyn Scan> {
        Arc::new(ReportScanner {
            manager: Arc::clone(&partition_store_manager),
            executor: io_executor.clone(),
            metadata: metadata.clone(),
            table,
        })
    };
    (scanner(Table::Registry), scanner(Table::MigrationJobs))
}

#[derive(Debug, Clone, Copy)]
enum Table {
    Registry,
    MigrationJobs,
}

/// Scans one of the two tables from this node's migration report.
struct ReportScanner {
    manager: Arc<PartitionStoreManager>,
    executor: QueryIoExecutor,
    metadata: Metadata,
    table: Table,
}

impl Debug for ReportScanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReportScanner")
            .field("table", &self.table)
            .finish_non_exhaustive()
    }
}

impl Scan for ReportScanner {
    fn scan(
        &self,
        projection: SchemaRef,
        _filters: &[Expr],
        batch_size: usize,
        limit: Option<usize>,
    ) -> SendableRecordBatchStream {
        // Index sizes cost a RocksDB estimate per replica: only when selected.
        let sizes = match self.table {
            Table::Registry if projection.field_with_name("approx_size_bytes").is_ok() => {
                IndexSizes::Estimate
            }
            _ => IndexSizes::Skip,
        };
        let report = self.manager.migration_report(&self.executor, sizes);
        let node = self.metadata.my_node_id();
        let (table, schema) = (self.table, projection.clone());
        let read = async move {
            let report = report
                .map_err(|err| DataFusionError::External(err.into()))?
                .await
                .map_err(|err| DataFusionError::External(err.into()))?;
            // The wall clock for the estimates, the driver's monotonic clock for
            // the deadlines of what blocks a job.
            let (now, instant) = (WallClock::now_ms().as_u64(), Instant::now());
            let batches = match table {
                Table::Registry => batches::<RegistryBuilder, _>(
                    schema,
                    batch_size,
                    entries(&report, limit),
                    |builder, (replica, entry)| append_entry(builder, node, replica, entry),
                )?,
                Table::MigrationJobs => batches::<MigrationJobsBuilder, _>(
                    schema,
                    batch_size,
                    jobs(&report, limit),
                    |builder, (replica, job)| append_job(builder, node, replica, job, now, instant),
                )?,
            };
            Ok::<_, DataFusionError>((batches, unreadable(&report)))
        };
        let items = stream::once(read).flat_map(|read| {
            let (batches, error) = read.unwrap_or_else(|error| (Vec::new(), Some(error)));
            stream::iter(batches.into_iter().map(Ok).chain(error.map(Err)))
        });
        Box::pin(RecordBatchStreamAdapter::new(projection, items))
    }
}

/// The error a node ends its rows with when some replica's records couldn't be
/// read, naming each replica and record.
fn unreadable(report: &MigrationReport) -> Option<DataFusionError> {
    if report.unreadable.is_empty() {
        return None;
    }
    let replicas: Vec<_> = report
        .unreadable
        .iter()
        .map(|replica| {
            format!(
                "partition {} ({}): {}",
                replica.partition_id, replica.record, replica.error
            )
        })
        .collect();
    Some(DataFusionError::Execution(format!(
        "unreadable migration records, rows of these replicas are missing: {}",
        replicas.join("; ")
    )))
}

/// Appends `rows` through `append`, cutting a batch every `batch_size` rows like
/// the other scanners, instead of one batch for a node's whole report. This
/// bounds the rows of a batch, not its bytes.
fn batches<B: Builder, R>(
    schema: SchemaRef,
    batch_size: usize,
    rows: impl Iterator<Item = R>,
    mut append: impl FnMut(&mut B, R),
) -> Result<Vec<RecordBatch>, DataFusionError> {
    let mut builder = B::new(schema);
    let mut batches = Vec::new();
    for row in rows {
        append(&mut builder, row);
        if builder.num_rows() >= batch_size.max(1) {
            batches.push(builder.finish_and_new()?);
        }
    }
    if !builder.empty() {
        batches.push(builder.finish()?);
    }
    Ok(batches)
}

/// A row of `registry`: an index or a migration.
#[derive(Debug, Clone, Copy)]
enum Entry<'a> {
    Index(&'a Index),
    Migration(&'a Migration),
}

/// The indexes, then the migrations, of every replica in `report`, at most
/// `limit` of them.
fn entries(
    report: &MigrationReport,
    limit: Option<usize>,
) -> impl Iterator<Item = (&ReplicaMigrations, Entry<'_>)> {
    report
        .replicas
        .iter()
        .flat_map(|replica| {
            let registry = &replica.registry;
            let indexes = registry.indexes().iter().map(Entry::Index);
            let migrations = registry.migrations().iter().map(Entry::Migration);
            indexes.chain(migrations).map(move |entry| (replica, entry))
        })
        .take(limit.unwrap_or(usize::MAX))
}

fn append_entry(
    builder: &mut RegistryBuilder,
    node: GenerationalNodeId,
    replica: &ReplicaMigrations,
    entry: Entry<'_>,
) {
    let mut row = builder.row();
    row.fmt_plain_node_id(node.as_plain());
    row.fmt_gen_node_id(node);
    row.partition_id(replica.partition_id.into());
    let index = match entry {
        Entry::Index(index) => index,
        Entry::Migration(migration) => {
            row.definition(&migration.definition);
            row.kind("migration");
            row.id(migration.id);
            match migration.state() {
                Some(MigrationState::InProgress) => {
                    row.status("in_progress");
                    row.job_id(migration.id);
                }
                Some(MigrationState::Complete) => row.status("complete"),
                None => row.fmt_status(format_args!("unknown({})", migration.state)),
            }
            let timeline = &migration.timeline;
            if let Some(at) = timeline.registered_at {
                row.registered_at(millis(at));
            }
            if let Some(by) = &timeline.registered_by {
                row.registered_by_version(by);
            }
            if let Some(at) = timeline.completed_at {
                row.completed_at(millis(at));
            }
            if let Some(at) = timeline.reset_at {
                row.reset_at(millis(at));
            }
            return;
        }
    };
    row.definition(&index.definition);
    row.kind(match index.kind {
        IndexKind::Secondary => "secondary",
        IndexKind::Aggregate => "aggregate",
        IndexKind::Unknown => "unknown",
    });
    row.id(index.id);
    row.status(match index.state {
        IndexStatus::Backfilling => "backfilling",
        IndexStatus::Complete => "complete",
        IndexStatus::Dropped => "dropped",
        IndexStatus::Unknown => "unknown",
    });
    if index.state == IndexStatus::Backfilling {
        row.job_id(index.id);
    }
    let timeline = &index.timeline;
    if let Some(at) = timeline.registered_at {
        row.registered_at(millis(at));
    }
    if let Some(by) = &timeline.registered_by {
        row.registered_by_version(by);
    }
    if let Some(at) = timeline.completed_at {
        row.completed_at(millis(at));
    }
    if let Some(at) = timeline.dropped_at {
        row.dropped_at(millis(at));
    }
    match (
        timeline.drop_reason,
        DropReason::from_number(timeline.drop_reason),
    ) {
        (0, _) => {}
        (_, Some(DropReason::Reset)) => row.drop_reason("reset"),
        (_, Some(DropReason::UnknownDefinition)) => row.drop_reason("unknown-definition"),
        (number, None) => row.fmt_drop_reason(format_args!("unknown({number})")),
    }
    if let Some(size) = replica.index_sizes.get(&index.id) {
        row.approx_size_bytes(*size);
    }
}

/// The unfinished jobs, and the finished ones not expired yet, of every replica
/// in `report`, at most `limit` of them.
fn jobs(
    report: &MigrationReport,
    limit: Option<usize>,
) -> impl Iterator<Item = (&ReplicaMigrations, &MigrationJob)> {
    report
        .replicas
        .iter()
        .flat_map(|replica| replica.jobs.entries().iter().map(move |job| (replica, job)))
        .take(limit.unwrap_or(usize::MAX))
}

/// Appends `job`'s row; `now` is the scan's time, for `eta`.
fn append_job(
    builder: &mut MigrationJobsBuilder,
    node: GenerationalNodeId,
    replica: &ReplicaMigrations,
    job: &MigrationJob,
    now: u64,
    instant: Instant,
) {
    let mut row = builder.row();
    row.fmt_plain_node_id(node.as_plain());
    row.fmt_gen_node_id(node);
    row.partition_id(replica.partition_id.into());
    row.job_id(job.id().as_u32());
    row.kind(match job.kind {
        MigrationJobKind::IndexBackfill => "index_backfill",
        MigrationJobKind::Migration => "migration",
        MigrationJobKind::Unknown => "unknown",
    });
    row.name(&job.name);
    row.status(match job.status {
        MigrationJobStatus::Pending => "pending",
        MigrationJobStatus::Running => "running",
        MigrationJobStatus::Waiting => "waiting",
        MigrationJobStatus::Completed => "completed",
        MigrationJobStatus::Cancelled => "cancelled",
        MigrationJobStatus::Unknown => "unknown",
    });
    // A migration changes each row itself.
    let active = match job.kind {
        MigrationJobKind::Migration => Some(true),
        _ => fill_capability(&job.name),
    };
    match active {
        Some(true) => row.mode("active"),
        Some(false) => row.mode("passive"),
        None => {}
    }
    let walk = job.walk.and_then(|walk| replica.walks.get(walk));
    if let Some(progress) = walk.and_then(|walk| replica.progress_percent(walk)) {
        row.progress_percent(progress);
    }
    if let Some(eta) = walk
        .zip(job.pass_started_at)
        .and_then(|(walk, started)| replica.eta(walk, started, now))
    {
        row.eta(millis(eta));
    }
    if let Some(reason) = replica.blocked_by(job, instant) {
        row.blocked_by(reason);
    }
    if let Some(at) = job.created_at {
        row.created_at(millis(at));
    }
    if let Some(at) = job.started_at {
        row.started_at(millis(at));
    }
    if let Some(at) = job.pass_started_at {
        row.pass_started_at(millis(at));
    }
    if let Some(at) = job.last_pass_finished_at {
        row.last_pass_finished_at(millis(at));
    }
    if let Some(at) = job.finished_at {
        row.finished_at(millis(at));
    }
    if let Some(at) = job.next_eligible_at {
        row.next_eligible_at(millis(at));
    }
    if let Some(at) = job.last_slice_at {
        row.last_slice_at(millis(at));
    }
    if let Some(at) = job.last_error_at {
        row.last_error_at(millis(at));
    }
    if let Some(by) = &job.created_by {
        row.created_by_version(by);
    }
    row.passes(job.passes);
    row.rows_examined(job.rows_examined);
    row.rows_changed(job.rows_changed);
    if let Some(rows) = job.rows_uncovered {
        row.rows_uncovered(rows);
    }
    if let Some(rows) = job.rows_uncovered_previous {
        row.rows_uncovered_previous(rows);
    }
    row.slices(job.slices);
    row.work_ms(job.work_us / 1000);
    if let Some(work_us) = job.last_slice_work_us {
        row.last_slice_work_ms(work_us / 1000);
    }
    row.failures(job.failures);
    row.consecutive_failures(job.consecutive_failures);
    if let Some(error) = &job.last_error {
        row.last_error(error);
    }
}

/// Wall-clock milliseconds as an Arrow timestamp.
fn millis(at: u64) -> i64 {
    i64::try_from(at).unwrap_or(i64::MAX)
}
