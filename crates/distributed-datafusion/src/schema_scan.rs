// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Prepared candidate reads. Native owner-local operators evaluate SQL residuals,
//! project output columns, aggregate, and apply limits above this source.

use std::fmt::{self, Debug};
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_expr::split_conjunction;
use datafusion::physical_plan::PhysicalExpr;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;

use restate_partition_store::ReadSnapshot;
use restate_partition_store::lookup::{LookupBinding, LookupKeys, LookupKeysOf};
use restate_storage_query_api::scan::CursorScan;
use restate_storage_query_api::selection::{self, Selection};
use restate_storage_query_api::{LogicalSchema, PreparedRead, RecordsOf, SchemaBinding};

use crate::access::PrimaryRead;
use crate::batches::{BatchSink, Batches, read_binding_into, read_lookup_into};
use crate::table_providers::{
    PartitionReadContext, PreparedPartitionRead, ReadMetrics, ScanPartition,
};

pub(crate) struct SchemaScanner<B: SchemaBinding> {
    binding: PhantomData<fn() -> B>,
}

impl<B: SchemaBinding> Debug for SchemaScanner<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SchemaScanner")
            .field("schema", &B::Schema::NAME)
            .finish_non_exhaustive()
    }
}

impl<B: SchemaBinding> SchemaScanner<B> {
    pub(crate) fn new() -> Self {
        Self {
            binding: PhantomData,
        }
    }

    fn prepare(
        &self,
        projection: SchemaRef,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        limit: Option<usize>,
    ) -> anyhow::Result<BoundSchemaRead<B>> {
        let prepared = B::prepare(&compile_selection::<B::Schema>(predicate.as_ref())?)?;
        Ok(BoundSchemaRead {
            projection,
            prepared,
            limit,
        })
    }
}

/// Compiles a scan predicate into the selection a storage binding prepares.
/// Contradictory predicates select nothing; dynamic filters are not compiled.
pub(crate) fn compile_selection<S: LogicalSchema>(
    predicate: Option<&Arc<dyn PhysicalExpr>>,
) -> anyhow::Result<Selection<S>> {
    let filters: Vec<_> = predicate
        .into_iter()
        .flat_map(split_conjunction)
        .filter(|expr| !selection::contains_dynamic_filter(expr))
        .cloned()
        .collect();
    Ok(
        if S::analyze(&filters, NonZeroUsize::new(16).unwrap())?.is_empty() {
            Selection::Empty
        } else {
            selection::compile::<S>(predicate)
        },
    )
}

impl<B: SchemaBinding> ScanPartition for SchemaScanner<B>
where
    ReadSnapshot: CursorScan<B::Table>,
    for<'a> <B::Table as restate_storage_query_api::scan::Table>::Record<'a>: Copy + Send,
{
    fn prepare_read(
        self: Arc<Self>,
        access: PrimaryRead,
        projection: SchemaRef,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        limit: Option<usize>,
    ) -> anyhow::Result<Arc<dyn PreparedPartitionRead>> {
        anyhow::ensure!(
            matches!(access, PrimaryRead::Scan),
            "schema source requires a scan"
        );
        Ok(Arc::new(self.prepare(projection, predicate, limit)?))
    }
}

struct BoundSchemaRead<B: SchemaBinding> {
    projection: SchemaRef,
    prepared: PreparedRead<B::Table>,
    limit: Option<usize>,
}

impl<B: SchemaBinding> Debug for BoundSchemaRead<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundSchemaRead")
            .field("schema", &B::Schema::NAME)
            .finish_non_exhaustive()
    }
}

impl<B: SchemaBinding> PreparedPartitionRead for BoundSchemaRead<B>
where
    ReadSnapshot: CursorScan<B::Table>,
    for<'a> <B::Table as restate_storage_query_api::scan::Table>::Record<'a>: Copy + Send,
{
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
        anyhow::ensure!(batch_size > 0, "schema scan batch size must be positive");
        let (store, executor) = context.storage_read()?;
        let (store, executor) = (store.clone(), executor.clone());
        let prepared = self.prepared.clone();
        let schema = Arc::clone(&self.projection);
        let limit = self.limit;
        let mut stream = RecordBatchReceiverStream::builder(Arc::clone(&schema), 1);
        let tx = stream.tx();
        stream.spawn(async move {
            if limit == Some(0) {
                return Ok(());
            }
            read_binding_into::<B>(
                store,
                &executor,
                &prepared,
                BatchSink::new(
                    Batches::<B::Schema>::new(schema, batch_size, limit, metrics.elapsed_compute),
                    tx,
                ),
            )
            .await
        });
        Ok(stream.build())
    }
}

/// A [`SchemaScanner`] that can also read a set of complete keys, for bindings
/// that support lookups ([`LookupBinding`]).
pub(crate) struct LookupScanner<B: LookupBinding>(SchemaScanner<B>);

impl<B: LookupBinding> LookupScanner<B> {
    pub(crate) fn new() -> Self {
        Self(SchemaScanner::new())
    }
}

impl<B: LookupBinding> Debug for LookupScanner<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("LookupScanner")
            .field(&B::Schema::NAME)
            .finish()
    }
}

impl<B: LookupBinding> ScanPartition for LookupScanner<B>
where
    ReadSnapshot: CursorScan<B::Table> + CursorScan<RecordsOf<B::Table, LookupKeysOf<B>>>,
    for<'a> <B::Table as restate_storage_query_api::scan::Table>::Record<'a>: Copy + Send,
{
    fn supports_primary_lookup(&self) -> bool {
        true
    }

    fn prepare_read(
        self: Arc<Self>,
        access: PrimaryRead,
        projection: SchemaRef,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        limit: Option<usize>,
    ) -> anyhow::Result<Arc<dyn PreparedPartitionRead>> {
        Ok(match access {
            PrimaryRead::Scan => Arc::new(self.0.prepare(projection, predicate, limit)?),
            // The predicate stays with the plan, which applies it to the rows.
            PrimaryRead::MultiGet(keys) => {
                keys.validate()?;
                Arc::new(BoundLookupRead::<B> {
                    projection,
                    keys: Arc::new(LookupKeys::decode(keys.len(), &keys.encoded)?),
                    limit,
                })
            }
            PrimaryRead::Index(index) => anyhow::bail!("unsupported index '{}'", index.name()),
        })
    }
}

struct BoundLookupRead<B: LookupBinding> {
    projection: SchemaRef,
    keys: Arc<LookupKeysOf<B>>,
    limit: Option<usize>,
}

impl<B: LookupBinding> Debug for BoundLookupRead<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundLookupRead")
            .field("schema", &B::Schema::NAME)
            .field("keys", &self.keys.count())
            .finish_non_exhaustive()
    }
}

impl<B: LookupBinding> PreparedPartitionRead for BoundLookupRead<B>
where
    ReadSnapshot: CursorScan<RecordsOf<B::Table, LookupKeysOf<B>>>,
    for<'a> <B::Table as restate_storage_query_api::scan::Table>::Record<'a>: Copy + Send,
{
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
        anyhow::ensure!(batch_size > 0, "lookup batch size must be positive");
        let (store, executor) = context.storage_read()?;
        // The planner sends each partition only its own keys.
        anyhow::ensure!(
            !self.keys.outside(store.partition_key_range()),
            "primary keys are outside their partition"
        );
        let (store, executor) = (store.clone(), executor.clone());
        let keys = Arc::clone(&self.keys);
        let schema = Arc::clone(&self.projection);
        let limit = self.limit;
        let mut stream = RecordBatchReceiverStream::builder(Arc::clone(&schema), 1);
        let tx = stream.tx();
        stream.spawn(async move {
            if limit == Some(0) {
                return Ok(());
            }
            read_lookup_into::<B>(
                store,
                &executor,
                keys,
                BatchSink::new(
                    Batches::<B::Schema>::new(schema, batch_size, limit, metrics.elapsed_compute),
                    tx,
                ),
            )
            .await
        });
        Ok(stream.build())
    }
}

#[cfg(test)]
mod tests;
