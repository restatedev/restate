// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::RangeBounds;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use datafusion::physical_plan::{PhysicalExpr, SendableRecordBatchStream};

use restate_storage_api::StorageError;
use restate_storage_api::scan::ScanOutcome;
use restate_storage_query_api::ordering::{OrderFunction, OrderInput};
use restate_storage_query_api::schema::row_written;
use restate_storage_query_api::{AsyncRowSink, LogicalSchema, RowSink};
use restate_types::identifiers::PartitionKey;
use restate_types::sharding::KeyRange;
use restate_worker_api::{CapturedPartitionState, PartitionCaptureRequirements};

use crate::access::PrimaryRead;
use crate::batches::{BatchSink, Batches};
use crate::selection::Domain;
use crate::table_providers::{
    PartitionReadContext, PreparedPartitionRead, ReadMetrics, ScanPartition,
};

type SelectLiveKeys = fn(&[Arc<dyn PhysicalExpr>]) -> anyhow::Result<Domain<PartitionKey>>;

/// Converts admission-captured live rows into Arrow batches. Capture preserves
/// backend ordering; this scanner applies key restrictions. Native DataFusion
/// operators evaluate SQL residuals above this candidate stream. An optional limit
/// is a safe candidate bound supplied by the planner, never a post-filter row budget.
#[derive(derive_more::Debug)]
#[debug("LivePartitionScanner")]
pub(crate) struct LivePartitionScanner<S: LogicalSchema, R> {
    requirements: PartitionCaptureRequirements,
    keys: SelectLiveKeys,
    read: fn(&CapturedPartitionState) -> Option<&Arc<[R]>>,
    partition_key: fn(&R) -> PartitionKey,
    append: fn(&mut S::RowBuilder, &R),
    order: Option<LiveOrder<R>>,
}

/// For a source whose reads declare an order of their rows' IDs: a row's values
/// of the ordering functions, written as its key fields, and whether two rows
/// come in that order. A read whose rows don't fails, rather than letting a plan
/// trust the order.
struct LiveOrder<R> {
    key: OrderKeyOf<R>,
    in_order: fn(&R, &R) -> bool,
}

/// Calls `with` on the byte form of a function's value of a row's ID; `false` if
/// the row has none.
type OrderKeyOf<R> = fn(&R, OrderFunction, &mut dyn FnMut(&[u8])) -> bool;

/// A live row as the input of the ordering functions.
struct LiveRecord<'a, R>(&'a R, OrderKeyOf<R>);

impl<R> OrderInput for LiveRecord<'_, R> {
    fn order_key<T>(&self, function: OrderFunction, with: impl FnOnce(&[u8]) -> T) -> Option<T> {
        let mut with = Some(with);
        let mut value = None;
        (self.1)(self.0, function, &mut |bytes| {
            value = with.take().map(|with| with(bytes));
        });
        value
    }
}

impl<S: LogicalSchema, R> LivePartitionScanner<S, R> {
    pub(crate) fn new(
        requirements: PartitionCaptureRequirements,
        keys: SelectLiveKeys,
        read: fn(&CapturedPartitionState) -> Option<&Arc<[R]>>,
        partition_key: fn(&R) -> PartitionKey,
        append: fn(&mut S::RowBuilder, &R),
    ) -> Self {
        Self {
            requirements,
            keys,
            read,
            partition_key,
            append,
            order: None,
        }
    }

    /// Declares that rows come in the order of their IDs: `key` gives a row's
    /// values of the ordering functions, and `in_order` whether two rows come in
    /// order. See [`LiveOrder`].
    pub(crate) fn ordered_by(mut self, key: OrderKeyOf<R>, in_order: fn(&R, &R) -> bool) -> Self {
        self.order = Some(LiveOrder { key, in_order });
        self
    }
}

impl<S: LogicalSchema, R: Send + Sync + 'static> ScanPartition for LivePartitionScanner<S, R> {
    fn requires_leader(&self) -> bool {
        true
    }

    fn prepare_read(
        self: Arc<Self>,
        access: PrimaryRead,
        projection: SchemaRef,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        limit: Option<usize>,
    ) -> anyhow::Result<Arc<dyn PreparedPartitionRead>> {
        anyhow::ensure!(
            matches!(access, PrimaryRead::Scan),
            "live source requires a scan"
        );
        let range = crate::selection::key_range(&(self.keys)(predicate.as_slice())?);
        Ok(Arc::new(PreparedLiveRead {
            scanner: self,
            range,
            projection,
            limit,
        }))
    }
}

#[derive(derive_more::Debug)]
#[debug("PreparedLiveRead")]
struct PreparedLiveRead<S: LogicalSchema, R> {
    scanner: Arc<LivePartitionScanner<S, R>>,
    range: Option<KeyRange>,
    projection: SchemaRef,
    limit: Option<usize>,
}

impl<S: LogicalSchema, R: Send + Sync + 'static> PreparedPartitionRead for PreparedLiveRead<S, R> {
    fn capture_requirements(&self) -> PartitionCaptureRequirements {
        self.scanner.requirements
    }

    fn execute(
        &self,
        context: PartitionReadContext,
        batch_size: usize,
        metrics: ReadMetrics,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        let projection = Arc::clone(&self.projection);
        let range = self.range;
        let mut stream = RecordBatchReceiverStream::builder(Arc::clone(&projection), 1);
        let tx = stream.tx();
        let rows = Arc::clone((self.scanner.read)(context.captured()).ok_or(
            restate_worker_api::PartitionReadError::LiveCaptureUnavailable(context.partition_id()),
        )?);
        let partition_key = self.scanner.partition_key;
        let append = self.scanner.append;
        let order = self
            .scanner
            .order
            .as_ref()
            .map(|order| (order.key, order.in_order));
        let limit = self.limit;
        stream.spawn(async move {
            let Some(range) = range else {
                return Ok(());
            };
            let mut sink = BatchSink::new(
                Batches::<S>::new(projection, batch_size, limit, metrics.elapsed_compute),
                tx,
            );
            let outcome = async {
                let mut previous = None;
                for row in rows
                    .iter()
                    .filter(|row| range.contains(&partition_key(row)))
                {
                    // A read that is done stops before checking a row it won't write.
                    if sink.reserve().await?.is_break() {
                        return Ok(ScanOutcome::Stopped(()));
                    }
                    if let Some((key, in_order)) = order {
                        if previous.is_some_and(|previous| !in_order(previous, row)) {
                            return Err(StorageError::Generic(anyhow::anyhow!(
                                "live rows came out of the order their source declares"
                            )));
                        }
                        previous = Some(row);
                        sink.set_record(&LiveRecord(row, key));
                    }
                    row_written(sink.append_with(|builder| {
                        append(builder, row);
                        Ok(())
                    }))?;
                }
                Ok(ScanOutcome::Exhausted(()))
            }
            .await;
            sink.finish(outcome).await;
            Ok(())
        });
        Ok(stream.build())
    }
}
