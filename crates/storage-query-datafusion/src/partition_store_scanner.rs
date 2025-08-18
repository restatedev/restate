// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::RangeInclusive;
use std::sync::Arc;
use std::{fmt::Debug, ops::ControlFlow};

use anyhow::anyhow;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::PhysicalExpr;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use futures::{Stream, StreamExt};

use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_storage_api::StorageError;
use restate_types::identifiers::{PartitionId, PartitionKey};

use crate::table_providers::ScanPartition;
use crate::table_util::BatchSender;

pub trait ScanLocalPartition: Send + Sync + Debug + 'static {
    type Builder: crate::table_util::Builder + Send;
    type Item: Send;

    fn scan_partition_store(
        partition_store: &PartitionStore,
        range: RangeInclusive<PartitionKey>,
    ) -> Result<impl Stream<Item = restate_storage_api::Result<Self::Item>> + Send, StorageError>;

    fn append_row(row_builder: &mut Self::Builder, string_buffer: &mut String, value: Self::Item);
}

// ScanLocalPartitionInPlace can be used for large or performance sensitive tables to build batches on io threads (instead of sending rows cross-thread)
pub trait ScanLocalPartitionInPlace: Send + Sync + Debug + 'static {
    type Builder: crate::table_util::Builder + Send;
    type Item: Send;

    fn for_each_row<F: FnMut(Self::Item) -> ControlFlow<()> + Send + Sync + 'static>(
        partition_store: &PartitionStore,
        range: RangeInclusive<PartitionKey>,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        f: F,
    ) -> Result<impl Future<Output = restate_storage_api::Result<()>> + Send, StorageError>;

    fn append_row(row_builder: &mut Self::Builder, string_buffer: &mut String, value: Self::Item);
}

pub struct ScanStream;
pub struct ScanInPlace;

#[derive(Clone, derive_more::Debug)]
pub struct LocalPartitionsScanner<S, M = ScanStream> {
    #[debug(skip)]
    partition_store_manager: PartitionStoreManager,
    _marker: std::marker::PhantomData<(S, M)>,
}

impl<S> LocalPartitionsScanner<S, ScanStream>
where
    S: ScanLocalPartition,
{
    pub fn new(partition_store_manager: PartitionStoreManager, _scanner: S) -> Self {
        Self {
            partition_store_manager,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<S> LocalPartitionsScanner<S, ScanInPlace>
where
    S: ScanLocalPartitionInPlace,
{
    pub fn new_in_place(partition_store_manager: PartitionStoreManager, _scanner: S) -> Self {
        Self {
            partition_store_manager,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<S, RB, T> LocalPartitionsScanner<S, ScanStream>
where
    S: ScanLocalPartition<Builder = RB, Item = T>,
    RB: crate::table_util::Builder + Send + Sync + 'static,
    T: Send,
{
    fn scan_partition(
        &self,
        partition_id: PartitionId,
        range: RangeInclusive<PartitionKey>,
        projection: SchemaRef,
        batch_size: usize,
        limit: Option<usize>,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        let mut stream_builder = RecordBatchReceiverStream::builder(projection.clone(), 1);
        let tx = stream_builder.tx();
        let partition_store_manager = self.partition_store_manager.clone();
        let background_task = async move {
            let partition_store = partition_store_manager.get_partition_store(partition_id).await.ok_or_else(|| {
                // make sure that the consumer of this stream to learn about the fact that this node does not have
                // that partition anymore, so that it can decide how to react to this.
                // for example, they can retry or fail the query with a useful message.
                let err = anyhow!("partition {} doesn't exist on this node, this is benign if the partition is being transferred out of/into this node.", partition_id);
                DataFusionError::External(err.into())
            })?;

            let rows = S::scan_partition_store(&partition_store, range)
                .map_err(|e| DataFusionError::External(e.into()))?;
            let rows = match limit {
                Some(limit) => rows.take(limit).left_stream(),
                None => rows.right_stream(),
            };
            let mut builder = S::Builder::new(projection.clone());
            let mut temp = String::new();

            tokio::pin!(rows);
            while let Some(Ok(row)) = rows.next().await {
                S::append_row(&mut builder, &mut temp, row);
                if builder.num_rows() >= batch_size {
                    let batch = builder.finish();
                    if tx.send(batch).await.is_err() {
                        // not sure what to do here?
                        // the other side has hung up on us.
                        // we probably don't want to panic, is it will cause the entire process to exit
                        return Ok(());
                    }
                    builder = S::Builder::new(projection.clone());
                }
            }
            if !builder.empty() {
                let result = builder.finish();
                let _ = tx.send(result).await;
            }
            Ok(())
        };
        stream_builder.spawn(background_task);
        Ok(stream_builder.build())
    }
}

impl<S, RB, T> LocalPartitionsScanner<S, ScanInPlace>
where
    S: ScanLocalPartitionInPlace<Builder = RB, Item = T>,
    RB: crate::table_util::Builder + Send + Sync + 'static,
    T: Send,
{
    fn scan_partition(
        &self,
        partition_id: PartitionId,
        range: RangeInclusive<PartitionKey>,
        projection: SchemaRef,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        batch_size: usize,
        mut limit: Option<usize>,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        let partition_store_manager = self.partition_store_manager.clone();
        let mut stream_builder = RecordBatchReceiverStream::builder(projection.clone(), 1);
        let tx = stream_builder.tx();

        let background_task = async move {
            let partition_store = partition_store_manager.get_partition_store(partition_id).await.ok_or_else(|| {
                // make sure that the consumer of this stream to learn about the fact that this node does not have
                // that partition anymore, so that it can decide how to react to this.
                // for example, they can retry or fail the query with a useful message.
                let err = anyhow!("partition {} doesn't exist on this node, this is benign if the partition is being transferred out of/into this node.", partition_id);
                DataFusionError::External(err.into())
            })?;

            // will send the last batch on Drop.
            let mut batch_sender = BatchSender::new(projection.clone(), tx);
            let mut temp = String::new();

            S::for_each_row(&partition_store, range, predicate, move |row| {
                if let Some(0) = limit {
                    return ControlFlow::Break(());
                }
                S::append_row(batch_sender.builder_mut(), &mut temp, row);

                if batch_sender.num_rows() >= batch_size && batch_sender.send().is_err() {
                    // the other side has hung up on us.
                    return ControlFlow::Break(());
                }

                match &mut limit {
                    Some(limit) => {
                        *limit -= 1; // we already checked for 0 above
                        if *limit == 0 {
                            ControlFlow::Break(())
                        } else {
                            ControlFlow::Continue(())
                        }
                    }
                    None => ControlFlow::Continue(()),
                }
            })
            .map_err(|err| DataFusionError::External(err.into()))?
            .await
            .map_err(|err| DataFusionError::External(err.into()))?;

            Ok(())
        };
        stream_builder.spawn(background_task);
        Ok(stream_builder.build())
    }
}

impl<S, RB, T> ScanPartition for LocalPartitionsScanner<S, ScanStream>
where
    S: ScanLocalPartition<Builder = RB, Item = T>,
    RB: crate::table_util::Builder + Send + Sync + 'static,
    T: Send,
{
    fn scan_partition(
        &self,
        partition_id: PartitionId,
        range: RangeInclusive<PartitionKey>,
        projection: SchemaRef,
        _predicate: Option<Arc<dyn PhysicalExpr>>,
        batch_size: usize,
        limit: Option<usize>,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        self.scan_partition(partition_id, range, projection, batch_size, limit)
    }
}

impl<S, RB, T> ScanPartition for LocalPartitionsScanner<S, ScanInPlace>
where
    S: ScanLocalPartitionInPlace<Builder = RB, Item = T>,
    RB: crate::table_util::Builder + Send + Sync + 'static,
    T: Send,
{
    fn scan_partition(
        &self,
        partition_id: PartitionId,
        range: RangeInclusive<PartitionKey>,
        projection: SchemaRef,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        batch_size: usize,
        limit: Option<usize>,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        self.scan_partition(
            partition_id,
            range,
            projection,
            predicate,
            batch_size,
            limit,
        )
    }
}
