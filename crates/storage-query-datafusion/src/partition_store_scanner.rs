// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Debug;
use std::ops::RangeInclusive;

use anyhow::anyhow;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use futures::{Stream, StreamExt};

use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_storage_api::StorageError;
use restate_types::identifiers::{PartitionId, PartitionKey};

use crate::table_providers::ScanPartition;

pub trait ScanLocalPartition: Send + Sync + Debug + 'static {
    type Builder: crate::table_util::Builder + Send;
    type Item: Send;

    fn scan_partition_store(
        partition_store: &PartitionStore,
        range: RangeInclusive<PartitionKey>,
    ) -> Result<impl Stream<Item = restate_storage_api::Result<Self::Item>> + Send, StorageError>;

    fn append_row(row_builder: &mut Self::Builder, string_buffer: &mut String, value: Self::Item);
}

pub trait ScanLocalPartitionMut: Send + Sync + Debug + 'static {
    type Builder: crate::table_util::Builder + Send;
    type Item: Send;

    fn scan_partition_store_mut<
        F: FnMut(Option<Result<Self::Item, StorageError>>) -> bool + Send + Sync + 'static,
    >(
        partition_store: &PartitionStore,
        range: RangeInclusive<PartitionKey>,
        f: F,
    ) -> Result<impl Future<Output = restate_storage_api::Result<()>> + Send, StorageError>;

    fn append_row(row_builder: &mut Self::Builder, string_buffer: &mut String, value: Self::Item);
}

pub struct ScanMut;
pub struct NoScanMut;

#[derive(Clone, derive_more::Debug)]
pub struct LocalPartitionsScanner<S, M = NoScanMut> {
    #[debug(skip)]
    partition_store_manager: PartitionStoreManager,
    _marker: std::marker::PhantomData<(S, M)>,
}

impl<S> LocalPartitionsScanner<S, NoScanMut>
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

impl<S> LocalPartitionsScanner<S, ScanMut>
where
    S: ScanLocalPartitionMut,
{
    pub fn new_mut(partition_store_manager: PartitionStoreManager, _scanner: S) -> Self {
        Self {
            partition_store_manager,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<S, RB, T> LocalPartitionsScanner<S, NoScanMut>
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
                if builder.full() {
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

impl<S, RB, T> LocalPartitionsScanner<S, ScanMut>
where
    S: ScanLocalPartitionMut<Builder = RB, Item = T>,
    RB: crate::table_util::Builder + Send + Sync + 'static,
    T: Send,
{
    fn scan_partition(
        &self,
        partition_id: PartitionId,
        range: RangeInclusive<PartitionKey>,
        projection: SchemaRef,
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

            let mut builder = S::Builder::new(projection.clone());
            let mut temp = String::new();

            let scan_fut = S::scan_partition_store_mut(&partition_store, range, move |item| {
                if let Some(Ok(row)) = item {
                    S::append_row(&mut builder, &mut temp, row);
                    if let Some(limit) = &mut limit {
                        *limit -= 1;
                    }
                    if builder.full() {
                        let old_builder =
                            std::mem::replace(&mut builder, S::Builder::new(projection.clone()));
                        let batch = old_builder.finish();
                        if tx.blocking_send(batch).is_err() {
                            // not sure what to do here?
                            // the other side has hung up on us.
                            // we probably don't want to panic, is it will cause the entire process to exit
                            return false;
                        }
                        builder = S::Builder::new(projection.clone());
                    }
                }

                if !builder.empty() {
                    let old_builder =
                        std::mem::replace(&mut builder, S::Builder::new(projection.clone()));
                    let result = old_builder.finish();
                    let _ = tx.blocking_send(result);
                }

                limit.is_none_or(|limit| limit > 0)
            })
            .map_err(|err| DataFusionError::External(err.into()))?;

            scan_fut
                .await
                .map_err(|err| DataFusionError::External(err.into()))
        };
        stream_builder.spawn(background_task);
        Ok(stream_builder.build())
    }
}

impl<S, RB, T> ScanPartition for LocalPartitionsScanner<S, NoScanMut>
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
        limit: Option<usize>,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        self.scan_partition(partition_id, range, projection, limit)
    }
}

impl<S, RB, T> ScanPartition for LocalPartitionsScanner<S, ScanMut>
where
    S: ScanLocalPartitionMut<Builder = RB, Item = T>,
    RB: crate::table_util::Builder + Send + Sync + 'static,
    T: Send,
{
    fn scan_partition(
        &self,
        partition_id: PartitionId,
        range: RangeInclusive<PartitionKey>,
        projection: SchemaRef,
        limit: Option<usize>,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        self.scan_partition(partition_id, range, projection, limit)
    }
}
