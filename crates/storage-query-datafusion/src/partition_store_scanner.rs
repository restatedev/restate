// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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

use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_storage_api::StorageError;
use restate_types::identifiers::{PartitionId, PartitionKey};

use crate::table_providers::ScanPartition;
use crate::table_util::BatchSender;

pub trait ScanLocalPartition: Send + Sync + Debug + 'static {
    type Builder: crate::table_util::Builder + Send;
    type Item<'a>: Send;
    type ConversionError;

    fn for_each_row<
        F: for<'a> FnMut(Self::Item<'a>) -> ControlFlow<Result<(), Self::ConversionError>>
            + Send
            + Sync
            + 'static,
    >(
        partition_store: &PartitionStore,
        range: RangeInclusive<PartitionKey>,
        f: F,
    ) -> Result<impl Future<Output = restate_storage_api::Result<()>> + Send, StorageError>;

    fn append_row<'a>(
        row_builder: &mut Self::Builder,
        value: Self::Item<'a>,
    ) -> Result<(), Self::ConversionError>;
}

#[derive(Clone, derive_more::Debug)]
pub struct LocalPartitionsScanner<S> {
    #[debug(skip)]
    partition_store_manager: Arc<PartitionStoreManager>,
    _marker: std::marker::PhantomData<S>,
}

impl<S> LocalPartitionsScanner<S>
where
    S: ScanLocalPartition,
{
    pub fn new(partition_store_manager: Arc<PartitionStoreManager>, _scanner: S) -> Self {
        Self {
            partition_store_manager,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<S, RB> LocalPartitionsScanner<S>
where
    S: ScanLocalPartition<Builder = RB>,
    RB: crate::table_util::Builder + Send + Sync + 'static,
{
    fn scan_partition(
        &self,
        partition_id: PartitionId,
        range: RangeInclusive<PartitionKey>,
        projection: SchemaRef,
        _predicate: Option<Arc<dyn PhysicalExpr>>,
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

            S::for_each_row(&partition_store, range, move |row| {
                if let Some(0) = limit {
                    return ControlFlow::Break(Ok(()));
                }
                match S::append_row(batch_sender.builder_mut(), row) {
                    Ok(()) => {}
                    err => return ControlFlow::Break(err),
                }

                if batch_sender.num_rows() >= batch_size && batch_sender.send().is_err() {
                    // the other side has hung up on us.
                    return ControlFlow::Break(Ok(()));
                }

                match &mut limit {
                    Some(limit) => {
                        *limit -= 1; // we already checked for 0 above
                        if *limit == 0 {
                            ControlFlow::Break(Ok(()))
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

impl<S, RB> ScanPartition for LocalPartitionsScanner<S>
where
    S: ScanLocalPartition<Builder = RB>,
    RB: crate::table_util::Builder + Send + Sync + 'static,
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
