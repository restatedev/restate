// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use futures::{Stream, StreamExt};
use std::fmt::Debug;
use std::ops::RangeInclusive;
use std::sync::Arc;
use tracing::warn;

use crate::remote_query_scanner_client::{remote_scan_as_datafusion_stream, RemoteScannerService};
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::table_providers::ScanPartition;
use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_types::identifiers::{PartitionId, PartitionKey};
// ----- local partition scanner -----

pub trait ScanLocalPartition: Send + Sync + Debug + 'static {
    type Builder;
    type Item;

    fn scan_partition_store(
        partition_store: &PartitionStore,
        range: RangeInclusive<PartitionKey>,
    ) -> impl Stream<Item = restate_storage_api::Result<Self::Item>> + Send;

    fn append_row(row_builder: &mut Self::Builder, string_buffer: &mut String, value: Self::Item);
}

#[derive(Clone, Debug)]
pub struct LocalPartitionsScanner<S> {
    partition_store_manager: PartitionStoreManager,
    _marker: std::marker::PhantomData<S>,
}

impl<S> LocalPartitionsScanner<S>
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

impl<S, RB, T> ScanPartition for LocalPartitionsScanner<S>
where
    S: ScanLocalPartition<Builder = RB, Item = T>,
    RB: crate::table_util::Builder + Send,
    T: Send,
{
    fn scan_partition(
        &self,
        partition_id: PartitionId,
        range: RangeInclusive<PartitionKey>,
        projection: SchemaRef,
    ) -> SendableRecordBatchStream {
        let mut stream_builder = RecordBatchReceiverStream::builder(projection.clone(), 16);
        let tx = stream_builder.tx();
        let partition_store_manager = self.partition_store_manager.clone();
        let background_task = async move {
            let Some(partition_store) = partition_store_manager
                .get_partition_store(partition_id)
                .await
            else {
                warn!("partition {} doesn't exist, this is benign if the partition is being transferred out of this node", partition_id);
                return Ok(());
            };

            let rows = S::scan_partition_store(&partition_store, range);
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
        stream_builder.build()
    }
}

// ----- remote partition scanner -----

#[derive(Clone, Debug)]
pub struct RemotePartitionsScanner {
    manager: RemoteScannerManager,
    svc: Arc<dyn RemoteScannerService>,
    table_name: String,
}

impl RemotePartitionsScanner {
    pub fn new(
        manager: RemoteScannerManager,
        svc: Arc<dyn RemoteScannerService>,
        table: impl Into<String>,
    ) -> Self {
        Self {
            manager,
            svc,
            table_name: table.into(),
        }
    }
}

impl ScanPartition for RemotePartitionsScanner {
    fn scan_partition(
        &self,
        partition_id: PartitionId,
        range: RangeInclusive<PartitionKey>,
        projection: SchemaRef,
    ) -> SendableRecordBatchStream {
        let target = self.manager.get_partition_target_node(partition_id);
        remote_scan_as_datafusion_stream(
            self.svc.clone(),
            target,
            partition_id,
            range,
            self.table_name.clone(),
            projection,
        )
    }
}
