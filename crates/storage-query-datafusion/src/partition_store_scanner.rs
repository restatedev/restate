// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::Instant;
use std::{fmt::Debug, ops::ControlFlow};

use anyhow::anyhow;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::PhysicalExpr;
use datafusion::physical_plan::metrics::Time;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;

use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_storage_api::StorageError;
use restate_storage_api::scan::{RowConsumer, ScanClosePolicy, ScanOutcome};
use restate_types::identifiers::PartitionId;
use restate_types::sharding::KeyRange;

use crate::table_providers::ScanPartition;
use crate::table_util::BatchSender;

pub trait ScanLocalPartitionFilter {
    fn new(range: KeyRange, predicate: Option<Arc<dyn PhysicalExpr>>) -> Self;
}

impl ScanLocalPartitionFilter for KeyRange {
    fn new(range: KeyRange, _predicate: Option<Arc<dyn PhysicalExpr>>) -> Self {
        range
    }
}

pub trait ScanLocalPartition: Send + Sync + Debug + 'static {
    type Builder: crate::table_util::Builder + Send;
    type Item<'a>: Send;
    type ConversionError: Into<anyhow::Error>;
    type Filter: ScanLocalPartitionFilter + Send + Sync + 'static;

    fn for_each_row<F: for<'a> RowConsumer<Item<'a> = Self::Item<'a>>>(
        partition_store: &PartitionStore,
        filter: Self::Filter,
        f: F,
    ) -> Result<
        impl Future<Output = restate_storage_api::Result<ScanOutcome<F>>> + Send,
        StorageError,
    >;

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
    #[allow(clippy::too_many_arguments)]
    fn scan_partition(
        &self,
        partition_id: PartitionId,
        range: KeyRange,
        projection: SchemaRef,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        batch_size: usize,
        limit: Option<usize>,
        elapsed_compute: Time,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        let partition_store_manager = self.partition_store_manager.clone();
        let mut stream_builder = RecordBatchReceiverStream::builder(projection.clone(), 1);
        let tx = stream_builder.tx();

        let background_task = async move {
            let partition_store = partition_store_manager
                .get_partition_store(partition_id)
                .await
                .ok_or_else(|| {
                    DataFusionError::External(
                        anyhow!(
                            "partition {partition_id} is unavailable on this node; retry the query"
                        )
                        .into(),
                    )
                })?;

            let consumer = BatchConsumer::<S> {
                batch_sender: BatchSender::new(
                    projection,
                    tx,
                    predicate.clone(),
                    batch_size,
                    limit,
                ),
                elapsed_compute: ElapsedCompute::new(elapsed_compute),
            };
            let consumer =
                S::for_each_row(&partition_store, S::Filter::new(range, predicate), consumer)
                    .map_err(|err| map_scan_error(partition_id, err))?
                    .await
                    .map_err(|err| map_scan_error(partition_id, err))?;
            // The scan is over: send what's left.
            consumer.into_inner().batch_sender.finish().await
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
        range: KeyRange,
        projection: SchemaRef,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        batch_size: usize,
        limit: Option<usize>,
        elapsed_compute: Time,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        self.scan_partition(
            partition_id,
            range,
            projection,
            predicate,
            batch_size,
            limit,
            elapsed_compute,
        )
    }
}

fn map_scan_error(partition_id: PartitionId, error: StorageError) -> DataFusionError {
    DataFusionError::External(
        anyhow::Error::new(error)
            .context(format!("scan of partition {partition_id} failed"))
            .into(),
    )
}

struct BatchConsumer<S: ScanLocalPartition> {
    batch_sender: BatchSender<S::Builder>,
    elapsed_compute: ElapsedCompute,
}

impl<S: ScanLocalPartition> RowConsumer for BatchConsumer<S> {
    type Item<'a> = S::Item<'a>;

    // A query only needs the partition while it's open: if the partition closes
    // or moves, stop the scan instead of holding it open.
    const SCAN_CLOSE_POLICY: ScanClosePolicy = ScanClosePolicy::CancelOnClose;

    async fn consume(
        &mut self,
        row: Self::Item<'_>,
    ) -> restate_storage_api::Result<ControlFlow<()>> {
        self.elapsed_compute.start();
        S::append_row(self.batch_sender.builder_mut(), row)
            .map_err(|error| StorageError::Conversion(error.into()))?;
        self.batch_sender
            .send_if_needed()
            .await
            .map_err(|error| StorageError::Generic(error.into()))
    }
}

struct ElapsedCompute {
    time: Time,
    start: Option<Instant>,
}

impl ElapsedCompute {
    fn new(time: Time) -> Self {
        Self { time, start: None }
    }

    fn start(&mut self) {
        self.start.get_or_insert_with(Instant::now);
    }
}

impl Drop for ElapsedCompute {
    fn drop(&mut self) {
        if let Some(start) = &self.start {
            self.time.add_elapsed(*start)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use bytes::Bytes;
    use datafusion::arrow::datatypes::DataType;
    use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
    use datafusion::physical_expr::ScalarFunctionExpr;
    use datafusion::physical_plan::metrics::Time;
    use datafusion::scalar::ScalarValue;
    use futures::StreamExt;
    use tokio::sync::Notify;
    use tokio::time::timeout;

    use restate_partition_store::PartitionStore;
    use restate_storage_api::StorageError;
    use restate_storage_api::Transaction;
    use restate_storage_api::scan::{RowConsumer, ScanOutcome};
    use restate_storage_api::state_table::{ScanStateTable, WriteStateTable};
    use restate_types::identifiers::{PartitionId, ServiceId, WithPartitionKey};
    use restate_types::sharding::KeyRange;

    use super::{LocalPartitionsScanner, ScanLocalPartition};
    use crate::mocks::MockQueryEngine;
    use crate::state::schema::StateBuilder;

    #[derive(Debug)]
    struct TestScanner;

    impl ScanLocalPartition for TestScanner {
        type Builder = StateBuilder;
        type Item<'a> = (ServiceId, Bytes, &'a [u8]);
        type ConversionError = std::convert::Infallible;
        type Filter = KeyRange;

        fn for_each_row<F: for<'a> RowConsumer<Item<'a> = Self::Item<'a>>>(
            partition_store: &PartitionStore,
            range: KeyRange,
            consumer: F,
        ) -> Result<
            impl Future<Output = restate_storage_api::Result<ScanOutcome<F>>> + Send,
            StorageError,
        > {
            partition_store.for_each_user_state(range, consumer)
        }

        fn append_row<'a>(
            builder: &mut Self::Builder,
            (service_id, _, _): Self::Item<'a>,
        ) -> Result<(), Self::ConversionError> {
            builder.row().partition_key(service_id.partition_key());
            Ok(())
        }
    }

    #[restate_core::test]
    async fn closing_partition_cancels_scan_waiting_to_send_batch() {
        const DEADLINE: Duration = Duration::from_secs(10);

        let mut engine = MockQueryEngine::create().await;
        let mut txn = engine.partition_store().transaction();
        let service_id = ServiceId::with_partition_key(42, "svc", "key");
        for key in [b"a", b"b"] {
            txn.put_user_state(&service_id, &Bytes::from_static(key), b"v")
                .unwrap();
        }
        txn.commit().await.unwrap();
        drop(txn);

        // The result channel holds one batch. Reaching the second batch's
        // predicate means the consumer is about to wait for room in that channel.
        let blocked = Arc::new(Notify::new());
        let predicate = create_udf(
            "observe_batches",
            vec![],
            DataType::Boolean,
            Volatility::Volatile,
            Arc::new({
                let blocked = Arc::clone(&blocked);
                let batches = AtomicUsize::new(0);
                move |_| {
                    if batches.fetch_add(1, Ordering::Relaxed) == 1 {
                        blocked.notify_one();
                    }
                    Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))))
                }
            }),
        );
        let schema = Arc::new(StateBuilder::schema().project(&[0]).unwrap());
        let predicate = Arc::new(
            ScalarFunctionExpr::try_new(Arc::new(predicate), vec![], &schema, Default::default())
                .unwrap(),
        );
        let manager = engine.partition_store_manager();
        let scanner = LocalPartitionsScanner::new(Arc::clone(manager), TestScanner);
        let elapsed_compute = Time::new();
        let mut stream = scanner
            .scan_partition(
                PartitionId::MIN,
                KeyRange::FULL,
                schema,
                Some(predicate),
                1,
                None,
                elapsed_compute.clone(),
            )
            .unwrap();

        timeout(DEADLINE, blocked.notified()).await.unwrap();
        assert_eq!(elapsed_compute.value(), 0, "the scan is still running");
        timeout(DEADLINE, manager.close(PartitionId::MIN))
            .await
            .expect("closing must not wait for the query to consume batches");

        // The consumer records elapsed time on drop. It must be released before
        // we read any buffered output, even though sending its next batch is blocked.
        timeout(DEADLINE, async {
            while elapsed_compute.value() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the blocked query consumer is dropped when the partition closes");

        let batch = timeout(DEADLINE, stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(batch.num_rows(), 1);
        let error = timeout(DEADLINE, stream.next())
            .await
            .unwrap()
            .expect("an interrupted scan must fail instead of ending successfully")
            .unwrap_err();
        let mut cause: &(dyn std::error::Error + 'static) = &error;
        loop {
            if let Some(error) = cause.downcast_ref::<StorageError>() {
                assert!(matches!(error, StorageError::ScanInterrupted));
                break;
            }
            cause = cause.source().expect("the storage error is preserved");
        }
    }
}
