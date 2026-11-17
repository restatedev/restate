// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::ControlFlow;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::metrics::Time;
use futures::{FutureExt, StreamExt};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use restate_partition_store::{PartitionStore, PartitionStoreManager, QueryIoExecutor};
use restate_rocksdb::RocksDbManager;
use restate_storage_api::StorageError;
use restate_storage_query_api::scan::{CursorStep, ReadCursor};
use restate_storage_query_api::schema::RecordProgress;
use restate_storage_query_api::selection::SelectionPrecision;
use restate_storage_query_api::{RowSink, ShardingScheme, Table, define_schema};
use restate_types::identifiers::PartitionId;
use restate_types::partition_table::Partition;
use restate_types::sharding::KeyRange;

use super::*;

define_schema! {
    stream_rows {
        name: "stream_rows",
        sharding: ShardingScheme::Unsharded(None),
        columns {
            value: u64 => DataType::UInt64,
        }
    }
}

struct ExpandingTable;

impl Table for ExpandingTable {
    type Record<'a> = (usize, &'a Progress);
    /// The store to read; each read opens its own cursor over it.
    type Scan = Arc<ExpandingStore>;
}

struct ExpandingBinding;

impl SchemaBinding for ExpandingBinding {
    type Schema = StreamRows;
    type Table = ExpandingTable;
    type Materializer = usize;

    fn materialize_next(
        (rows, progress): (usize, &Progress),
        next: &mut usize,
        emit: &mut impl RowSink<StreamRows>,
    ) -> restate_storage_api::Result<RecordProgress> {
        if *next == rows {
            return Ok(RecordProgress::Done);
        }
        let row = StreamRowsRow {
            value: *next as u64,
        };
        if let ControlFlow::Break(result) = emit.append(&row) {
            result?;
        }
        *next += 1;
        progress.rows.fetch_add(1, Ordering::SeqCst);
        progress.changed.notify_one();
        Ok(if *next == rows {
            RecordProgress::Done
        } else {
            RecordProgress::More
        })
    }

    fn prepare(
        _: &Selection<StreamRows>,
    ) -> restate_storage_api::Result<PreparedRead<ExpandingTable>> {
        unreachable!("the tests prepare each read for its store, see `prepared`")
    }

    fn materialize(
        (rows, progress): (usize, &Progress),
        emit: &mut impl for<'a> FnMut(&'a StreamRowsRow) -> ControlFlow<restate_storage_api::Result<()>>,
    ) -> ControlFlow<restate_storage_api::Result<()>> {
        for value in 0..rows {
            progress.rows.fetch_add(1, Ordering::SeqCst);
            progress.changed.notify_one();
            emit(&StreamRowsRow {
                value: value as u64,
            })?;
        }
        ControlFlow::Continue(())
    }
}

#[derive(Debug, Default)]
struct Progress {
    rows: AtomicUsize,
    changed: Notify,
    stopped: Notify,
}

#[derive(Debug)]
struct ExpandingStore {
    rows: usize,
    fail: bool,
    progress: Arc<Progress>,
    hold: Option<Hold>,
}

/// Blocks the IO worker in a cursor's first step, as a slow native call does,
/// until released.
#[derive(Debug)]
struct Hold {
    started: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
}

impl Hold {
    /// A held store with one row, the signal that a worker holds it, and its release.
    fn store() -> (
        Arc<ExpandingStore>,
        tokio::sync::oneshot::Receiver<()>,
        std::sync::mpsc::Sender<()>,
    ) {
        let (started, start) = tokio::sync::oneshot::channel();
        let (release, released) = std::sync::mpsc::channel();
        let store = ExpandingStore {
            rows: 1,
            fail: false,
            progress: Default::default(),
            hold: Some(Hold {
                started: std::sync::Mutex::new(Some(started)),
                release: std::sync::Mutex::new(released),
            }),
        };
        (Arc::new(store), start, release)
    }

    fn block(&self) {
        if let Some(started) = self.started.lock().unwrap().take() {
            let _ = started.send(());
        }
        let _ = self.release.lock().unwrap().recv();
    }
}

struct ExpandingCursor {
    store: Arc<ExpandingStore>,
    started: bool,
}

impl ReadCursor<ExpandingTable> for ExpandingCursor {
    fn advance(&mut self) -> restate_storage_api::Result<CursorStep> {
        if !std::mem::replace(&mut self.started, true) {
            if let Some(hold) = &self.store.hold {
                hold.block();
            }
            Ok(CursorStep::Record)
        } else if self.store.fail {
            Err(StorageError::DataIntegrityError)
        } else {
            Ok(CursorStep::Done)
        }
    }

    fn current(&self) -> restate_storage_api::Result<(usize, &Progress)> {
        Ok((self.store.rows, &self.store.progress))
    }
}

impl Drop for ExpandingCursor {
    fn drop(&mut self) {
        self.store.progress.stopped.notify_one();
    }
}

// The synthetic table reads through a real snapshot, so its cursors run on the
// query IO pool, as those of storage tables do.
impl CursorScan<ExpandingTable> for ReadSnapshot {
    type Cursor = ExpandingCursor;

    fn scan_cursor(
        &self,
        store: Arc<Arc<ExpandingStore>>,
    ) -> restate_storage_api::Result<ExpandingCursor> {
        Ok(ExpandingCursor {
            store: Arc::unwrap_or_clone(store),
            started: false,
        })
    }
}

/// A read of `store`, prepared once and reused by every execution.
fn prepared(store: &Arc<ExpandingStore>) -> PreparedRead<ExpandingTable> {
    PreparedRead::new(Arc::clone(store), SelectionPrecision::Exact)
}

/// A snapshot of an empty partition, and a query IO pool with one worker: a
/// read that holds the worker blocks every other read.
async fn storage() -> (PartitionStore, ReadSnapshot, QueryIoExecutor) {
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    let store = manager
        .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
        .await
        .unwrap();
    let (snapshot, _) = store.query_read(CancellationToken::new()).unwrap();
    (store, snapshot, QueryIoExecutor::new(NonZeroUsize::MIN))
}

fn stream(
    snapshot: &ReadSnapshot,
    executor: &QueryIoExecutor,
    prepared: PreparedRead<ExpandingTable>,
    limit: Option<usize>,
) -> SendableRecordBatchStream {
    let schema = StreamRows::arrow_schema();
    let mut stream = RecordBatchReceiverStream::builder(Arc::clone(&schema), 1);
    let sink = BatchSink::new(
        Batches::<StreamRows>::new(schema, 2, limit, Time::new()),
        stream.tx(),
    );
    let (snapshot, executor) = (snapshot.clone(), executor.clone());
    stream.spawn(async move {
        if limit == Some(0) {
            return Ok(());
        }
        read_binding_into::<ExpandingBinding>(snapshot, &executor, &prepared, sink).await
    });
    stream.build()
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn bounded_expansion_reuses_preparation_and_preserves_terminal_errors() {
    let (_store, snapshot, executor) = storage().await;
    for (rows, fail, limit, expected, expected_errors) in [
        (5, false, None, vec![2, 2, 1], 0),
        (0, false, None, vec![], 0),
        (5, true, None, vec![2, 2], 1),
        (1000, true, Some(3), vec![2, 1], 0),
        (1000, true, Some(2), vec![2], 0),
        (1000, true, Some(0), vec![], 0),
    ] {
        let store = Arc::new(ExpandingStore {
            rows,
            fail,
            progress: Default::default(),
            hold: None,
        });
        let read = prepared(&store);
        // Repeated execution starts independent cursors and builders.
        for _ in 0..2 {
            let before = store.progress.rows.load(Ordering::SeqCst);
            let mut stream = stream(&snapshot, &executor, read.clone(), limit);
            let (sizes, values, errors) = tokio::time::timeout(Duration::from_secs(5), async {
                let mut sizes = Vec::new();
                let mut values = Vec::new();
                let mut errors = 0;
                while let Some(batch) = stream.next().await {
                    match batch {
                        Ok(batch) => {
                            assert_eq!(errors, 0, "no rows may follow a terminal failure");
                            sizes.push(batch.num_rows());
                            let column = batch
                                .column(0)
                                .as_any()
                                .downcast_ref::<datafusion::arrow::array::UInt64Array>()
                                .unwrap();
                            values.extend_from_slice(column.values());
                        }
                        Err(_) => errors += 1,
                    }
                }
                (sizes, values, errors)
            })
            .await
            .expect("bounded source must terminate");
            assert_eq!(
                sizes, expected,
                "one physical record must respect logical-row batch bounds"
            );
            assert_eq!(
                values,
                (0..expected.iter().sum::<usize>() as u64).collect::<Vec<_>>()
            );
            assert_eq!(
                errors, expected_errors,
                "late failure must survive earlier batches"
            );
            assert_eq!(
                store.progress.rows.load(Ordering::SeqCst) - before,
                rows.min(limit.unwrap_or(usize::MAX)),
                "source_limit_stops_inside_expansion_without_materializing_another_row"
            );
        }
    }
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_output_releases_the_iterator_and_storage_worker() {
    let (_store, snapshot, executor) = storage().await;
    let store = Arc::new(ExpandingStore {
        rows: 1000,
        fail: false,
        progress: Default::default(),
        hold: None,
    });
    let output = stream(&snapshot, &executor, prepared(&store), None);
    tokio::time::timeout(Duration::from_secs(5), async {
        while store.progress.rows.load(Ordering::SeqCst) < 2 {
            store.progress.changed.notified().await;
        }
    })
    .await
    .expect("producer must reach the bounded channel");
    drop(output);
    tokio::time::timeout(Duration::from_secs(5), store.progress.stopped.notified())
        .await
        .expect("dropping_output_releases_the_cursor");
    let other = Arc::new(ExpandingStore {
        rows: 3,
        fail: false,
        progress: Default::default(),
        hold: None,
    });
    let other_batches = tokio::time::timeout(Duration::from_secs(5), async {
        stream(&snapshot, &executor, prepared(&other), None)
            .collect::<Vec<_>>()
            .await
    })
    .await
    .expect("released_iterator_frees_the_storage_worker");
    assert_eq!(
        other_batches
            .iter()
            .map(|batch| batch.as_ref().unwrap().num_rows())
            .sum::<usize>(),
        3
    );
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_releases_queued_cursors_but_waits_for_running_storage() {
    let (store, _, executor) = storage().await;
    let read = |snapshot: &ReadSnapshot, prepared: &PreparedRead<ExpandingTable>| {
        let (tx, _) = tokio::sync::mpsc::channel(1);
        let sink = BatchSink::new(
            Batches::<StreamRows>::new(StreamRows::arrow_schema(), 2, None, Time::new()),
            tx,
        );
        let (snapshot, executor, prepared) = (snapshot.clone(), executor.clone(), prepared.clone());
        Box::pin(async move {
            read_binding_into::<ExpandingBinding>(snapshot, &executor, &prepared, sink).await
        })
    };

    // A read holds the only IO worker in a blocking call.
    let (held, start, release) = Hold::store();
    let (snapshot, running_closed) = store.query_read(CancellationToken::new()).unwrap();
    let mut running = read(&snapshot, &prepared(&held));
    drop(snapshot);
    assert!(running.as_mut().now_or_never().is_none());
    start.await.unwrap();

    // A second read waits in the queue. Dropping it releases its request and
    // snapshot at once, without a worker, and its cursor never opens.
    let queued_store = Arc::new(ExpandingStore {
        rows: 1,
        fail: false,
        progress: Default::default(),
        hold: None,
    });
    let (snapshot, queued_closed) = store.query_read(CancellationToken::new()).unwrap();
    let mut queued = read(&snapshot, &prepared(&queued_store));
    drop(snapshot);
    assert!(queued.as_mut().now_or_never().is_none());
    drop(queued);
    tokio::time::timeout(Duration::from_secs(5), queued_closed)
        .await
        .expect("queued_reader_releases_without_an_available_io_thread");
    assert_eq!(Arc::strong_count(&queued_store), 1);
    assert_eq!(queued_store.progress.rows.load(Ordering::SeqCst), 0);

    // Dropping the running read doesn't release its snapshot until the
    // blocking call returns and the worker drops the cursor.
    drop(running);
    let mut running_closed = Box::pin(running_closed);
    assert!(
        running_closed.as_mut().now_or_never().is_none(),
        "dropped_waiter_is_not_running_storage_completion"
    );
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), running_closed)
        .await
        .expect("running_reader_releases_once_storage_returns");
}

/// Reads one record of `rows` rows, followed by a failure when `fail` is set,
/// through the binding path or the one-row-per-record path.
async fn collect(
    snapshot: &ReadSnapshot,
    executor: &QueryIoExecutor,
    binding: bool,
    (rows, fail): (usize, bool),
    batch_size: usize,
    limit: Option<usize>,
) -> Vec<datafusion::common::Result<RecordBatch>> {
    let store = Arc::new(ExpandingStore {
        rows,
        fail,
        progress: Default::default(),
        hold: None,
    });
    let read = prepared(&store);
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let sink = BatchSink::new(
        Batches::<StreamRows>::new(StreamRows::arrow_schema(), batch_size, limit, Time::new()),
        tx,
    );
    let snapshot = snapshot.clone();
    let read = async {
        if binding {
            read_binding_into::<ExpandingBinding>(snapshot, executor, &read, sink).await
        } else {
            crate::batches::scan_rows_into::<ExpandingTable, StreamRows, _>(
                snapshot,
                executor,
                Arc::clone(read.request()),
                sink,
                |_, batches| {
                    if let ControlFlow::Break(result) = batches.append(&StreamRowsRow { value: 0 })
                    {
                        result?;
                    }
                    Ok(())
                },
            )
            .await
        }
    };
    let collect = async {
        let mut batches = Vec::new();
        while let Some(batch) = rx.recv().await {
            batches.push(batch);
        }
        batches
    };
    let (read, batches) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(read, collect)
    })
    .await
    .expect("the read terminates");
    read.unwrap();
    batches
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn reads_stop_at_the_limit_and_send_full_batches_before_the_next_storage_step() {
    let (_store, snapshot, executor) = storage().await;
    let rows = |batches: &[datafusion::common::Result<RecordBatch>]| {
        batches
            .iter()
            .map(|batch| batch.as_ref().map(RecordBatch::num_rows).map_err(|_| ()))
            .collect::<Vec<_>>()
    };
    for binding in [true, false] {
        // The limit is reached by the record's last row: the read stops before
        // the cursor would fail.
        let limited = collect(&snapshot, &executor, binding, (1, true), 2, Some(1)).await;
        assert_eq!(rows(&limited), [Ok(1)], "binding={binding}");
        // A batch filled by the record's last row is sent before the cursor fails.
        let record = if binding { 2 } else { 1 };
        let full = collect(&snapshot, &executor, binding, (record, true), record, None).await;
        assert_eq!(rows(&full), [Ok(record), Err(())], "binding={binding}");
        // Rows of an unfinished batch are dropped on failure.
        let partial = collect(&snapshot, &executor, binding, (1, true), 2, None).await;
        assert_eq!(rows(&partial), [Err(())], "binding={binding}");
    }
    // A limit reached inside a record stops there too.
    let inside = collect(&snapshot, &executor, true, (2, true), 2, Some(1)).await;
    assert_eq!(rows(&inside), [Ok(1)]);
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn final_output_waits_without_holding_the_snapshot() {
    let (store, snapshot, executor) = storage().await;
    let closed = snapshot.closed();
    // Three rows in batches of two, into a channel with room for one batch: the
    // last batch waits until the first is taken.
    let records = Arc::new(ExpandingStore {
        rows: 3,
        fail: false,
        progress: Default::default(),
        hold: None,
    });
    let request = prepared(&records);
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let sink = BatchSink::new(
        Batches::<StreamRows>::new(StreamRows::arrow_schema(), 2, None, Time::new()),
        tx,
    );
    let pool = executor.clone();
    let mut read = Box::pin(async move {
        read_binding_into::<ExpandingBinding>(snapshot, &pool, &request, sink).await
    });
    assert!(read.as_mut().now_or_never().is_none());
    tokio::time::timeout(Duration::from_secs(5), records.progress.stopped.notified())
        .await
        .expect("the cursor is done");
    // An empty read of another snapshot runs after the first on the one worker.
    let (other, _) = store.query_read(CancellationToken::new()).unwrap();
    let (other_tx, _other_rx) = tokio::sync::mpsc::channel(1);
    let empty = Arc::new(ExpandingStore {
        rows: 0,
        fail: false,
        progress: Default::default(),
        hold: None,
    });
    read_binding_into::<ExpandingBinding>(
        other,
        &executor,
        &prepared(&empty),
        BatchSink::new(
            Batches::<StreamRows>::new(StreamRows::arrow_schema(), 2, None, Time::new()),
            other_tx,
        ),
    )
    .await
    .unwrap();
    assert!(
        read.as_mut().now_or_never().is_none(),
        "the last batch waits for room"
    );
    assert!(
        Box::pin(closed).now_or_never().is_some(),
        "the snapshot is released before the wait"
    );
    assert_eq!(rx.recv().await.unwrap().unwrap().num_rows(), 2);
    read.await.unwrap();
    assert_eq!(rx.recv().await.unwrap().unwrap().num_rows(), 1);
}

/// Dropping a lookup's output while its producer waits for room releases the
/// read's snapshot and the storage worker; the same prepared keys then read
/// completely.
#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_lookup_output_releases_the_snapshot_and_storage_worker() {
    use restate_clock::UniqueTimestamp;
    use restate_limiter::LimitKey;
    use restate_partition_store::lookup::LookupKeys;
    use restate_partition_store::vqueue_table::{VQueueMetaBinding, VQueueMetaTable};
    use restate_storage_api::Transaction;
    use restate_storage_api::vqueue_table::WriteVQueueTable;
    use restate_storage_api::vqueue_table::metadata::{VQueueLink, VQueueMeta};
    use restate_storage_query_api::tables::vqueue_meta::VqueueMeta;
    use restate_types::time::MillisSinceEpoch;
    use restate_types::vqueues::VQueueId;

    use crate::batches::read_lookup_into;

    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    let mut store = manager
        .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
        .await
        .unwrap();
    let created =
        UniqueTimestamp::try_from_unix_millis(MillisSinceEpoch::new(1_744_010_000_000)).unwrap();
    let ids: std::collections::BTreeSet<_> =
        (0..50).map(|n| VQueueId::custom(n * 1000, "q")).collect();
    let mut tx = store.transaction();
    for id in &ids {
        tx.create_vqueue(
            id,
            &VQueueMeta::new(created, None, LimitKey::None, VQueueLink::None),
        );
    }
    tx.commit().await.unwrap();
    drop(tx);
    let keys = Arc::new(
        LookupKeys::<VQueueMetaTable>::decode(
            ids.len(),
            &LookupKeys::<VQueueMetaTable>::encode(&ids),
        )
        .unwrap(),
    );
    let executor = QueryIoExecutor::new(NonZeroUsize::MIN);
    // One row per batch into a one-batch channel: the producer waits for room.
    let lookup = |snapshot: ReadSnapshot| {
        let schema = VqueueMeta::arrow_schema();
        let mut stream = RecordBatchReceiverStream::builder(Arc::clone(&schema), 1);
        let sink = BatchSink::new(
            Batches::<VqueueMeta>::new(schema, 1, None, Time::new()),
            stream.tx(),
        );
        let (executor, keys) = (executor.clone(), Arc::clone(&keys));
        stream.spawn(async move {
            read_lookup_into::<VQueueMetaBinding>(snapshot, &executor, keys, sink).await
        });
        stream.build()
    };

    let (snapshot, closed) = store.query_read(CancellationToken::new()).unwrap();
    let mut output = lookup(snapshot);
    let first = tokio::time::timeout(Duration::from_secs(5), output.next())
        .await
        .expect("the lookup produces its first row")
        .unwrap()
        .unwrap();
    assert_eq!(first.num_rows(), 1);
    drop(output);
    tokio::time::timeout(Duration::from_secs(5), closed)
        .await
        .expect("dropping_lookup_output_releases_its_snapshot");

    // The only storage worker is free again, and the same keys read in full.
    let (snapshot, closed) = store.query_read(CancellationToken::new()).unwrap();
    let rows = tokio::time::timeout(Duration::from_secs(5), async {
        lookup(snapshot)
            .map(|batch| batch.unwrap().num_rows())
            .collect::<Vec<_>>()
            .await
    })
    .await
    .expect("the released worker serves the next lookup");
    assert_eq!(rows.iter().sum::<usize>(), ids.len());
    tokio::time::timeout(Duration::from_secs(5), closed)
        .await
        .expect("a finished lookup releases its snapshot");
}
