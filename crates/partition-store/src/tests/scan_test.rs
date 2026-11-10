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
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::Notify;
use tokio::time::timeout;

use restate_storage_api::scan::{RowConsumer, ScanClosePolicy, ScanOutcome};
use restate_storage_api::state_table::WriteStateTable;
use restate_storage_api::{StorageError, Transaction};
use restate_types::identifiers::{PartitionId, ServiceId};
use restate_types::sharding::KeyRange;

use super::storage_test_environment_with_manager;
use crate::scan::{ScanOp, ScanPlan, ScanSequence, TableScan};
use crate::state_table::{ScopedStateKey, StateKey};

const DEADLINE: Duration = Duration::from_secs(10);

/// Reads the user-state values of a partition-key range in the legacy or the
/// scoped table.
struct StateValues {
    range: KeyRange,
    scoped: bool,
}

impl ScanOp for StateValues {
    type Item<'a> = &'a [u8];
    const NAME: &'static str = "test-state-values";

    fn plan(&self) -> ScanPlan {
        if self.scoped {
            ScanPlan::range(TableScan::ScanPartitionKeyRange::<ScopedStateKey>(
                self.range,
            ))
        } else {
            ScanPlan::range(TableScan::ScanPartitionKeyRange::<StateKey>(self.range))
        }
    }

    async fn visit<C>(
        &self,
        _key: &[u8],
        value: &[u8],
        consumer: &mut C,
    ) -> restate_storage_api::Result<ControlFlow<()>>
    where
        C: for<'a> RowConsumer<Item<'a> = &'a [u8]>,
    {
        consumer.consume(value).await
    }
}

/// The user-state values of a partition-key range, in both tables.
fn state_values(range: KeyRange) -> Vec<StateValues> {
    vec![
        StateValues {
            range,
            scoped: false,
        },
        StateValues {
            range,
            scoped: true,
        },
    ]
}

/// Collects values, and stops after `stop_after` of them if set.
#[derive(Default)]
struct Collect {
    values: Vec<Bytes>,
    stop_after: Option<usize>,
}

impl RowConsumer for Collect {
    type Item<'a> = &'a [u8];

    async fn consume(
        &mut self,
        value: Self::Item<'_>,
    ) -> restate_storage_api::Result<ControlFlow<()>> {
        self.values.push(Bytes::copy_from_slice(value));
        Ok(if self.stop_after == Some(self.values.len()) {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        })
    }
}

/// Waits forever on its first row, and records when it's dropped.
struct Blocked {
    started: Arc<Notify>,
    dropped: Arc<AtomicBool>,
}

impl RowConsumer for Blocked {
    type Item<'a> = &'a [u8];

    async fn consume(&mut self, _: Self::Item<'_>) -> restate_storage_api::Result<ControlFlow<()>> {
        self.started.notify_one();
        std::future::pending().await
    }
}

impl Drop for Blocked {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::Relaxed);
    }
}

#[restate_core::test]
async fn scan_chains_stops_and_is_interrupted_on_close() {
    let (manager, mut store) = storage_test_environment_with_manager(true).await;
    let first = ServiceId::with_partition_key(20, "svc", "first");
    let second = ServiceId::with_partition_key(10, "svc", "second");
    let mut txn = store.transaction();
    for (service_id, key, value) in [
        (&first, "k1", "a"),
        (&second, "k2", "b"),
        (&second, "k3", "c"),
    ] {
        txn.put_user_state(service_id, &Bytes::from_static(key.as_bytes()), value)
            .unwrap();
    }
    txn.commit().await.unwrap();
    drop(txn);

    let executor = store.partition_db().rocksdb().executor().low_pri();
    let chained = || state_values(KeyRange::new(20, 20)).chain(state_values(KeyRange::new(10, 10)));

    // Chained scans run in order, and the consumer comes back once all are exhausted.
    let outcome = store
        .scan(
            executor,
            chained(),
            ScanClosePolicy::FinishBeforeClose,
            Collect::default(),
        )
        .await
        .unwrap();
    let ScanOutcome::Exhausted(Collect { values, .. }) = outcome else {
        panic!("the scan should be exhausted");
    };
    assert_eq!(values, ["a", "b", "c"]);

    // A consumer that stops ends the whole chain, not just the current scan.
    let outcome = store
        .scan(
            executor,
            chained(),
            ScanClosePolicy::FinishBeforeClose,
            Collect {
                stop_after: Some(1),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let ScanOutcome::Stopped(Collect { values, .. }) = outcome else {
        panic!("the consumer should have stopped the scan");
    };
    assert_eq!(values, ["a"]);

    // Closing the partition interrupts a `CancelOnClose` scan, even while its
    // consumer waits, and drops the consumer instead of waiting for it.
    let started = Arc::new(Notify::new());
    let dropped = Arc::new(AtomicBool::new(false));
    let scan = tokio::spawn({
        let store = store.clone();
        let consumer = Blocked {
            started: Arc::clone(&started),
            dropped: Arc::clone(&dropped),
        };
        async move {
            let executor = store.partition_db().rocksdb().executor().low_pri();
            store
                .scan(
                    executor,
                    state_values(KeyRange::FULL),
                    ScanClosePolicy::CancelOnClose,
                    consumer,
                )
                .await
                .map(drop)
        }
    });
    timeout(DEADLINE, started.notified()).await.unwrap();
    timeout(DEADLINE, manager.close(PartitionId::MIN))
        .await
        .expect("closing must not wait for the blocked scan");
    let result = timeout(DEADLINE, scan).await.unwrap().unwrap();
    assert!(matches!(result, Err(StorageError::ScanInterrupted)));
    assert!(dropped.load(Ordering::Relaxed));
}
