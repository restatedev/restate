// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

use googletest::prelude::*;
use tokio::sync::Barrier;
use tokio::task::{JoinHandle, JoinSet};
use tokio_stream::StreamExt;
use tracing::info;

use restate_core::{TaskCenter, TaskCenterFutureExt, TaskHandle, TaskKind};
use restate_metadata_store::retry_on_retryable_error;
use restate_test_util::let_assert;
use restate_types::logs::metadata::{LogletConfig, SegmentIndex};
use restate_types::logs::{KeyFilter, Lsn, SequenceNumber, TailState};
use restate_types::retries::RetryPolicy;

use super::Loglet;
use crate::loglet::{AppendError, FindTailOptions};
use crate::loglet_wrapper::LogletWrapper;

async fn wait_for_trim(loglet: &LogletWrapper, required_trim_point: Lsn) -> anyhow::Result<()> {
    for _ in 0..3 {
        let trim_point = loglet.get_trim_point().await?;
        if trim_point.is_none() || trim_point.is_some_and(|t| t < required_trim_point) {
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        } else {
            return Ok(());
        }
    }
    let trim_point = loglet.get_trim_point().await?;
    anyhow::bail!(
        "Trim point didn't reach the required point, current = {:?}, waiting_for={}",
        trim_point,
        required_trim_point
    )
}

/// Validates that high-level behaviour of the loglet is correct but it should not be considered
/// as full-spec validation.
///
/// The test requires that the loglet is empty and unsealed. It assumes that the loglet
/// is started, initialized, and ready for reads and writes. It also assumes that this loglet
/// provide contiguous offsets that start from Lsn::OLDEST.
pub async fn gapless_loglet_smoke_test(loglet: Arc<dyn Loglet>) -> googletest::Result<()> {
    let loglet = LogletWrapper::new(
        SegmentIndex::from(1),
        Lsn::OLDEST,
        None,
        LogletConfig::for_testing(),
        loglet,
    );

    assert_eq!(None, loglet.get_trim_point().await?);
    {
        let tail = loglet.find_tail(FindTailOptions::default()).await?;
        assert_eq!(Lsn::OLDEST, tail.offset());
        assert!(!tail.is_sealed());
    }

    let start: u64 = Lsn::OLDEST.into();
    let end: u64 = start + 3;
    for i in start..end {
        // Append i
        let offset = loglet.append(format!("record{i}").into()).await?;
        assert_eq!(Lsn::new(i), offset);
        assert_eq!(None, loglet.get_trim_point().await?);
        {
            let tail = loglet.find_tail(FindTailOptions::default()).await?;
            assert_eq!(Lsn::new(i + 1), tail.offset());
            assert!(!tail.is_sealed());
        }
    }

    // read record 1 (reading from OLDEST)
    let_assert!(Some(record) = loglet.read_opt(Lsn::OLDEST).await?);
    let offset = record.sequence_number();
    assert_that!(record.sequence_number(), eq(Lsn::OLDEST));
    assert!(record.is_data_record());
    assert_that!(
        record.decode_unchecked::<String>(),
        eq("record1".to_owned())
    );

    // read record 2
    let_assert!(Some(record) = loglet.read_opt(offset.next()).await?);
    let offset = record.sequence_number();
    assert_that!(record.sequence_number(), eq(Lsn::new(2)));
    assert!(record.is_data_record());
    assert_that!(
        record.decode_unchecked::<String>(),
        eq("record2".to_owned())
    );

    // read record 3
    let_assert!(Some(record) = loglet.read_opt(offset.next()).await?);
    assert_that!(record.sequence_number(), eq(Lsn::new(3)));
    assert!(record.is_data_record());
    assert_that!(
        record.decode_unchecked::<String>(),
        eq("record3".to_owned())
    );

    // trim point and tail didn't change
    assert_eq!(None, loglet.get_trim_point().await?);
    {
        let tail = loglet.find_tail(FindTailOptions::default()).await?;
        assert_eq!(Lsn::new(end), tail.offset());
        assert!(!tail.is_sealed());
    }

    // read from the future returns None
    assert!(loglet.read_opt(Lsn::new(end)).await?.is_none());

    let handle1: TaskHandle<googletest::Result<()>> =
        TaskCenter::spawn_unmanaged(TaskKind::TestRunner, "read", {
            let loglet = loglet.clone();
            async move {
                // read future record 4
                let record = loglet.read(Lsn::new(4)).await?;
                assert_that!(record.sequence_number(), eq(Lsn::new(4)));
                assert!(record.is_data_record());
                assert_that!(
                    record.decode_unchecked::<String>(),
                    eq("record4".to_owned())
                );
                Ok(())
            }
        })?;

    // Waiting for 10
    let handle2: TaskHandle<googletest::Result<()>> =
        TaskCenter::spawn_unmanaged(TaskKind::TestRunner, "read", {
            let loglet = loglet.clone();
            async move {
                // read future record 10
                let record = loglet.read(Lsn::new(10)).await?;
                assert_that!(record.sequence_number(), eq(Lsn::new(10)));
                assert!(record.is_data_record());
                assert_that!(
                    record.decode_unchecked::<String>(),
                    eq("record10".to_owned())
                );

                Ok(())
            }
        })?;

    // Giving a chance to other tasks to work.
    tokio::task::yield_now().await;
    assert!(
        !handle1.is_finished(),
        "record 4 should not be ready to read yet"
    );

    // Append 4
    let offset = loglet.append("record4".into()).await?;
    assert_eq!(Lsn::new(4), offset);
    assert_eq!(None, loglet.get_trim_point().await?);
    {
        let tail = loglet.find_tail(FindTailOptions::default()).await?;
        assert_eq!(Lsn::new(5), tail.offset());
        assert!(!tail.is_sealed());
    }

    assert!(
        tokio::time::timeout(Duration::from_secs(5), handle1)
            .await??
            .is_ok()
    );

    tokio::task::yield_now().await;
    // Only handle1 should have finished work.
    assert!(!handle2.is_finished());

    // test timeout future items
    let start = tokio::time::Instant::now();
    let res = tokio::time::timeout(Duration::from_secs(10), handle2).await;

    // We have timedout waiting.
    assert!(res.is_err());
    assert!(start.elapsed() >= Duration::from_secs(10));
    // Tail didn't change.
    assert_eq!(
        Lsn::new(5),
        loglet.find_tail(FindTailOptions::default()).await?.offset()
    );

    // trim the loglet to and including 3
    loglet.trim(Lsn::new(3)).await?;
    assert_eq!(Some(Lsn::new(3)), loglet.get_trim_point().await?);

    // tail didn't change
    {
        let tail = loglet.find_tail(FindTailOptions::default()).await?;
        assert_eq!(Lsn::new(5), tail.offset());
        assert!(!tail.is_sealed());
    }

    let_assert!(Some(record) = loglet.read_opt(Lsn::OLDEST).await?);
    assert_that!(record.sequence_number(), eq(Lsn::OLDEST));
    assert!(record.is_trim_gap());
    assert_that!(record.trim_gap_to_sequence_number(), eq(Some(Lsn::new(3))));

    let_assert!(Some(record) = loglet.read_opt(Lsn::from(4)).await?);
    assert_that!(record.sequence_number(), eq(Lsn::new(4)));
    assert!(record.is_data_record());
    assert_that!(
        record.decode_unchecked::<String>(),
        eq("record4".to_owned())
    );

    Ok(())
}

/// Validates that basic operation of loglet read stream is behaving according to spec.
///
/// The test requires that the loglet is empty and unsealed. It assumes that the loglet
/// is started, initialized, and ready for reads and writes. It also assumes that this loglet
/// starts from Lsn::OLDEST.
pub async fn single_loglet_readstream(loglet: Arc<dyn Loglet>) -> googletest::Result<()> {
    let loglet = LogletWrapper::new(
        SegmentIndex::from(1),
        Lsn::OLDEST,
        None,
        LogletConfig::for_testing(),
        loglet,
    );

    let read_from_offset = Lsn::new(6);
    let mut reader = loglet
        .clone()
        .create_read_stream(KeyFilter::Any, read_from_offset)
        .await?;

    {
        // no records have been written yet.
        let tail = loglet.find_tail(FindTailOptions::default()).await?;
        assert_eq!(Lsn::OLDEST, tail.offset());
        assert!(!tail.is_sealed());
    }
    // We didn't perform any reads yet, read_pointer shouldn't have moved.
    assert_eq!(read_from_offset, reader.read_pointer());

    // The reader is expected to wait/block until records appear at offset 6. It then reads 5 records (offsets [6-10]).
    let read_counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = read_counter.clone();
    let reader_bg_handle: JoinHandle<googletest::Result<()>> = tokio::spawn(async move {
        for i in 6..=10 {
            let record = reader.next().await.expect("to never terminate")?;
            let expected_offset = Lsn::new(i);
            counter_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            assert_eq!(expected_offset, record.sequence_number());
            assert!(reader.read_pointer() > expected_offset);
            assert_that!(
                record.decode_unchecked::<String>(),
                eq(format!("record{expected_offset}"))
            );
        }
        Ok(())
    });

    tokio::task::yield_now().await;
    // Not finished, we still didn't append records
    assert!(!reader_bg_handle.is_finished());

    // append 5 records to the log (offsets [1-5])
    for i in 1..=5 {
        let offset = loglet.append(format!("record{i}").into()).await?;
        info!(?offset, "appended record");
        assert_eq!(Lsn::new(i), offset);
    }

    // Written records are not enough for the reader to finish.
    // Not finished, we still didn't append records
    tokio::task::yield_now().await;
    assert!(!reader_bg_handle.is_finished());
    assert_eq!(0, read_counter.load(std::sync::atomic::Ordering::Relaxed));

    // write 5 more records.
    for i in 6..=10 {
        loglet.append(format!("record{i}").into()).await?;
    }

    // reader has finished
    reader_bg_handle.await??;
    assert_eq!(5, read_counter.load(std::sync::atomic::Ordering::Relaxed));

    Ok(())
}

/// Validates that operation of loglet read stream follows the spec when facing trims
///
/// The test requires that the loglet is empty and unsealed. It assumes that the loglet
/// is started, initialized, and ready for reads and writes. It also assumes that this loglet
/// starts from LogletOffset::OLDEST.
pub async fn single_loglet_readstream_with_trims(
    loglet: Arc<dyn Loglet>,
) -> googletest::Result<()> {
    let loglet = LogletWrapper::new(
        SegmentIndex::from(1),
        Lsn::OLDEST,
        None,
        LogletConfig::for_testing(),
        loglet,
    );

    assert_eq!(None, loglet.get_trim_point().await?);
    {
        let tail = loglet.find_tail(FindTailOptions::default()).await?;
        assert_eq!(Lsn::OLDEST, tail.offset());
        assert!(!tail.is_sealed());
    }

    // append 10 records. Offsets [1..10]
    for i in 1..=10 {
        loglet.append(format!("record{i}").into()).await?;
    }

    // Lsn(5) is trimmed, 5 records left [6..10]
    loglet.trim(Lsn::new(5)).await?;

    assert_eq!(
        Lsn::new(11),
        loglet.find_tail(FindTailOptions::default()).await?.offset()
    );
    // retry if loglet needs time to perform the trim.
    wait_for_trim(&loglet, Lsn::new(5))
        .await
        .into_test_result()?;

    let mut read_stream = loglet
        .clone()
        .create_read_stream(KeyFilter::Any, Lsn::OLDEST)
        .await?;

    let record = read_stream.next().await.unwrap()?;
    assert_that!(record.sequence_number(), eq(Lsn::new(1)));
    assert!(record.is_trim_gap());
    assert_that!(record.trim_gap_to_sequence_number(), eq(Some(Lsn::new(5))));

    assert!(read_stream.read_pointer() > Lsn::new(5));

    // Read two records (6, 7)
    for offset in 6..8 {
        let record = read_stream.next().await.unwrap()?;

        assert_that!(record.sequence_number(), eq(Lsn::new(offset)));
        assert!(record.is_data_record());
        assert_that!(
            record.decode_unchecked::<String>(),
            eq(format!("record{offset}"))
        );
    }
    assert!(!read_stream.is_terminated());
    assert_eq!(Lsn::new(8), read_stream.read_pointer());

    // tail didn't move.
    {
        let tail = loglet.find_tail(FindTailOptions::default()).await?;
        assert_eq!(Lsn::new(11), tail.offset());
        assert!(!tail.is_sealed());
    }

    // trimming beyond the release point will trim everything.
    loglet.trim(Lsn::new(u64::MAX)).await?;

    // retry if loglet needs time to perform the trim.
    wait_for_trim(&loglet, Lsn::new(10))
        .await
        .into_test_result()?;
    // tail is not impacted by the trim operation
    {
        let tail = loglet.find_tail(FindTailOptions::default()).await?;
        assert_eq!(Lsn::new(11), tail.offset());
        assert!(!tail.is_sealed());
    }

    // Add 10 more records [11..20]
    for i in 11..=20 {
        loglet.append(format!("record{i}").into()).await?;
    }

    // When reading record 8, it's acceptable to observe the record, or the trim gap. Both are
    // acceptable because replicated loglet read stream's read-head cannot be completely disabled.
    // Its minimum is to immediately read the next record after consuming the last one, so we'll
    // see record8 because it's already cached.
    //
    // read stream should send a gap from 8->10
    let record = read_stream.next().await.unwrap()?;
    assert_that!(record.sequence_number(), eq(Lsn::new(8)));
    if record.is_trim_gap() {
        assert!(record.is_trim_gap());
        assert_that!(record.trim_gap_to_sequence_number(), eq(Some(Lsn::new(10))));
    } else {
        // data record.
        assert_that!(record.decode_unchecked::<String>(), eq("record8"));
        // next record should be the trim gap
        let record = read_stream.next().await.unwrap()?;
        assert_that!(record.sequence_number(), eq(Lsn::new(9)));
        assert!(record.is_trim_gap());
        assert_that!(record.trim_gap_to_sequence_number(), eq(Some(Lsn::new(10))));
    }

    for i in 11..=20 {
        let record = read_stream.next().await.unwrap()?;
        assert_that!(record.sequence_number(), eq(Lsn::new(i)));
        assert!(record.is_data_record());
        assert_that!(
            record.decode_unchecked::<String>(),
            eq(format!("record{i}"))
        );
    }
    // we are at tail. polling should return pending.
    let pinned = std::pin::pin!(read_stream.next());
    let next_is_pending = futures::poll!(pinned);
    assert!(matches!(next_is_pending, std::task::Poll::Pending));

    Ok(())
}

/// Validates that appends fail after find_tail() returned Sealed()
pub async fn append_after_seal(loglet: Arc<dyn Loglet>) -> googletest::Result<()> {
    let loglet = LogletWrapper::new(
        SegmentIndex::from(1),
        Lsn::OLDEST,
        None,
        LogletConfig::for_testing(),
        loglet,
    );

    assert_eq!(None, loglet.get_trim_point().await?);
    {
        let tail = loglet.find_tail(FindTailOptions::default()).await?;
        assert_eq!(Lsn::OLDEST, tail.offset());
        assert!(!tail.is_sealed());
    }

    // append 5 records. Offsets [1..5]
    for i in 1..=5 {
        loglet.append(format!("record{i}").into()).await?;
    }

    loglet.seal().await?;

    // attempt to append 5 records. Offsets [6..10]. Expected to fail since seal happened on the same client.
    for i in 6..=10 {
        let res = loglet.append(format!("record{i}").into()).await;
        assert_that!(res, err(pat!(AppendError::Sealed)));
    }

    let tail = loglet.find_tail(FindTailOptions::default()).await?;
    // Seal must be applied after commit index 5 since it has been acknowledged (tail is 6 or higher)
    assert_that!(tail, pat!(TailState::Sealed(gt(Lsn::new(5)))));

    Ok(())
}

/// Validates that appends fail after find_tail() returned Sealed()
pub async fn append_after_seal_concurrent(loglet: Arc<dyn Loglet>) -> googletest::Result<()> {
    use futures::TryStreamExt as _;

    const WARMUP_APPENDS: usize = 200;
    const CONCURRENT_APPENDERS: usize = 20;

    let loglet = LogletWrapper::new(
        SegmentIndex::from(1),
        Lsn::OLDEST,
        None,
        LogletConfig::for_testing(),
        loglet,
    );

    assert_eq!(None, loglet.get_trim_point().await?);
    {
        let tail = loglet.find_tail(FindTailOptions::default()).await?;
        assert_eq!(Lsn::OLDEST, tail.offset());
        assert!(!tail.is_sealed());
    }

    // +1 for the main task waiting on all concurrent appenders
    let append_barrier = Arc::new(Barrier::new(CONCURRENT_APPENDERS + 1));

    let mut appenders: JoinSet<googletest::Result<_>> = JoinSet::new();
    for appender_id in 0..CONCURRENT_APPENDERS {
        appenders.spawn({
            let loglet = loglet.clone();
            let append_barrier = append_barrier.clone();
            async move {
                let mut i = 1;
                let mut committed = Vec::new();
                let mut warmup = true;
                loop {
                    let res = loglet
                        .append(format!("appender-{appender_id}-record{i}").into())
                        .await;
                    i += 1;
                    if i > WARMUP_APPENDS && warmup {
                        println!("appender({appender_id}) - warmup complete....");
                        append_barrier.wait().await;
                        warmup = false;
                    }
                    match res {
                        Ok(offset) => {
                            committed.push(offset);
                        }
                        Err(AppendError::Sealed) => {
                            println!("append failed({i}) => SEALED");
                            break;
                        }
                        Err(AppendError::ReconfigurationNeeded(reason)) => {
                            println!("append failed({i}) => ReconfigurationNeeded({reason})");
                            break;
                        }
                        Err(AppendError::Shutdown(_)) => {
                            break;
                        }
                        Err(e) => fail!("unexpected error: {}", e)?,
                    }
                    // give a chance to other tasks to work
                    tokio::task::yield_now().await;
                }
                Ok(committed)
            }
            .in_current_tc_as_task(TaskKind::TestRunner, "test-appender")
        });
    }

    let first_observed_seal = tokio::task::spawn({
        let loglet = loglet.clone();
        async move {
            loop {
                let res = loglet
                    .find_tail(FindTailOptions::default())
                    .await
                    .expect("find_tail succeeds");
                if res.is_sealed() {
                    return res.offset();
                }
                // give a chance to other tasks to work
                tokio::task::yield_now().await;
            }
        }
        .in_current_tc_as_task(TaskKind::TestRunner, "find-tail")
    });

    // Wait for some warmup appends
    println!("Awaiting all appenders to reach at least {WARMUP_APPENDS} appends");
    append_barrier.wait().await;
    // Go places and do other things.
    for _ in 0..5 {
        tokio::task::yield_now().await;
    }

    loglet.seal().await?;
    // fails immediately
    assert_that!(
        loglet.append("failed-record".into()).await,
        err(pat!(AppendError::Sealed))
    );

    let tail = loglet.find_tail(FindTailOptions::default()).await?;
    assert!(tail.is_sealed());
    let first_observed_seal = first_observed_seal.await?;
    println!("Sealed tail={tail:?}, first observed seal at={first_observed_seal}");

    let mut all_committed = BTreeSet::new();
    while let Some(handle) = appenders.join_next().await {
        let mut committed = handle??;
        assert!(!committed.is_empty());
        let committed_len = committed.len();
        assert_that!(committed_len, ge(WARMUP_APPENDS));
        let tail_record = committed.pop().unwrap();
        // tail must be beyond seal point
        assert_that!(tail.offset(), gt(tail_record));
        println!("Committed len={committed_len}, last appended={tail_record}");
        // ensure that all committed records are unique
        assert!(all_committed.insert(tail_record));
        for offset in committed {
            assert!(all_committed.insert(offset));
        }
    }

    // All (acknowledged) appends must have offsets less than the tail observed at the first
    // Sealed() response of find_tail()
    println!(
        "last_acked={}, first observed seal at={first_observed_seal}",
        all_committed.last().unwrap()
    );
    assert!(first_observed_seal > *all_committed.last().unwrap());

    let reader = loglet
        .clone()
        .create_read_stream_with_tail(KeyFilter::Any, Lsn::OLDEST, Some(tail.offset()))
        .await?;

    let records: BTreeSet<Lsn> = reader
        .try_filter_map(|x| std::future::ready(Ok(Some(x.sequence_number()))))
        .try_collect()
        .await?;

    // every record committed must be observed in readstream, and it's acceptable for the
    // readstream to include more records.
    assert_that!(all_committed.len(), le(records.len()));
    assert!(all_committed.is_subset(&records));

    Ok(())
}

/// Validates that an empty loglet can be sealed
pub async fn seal_empty(loglet: Arc<dyn Loglet>) -> googletest::Result<()> {
    let loglet = LogletWrapper::new(
        SegmentIndex::from(1),
        Lsn::OLDEST,
        None,
        LogletConfig::for_testing(),
        loglet,
    );

    assert_eq!(None, loglet.get_trim_point().await?);
    {
        let tail = loglet.find_tail(FindTailOptions::default()).await?;
        assert_eq!(Lsn::OLDEST, tail.offset());
        assert!(!tail.is_sealed());
    }

    let mut watch = loglet.watch_tail();
    {
        // last known tail should be immediately available through the tail watch
        let tail = watch
            .next()
            .await
            .expect("get the last known tail immediately");
        assert_eq!(Lsn::OLDEST, tail.offset());
        assert!(!tail.is_sealed());
    }

    let retry_policy = RetryPolicy::exponential(
        Duration::from_millis(100),
        2.0,
        Some(10),
        Some(Duration::from_secs(1)),
    );
    retry_on_retryable_error(retry_policy, || loglet.seal()).await?;
    let tail = loglet.find_tail(FindTailOptions::default()).await?;
    assert_eq!(Lsn::OLDEST, tail.offset());
    assert!(tail.is_sealed());

    {
        // last known tail should be immediately available through the tail watch
        let tail = watch.next().await.expect("see the sealed tail");
        assert_eq!(Lsn::OLDEST, tail.offset());
        assert!(tail.is_sealed());
    }

    Ok(())
}
