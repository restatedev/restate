// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use googletest::prelude::*;
use restate_types::logs::metadata::SegmentIndex;
use tokio::sync::Barrier;
use tokio::task::{JoinHandle, JoinSet};

use restate_test_util::let_assert;
use restate_types::logs::{KeyFilter, Keys, Lsn, SequenceNumber};
use tokio_stream::StreamExt;
use tracing::info;

use super::{Loglet, LogletOffset};
use crate::loglet::{AppendError, LogletBase, LogletReadStream};
use crate::loglet_wrapper::LogletWrapper;
use crate::{setup_panic_handler, LogRecord, Record, TailState, TrimGap};

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
    setup_panic_handler();
    let loglet = LogletWrapper::new(SegmentIndex::from(1), Lsn::OLDEST, None, loglet);

    assert_eq!(None, loglet.get_trim_point().await?);
    {
        let tail = loglet.find_tail().await?;
        assert_eq!(Lsn::OLDEST, tail.offset());
        assert!(!tail.is_sealed());
    }

    let start: u64 = Lsn::OLDEST.into();
    let end: u64 = start + 3;
    for i in start..end {
        // Append i
        let offset = loglet
            .append(&Bytes::from(format!("record{}", i)), &Keys::None)
            .await?;
        assert_eq!(Lsn::new(i), offset);
        assert_eq!(None, loglet.get_trim_point().await?);
        {
            let tail = loglet.find_tail().await?;
            assert_eq!(Lsn::new(i + 1), tail.offset());
            assert!(!tail.is_sealed());
        }
    }

    // read record 1 (reading from OLDEST)
    let_assert!(Some(log_record) = loglet.read_opt(Lsn::OLDEST).await?);
    let LogRecord { offset, record } = log_record;
    assert_eq!(Lsn::OLDEST, offset);
    assert!(record.is_data());
    assert_eq!(Some(&Bytes::from_static(b"record1")), record.payload());

    // read record 2
    let_assert!(Some(log_record) = loglet.read_opt(offset.next()).await?);
    let LogRecord { offset, record } = log_record;
    assert_eq!(Lsn::new(2), offset);
    assert_eq!(Some(&Bytes::from_static(b"record2")), record.payload());

    // read record 3
    let_assert!(Some(log_record) = loglet.read_opt(offset.next()).await?);
    let LogRecord { offset, record } = log_record;
    assert_eq!(Lsn::new(3), offset);
    assert_eq!(Some(&Bytes::from_static(b"record3")), record.payload());

    // trim point and tail didn't change
    assert_eq!(None, loglet.get_trim_point().await?);
    {
        let tail = loglet.find_tail().await?;
        assert_eq!(Lsn::new(end), tail.offset());
        assert!(!tail.is_sealed());
    }

    // read from the future returns None
    assert!(loglet.read_opt(Lsn::new(end)).await?.is_none());

    let handle1: JoinHandle<googletest::Result<()>> = tokio::spawn({
        let loglet = loglet.clone();
        async move {
            // read future record 4
            let LogRecord { offset, record } = loglet.read(Lsn::new(4)).await?;
            assert_eq!(Lsn::new(4), offset);
            assert_eq!(Some(&Bytes::from_static(b"record4")), record.payload());
            Ok(())
        }
    });

    // Waiting for 10
    let handle2: JoinHandle<googletest::Result<()>> = tokio::spawn({
        let loglet = loglet.clone();
        async move {
            // read future record 10
            let LogRecord { offset, record } = loglet.read(Lsn::new(10)).await?;
            assert_eq!(Lsn::new(10), offset);
            assert_eq!(Some(&Bytes::from_static(b"record10")), record.payload());
            Ok(())
        }
    });

    // Giving a chance to other tasks to work.
    tokio::task::yield_now().await;
    assert!(
        !handle1.is_finished(),
        "record 4 should not be ready to read yet"
    );

    // Append 4
    let offset = loglet
        .append(&Bytes::from_static(b"record4"), &Keys::None)
        .await?;
    assert_eq!(Lsn::new(4), offset);
    assert_eq!(None, loglet.get_trim_point().await?);
    {
        let tail = loglet.find_tail().await?;
        assert_eq!(Lsn::new(5), tail.offset());
        assert!(!tail.is_sealed());
    }

    assert!(tokio::time::timeout(Duration::from_secs(5), handle1)
        .await??
        .is_ok());

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
    assert_eq!(Lsn::new(5), loglet.find_tail().await?.offset());

    // trim the loglet to and including 3
    loglet.trim(Lsn::new(3)).await?;
    assert_eq!(Some(Lsn::new(3)), loglet.get_trim_point().await?);

    // tail didn't change
    {
        let tail = loglet.find_tail().await?;
        assert_eq!(Lsn::new(5), tail.offset());
        assert!(!tail.is_sealed());
    }

    let_assert!(Some(log_record) = loglet.read_opt(Lsn::OLDEST).await?);
    let LogRecord { offset, record } = log_record;
    assert_eq!(Lsn::OLDEST, offset);
    assert!(record.is_trim_gap());
    assert_eq!(Lsn::new(3), record.try_as_trim_gap_ref().unwrap().to);

    let_assert!(Some(log_record) = loglet.read_opt(Lsn::from(4)).await?);
    let LogRecord { offset, record } = log_record;
    assert_eq!(Lsn::from(4), offset);
    assert!(record.is_data());
    assert_eq!(Some(&Bytes::from_static(b"record4")), record.payload());

    Ok(())
}

/// Validates that basic operation of loglet read stream is behaving according to spec.
///
/// The test requires that the loglet is empty and unsealed. It assumes that the loglet
/// is started, initialized, and ready for reads and writes. It also assumes that this loglet
/// starts from Lsn::OLDEST.
pub async fn single_loglet_readstream(loglet: Arc<dyn Loglet>) -> googletest::Result<()> {
    setup_panic_handler();
    let loglet = LogletWrapper::new(SegmentIndex::from(1), Lsn::OLDEST, None, loglet);

    let read_from_offset = Lsn::new(6);
    let mut reader = loglet
        .clone()
        .create_wrapped_read_stream(KeyFilter::Any, read_from_offset)
        .await?;

    {
        // no records have been written yet.
        let tail = loglet.find_tail().await?;
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
            assert_eq!(expected_offset, record.offset);
            assert!(reader.read_pointer() > expected_offset);
            assert_eq!(
                Some(&Bytes::from(format!("record{}", expected_offset))),
                record.record.payload()
            );
        }
        Ok(())
    });

    tokio::task::yield_now().await;
    // Not finished, we still didn't append records
    assert!(!reader_bg_handle.is_finished());

    // append 5 records to the log (offsets [1-5])
    for i in 1..=5 {
        let offset = loglet
            .append(&Bytes::from(format!("record{}", i)), &Keys::None)
            .await?;
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
        loglet
            .append(&Bytes::from(format!("record{}", i)), &Keys::None)
            .await?;
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
    setup_panic_handler();
    let loglet = LogletWrapper::new(SegmentIndex::from(1), Lsn::OLDEST, None, loglet);

    assert_eq!(None, loglet.get_trim_point().await?);
    {
        let tail = loglet.find_tail().await?;
        assert_eq!(Lsn::OLDEST, tail.offset());
        assert!(!tail.is_sealed());
    }

    // append 10 records. Offsets [1..10]
    for i in 1..=10 {
        loglet
            .append(&Bytes::from(format!("record{}", i)), &Keys::None)
            .await?;
    }

    // Lsn(5) is trimmed, 5 records left [6..10]
    loglet.trim(Lsn::new(5)).await?;

    assert_eq!(Lsn::new(11), loglet.find_tail().await?.offset());
    // retry if loglet needs time to perform the trim.
    wait_for_trim(&loglet, Lsn::new(5))
        .await
        .into_test_result()?;

    let mut read_stream = loglet
        .clone()
        .create_wrapped_read_stream(KeyFilter::Any, Lsn::OLDEST)
        .await?;

    let record = read_stream.next().await.unwrap()?;
    assert_that!(
        record,
        pat!(LogRecord {
            offset: eq(Lsn::new(1)),
            record: pat!(Record::TrimGap(pat!(TrimGap {
                to: eq(Lsn::new(5)),
            })))
        })
    );

    assert!(read_stream.read_pointer() > Lsn::new(5));

    // Read two records (6, 7)
    for offset in 6..8 {
        let record = read_stream.next().await.unwrap()?;
        assert_that!(
            record,
            pat!(LogRecord {
                offset: eq(Lsn::new(offset)),
                record: pat!(Record::Data(_))
            })
        );
    }
    assert!(!read_stream.is_terminated());
    assert_eq!(Lsn::new(8), read_stream.read_pointer());

    // tail didn't move.
    {
        let tail = loglet.find_tail().await?;
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
        let tail = loglet.find_tail().await?;
        assert_eq!(Lsn::new(11), tail.offset());
        assert!(!tail.is_sealed());
    }

    // Add 10 more records [11..20]
    for i in 11..=20 {
        loglet
            .append(&Bytes::from(format!("record{}", i)), &Keys::None)
            .await?;
    }

    // read stream should send a gap from 8->10
    let record = read_stream.next().await.unwrap()?;
    assert_that!(
        record,
        pat!(LogRecord {
            offset: eq(Lsn::new(8)),
            record: pat!(Record::TrimGap(pat!(TrimGap {
                to: eq(Lsn::new(10)),
            })))
        })
    );

    for i in 11..=20 {
        let record = read_stream.next().await.unwrap()?;
        let expected_payload = Bytes::from(format!("record{}", i));
        assert_that!(
            record,
            eq(LogRecord {
                offset: Lsn::new(i),
                record: Record::Data(expected_payload),
            })
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
    setup_panic_handler();
    let loglet = LogletWrapper::new(SegmentIndex::from(1), Lsn::OLDEST, None, loglet);

    assert_eq!(None, loglet.get_trim_point().await?);
    {
        let tail = loglet.find_tail().await?;
        assert_eq!(Lsn::OLDEST, tail.offset());
        assert!(!tail.is_sealed());
    }

    // append 5 records. Offsets [1..5]
    for i in 1..=5 {
        loglet
            .append(&Bytes::from(format!("record{}", i)), &Keys::None)
            .await?;
    }

    loglet.seal().await?;

    // attempt to append 5 records. Offsets [6..10]. Expected to fail since seal happened on the same client.
    for i in 6..=10 {
        let res = loglet
            .append(&Bytes::from(format!("record{}", i)), &Keys::None)
            .await;
        assert_that!(res, err(pat!(AppendError::Sealed)));
    }

    let tail = loglet.find_tail().await?;
    // Seal must be applied after commit index 5 since it has been acknowledged (tail is 6 or higher)
    assert_that!(tail, pat!(TailState::Sealed(gt(Lsn::new(5)))));

    Ok(())
}

/// Validates that appends fail after find_tail() returned Sealed()
pub async fn append_after_seal_concurrent(loglet: Arc<dyn Loglet>) -> googletest::Result<()> {
    use futures::TryStreamExt as _;

    const WARMUP_APPENDS: usize = 200;
    const CONCURRENT_APPENDERS: usize = 20;

    setup_panic_handler();
    let loglet = LogletWrapper::new(SegmentIndex::from(1), Lsn::OLDEST, None, loglet);

    assert_eq!(None, loglet.get_trim_point().await?);
    {
        let tail = loglet.find_tail().await?;
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
                        .append(
                            &Bytes::from(format!("appender-{}-record{}", appender_id, i)),
                            &Keys::None,
                        )
                        .await;
                    i += 1;
                    if i > WARMUP_APPENDS && warmup {
                        println!("appender({}) - warmup complete....", appender_id);
                        append_barrier.wait().await;
                        warmup = false;
                    }
                    match res {
                        Ok(offset) => {
                            committed.push(offset);
                        }
                        Err(AppendError::Sealed) => {
                            break;
                        }
                        Err(e) => fail!("unexpected error: {}", e)?,
                    }
                    // give a chance to other tasks to work
                    tokio::task::yield_now().await;
                }
                Ok(committed)
            }
        });
    }

    let first_observed_seal = tokio::task::spawn({
        let loglet = loglet.clone();
        async move {
            loop {
                let res = loglet.find_tail().await.expect("find_tail succeeds");
                if res.is_sealed() {
                    return res.offset();
                }
                // give a chance to other tasks to work
                tokio::task::yield_now().await;
            }
        }
    });

    // Wait for some warmup appends
    println!(
        "Awaiting all appenders to reach at least {} appends",
        WARMUP_APPENDS
    );
    append_barrier.wait().await;
    // Go places and do other things.
    for _ in 0..5 {
        tokio::task::yield_now().await;
    }

    loglet.seal().await?;
    // fails immediately
    assert_that!(
        loglet
            .append(&Bytes::from_static(b"failed-record"), &Keys::None)
            .await,
        err(pat!(AppendError::Sealed))
    );

    let tail = loglet.find_tail().await?;
    assert!(tail.is_sealed());
    let first_observed_seal = first_observed_seal.await?;
    println!(
        "Sealed tail={:?}, first observed seal at={}",
        tail, first_observed_seal
    );

    let mut all_committed = BTreeSet::new();
    while let Some(handle) = appenders.join_next().await {
        let mut committed = handle??;
        assert!(!committed.is_empty());
        let committed_len = committed.len();
        assert!(committed_len >= WARMUP_APPENDS);
        let tail_record = committed.pop().unwrap();
        // tail must be beyond seal point
        assert!(tail.offset() > tail_record);
        println!(
            "Committed len={}, last appended={}",
            committed_len, tail_record
        );
        // ensure that all committed records are unique
        assert!(all_committed.insert(tail_record));
        for offset in committed {
            assert!(all_committed.insert(offset));
        }
    }

    // All (acknowledged) appends must have offsets less than the tail observed at the first
    // Sealed() response of find_tail()
    assert!(first_observed_seal > *all_committed.last().unwrap());

    let reader = loglet
        .clone()
        .create_read_stream_with_tail(KeyFilter::Any, Lsn::OLDEST, Some(tail.offset()))
        .await?;

    let records: BTreeSet<Lsn> = reader
        .try_filter_map(|x| std::future::ready(Ok(Some(x.offset))))
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
    setup_panic_handler();

    assert_eq!(None, loglet.get_trim_point().await?);
    {
        let tail = loglet.find_tail().await?;
        assert_eq!(LogletOffset::OLDEST, tail.offset());
        assert!(!tail.is_sealed());
    }

    let mut watch = loglet.watch_tail();
    {
        // last known tail should be immediately available through the tail watch
        let tail = watch
            .next()
            .await
            .expect("get the last known tail immediately");
        assert_eq!(LogletOffset::OLDEST, tail.offset());
        assert!(!tail.is_sealed());
    }

    loglet.seal().await?;
    let tail = loglet.find_tail().await?;
    assert_eq!(LogletOffset::OLDEST, tail.offset());
    assert!(tail.is_sealed());

    {
        // last known tail should be immediately available through the tail watch
        let tail = watch.next().await.expect("see the sealed tail");
        assert_eq!(LogletOffset::OLDEST, tail.offset());
        assert!(tail.is_sealed());
    }

    Ok(())
}
