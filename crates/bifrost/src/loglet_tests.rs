// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use googletest::prelude::*;
use tokio::task::JoinHandle;

use restate_test_util::let_assert;
use restate_types::logs::SequenceNumber;
use tracing::info;

use crate::loglet::{Loglet, LogletOffset};
use crate::{LogRecord, Record, TrimGap};

fn setup() {
    // Make sure that panics exits the process.
    let orig_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        // invoke the default handler and exit the process
        orig_hook(panic_info);
        std::process::exit(1);
    }));
}

async fn wait_for_trim(
    loglet: &Arc<dyn Loglet>,
    required_trim_point: LogletOffset,
) -> anyhow::Result<()> {
    for _ in 0..3 {
        let trim_point = loglet.get_trim_point().await?;
        if trim_point.is_none() || trim_point.is_some_and(|t| t < required_trim_point) {
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        } else {
            assert_eq!(Some(required_trim_point), trim_point);
            break;
        }
    }
    Ok(())
}

/// Validates that high-level behaviour of the loglet is correct but it should not be considered
/// as full-spec validation.
///
/// The test requires that the loglet is empty and unsealed. It assumes that the loglet
/// is started, initialized, and ready for reads and writes. It also assumes that this loglet
/// provide contiguous offsets that start from LogletOffset::OLDEST.
pub async fn gapless_loglet_smoke_test(loglet: Arc<dyn Loglet>) -> googletest::Result<()> {
    setup();

    assert_eq!(None, loglet.get_trim_point().await?);
    {
        let tail = loglet.find_tail().await?;
        assert_eq!(LogletOffset::OLDEST, tail.offset());
        assert!(!tail.is_sealed());
    }

    let start: u64 = LogletOffset::OLDEST.into();
    let end: u64 = start + 3;
    for i in start..end {
        // Append i
        let offset = loglet.append(Bytes::from(format!("record{}", i))).await?;
        assert_eq!(LogletOffset::from(i), offset);
        assert_eq!(None, loglet.get_trim_point().await?);
        {
            let tail = loglet.find_tail().await?;
            assert_eq!(LogletOffset::from(i + 1), tail.offset());
            assert!(!tail.is_sealed());
        }
    }

    // read record 1 (reading from OLDEST)
    let_assert!(Some(log_record) = loglet.read_next_single_opt(LogletOffset::OLDEST).await?);
    let LogRecord { offset, record } = log_record;
    assert_eq!(LogletOffset::OLDEST, offset,);
    assert!(record.is_data());
    assert_eq!(Some(&Bytes::from_static(b"record1")), record.payload());

    // read record 2
    let_assert!(Some(log_record) = loglet.read_next_single_opt(offset.next()).await?);
    let LogRecord { offset, record } = log_record;
    assert_eq!(LogletOffset::from(2), offset);
    assert_eq!(Some(&Bytes::from_static(b"record2")), record.payload());

    // read record 3
    let_assert!(Some(log_record) = loglet.read_next_single_opt(offset.next()).await?);
    let LogRecord { offset, record } = log_record;
    assert_eq!(LogletOffset::from(3), offset);
    assert_eq!(Some(&Bytes::from_static(b"record3")), record.payload());

    // trim point and tail didn't change
    assert_eq!(None, loglet.get_trim_point().await?);
    {
        let tail = loglet.find_tail().await?;
        assert_eq!(LogletOffset::from(end), tail.offset());
        assert!(!tail.is_sealed());
    }

    // read from the future returns None
    assert!(loglet
        .read_next_single_opt(LogletOffset::from(end))
        .await?
        .is_none());

    let handle1: JoinHandle<googletest::Result<()>> = tokio::spawn({
        let loglet = loglet.clone();
        async move {
            // read future record 4
            let LogRecord { offset, record } =
                loglet.read_next_single(LogletOffset::from(4)).await?;
            assert_eq!(LogletOffset(4), offset);
            assert_eq!(Some(&Bytes::from_static(b"record4")), record.payload());
            Ok(())
        }
    });

    // Waiting for 10
    let handle2: JoinHandle<googletest::Result<()>> = tokio::spawn({
        let loglet = loglet.clone();
        async move {
            // read future record 10
            let LogRecord { offset, record } =
                loglet.read_next_single(LogletOffset::from(10)).await?;
            assert_eq!(LogletOffset(10), offset);
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
    let offset = loglet.append(Bytes::from_static(b"record4")).await?;
    assert_eq!(LogletOffset(4), offset);
    assert_eq!(None, loglet.get_trim_point().await?);
    {
        let tail = loglet.find_tail().await?;
        assert_eq!(LogletOffset::from(5), tail.offset());
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
    assert_eq!(LogletOffset(5), loglet.find_tail().await?.offset());

    // trim the loglet to and including 4
    loglet.trim(LogletOffset::from(3)).await?;
    assert_eq!(Some(LogletOffset::from(3)), loglet.get_trim_point().await?);

    // tail didn't change
    {
        let tail = loglet.find_tail().await?;
        assert_eq!(LogletOffset::from(5), tail.offset());
        assert!(!tail.is_sealed());
    }

    let_assert!(Some(log_record) = loglet.read_next_single_opt(LogletOffset::OLDEST).await?);
    let LogRecord { offset, record } = log_record;
    assert_eq!(LogletOffset::OLDEST, offset);
    assert!(record.is_trim_gap());
    assert_eq!(
        LogletOffset::from(3),
        record.try_as_trim_gap_ref().unwrap().to
    );

    let_assert!(Some(log_record) = loglet.read_next_single_opt(LogletOffset::from(4)).await?);
    let LogRecord { offset, record } = log_record;
    assert_eq!(LogletOffset::from(4), offset);
    assert!(record.is_data());
    assert_eq!(Some(&Bytes::from_static(b"record4")), record.payload());

    Ok(())
}

/// Validates that basic operation of loglet read stream is behaving according to spec.
///
/// The test requires that the loglet is empty and unsealed. It assumes that the loglet
/// is started, initialized, and ready for reads and writes. It also assumes that this loglet
/// starts from LogletOffset::OLDEST.
pub async fn single_loglet_readstream_test(loglet: Arc<dyn Loglet>) -> googletest::Result<()> {
    setup();

    let read_from_offset = LogletOffset::from(6);
    let mut reader = loglet.clone().create_read_stream(read_from_offset).await?;

    {
        // no records have been written yet.
        let tail = loglet.find_tail().await?;
        assert_eq!(LogletOffset::OLDEST, tail.offset());
        assert!(!tail.is_sealed());
    }
    // We didn't perform any reads yet, read_pointer shouldn't have moved.
    assert_eq!(read_from_offset, reader.read_pointer());

    // spawn a reader that reads 5 records and exits. The reader is expected to wait/block until
    // records appear at offset 5 The reader is expected to wait/block until records appear at
    // offset 6. It reads [6-10]
    let read_counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = read_counter.clone();
    let reader_bg_handle: JoinHandle<googletest::Result<()>> = tokio::spawn(async move {
        for i in 6..=10 {
            let record = reader.next().await.expect("to never terminate")?;
            let expected_offset = LogletOffset::from(i);
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
        let offset = loglet.append(Bytes::from(format!("record{}", i))).await?;
        info!(?offset, "appended record");
        assert_eq!(LogletOffset::from(i), offset);
    }

    // Written records are not enough for the reader to finish.
    // Not finished, we still didn't append records
    tokio::task::yield_now().await;
    assert!(!reader_bg_handle.is_finished());
    assert_eq!(0, read_counter.load(std::sync::atomic::Ordering::Relaxed));

    // write 5 more records.
    for i in 6..=10 {
        loglet.append(Bytes::from(format!("record{}", i))).await?;
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
pub async fn single_loglet_readstream_test_with_trims(
    loglet: Arc<dyn Loglet>,
) -> googletest::Result<()> {
    setup();

    assert_eq!(None, loglet.get_trim_point().await?);
    {
        let tail = loglet.find_tail().await?;
        assert_eq!(LogletOffset::OLDEST, tail.offset());
        assert!(!tail.is_sealed());
    }

    // append 10 records. Offsets [1..10]
    for i in 1..=10 {
        loglet.append(Bytes::from(format!("record{}", i))).await?;
    }

    // Lsn(5) is trimmed, 5 records left [6..10]
    loglet.trim(LogletOffset::from(5)).await?;

    assert_eq!(LogletOffset::from(11), loglet.find_tail().await?.offset());
    // retry if loglet needs time to perform the trim.
    wait_for_trim(&loglet, LogletOffset::from(5))
        .await
        .into_test_result()?;

    let mut read_stream = loglet
        .clone()
        .create_read_stream(LogletOffset::OLDEST)
        .await?;

    let record = read_stream.next().await.unwrap()?;
    assert_that!(
        record,
        pat!(LogRecord {
            offset: eq(LogletOffset::from(1)),
            record: pat!(Record::TrimGap(pat!(TrimGap {
                to: eq(LogletOffset::from(5)),
            })))
        })
    );

    assert!(read_stream.read_pointer() > LogletOffset::from(5));

    // Read two records (6, 7)
    for offset in 6..8 {
        let record = read_stream.next().await.unwrap()?;
        assert_that!(
            record,
            pat!(LogRecord {
                offset: eq(LogletOffset::from(offset)),
                record: pat!(Record::Data(_))
            })
        );
    }
    assert!(!read_stream.is_terminated());
    assert_eq!(LogletOffset::from(8), read_stream.read_pointer());

    // tail didn't move.
    {
        let tail = loglet.find_tail().await?;
        assert_eq!(LogletOffset::from(11), tail.offset());
        assert!(!tail.is_sealed());
    }

    // trimming beyond the release point will trim everything.
    loglet.trim(LogletOffset::from(u64::MAX)).await?;

    // retry if loglet needs time to perform the trim.
    wait_for_trim(&loglet, LogletOffset::from(10))
        .await
        .into_test_result()?;
    // tail is not impacted by the trim operation
    {
        let tail = loglet.find_tail().await?;
        assert_eq!(LogletOffset::from(11), tail.offset());
        assert!(!tail.is_sealed());
    }

    // Add 10 more records [11..20]
    for i in 11..=20 {
        loglet.append(Bytes::from(format!("record{}", i))).await?;
    }

    // read stream should send a gap from 8->10
    let record = read_stream.next().await.unwrap()?;
    assert_that!(
        record,
        pat!(LogRecord {
            offset: eq(LogletOffset::from(8)),
            record: pat!(Record::TrimGap(pat!(TrimGap {
                to: eq(LogletOffset::from(10)),
            })))
        })
    );

    for i in 11..=20 {
        let record = read_stream.next().await.unwrap()?;
        let expected_payload = Bytes::from(format!("record{}", i));
        assert_that!(
            record,
            eq(LogRecord {
                offset: LogletOffset::from(i),
                record: Record::Data(expected_payload),
                //record: pat!(Record::Data(_))
            })
        );
    }
    // we are at tail. polling should return pending.
    let pinned = std::pin::pin!(read_stream.next());
    let next_is_pending = futures::poll!(pinned);
    assert!(matches!(next_is_pending, std::task::Poll::Pending));

    Ok(())
}
