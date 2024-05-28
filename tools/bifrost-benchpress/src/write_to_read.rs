// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::{Duration, Instant};

use bytes::{Buf, BufMut, BytesMut};
use futures::StreamExt;
use hdrhistogram::Histogram;
use tracing::info;

use restate_bifrost::{Bifrost, Error as BifrostError};
use restate_bifrost::{LogRecord, Record};
use restate_core::{cancellation_watcher, TaskCenter, TaskKind};
use restate_types::logs::{LogId, Lsn, Payload, SequenceNumber};

use crate::util::print_latencies;
use crate::Arguments;

#[derive(Debug, Clone, clap::Parser)]
pub struct WriteToReadOpts {}

pub async fn run(
    _common_args: &Arguments,
    _args: &WriteToReadOpts,
    tc: TaskCenter,
    bifrost: Bifrost,
) -> anyhow::Result<()> {
    let log_id = LogId::from(0);
    let clock = quanta::Clock::new();
    // Create two tasks, one that writes to the log continously and another one that reads from the
    // log and measures the latency. Collect latencies in a histogram and print the histogram
    // before the test ends.
    let reader_task = tc.spawn(TaskKind::TestRunner, "test-log-reader", None, {
        let bifrost = bifrost.clone();
        let clock = clock.clone();
        async move {
            let mut read_stream = bifrost
                .create_reader(log_id, Lsn::INVALID, Lsn::MAX)
                .await?;
            let mut counter = 0;
            let mut cancel = std::pin::pin!(cancellation_watcher());
            let mut lag_latencies = Histogram::<u64>::new(3)?;
            let mut on_record = |record: Result<LogRecord, BifrostError>| {
                if let Ok(record) = record {
                    if let Record::Data(data) = record.record {
                        let latency_raw = data.into_body().get_u64();
                        let latency = clock.scaled(latency_raw).elapsed();
                        lag_latencies.record(latency.as_nanos() as u64)?;
                    } else {
                        panic!("Unexpected non-data record");
                    }
                } else {
                    panic!("Unexpected error");
                }
                anyhow::Ok(())
            };
            loop {
                counter += 1;
                tokio::select! {
                    _ = &mut cancel =>  {
                        // test is over. print histogram
                        info!("Reader stopping");
                        break;
                    }
                    record = read_stream.next() => {
                        let record = record.expect("stream will not terminate");
                        on_record(record)?;
                    }
                }
                if counter % 1000 == 0 {
                    info!("Read {} records", counter);
                }
            }

            println!("Total records read: {}", counter);
            print_latencies("read lag", lag_latencies);
            Ok(())
        }
    })?;

    let writer_task = tc.spawn(TaskKind::TestRunner, "test-log-appender", None, {
        let mut bifrost = bifrost.clone();
        let clock = clock.clone();
        async move {
            let mut append_latencies = Histogram::<u64>::new(3)?;
            let mut counter = 0;
            loop {
                counter += 1;
                let start = Instant::now();
                let mut bytes = BytesMut::default();
                bytes.put_u64(clock.raw());
                let payload = Payload::new(bytes);
                if bifrost.append(log_id, payload).await.is_err() {
                    println!("Total records written: {}", counter);
                    print_latencies("append latency", append_latencies);
                    break;
                };

                append_latencies.record(start.elapsed().as_nanos() as u64)?;
                if counter % 1000 == 0 {
                    info!("Appended {} records", counter);
                }
            }
            anyhow::Ok(())
        }
    })?;

    tokio::time::sleep(Duration::from_secs(5)).await;

    tc.cancel_task(reader_task).unwrap().await?;
    info!("Reader stopped");
    let _ = tc.cancel_task(writer_task);
    Ok(())
}
