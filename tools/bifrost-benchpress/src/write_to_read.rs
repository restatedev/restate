// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Instant;

use anyhow::Result;
use bytes::BytesMut;
use futures::StreamExt;
use hdrhistogram::Histogram;
use tracing::info;

use restate_bifrost::{Bifrost, ErrorRecoveryStrategy};
use restate_core::{Metadata, TaskCenter, TaskHandle, TaskKind};
use restate_types::logs::{KeyFilter, LogId, Lsn, SequenceNumber, WithKeys};

use crate::Arguments;
use crate::util::{DummyPayload, print_latencies};

const LOG_ID: LogId = LogId::new(0);

#[derive(Debug, Clone, clap::Parser)]
pub struct WriteToReadOpts {
    /// Maximum number of records to be written to bifrost loglet on every append attempt.
    #[clap(long, default_value = "2000")]
    max_batch_size: usize,

    /// Number of records that can be buffered in background appender before back-pressure kicks
    /// in.
    #[clap(long, default_value = "5000")]
    write_buffer_size: usize,

    /// The number of records to write during this test
    #[clap(long, default_value = "400000")]
    num_records: usize,

    /// The payload size of each record in bytes. Default is 500 bytes
    #[clap(long, default_value = "500")]
    payload_size: usize,
}

pub async fn run(_common_args: &Arguments, args: &WriteToReadOpts, bifrost: Bifrost) -> Result<()> {
    let clock = quanta::Clock::new();
    // Create two tasks, one that writes to the log continuously and another one that reads from the
    // log and measures the latency. Collect latencies in a histogram and print the histogram
    // before the test ends.
    let reader_task: TaskHandle<Result<_>> =
        TaskCenter::spawn_unmanaged(TaskKind::PartitionProcessor, "test-log-reader", {
            let clock = clock.clone();
            let args = args.clone();
            let bifrost = bifrost.clone();
            async move {
                let mut read_stream =
                    bifrost.create_reader(LOG_ID, KeyFilter::Any, Lsn::OLDEST, Lsn::MAX)?;
                let mut read_latency = Histogram::<u64>::new(3)?;
                let start = Instant::now();
                for counter in 1..=args.num_records {
                    let record = read_stream.next().await.expect("stream will not terminate");
                    if let Ok(record) = record {
                        let data = record.try_decode::<DummyPayload>().expect("data record")?;
                        let latency = clock.scaled(data.precise_ts).elapsed();
                        read_latency.record(latency.as_nanos() as u64)?;
                    } else {
                        panic!("Unexpected error");
                    }

                    if counter % 10000 == 0 {
                        info!("Read {} records", counter);
                    }
                    tokio::task::consume_budget().await
                }
                let total_time = start.elapsed();
                info!(
                    "Reader stopped. Total time: {:?}. Read throughput: {} ops/s",
                    total_time,
                    args.num_records as f64 / total_time.as_secs_f64(),
                );
                Ok(read_latency)
            }
        })?;

    let writer_task: TaskHandle<Result<_>> =
        TaskCenter::spawn_unmanaged(TaskKind::PartitionProcessor, "test-log-appender", {
            let clock = clock.clone();
            let bifrost = bifrost.clone();
            let args = args.clone();
            let blob = BytesMut::zeroed(args.payload_size).freeze();
            async move {
                let mut append_latency = Histogram::<u64>::new(3)?;
                let mut appender_handle = bifrost
                    .create_background_appender(
                        LOG_ID,
                        ErrorRecoveryStrategy::ExtendChainPreferred,
                        args.write_buffer_size,
                        args.max_batch_size,
                    )?
                    .start("writer")?;
                let sender = appender_handle.sender();
                let start = Instant::now();
                for counter in 1..=args.num_records {
                    let start = Instant::now();
                    let record = DummyPayload {
                        precise_ts: clock.raw(),
                        blob: blob.clone(),
                    };
                    sender.enqueue(record.with_no_keys()).await.unwrap();
                    append_latency.record(start.elapsed().as_nanos() as u64)?;
                    if counter % 10000 == 0 {
                        info!("Appended {} records", counter);
                    }
                    tokio::task::consume_budget().await
                }
                info!("Appender waiting for drain");
                appender_handle.drain().await?;

                let total_time = start.elapsed();
                println!(
                    "Total records written: {}. Total time: {:?}, append throughput: {} ops/s",
                    args.num_records,
                    total_time,
                    args.num_records as f64 / total_time.as_secs_f64(),
                );
                Ok(append_latency)
            }
        })?;

    let (append_latency, read_latency) = tokio::join!(writer_task, reader_task);
    let (append_latency, read_latency) = (append_latency??, read_latency??);

    println!(
        "Log Chain: {:#?}",
        Metadata::current().logs_ref().chain(&LOG_ID).unwrap()
    );
    println!("Payload size per record: {} bytes", args.payload_size);
    println!();
    print_latencies("append latency", append_latency);
    print_latencies("write-to-read latency", read_latency);
    TaskCenter::shutdown_node("completed", 0).await;
    Ok(())
}
