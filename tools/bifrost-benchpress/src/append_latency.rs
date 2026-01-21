// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Instant;

use bytes::BytesMut;
use hdrhistogram::Histogram;
use tracing::info;

use restate_bifrost::{Bifrost, ErrorRecoveryStrategy};
use restate_types::logs::{LogId, WithKeys};

use crate::Arguments;
use crate::util::{DummyPayload, print_latencies};

const LOG_ID: LogId = LogId::new(0);

#[derive(Debug, Clone, clap::Parser)]
pub struct AppendLatencyOpts {
    /// The number of records to write during this test
    #[arg(long, default_value = "1000000")]
    pub num_records: u64,

    /// The payload size of each record in bytes. Default is 500 bytes
    #[clap(long, default_value = "500")]
    payload_size: usize,
}

pub async fn run(
    _common_args: &Arguments,
    args: &AppendLatencyOpts,
    bifrost: Bifrost,
) -> anyhow::Result<()> {
    let blob = BytesMut::zeroed(args.payload_size).freeze();
    let mut append_latencies = Histogram::<u64>::new(3)?;
    let mut counter = 0;
    let mut appender =
        bifrost.create_appender(LOG_ID, ErrorRecoveryStrategy::ExtendChainPreferred)?;
    let start = Instant::now();
    loop {
        if counter >= args.num_records {
            break;
        }
        counter += 1;
        let start_append = Instant::now();
        let record = DummyPayload {
            precise_ts: 0,
            blob: blob.clone(),
        };
        let _ = appender.append(record.with_no_keys()).await?;
        append_latencies.record(start_append.elapsed().as_nanos() as u64)?;
        if counter % 1000 == 0 {
            info!("Appended {} records", counter);
        }
    }

    let total_time = start.elapsed();
    println!(
        "Total records written: {}. Total time: {:?}, append throughput: {} ops/s",
        args.num_records,
        total_time,
        args.num_records as f64 / total_time.as_secs_f64(),
    );
    print_latencies("append latency", append_latencies);
    Ok(())
}
