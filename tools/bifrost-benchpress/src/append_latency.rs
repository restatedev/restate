// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
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

use hdrhistogram::Histogram;
use tracing::info;

use restate_bifrost::Bifrost;
use restate_core::TaskCenter;
use restate_types::logs::{LogId, WithKeys};

use crate::util::print_latencies;
use crate::Arguments;

#[derive(Debug, Clone, clap::Parser)]
pub struct AppendLatencyOpts {
    #[arg(long, default_value = "5000")]
    pub num_records: u64,
}

pub async fn run(
    _common_args: &Arguments,
    opts: &AppendLatencyOpts,
    _tc: TaskCenter,
    bifrost: Bifrost,
) -> anyhow::Result<()> {
    let log_id = LogId::from(0);
    let data = Arc::new("1".repeat(1024).with_no_keys());
    let mut append_latencies = Histogram::<u64>::new(3)?;
    let mut counter = 0;
    let mut appender = bifrost.create_appender(log_id)?;
    loop {
        if counter >= opts.num_records {
            break;
        }
        counter += 1;
        let start = Instant::now();
        let _ = appender.append(data.clone()).await?;
        append_latencies.record(start.elapsed().as_nanos() as u64)?;
        if counter % 1000 == 0 {
            info!("Appended {} records", counter);
        }
    }
    println!("Total records written: {}", counter);
    print_latencies("append latency", append_latencies);
    Ok(())
}
