// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use anyhow::Context;
use bytesize::ByteSize;
use datafusion::arrow::array::{Array, Int64Array};
use datafusion::arrow::record_batch::RecordBatch;
use futures::StreamExt;
use metrics::{counter, gauge, histogram};
use tokio::time::{Instant, MissedTickBehavior};
use tracing::{debug, info};

use restate_core::cancellation_watcher;
use restate_storage_query_datafusion::context::QueryContext;
use restate_types::retries::with_jitter;

use crate::metric_definitions::{
    USAGE_STATE_SIZE_ACCOUNTING_QUERY_DURATION_SECONDS, USAGE_STATE_STORAGE_BYTE_SECONDS,
    USAGE_STATE_STORAGE_BYTES, describe_metrics,
};

pub(crate) const STORAGE_QUERY: &str =
    "SELECT sum(octet_length(key) + length(value)) AS total_state_size FROM state";

pub struct StorageAccountingTask {
    query_context: QueryContext,
    pub(crate) update_interval: Duration,
    last_reading_bytes: Option<u64>,
    last_reading_instant: Option<Instant>,
}

impl StorageAccountingTask {
    pub fn new(query_context: QueryContext, update_interval: Duration) -> Self {
        Self {
            query_context,
            update_interval,
            last_reading_bytes: None,
            last_reading_instant: None,
        }
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        describe_metrics();

        let effective_interval = with_jitter(self.update_interval, 0.1);
        let start_at = tokio::time::Instant::now() + effective_interval;
        let mut update_interval = tokio::time::interval_at(start_at, effective_interval);
        update_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        debug!(
            "Starting storage accounting task with refresh interval: {:?}",
            self.update_interval
        );
        let mut cancel = std::pin::pin!(cancellation_watcher());
        loop {
            tokio::select! {
                _ = update_interval.tick() => {
                    let start = Instant::now();
                    if let Err(e) = self.collect_and_update_state_storage_metrics(start).await {
                        info!("Storage accounting collection failed: {}", e);
                    }
                    histogram!(USAGE_STATE_SIZE_ACCOUNTING_QUERY_DURATION_SECONDS).record(start.elapsed().as_secs_f64());
                }
                _ = &mut cancel => {
                    break;
                }
            }
        }

        Ok(())
    }

    pub(crate) async fn collect_and_update_state_storage_metrics(
        &mut self,
        instant: Instant,
    ) -> anyhow::Result<()> {
        let batches: Vec<RecordBatch> = self
            .query_context
            .execute(STORAGE_QUERY)
            .await?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        let mut current_bytes = 0u64;
        for batch in batches {
            let total_state_size_column = batch
                .column_by_name("total_state_size")
                .context("Missing total_state_size column")?;

            let Some(array) = total_state_size_column
                .as_any()
                .downcast_ref::<Int64Array>()
            else {
                anyhow::bail!(format!(
                    "Unexpected total_state_size column type: {}",
                    total_state_size_column.data_type()
                ));
            };

            current_bytes = current_bytes.saturating_add(
                array
                    .iter()
                    .filter_map(|opt_val| opt_val.map(|v| v as u64))
                    .sum::<u64>(),
            );
        }

        let added_byte_seconds = if let (Some(previous_bytes), Some(last_measurement_instant)) =
            (self.last_reading_bytes, self.last_reading_instant)
        {
            let elapsed_seconds = instant
                .duration_since(last_measurement_instant)
                .as_secs_f64();
            let average_bytes = (previous_bytes + current_bytes) as f64 / 2.0;
            (average_bytes * elapsed_seconds) as u64
        } else {
            0
        };

        self.last_reading_bytes = Some(current_bytes);
        self.last_reading_instant = Some(instant);

        update_storage_usage_metrics(current_bytes, added_byte_seconds);

        Ok(())
    }
}

fn update_storage_usage_metrics(total_bytes: u64, byte_seconds_added: u64) {
    gauge!(USAGE_STATE_STORAGE_BYTES).set(total_bytes as f64);

    // u64 overflows after approximately 544 gigabyte-years; to the metric collection system
    // an overflow will look like the process just restarted - something it has to deal with anyway
    counter!(USAGE_STATE_STORAGE_BYTE_SECONDS).increment(byte_seconds_added);

    debug!(
        "Updated storage usage: current total {}, +{}-seconds",
        ByteSize::b(total_bytes).display(),
        ByteSize::b(byte_seconds_added).display(),
    );
}
