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

use datafusion::arrow::array::{Array, Int64Array, UInt64Array};
use datafusion::arrow::record_batch::RecordBatch;
use futures::StreamExt;
use tokio::time::{Instant, MissedTickBehavior, interval};
use tracing::{debug, error, info, warn};

use restate_core::{ShutdownError, TaskCenter, TaskId, TaskKind, cancellation_watcher};
use restate_storage_query_datafusion::context::QueryContext;
use restate_types::retries::with_jitter;

use super::metrics::update_storage_bytes_metrics;

pub(crate) const STORAGE_QUERY: &str =
    "SELECT sum(octet_length(key) + length(value)) as total_state_size FROM state";

pub struct StorageAccountingTask {
    query_context: QueryContext,
    pub(crate) update_interval: Duration,
    previous_bytes: Option<u64>,
    previous_timestamp: Option<Instant>,
    total_byte_seconds: f64,
}

impl StorageAccountingTask {
    pub fn new(query_context: QueryContext, update_interval: Duration) -> Self {
        Self {
            query_context,
            update_interval,
            previous_bytes: None,
            previous_timestamp: None,
            total_byte_seconds: 0.0,
        }
    }

    pub fn start(
        query_context: QueryContext,
        update_interval: Option<Duration>,
    ) -> Result<TaskId, ShutdownError> {
        let interval = update_interval.unwrap_or(Duration::from_secs(1));
        let mut task = Self::new(query_context, interval);

        TaskCenter::current().spawn_child(
            TaskKind::SystemService,
            "storage-accounting",
            async move {
                if let Err(err) = task.run().await {
                    error!("Storage accounting task failed: {}", err);
                }
                Ok(())
            },
        )
    }

    async fn run(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!(
            "Starting storage accounting task with interval: {:?}",
            self.update_interval
        );

        let mut update_interval = interval(with_jitter(self.update_interval, 0.1));
        update_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let mut cancel = std::pin::pin!(cancellation_watcher());

        loop {
            tokio::select! {
                _ = update_interval.tick() => {
                    let start = Instant::now();
                    if let Err(e) = self.collect_and_update_metrics().await {
                        warn!("Storage accounting collection failed: {}, continuing...", e);
                    } else {
                        debug!("Storage accounting completed in {:?}", start.elapsed());
                    }
                }
                _ = &mut cancel => {
                    info!("Storage accounting task shutting down");
                    break;
                }
            }
        }

        Ok(())
    }

    pub(crate) async fn collect_and_update_metrics(
        &mut self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        debug!("Collecting storage metrics via SQL query");

        let stream = self.query_context.execute(STORAGE_QUERY).await?;
        let batches: Vec<RecordBatch> = stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;

        let mut total_bytes = 0u64;

        for batch in batches {
            if batch.num_rows() == 0 {
                continue;
            }

            let total_state_size_column = batch
                .column_by_name("total_state_size")
                .ok_or("Missing total_state_size column")?;

            for i in 0..batch.num_rows() {
                if let Some(array) = total_state_size_column
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                {
                    if !array.is_null(i) {
                        total_bytes += array.value(i);
                    }
                } else if let Some(array) = total_state_size_column
                    .as_any()
                    .downcast_ref::<Int64Array>()
                {
                    if !array.is_null(i) {
                        total_bytes += array.value(i) as u64;
                    }
                }
            }
        }

        debug!("Collected total storage: {} bytes", total_bytes);

        let current_timestamp = Instant::now();

        if let (Some(prev_bytes), Some(prev_timestamp)) =
            (self.previous_bytes, self.previous_timestamp)
        {
            let time_diff_seconds = current_timestamp
                .duration_since(prev_timestamp)
                .as_secs_f64();
            let avg_bytes = (prev_bytes + total_bytes) as f64 / 2.0;
            let byte_seconds_increment = avg_bytes * time_diff_seconds;

            self.total_byte_seconds += byte_seconds_increment;

            debug!(
                "Byte-seconds increment: {:.2} (avg: {:.0} bytes × {:.2}s)",
                byte_seconds_increment, avg_bytes, time_diff_seconds
            );
        }

        self.previous_bytes = Some(total_bytes);
        self.previous_timestamp = Some(current_timestamp);

        update_storage_bytes_metrics(total_bytes, self.total_byte_seconds);

        Ok(())
    }
}
