// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Typed reads of the cluster's SQL tables.
//!
//! Rows are decoded through arrow's JSON writer into structs whose fields all default, so a server
//! that lacks a column (an older version) yields `None` instead of failing the whole read.

use std::collections::HashMap;

use arrow::json::ArrayWriter;
use futures::StreamExt;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use tonic::transport::Channel;

use restate_cli_util::CliContext;
use restate_core::protobuf::cluster_ctrl_svc::{QueryRequest, new_cluster_ctrl_client};

use crate::commands::sql::RecordBatchStream;

/// A partition's replica set as observed by the queried node (`partitions` table).
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct PartitionRow {
    pub partition_id: u32,
    pub v_current: Option<u32>,
    pub current_replica_set: Option<String>,
    pub v_next: Option<u32>,
    pub next_replica_set: Option<String>,
}

/// A log-server's state of one loglet (`loglet_workers` table), offsets are loglet-local.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct LogletWorkerRow {
    pub plain_node_id: String,
    pub log_id: u32,
    pub segment_index: u32,
    pub local_tail: Option<u32>,
    pub known_global_tail: Option<u32>,
    pub trim_point: Option<u32>,
    pub is_sealed: Option<bool>,
    pub seal_enqueued: Option<bool>,
    pub pending_seals: Option<u32>,
}

/// A bifrost read stream on a node (`bifrost_read_streams` table).
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct ReadStreamRow {
    pub plain_node_id: String,
    pub log_id: u32,
    pub read_pointer: Option<u64>,
    pub end_lsn: Option<u64>,
    pub state: Option<String>,
    pub current_segment: Option<u32>,
    pub is_tailing: Option<bool>,
}

#[derive(Default)]
pub struct SqlData {
    /// keyed by partition id
    pub memberships: HashMap<u32, PartitionRow>,
    /// log-servers' loglet workers, keyed by log id
    pub loglet_workers: HashMap<u32, Vec<LogletWorkerRow>>,
    /// nodes' read streams, keyed by log id
    pub read_streams: HashMap<u32, Vec<ReadStreamRow>>,
}

impl SqlData {
    /// Reads the tables concurrently; a table that can't be read is reported in `warnings`.
    pub async fn fetch(channel: Channel, warnings: &mut Vec<String>) -> Self {
        let (memberships, workers, readers) = tokio::join!(
            query::<PartitionRow>(channel.clone(), "SELECT * FROM partitions"),
            query::<LogletWorkerRow>(channel.clone(), "SELECT * FROM loglet_workers"),
            query::<ReadStreamRow>(channel, "SELECT * FROM bifrost_read_streams"),
        );

        let mut data = SqlData::default();
        match memberships {
            Ok(rows) => {
                data.memberships = rows.into_iter().map(|r| (r.partition_id, r)).collect();
            }
            Err(err) => warnings.push(format!("partition replica sets unavailable: {err:#}")),
        }
        match workers {
            Ok(rows) => {
                for row in rows {
                    data.loglet_workers.entry(row.log_id).or_default().push(row);
                }
            }
            Err(err) => warnings.push(format!("log tails and trim points unavailable: {err:#}")),
        }
        match readers {
            Ok(rows) => {
                for row in rows {
                    data.read_streams.entry(row.log_id).or_default().push(row);
                }
            }
            Err(err) => warnings.push(format!("read streams unavailable: {err:#}")),
        }
        data
    }
}

/// Runs `sql` on the cluster's query engine and decodes each row into `T`.
async fn query<T: DeserializeOwned>(channel: Channel, sql: &str) -> anyhow::Result<Vec<T>> {
    let response = new_cluster_ctrl_client(channel, &CliContext::get().network)
        .query(QueryRequest {
            query: sql.to_owned(),
        })
        .await?
        .into_inner();

    let mut stream = RecordBatchStream::from(response);
    let mut rows = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        if batch.num_rows() == 0 {
            continue;
        }
        let mut writer = ArrayWriter::new(Vec::new());
        writer.write(&batch)?;
        writer.finish()?;
        rows.extend(serde_json::from_slice::<Vec<T>>(&writer.into_inner())?);
    }
    Ok(rows)
}
