// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The cluster's SQL tables: read as JSON rows, kept that way in a dump, and decoded into typed
//! rows for the views.
//!
//! Rows decode into structs whose fields all default, so a server that lacks a column (an older
//! version) yields `None` instead of failing the whole read.

use std::collections::{BTreeMap, HashMap};

use arrow::json::ArrayWriter;
use futures::StreamExt;
use futures::future::join_all;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tonic::transport::Channel;

use restate_cli_util::CliContext;
use restate_core::protobuf::cluster_ctrl_svc::{QueryRequest, new_cluster_ctrl_client};

use crate::commands::sql::RecordBatchStream;

/// The tables the views read once per snapshot.
pub const SETTLED: &[&str] = &["partitions"];
/// The tables whose rows change by the second, sampled for rates.
pub const SAMPLED: &[&str] = &["loglet_workers", "bifrost_read_streams"];
/// More tables a dump keeps for a closer look; the views don't use them.
pub const DUMPED: &[&str] = &["nodes", "logs", "partition_state", "partition_replica_set"];

/// Tables by name: their rows, or why they couldn't be read.
pub type Tables = BTreeMap<String, Result<Vec<Value>, String>>;

/// Reads `tables` concurrently from the node behind `channel`.
pub async fn fetch(channel: &Channel, tables: &[&str]) -> Tables {
    join_all(tables.iter().map(|table| async move {
        let rows = query(channel.clone(), &format!("SELECT * FROM {table}"))
            .await
            .map_err(|err| format!("{err:#}"));
        ((*table).to_owned(), rows)
    }))
    .await
    .into_iter()
    .collect()
}

/// A partition's replica set as observed by the queried node (`partitions` table).
#[derive(Debug, Default, Deserialize)]
#[cfg_attr(test, derive(serde::Serialize))]
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
#[cfg_attr(test, derive(serde::Serialize))]
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
#[cfg_attr(test, derive(serde::Serialize))]
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
    /// Decodes the tables the views use; one that couldn't be read is reported in `warnings`.
    pub fn parse(tables: &Tables, warnings: &mut Vec<String>) -> Self {
        let mut data = SqlData::default();
        if let Some(rows) = rows::<PartitionRow>(tables, "partitions", warnings) {
            data.memberships = rows.into_iter().map(|r| (r.partition_id, r)).collect();
        }
        for row in rows::<LogletWorkerRow>(tables, "loglet_workers", warnings)
            .into_iter()
            .flatten()
        {
            data.loglet_workers.entry(row.log_id).or_default().push(row);
        }
        for row in rows::<ReadStreamRow>(tables, "bifrost_read_streams", warnings)
            .into_iter()
            .flatten()
        {
            data.read_streams.entry(row.log_id).or_default().push(row);
        }
        data
    }
}

/// The rows of `table` as `T`, if it was read.
fn rows<T: DeserializeOwned>(
    tables: &Tables,
    table: &str,
    warnings: &mut Vec<String>,
) -> Option<Vec<T>> {
    let rows = tables
        .get(table)?
        .as_ref()
        .map_err(String::clone)
        .and_then(|rows| {
            rows.iter()
                .map(|row| T::deserialize(row).map_err(|err| err.to_string()))
                .collect()
        });
    let what = match table {
        "partitions" => "partition replica sets",
        "loglet_workers" => "log tails and trim points",
        "bifrost_read_streams" => "read streams",
        other => other,
    };
    rows.inspect_err(|err| warnings.push(format!("{what} unavailable: {err}")))
        .ok()
}

/// Runs `sql` on the cluster's query engine.
async fn query(channel: Channel, sql: &str) -> anyhow::Result<Vec<Value>> {
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
        rows.extend(serde_json::from_slice::<Vec<Value>>(&writer.into_inner())?);
    }
    Ok(rows)
}
