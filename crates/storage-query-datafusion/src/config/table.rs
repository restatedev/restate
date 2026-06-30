// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use anyhow::Context;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use tokio::sync::mpsc::Sender;

use restate_core::Metadata;
use restate_types::GenerationalNodeId;
use restate_types::config::Configuration;
use restate_types::live::Live;

use super::schema::ConfigBuilder;
use crate::context::QueryContext;
use crate::node_fan_out::{AllNodeLocator, NodeFanOutTableProvider};
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::table_providers::Scan;
use crate::table_util::Builder;

pub(crate) const TABLE_NAME: &str = "config";

pub fn register_self(
    ctx: &QueryContext,
    metadata: Metadata,
    remote_scanner_manager: RemoteScannerManager,
    local_scanner: Option<Arc<dyn Scan>>,
) -> datafusion::common::Result<()> {
    let node_locator = Arc::new(AllNodeLocator::new(metadata));

    let config_table = NodeFanOutTableProvider::new(
        ConfigBuilder::schema(),
        node_locator,
        remote_scanner_manager,
        local_scanner,
        TABLE_NAME,
    );
    ctx.register_non_partitioned_table(TABLE_NAME, Arc::new(config_table))
}

pub fn create_scanner(metadata: Metadata, config: Live<Configuration>) -> Arc<dyn Scan> {
    Arc::new(ConfigScanner { metadata, config })
}

#[derive(Clone, derive_more::Debug)]
#[debug("ConfigScanner")]
struct ConfigScanner {
    metadata: Metadata,
    config: Live<Configuration>,
}

impl Scan for ConfigScanner {
    fn scan(
        &self,
        projection: SchemaRef,
        _filters: &[Expr],
        batch_size: usize,
        _limit: Option<usize>,
    ) -> SendableRecordBatchStream {
        let schema = projection.clone();
        let mut stream_builder = RecordBatchReceiverStream::builder(projection, 2);
        let tx = stream_builder.tx();
        let node_id = self.metadata.my_node_id();

        let config = self.config.snapshot();
        stream_builder.spawn(async move {
            for_each_config(schema, tx, node_id, config, batch_size)
                .await
                .map_err(|e| DataFusionError::Execution(e.to_string()))?;
            Ok(())
        });
        stream_builder.build()
    }
}

async fn for_each_config(
    schema: SchemaRef,
    tx: Sender<datafusion::common::Result<RecordBatch>>,
    node_id: GenerationalNodeId,
    config: Arc<Configuration>,
    batch_size: usize,
) -> anyhow::Result<()> {
    let mut builder = ConfigBuilder::new(schema);
    let json_value = serde_json::to_value(config).context("df config query")?;

    for (k, v) in flatten_json(&json_value) {
        {
            let mut row = builder.row();
            row.fmt_plain_node_id(node_id.as_plain());
            row.fmt_gen_node_id(node_id);
            row.key(k.as_str());
            row.value(if is_potentially_secret(k.as_str()) {
                "<REDACTED>"
            } else {
                v.as_str()
            });
        }

        if builder.num_rows() >= batch_size {
            let batch = builder.finish_and_new();
            if tx.send(batch).await.is_err() {
                // The receiver has hung up; stop scanning. We don't propagate an error
                // here as there's no one left to receive it.
                return Ok(());
            }
        }
    }
    if !builder.empty() {
        let result = builder.finish();
        let _ = tx.send(result).await;
    }
    Ok(())
}

fn flatten_json(val: &serde_json::Value) -> impl Iterator<Item = (String, String)> + '_ {
    let mut stack = vec![(String::new(), val)];

    std::iter::from_fn(move || {
        while let Some((parent, val)) = stack.pop() {
            match val {
                serde_json::Value::Object(v) => {
                    for (k, v) in v.iter().rev() {
                        let key = if parent.is_empty() {
                            k.to_string()
                        } else {
                            format!("{}.{}", parent, k)
                        };
                        stack.push((key, v));
                    }
                }
                // If the array is a list of objects, flatten it.
                serde_json::Value::Array(v)
                    if let Some(first) = v.first()
                        && first.is_object() =>
                {
                    for (i, v) in v.iter().enumerate().rev() {
                        let key = if parent.is_empty() {
                            format!("[{}]", i)
                        } else {
                            format!("{}[{}]", parent, i)
                        };
                        stack.push((key, v));
                    }
                }
                serde_json::Value::String(v) => return Some((parent, v.to_string())),
                _ => {
                    return Some((parent, serde_json::to_string(val).unwrap()));
                }
            }
        }

        None
    })
}

/// Best-effort secret detection.
/// Access to the config table is privileged anyways (via restatectl). Hence why it's ok
/// for this to be best effort.
///
/// Config keys are serialized in `kebab-case` (e.g. `aws-access-key-id`), so we match
/// against the hyphenated forms here.
fn is_potentially_secret(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    [
        "access-key",
        "password",
        "secret",
        "token",
        "authorization",
        "api-key",
        "cookie",
        "private-key",
        "key-pem",
        "jaas",
    ]
    .iter()
    .any(|needle| key.contains(needle))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flatten_json_works() {
        let json = serde_json::json!({
            "name": "node-name",
            "worker": {
                "test": "val",
                "test2": "val2",
            },
            "integer": 1,
            "arr": ["i", "j"],
            "nodes": [
                {
                    "addr": "addr1",
                },
                {
                    "addr": "addr2",
                },
            ]
        });
        let mut iter = flatten_json(&json);
        assert_eq!(
            iter.next(),
            Some(("arr".to_string(), "[\"i\",\"j\"]".to_string()))
        );
        assert_eq!(iter.next(), Some(("integer".to_string(), "1".to_string())));
        assert_eq!(
            iter.next(),
            Some(("name".to_string(), "node-name".to_string()))
        );
        assert_eq!(
            iter.next(),
            Some(("nodes[0].addr".to_string(), "addr1".to_string()))
        );
        assert_eq!(
            iter.next(),
            Some(("nodes[1].addr".to_string(), "addr2".to_string()))
        );
        assert_eq!(
            iter.next(),
            Some(("worker.test".to_string(), "val".to_string()))
        );
        assert_eq!(
            iter.next(),
            Some(("worker.test2".to_string(), "val2".to_string()))
        );
        assert_eq!(iter.next(), None);
    }
}
