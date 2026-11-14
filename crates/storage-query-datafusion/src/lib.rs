// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub mod context;
mod diagnostics;
mod environment;
pub mod local_scanners;
pub mod migration_records;
pub use catalog::{
    ClusterTables, MetadataTables, RegisterTable, TableInventoryBuilder, UserTables,
};
pub use environment::DataFusionEnv;

pub mod remote_query_scanner_server;

pub mod bifrost_read_stream;
mod catalog;
pub mod config;
mod deployment;
mod inbox;
mod invocation_state;
mod invocation_status;
mod journal;
mod journal_events;
mod live_scanners;
mod locks;
mod log;
pub mod loglet_worker;
mod node;
pub mod node_fan_out;
mod partition;
mod partition_replica_set;
mod partition_state;
mod partition_store_scanner;
mod promise;
mod rules;
mod scanner_task;
mod scheduler_status;
mod service;
mod state;
mod statistics;
mod table_macro;
mod table_providers;
mod user_limits;
mod vqueue_entry_status;
mod vqueue_meta;
mod vqueues;
pub use table_providers::Scan;
pub mod table_util;

use std::collections::VecDeque;
use std::sync::Arc;

pub use context::BuildError;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::ipc::convert::IpcSchemaEncoder;
use datafusion::arrow::ipc::writer::DictionaryTracker;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::PhysicalExpr;
use prost::Message;

use restate_types::memory::ByteCount;

#[cfg(test)]
pub(crate) mod mocks;

mod filter;
pub mod remote_query_scanner_client;
pub mod remote_query_scanner_manager;
#[cfg(test)]
mod tests;

pub(crate) fn encode_schema(schema: &Schema) -> Vec<u8> {
    let mut dictionary_tracker = DictionaryTracker::new(true);
    let fb = IpcSchemaEncoder::new()
        .with_dictionary_tracker(&mut dictionary_tracker)
        .schema_to_fb(schema);
    let ipc_bytes = fb.finished_data();
    ipc_bytes.to_vec()
}

pub(crate) fn decode_schema(ipc_bytes: &[u8]) -> anyhow::Result<Schema> {
    let ipc_schema = datafusion::arrow::ipc::root_as_schema(ipc_bytes)
        .map_err(|e| anyhow::anyhow!("unable to decode {}", e))?;
    let schema = datafusion::arrow::ipc::convert::fb_to_schema(ipc_schema);
    Ok(schema)
}

pub(crate) fn encode_expr(expr: &Arc<dyn PhysicalExpr>) -> Result<Vec<u8>, DataFusionError> {
    let node = datafusion_proto::physical_plan::to_proto::serialize_physical_expr(
        expr,
        &datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec {},
    )?;

    Ok(node.encode_to_vec())
}

pub(crate) fn decode_expr(
    ctx: &TaskContext,
    input_schema: &Schema,
    proto_bytes: &[u8],
) -> Result<Arc<dyn PhysicalExpr>, DataFusionError> {
    let node = datafusion_proto::protobuf::PhysicalExprNode::decode(proto_bytes)
        .map_err(|err| DataFusionError::External(err.into()))?;

    datafusion_proto::physical_plan::from_proto::parse_physical_expr(
        &node,
        ctx,
        input_schema,
        &datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec {},
    )
}

pub(crate) fn encode_record_batch(
    schema: &Schema,
    record_batch: RecordBatch,
) -> Result<Vec<u8>, DataFusionError> {
    let mut buf: Vec<u8> = vec![];
    let mut writer = datafusion::arrow::ipc::writer::StreamWriter::try_new(&mut buf, schema)?;
    writer.write(&record_batch)?;
    writer.finish()?;
    writer.flush()?;
    Ok(buf)
}

/// Encodes `batch` into remote-scan messages of at most `budget` bytes each, in
/// row order, and appends them to `out`.
///
/// Each message is a complete Arrow IPC stream (schema, one batch, end marker),
/// so a receiver decodes each one on its own. A batch that fits goes out as one
/// message, unchanged. A larger one is cut into row slices sized from how far it
/// overshoots, and each slice is checked again. Slicing copies no data, and the
/// IPC writer encodes only a slice's own rows.
///
/// For example, with a 32 MiB budget, 128 state rows of 1 MiB each (about
/// 128 MiB encoded) go out as four messages of 26 rows and one of 24.
///
/// A single row larger than `budget` can't be sent: the error names the setting
/// that raises the limit.
pub(crate) fn encode_record_batch_within(
    schema: &Schema,
    batch: RecordBatch,
    budget: usize,
    out: &mut VecDeque<Vec<u8>>,
) -> Result<(), DataFusionError> {
    let encoded = encode_record_batch(schema, batch.clone())?;
    if encoded.len() <= budget {
        out.push_back(encoded);
        return Ok(());
    }
    let rows = batch.num_rows();
    if rows <= 1 {
        return Err(DataFusionError::ResourcesExhausted(format!(
            "a row encodes to {}, more than a remote-scan message can carry ({}); \
             raise `networking.message-size-limit` on all nodes to read it",
            ByteCount::from(encoded.len()),
            ByteCount::from(budget),
        )));
    }
    let rows_per_slice = rows.div_ceil(encoded.len().div_ceil(budget));
    for offset in (0..rows).step_by(rows_per_slice) {
        let slice = batch.slice(offset, rows_per_slice.min(rows - offset));
        encode_record_batch_within(schema, slice, budget, out)?;
    }
    Ok(())
}

pub(crate) fn decode_record_batch(bytes: &[u8]) -> Result<RecordBatch, DataFusionError> {
    let buffer = std::io::Cursor::new(bytes);
    let mut stream = datafusion::arrow::ipc::reader::StreamReader::try_new(buffer, None)?;
    if let Some(maybe_batch) = stream.next() {
        let batch = maybe_batch?;
        Ok(batch)
    } else {
        Err(DataFusionError::Internal(
            "Failure parsing a record batch".to_string(),
        ))
    }
}

#[cfg(test)]
mod serde_tests {
    use std::collections::VecDeque;

    use datafusion::arrow::array::{
        BooleanArray, Int32Array, LargeBinaryArray, LargeStringArray, ListBuilder, RecordBatch,
        StringBuilder, UInt64Array,
    };
    use datafusion::arrow::compute::concat_batches;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::execution::TaskContext;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_plan::PhysicalExpr;
    use datafusion::physical_plan::expressions::{BinaryExpr, Column, Literal};
    use datafusion::scalar::ScalarValue;
    use std::sync::Arc;

    #[test]
    pub fn round_trip_schema() {
        let schema = crate::state::schema::StateBuilder::schema();
        let buf = super::encode_schema(&schema);

        let got = super::decode_schema(&buf).expect("deserialization works");

        assert_eq!(*schema, got);
    }

    #[test]
    pub fn round_trip_record_batch() {
        let id = Int32Array::from(vec![0, 1, 2, 3, 4, 5]);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            vec![Arc::new(id)],
        )
        .unwrap();

        let buf = super::encode_record_batch(&batch.schema(), batch).expect("it to work");
        let batch = super::decode_record_batch(&buf).expect("to work");

        let int32array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Failed to downcast");

        assert_eq!(int32array.value(0), 0);
        assert_eq!(int32array.value(1), 1);
        assert_eq!(int32array.value(2), 2);
        assert_eq!(int32array.value(3), 3);
        assert_eq!(int32array.value(4), 4);
        assert_eq!(int32array.value(5), 5);
    }

    #[test]
    pub fn round_trip_expr() {
        let schema = Schema::new(vec![Field::new("status", DataType::LargeUtf8, false)]);

        let physical_expr: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("status", 0)),
            Operator::Eq,
            Arc::new(Literal::new(ScalarValue::LargeUtf8(Some(
                "completed".into(),
            )))),
        ));

        let buf = super::encode_expr(&physical_expr).expect("it to work");
        let decoded = super::decode_expr(&TaskContext::default(), &schema, &buf).expect("to work");

        assert_eq!(&physical_expr, &decoded);
    }

    #[test]
    fn oversized_batches_split_into_messages_within_the_budget() {
        let item = Arc::new(Field::new("item", DataType::Utf8, true));
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::LargeUtf8, true),
            Field::new("value", DataType::LargeBinary, false),
            Field::new("count", DataType::UInt64, false),
            Field::new("done", DataType::Boolean, false),
            Field::new("tags", DataType::List(item), false),
        ]));
        let rows = 100u64;
        let mut tags = ListBuilder::new(StringBuilder::new());
        for row in 0..rows {
            tags.append_value([Some(format!("tag-{row}")), None]);
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(LargeStringArray::from_iter(
                    (0..rows).map(|row| (row % 3 != 0).then(|| format!("row-{row}"))),
                )),
                Arc::new(LargeBinaryArray::from_iter_values(
                    (0..rows).map(|row| vec![row as u8; 1024]),
                )),
                Arc::new(UInt64Array::from_iter_values(0..rows)),
                Arc::new(BooleanArray::from_iter(
                    (0..rows).map(|row| Some(row % 2 == 0)),
                )),
                Arc::new(tags.finish()),
            ],
        )
        .unwrap();
        let encode =
            |batch: &RecordBatch| super::encode_record_batch(&schema, batch.clone()).unwrap();
        let within = |batch: &RecordBatch, budget| {
            let mut messages = VecDeque::new();
            super::encode_record_batch_within(&schema, batch.clone(), budget, &mut messages)
                .map(|()| messages)
        };
        let budget = 16 * 1024;

        // A batch that fits goes out unchanged, as one message.
        let small = batch.slice(0, 2);
        assert_eq!(within(&small, budget).unwrap(), [encode(&small)]);

        // A larger one is cut into messages that fit, which decode back to the
        // whole batch, in order.
        let messages = within(&batch, budget).unwrap();
        assert!(messages.len() > 1);
        assert!(messages.iter().all(|message| message.len() <= budget));
        let decoded: Vec<_> = messages
            .iter()
            .map(|message| super::decode_record_batch(message).unwrap())
            .collect();
        assert_eq!(concat_batches(&schema, &decoded).unwrap(), batch);

        // Each message carries only its own rows. If slices carried their
        // parent's buffers, the messages would add up to several whole batches.
        let total: usize = messages.iter().map(Vec::len).sum();
        assert!(total < 2 * encode(&batch).len(), "{total}");

        // A single row larger than the budget can't be sent.
        let error = within(&batch, 512).unwrap_err().to_string();
        assert!(error.contains("networking.message-size-limit"), "{error}");
    }
}
