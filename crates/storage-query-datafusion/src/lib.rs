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

/// Encodes the next response of a remote scan from `batch`, within `budget` bytes.
///
/// The whole batch is encoded first. If it fits, that encoding is returned. If
/// not, its size decides how to cut the batch into row slices: the first slice is
/// encoded and returned, and the others go to the front of `pending`, in row
/// order, to be encoded when the requester asks for them. Slices share the
/// batch's buffers, so queueing them copies nothing, and no encodings wait in the
/// queue. For example, with a 32 MiB budget, 128 state rows of 1 MiB each (about
/// 128 MiB) go out as four responses of 26 rows and one of 24.
///
/// A slice is checked when its turn comes, so a row too large to send fails the
/// request that reaches it, after the rows before it were sent.
///
/// For the column types V1 tables use, the Arrow IPC writer encodes only a
/// slice's own rows. View types (`Utf8View`, `BinaryView`, `ListView`) and
/// dictionaries keep their parent's data or whole dictionary when sliced, so they
/// don't shrink; the actual encoded size is checked either way. A single row that
/// can't fit fails with an error that names `networking.message-size-limit`.
///
/// This is a temporary, V1-only fix. V1 builds batches by row count and cuts them
/// by size only here, just before sending. The intended approach bounds the bytes
/// a query has in flight where its rows are read, under a memory limit for the
/// query; that fits the V2 engine, which shouldn't copy this.
pub(crate) fn encode_next(
    schema: &Schema,
    mut batch: RecordBatch,
    budget: usize,
    pending: &mut VecDeque<RecordBatch>,
) -> Result<Vec<u8>, DataFusionError> {
    loop {
        let encoded = encode_record_batch(schema, batch.clone())?;
        if encoded.len() <= budget {
            return Ok(encoded);
        }
        let rows = batch.num_rows();
        if rows <= 1 {
            return Err(DataFusionError::ResourcesExhausted(format!(
                "one row of this result, with its schema, encodes to {}, more than one \
                 query response can carry ({}); raise `networking.message-size-limit` on \
                 all nodes to read it",
                ByteCount::from(encoded.len()),
                ByteCount::from(budget),
            )));
        }
        let rows_per_slice = rows.div_ceil(encoded.len().div_ceil(budget));
        for offset in (0..rows).step_by(rows_per_slice).rev() {
            pending.push_front(batch.slice(offset, rows_per_slice.min(rows - offset)));
        }
        batch = pending
            .pop_front()
            .expect("a batch over the budget has at least two slices");
    }
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
        BooleanArray, DictionaryArray, Int32Array, LargeBinaryArray, LargeStringArray, ListBuilder,
        RecordBatch, StringBuilder, UInt64Array,
    };
    use datafusion::arrow::compute::concat_batches;
    use datafusion::arrow::datatypes::Int32Type;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::error::DataFusionError;
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

    /// A batch with text, binary, number, boolean, list and dictionary columns,
    /// some with nulls. Each row carries `value_size` bytes of binary data.
    fn batch(rows: u64, value_size: usize) -> RecordBatch {
        let item = Arc::new(Field::new("item", DataType::Utf8, true));
        let kind = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::LargeUtf8, true),
            Field::new("value", DataType::LargeBinary, false),
            Field::new("count", DataType::UInt64, false),
            Field::new("done", DataType::Boolean, false),
            Field::new("tags", DataType::List(item), false),
            Field::new("kind", kind, false),
        ]));
        let mut tags = ListBuilder::new(StringBuilder::new());
        for row in 0..rows {
            tags.append_value([Some(format!("tag-{row}")), None]);
        }
        let kinds: DictionaryArray<Int32Type> = (0..rows)
            .map(|row| ["inbox", "running"][usize::from(row % 2 == 1)])
            .collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(LargeStringArray::from_iter(
                    (0..rows).map(|row| (row % 3 != 0).then(|| format!("row-{row}"))),
                )),
                Arc::new(LargeBinaryArray::from_iter_values(
                    (0..rows).map(|row| vec![row as u8; value_size]),
                )),
                Arc::new(UInt64Array::from_iter_values(0..rows)),
                Arc::new(BooleanArray::from_iter(
                    (0..rows).map(|row| Some(row % 2 == 0)),
                )),
                Arc::new(tags.finish()),
                Arc::new(kinds),
            ],
        )
        .unwrap()
    }

    #[test]
    fn oversized_batches_go_out_as_slices_one_per_request() {
        let batch = batch(100, 1024);
        let schema = batch.schema();
        // Answers `next` requests until `batch` is sent, as the scanner does.
        // Also returns how many slices were queued after the first response.
        let respond =
            |batch: RecordBatch, budget| -> Result<(Vec<Vec<u8>>, usize), DataFusionError> {
                let mut pending = VecDeque::new();
                let mut messages = vec![super::encode_next(&schema, batch, budget, &mut pending)?];
                let queued = pending.len();
                while let Some(slice) = pending.pop_front() {
                    messages.push(super::encode_next(&schema, slice, budget, &mut pending)?);
                }
                Ok((messages, queued))
            };
        let budget = 16 * 1024;

        // A batch that fits goes out unchanged, as one response.
        let small = batch.slice(0, 2);
        let (messages, queued) = respond(small.clone(), budget).unwrap();
        assert_eq!(
            messages,
            [super::encode_record_batch(&schema, small).unwrap()]
        );
        assert_eq!(queued, 0);

        // A larger one: the first response carries one slice, and the others
        // wait as slices, not encodings. Every response fits, and together they
        // decode back to the whole batch, in order.
        let (messages, queued) = respond(batch.clone(), budget).unwrap();
        assert!(queued > 0);
        assert!(messages.iter().all(|message| message.len() <= budget));
        let decoded: Vec<_> = messages
            .iter()
            .map(|message| super::decode_record_batch(message).unwrap())
            .collect();
        assert_eq!(concat_batches(&schema, &decoded).unwrap(), batch);

        // Each response carries only its own rows. If slices carried their
        // parent's buffers, the responses would add up to several whole batches.
        let total: usize = messages.iter().map(Vec::len).sum();
        let whole = super::encode_record_batch(&schema, batch.clone())
            .unwrap()
            .len();
        assert!(total < 2 * whole, "{total} vs {whole}");

        // A single row larger than the budget can't be sent.
        let error = respond(batch, 512).unwrap_err().to_string();
        assert!(error.contains("networking.message-size-limit"), "{error}");

        // A small row followed by one too large to send: the first request gets
        // the small row, and the request that reaches the large one fails.
        let small = self::batch(1, 16);
        let large = self::batch(1, 4096);
        let size = |batch: &RecordBatch| {
            super::encode_record_batch(&schema, batch.clone())
                .unwrap()
                .len()
        };
        let budget = (size(&small) + size(&large)) / 2;
        let mut pending = VecDeque::new();
        let first = super::encode_next(
            &schema,
            concat_batches(&schema, [&small, &large]).unwrap(),
            budget,
            &mut pending,
        )
        .unwrap();
        assert_eq!(super::decode_record_batch(&first).unwrap(), small);
        let slice = pending.pop_front().unwrap();
        let error = super::encode_next(&schema, slice, budget, &mut pending)
            .unwrap_err()
            .to_string();
        assert!(error.contains("networking.message-size-limit"), "{error}");
    }
}
