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

pub mod remote_query_scanner_server;

mod deployment;
mod idempotency;
mod inbox;
mod invocation_state;
mod invocation_status;
mod journal;
mod journal_events;
mod keyed_service_status;
mod log;
mod node;
mod partition;
mod partition_replica_set;
mod partition_state;
mod partition_store_scanner;
mod promise;
mod scanner_task;
mod service;
mod state;
mod statistics;
#[cfg(feature = "table_docs")]
pub mod table_docs;
mod table_macro;
mod table_providers;
mod table_util;
mod udfs;

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

#[cfg(test)]
pub(crate) mod mocks;

pub mod empty_invoker_status_handle;
mod partition_filter;
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
    use datafusion::arrow::array::{Int32Array, RecordBatch};
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
}
