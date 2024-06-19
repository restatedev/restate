// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::FlightData;
use axum::body::StreamBody;
use axum::extract::State;
use axum::response::IntoResponse;
use axum::{http, Json};
use bytes::Bytes;
use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BinaryArray, GenericByteArray, StringArray,
};
use datafusion::arrow::buffer::{OffsetBuffer, ScalarBuffer};
use datafusion::arrow::datatypes::{ByteArrayType, DataType, Field, FieldRef, Schema, SchemaRef};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::record_batch::RecordBatch;
use futures::{ready, Stream, StreamExt, TryStreamExt};
use okapi_operation::*;
use schemars::JsonSchema;
use serde::Deserialize;
use serde_with::serde_as;

use restate_core::network::protobuf::node_svc::StorageQueryRequest;

use super::error::StorageQueryError;
use crate::state::QueryServiceState;

#[serde_as]
#[derive(Debug, Deserialize, JsonSchema)]
pub struct QueryRequest {
    /// # Query
    ///
    /// SQL query to run against the storage
    #[serde_as(as = "serde_with::DisplayFromStr")]
    #[schemars(with = "String")]
    pub query: String,
}

/// Query storage
#[openapi(
    summary = "Query storage",
    description = "Query the storage API",
    operation_id = "query",
    tags = "storage",
    responses(ignore_return_type = true, from_type = "StorageQueryError")
)]
pub async fn query(
    State(state): State<Arc<QueryServiceState>>,
    #[request_body(required = true)] Json(payload): Json<QueryRequest>,
) -> Result<impl IntoResponse, StorageQueryError> {
    let mut worker_grpc_client = state.node_svc_client.clone();

    let response_stream = worker_grpc_client
        .query_storage(StorageQueryRequest {
            query: payload.query,
        })
        .await?
        .into_inner();

    let record_batch_stream = FlightRecordBatchStream::new_from_flight_data(
        response_stream
            .map_ok(|response| FlightData {
                data_header: response.header,
                data_body: response.data,
                ..FlightData::default()
            })
            .map_err(FlightError::from),
    );

    // create a stream without LargeUtf8 or LargeBinary columns as JS doesn't support these yet
    let result_stream = ConvertRecordBatchStream::new(record_batch_stream);

    let body = StreamBody::new(result_stream);
    Ok((
        [(
            http::header::CONTENT_TYPE,
            "application/vnd.apache.arrow.stream",
        )],
        body,
    ))
}

fn convert_schema(schema: SchemaRef) -> SchemaRef {
    let mut fields = Vec::with_capacity(schema.fields.len());
    for field in schema.fields.iter() {
        let data_type = match field.data_type() {
            // Represent binary as base64
            DataType::LargeBinary => DataType::Binary,
            DataType::LargeUtf8 => DataType::Utf8,
            other => other.clone(),
        };
        fields.push(FieldRef::new(Field::new(
            field.name(),
            data_type,
            field.is_nullable(),
        )));
    }
    SchemaRef::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

fn convert_record_batch(
    converted_schema: SchemaRef,
    batch: RecordBatch,
) -> Result<RecordBatch, ArrowError> {
    let mut columns = Vec::with_capacity(batch.columns().len());
    for (i, field) in batch.schema().fields.iter().enumerate() {
        match field.data_type() {
            // Represent binary as base64
            DataType::LargeBinary => {
                let col: BinaryArray = convert_array_offset(batch.column(i).as_binary::<i64>())?;
                columns.push(ArrayRef::from(Box::new(col) as Box<dyn Array>));
            }
            DataType::LargeUtf8 => {
                let col: StringArray = convert_array_offset(batch.column(i).as_string::<i64>())?;
                columns.push(ArrayRef::from(Box::new(col) as Box<dyn Array>));
            }
            _ => {
                columns.push(batch.column(i).clone());
            }
        };
    }
    RecordBatch::try_new(converted_schema, columns)
}

fn convert_array_offset<Before: ByteArrayType, After: ByteArrayType>(
    array: &GenericByteArray<Before>,
) -> Result<GenericByteArray<After>, ArrowError>
where
    After::Offset: TryFrom<Before::Offset>,
{
    let offsets = array
        .offsets()
        .iter()
        .map(|&o| After::Offset::try_from(o))
        .collect::<Result<ScalarBuffer<After::Offset>, _>>()
        .map_err(|_| ArrowError::CastError("offset conversion failed".into()))?;
    GenericByteArray::<After>::try_new(
        OffsetBuffer::new(offsets),
        array.values().clone(),
        array.nulls().cloned(),
    )
}

enum ConversionState {
    WaitForSchema,
    WaitForRecords(SchemaRef, StreamWriter<Vec<u8>>),
}

/// Convert the record batches so that they don't contain LargeUtf8 or LargeBinary columns as JS doesn't
/// support these yet.
struct ConvertRecordBatchStream {
    done: bool,
    state: ConversionState,

    record_batch_stream: FlightRecordBatchStream,
}

impl ConvertRecordBatchStream {
    fn new(record_batch_stream: FlightRecordBatchStream) -> Self {
        ConvertRecordBatchStream {
            done: false,
            state: ConversionState::WaitForSchema,
            record_batch_stream,
        }
    }
}

impl ConvertRecordBatchStream {
    fn create_stream_writer(
        record_batch: &RecordBatch,
    ) -> Result<(SchemaRef, StreamWriter<Vec<u8>>), ArrowError> {
        let converted_schema = convert_schema(record_batch.schema());
        let stream_writer = StreamWriter::try_new(Vec::new(), converted_schema.as_ref())?;

        Ok((converted_schema, stream_writer))
    }

    fn write_batch(
        converted_schema: &SchemaRef,
        stream_writer: &mut StreamWriter<Vec<u8>>,
        record_batch: RecordBatch,
    ) -> Result<(), ArrowError> {
        let record_batch = convert_record_batch(converted_schema.clone(), record_batch)?;
        stream_writer.write(&record_batch)
    }

    fn process_record(
        mut self: Pin<&mut Self>,
        record_batch: Result<RecordBatch, FlightError>,
    ) -> Result<Bytes, FlightError> {
        let record_batch = record_batch?;
        match &mut self.state {
            ConversionState::WaitForSchema => {
                let (converted_schema, mut stream_writer) =
                    Self::create_stream_writer(&record_batch)?;
                Self::write_batch(&converted_schema, &mut stream_writer, record_batch)?;
                let bytes = Bytes::copy_from_slice(stream_writer.get_ref());
                stream_writer.get_mut().clear();
                self.state = ConversionState::WaitForRecords(converted_schema, stream_writer);
                Ok(bytes)
            }
            ConversionState::WaitForRecords(converted_schema, stream_writer) => {
                Self::write_batch(converted_schema, stream_writer, record_batch)?;
                let bytes = Bytes::copy_from_slice(stream_writer.get_ref());
                stream_writer.get_mut().clear();
                Ok(bytes)
            }
        }
    }
}

impl Stream for ConvertRecordBatchStream {
    type Item = Result<Bytes, FlightError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }

        let record_batch = ready!(self.record_batch_stream.poll_next_unpin(cx));

        if let Some(record_batch) = record_batch {
            match self.as_mut().process_record(record_batch) {
                Ok(bytes) => Poll::Ready(Some(Ok(bytes))),
                Err(err) => {
                    self.done = true;
                    Poll::Ready(Some(Err(err)))
                }
            }
        } else {
            self.done = true;
            if let ConversionState::WaitForRecords(_, stream_writer) = &mut self.state {
                if let Err(err) = stream_writer.finish() {
                    Poll::Ready(Some(Err(err.into())))
                } else {
                    let bytes = Bytes::copy_from_slice(stream_writer.get_ref());
                    stream_writer.get_mut().clear();
                    Poll::Ready(Some(Ok(bytes)))
                }
            } else {
                // CLI is expecting schema information
                if let (Some(schema), ConversionState::WaitForSchema) =
                    (self.record_batch_stream.schema(), &self.state)
                {
                    let schema_bytes = StreamWriter::try_new(Vec::new(), schema)
                        .and_then(|stream_writer| stream_writer.into_inner().map(Bytes::from))
                        .map_err(FlightError::from);

                    Poll::Ready(Some(schema_bytes))
                } else {
                    Poll::Ready(None)
                }
            }
        }
    }
}
