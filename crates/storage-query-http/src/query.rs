// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future;
use std::sync::Arc;

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
use datafusion::error::DataFusionError;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::{stream, StreamExt};
use okapi_operation::*;
use schemars::JsonSchema;
use serde::Deserialize;
use serde_with::serde_as;

use crate::error::StorageApiError;
use crate::state::EndpointState;

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
    responses(ignore_return_type = true, from_type = "StorageApiError")
)]
pub async fn query(
    State(state): State<Arc<EndpointState>>,
    #[request_body(required = true)] Json(payload): Json<QueryRequest>,
) -> Result<impl IntoResponse, StorageApiError> {
    let stream = state
        .query_context()
        .execute(payload.query.as_str())
        .await
        .map_err(StorageApiError::DataFusionError)?;

    // create a stream without LargeUtf8 or LargeBinary as JS doesn't support these yet
    let converted_schema = convert_schema(stream.schema());
    let converted_schema_cloned = converted_schema.clone();
    let converted_stream = RecordBatchStreamAdapter::new(
        converted_schema.clone(),
        stream.map(move |batch| {
            let converted_schema = converted_schema_cloned.clone();
            batch.and_then(|batch| {
                convert_record_batch(converted_schema, batch).map_err(DataFusionError::ArrowError)
            })
        }),
    );

    // create a stream with a labelled start and end
    let labelled_stream = stream::once(future::ready(LabelledStream::Start))
        .chain(converted_stream.map(LabelledStream::Batch))
        .chain(stream::once(future::ready(LabelledStream::End)));

    let mut stream_writer =
        StreamWriter::try_new(Vec::<u8>::new(), converted_schema.clone().as_ref())
            .map_err(DataFusionError::ArrowError)
            .map_err(StorageApiError::DataFusionError)?;

    let body = StreamBody::new(labelled_stream.map(
        move |label| -> Result<Bytes, DataFusionError> {
            match label {
                LabelledStream::Start => {
                    // starting bytes were already written during StreamWriter::try_new
                    let b = Bytes::copy_from_slice(stream_writer.get_ref());
                    stream_writer.get_mut().clear();
                    Ok(b)
                }
                LabelledStream::Batch(batch) => {
                    stream_writer.write(&batch?)?;
                    let b = Bytes::copy_from_slice(stream_writer.get_ref());
                    stream_writer.get_mut().clear();
                    Ok(b)
                }
                LabelledStream::End => {
                    stream_writer.finish()?;
                    let b = Bytes::copy_from_slice(stream_writer.get_ref());
                    stream_writer.get_mut().clear();
                    Ok(b)
                }
            }
        },
    ));
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

enum LabelledStream {
    Start,
    Batch(Result<RecordBatch, DataFusionError>),
    End,
}
