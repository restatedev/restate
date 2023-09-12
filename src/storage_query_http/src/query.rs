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
use std::io::Write;
use std::sync::Arc;

use axum::body::StreamBody;
use axum::extract::State;
use axum::response::IntoResponse;
use axum::{http, Json};
use base64::Engine;
use bytes::Bytes;
use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, GenericStringArray, OffsetSizeTrait, StringArray,
};
use datafusion::arrow::datatypes::{
    ArrowPrimitiveType, DataType, Field, FieldRef, Int64Type, Schema, SchemaRef, UInt64Type,
};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::json::writer::record_batches_to_json_rows;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use futures::{stream, StreamExt};
use okapi_operation::*;
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::map::Map as JsonMap;
use serde_json::Value;
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
    responses(
        ignore_return_type = true,
        response(
            status = "200",
            description = "OK",
            content = "Json<Vec<JsonMap<String, Value>>>",
        ),
        from_type = "StorageApiError",
    )
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
    let mut buf = Vec::new();
    let mut is_first = true;
    let body = StreamBody::new(
        stream::once(future::ready(Ok(Bytes::from_static(b"["))))
            .chain(stream.map(move |batch| -> Result<Bytes, DataFusionError> {
                // Binary types are not supported by record_batches_to_json_rows
                let b64_batch = convert_record_batch(batch?)?;
                for row in record_batches_to_json_rows(&[&b64_batch])? {
                    if is_first {
                        is_first = false
                    } else {
                        buf.write_all(b",")?;
                    }
                    serde_json::to_writer(&mut buf, &row)
                        .map_err(|error| ArrowError::JsonError(error.to_string()))?;
                }
                let b = Bytes::copy_from_slice(buf.as_slice());
                buf.clear();
                Ok(b)
            }))
            .chain(stream::once(future::ready(Ok(Bytes::from_static(b"]"))))),
    );
    Ok(([(http::header::CONTENT_TYPE, "application/json")], body))
}

fn convert_record_batch(batch: RecordBatch) -> Result<RecordBatch, ArrowError> {
    let mut fields = Vec::with_capacity(batch.schema().fields.len());
    let mut columns = Vec::with_capacity(batch.columns().len());
    for (i, field) in batch.schema().fields.iter().enumerate() {
        let (data_type, transform): (DataType, Box<dyn Fn(_) -> _>) = match field.data_type() {
            // Represent binary as base64
            DataType::LargeBinary => (DataType::LargeUtf8, Box::new(binary_array_to_b64::<i64>)),
            DataType::Binary => (DataType::Utf8, Box::new(binary_array_to_b64::<i32>)),
            // Represent 64 bit ints as strings
            DataType::UInt64 => (
                DataType::Utf8,
                Box::new(debug_primitive_array::<UInt64Type>),
            ),
            DataType::Int64 => (DataType::Utf8, Box::new(debug_primitive_array::<Int64Type>)),
            _ => {
                fields.push(field.clone());
                columns.push(batch.column(i).clone());
                continue;
            }
        };
        fields.push(FieldRef::new(Field::new(
            field.name(),
            data_type,
            field.is_nullable(),
        )));
        columns.push(ArrayRef::from(transform(batch.column(i))));
    }
    let schema = Schema::new_with_metadata(fields, batch.schema().metadata().clone());
    RecordBatch::try_new(SchemaRef::new(schema), columns)
}

fn binary_array_to_b64<OffsetSize: OffsetSizeTrait>(arr: &ArrayRef) -> Box<dyn Array> {
    Box::new(GenericStringArray::<OffsetSize>::from(
        arr.as_binary::<OffsetSize>()
            .iter()
            .map(|b| -> Option<String> { Some(base64::prelude::BASE64_STANDARD.encode(b?)) })
            .collect::<Vec<Option<String>>>(),
    ))
}

fn debug_primitive_array<T: ArrowPrimitiveType>(arr: &ArrayRef) -> Box<dyn Array> {
    Box::new(StringArray::from(
        arr.as_primitive::<T>()
            .iter()
            .map(|b: Option<T::Native>| -> Option<String> { Some(format!("{:?}", b?)) })
            .collect::<Vec<Option<String>>>(),
    ))
}
