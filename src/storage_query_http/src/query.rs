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
use datafusion::arrow::array::{Array, ArrayRef, AsArray, GenericStringArray, OffsetSizeTrait};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef};
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
                let b64_batch = record_batch_bytes_to_b64(batch?)?;
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

fn record_batch_bytes_to_b64(batch: RecordBatch) -> Result<RecordBatch, ArrowError> {
    let mut fields = Vec::with_capacity(batch.schema().fields.len());
    let mut columns = Vec::with_capacity(batch.columns().len());
    for (i, field) in batch.schema().fields.iter().enumerate() {
        match field.data_type() {
            DataType::LargeBinary => {
                fields.push(FieldRef::new(Field::new(
                    field.name(),
                    DataType::LargeUtf8,
                    field.is_nullable(),
                )));
                let arr = binary_array_to_b64::<i64>(batch.column(i));
                columns.push(ArrayRef::from(arr));
            }
            DataType::Binary => {
                fields.push(FieldRef::new(Field::new(
                    field.name(),
                    DataType::Utf8,
                    field.is_nullable(),
                )));
                let arr = binary_array_to_b64::<i32>(batch.column(i));
                columns.push(ArrayRef::from(arr));
            }
            _ => {
                fields.push(field.clone());
                columns.push(batch.column(i).clone());
            }
        }
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
