// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::io::Write;
use std::pin::Pin;
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Json, http};
use bytes::Bytes;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::json::writer::JsonArray;
use datafusion::common::{DataFusionError, ParamValues, ScalarValue};
use futures::{StreamExt, TryStreamExt};
use http::{HeaderMap, HeaderValue};
use http_body::Frame;
use http_body_util::StreamBody;
use parking_lot::Mutex;
use serde::Serialize;

use restate_admin_rest_model::query::{QueryParams, QueryRequest};
use restate_core::network::TransportConnect;
use restate_types::invocation::client::InvocationClient;
use restate_types::schema::registry::{DiscoveryClient, MetadataService, TelemetryClient};

use crate::query_utils::{RecordBatchWriter, WriteRecordBatchStream};
use crate::state::AdminServiceState;

const RETRY_AFTER_HEADER: &str = "Retry-After";

/// Error response for query endpoint.
#[derive(Debug, Serialize, utoipa::ToSchema)]
struct QueryErrorBody {
    message: String,
}

/// Errors that can occur when executing a query.
#[derive(Debug, thiserror::Error)]
pub(crate) enum QueryError {
    #[error("Datafusion error: {0}")]
    Datafusion(#[from] datafusion::error::DataFusionError),
    #[error("Invalid value for placeholder {0}: expected a string, number, boolean or null")]
    InvalidParam(String),
    #[error("Query service not available")]
    Unavailable,
    #[error("Rate limited")]
    RateLimited(#[from] gardal::RateLimited),
}

impl From<restate_storage_query_datafusion::context::QueryError> for QueryError {
    fn from(err: restate_storage_query_datafusion::context::QueryError) -> Self {
        match err {
            restate_storage_query_datafusion::context::QueryError::DataFusion(e) => {
                Self::Datafusion(e)
            }
            restate_storage_query_datafusion::context::QueryError::RateLimited(e) => {
                Self::RateLimited(e)
            }
        }
    }
}

impl IntoResponse for QueryError {
    fn into_response(self) -> Response {
        let mut headers = http::HeaderMap::new();
        let status_code = match &self {
            QueryError::Datafusion(datafusion::error::DataFusionError::Plan(_))
            | QueryError::Datafusion(datafusion::error::DataFusionError::SchemaError(_, _))
            | QueryError::Datafusion(datafusion::error::DataFusionError::SQL(_, _))
            | QueryError::InvalidParam(_) => StatusCode::BAD_REQUEST,
            QueryError::Datafusion(_) => StatusCode::INTERNAL_SERVER_ERROR,
            QueryError::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
            QueryError::RateLimited(e) => {
                headers.insert(
                    RETRY_AFTER_HEADER,
                    HeaderValue::from(std::cmp::max(e.earliest_retry_after().as_secs(), 1)),
                );
                StatusCode::TOO_MANY_REQUESTS
            }
        };

        (
            status_code,
            headers,
            Json(QueryErrorBody {
                message: self.to_string(),
            }),
        )
            .into_response()
    }
}

/// Query the system and service state by using SQL.
#[utoipa::path(
    post,
    path = "/query",
    operation_id = "query",
    tag = "introspection",
    responses(
        (status = 200, description = "Query results",
            content (
                ("application/vnd.apache.arrow.stream"),
                ("application/json", example = json!({"rows": []}))
            )),
        (status = 400, description = "Invalid query or parameters", body = QueryErrorBody),
        (status = 500, description = "Datafusion error", body = QueryErrorBody),
        (status = 503, description = "Query service not available", body = QueryErrorBody),
    )
)]
pub(crate) async fn query<Metadata, Discovery, Telemetry, Invocations, Transport>(
    State(state): State<AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport>>,
    headers: HeaderMap,
    Json(payload): Json<QueryRequest>,
) -> Result<Response, QueryError>
where
    Metadata: MetadataService + Send + Sync + Clone + 'static,
    Discovery: DiscoveryClient + Send + Sync + Clone + 'static,
    Telemetry: TelemetryClient + Send + Sync + Clone + 'static,
    Invocations: InvocationClient + Send + Sync + Clone + 'static,
    Transport: TransportConnect,
{
    let Some(query_context) = state.query_context.as_ref() else {
        return Err(QueryError::Unavailable);
    };

    let params = payload.params.map(to_param_values).transpose()?;
    let query_result = query_context
        .execute_with_params(&payload.query, params)
        .await?;

    let (result_stream, content_type) = match headers.get(http::header::ACCEPT) {
        Some(v) if v == HeaderValue::from_static("application/json") => (
            WriteRecordBatchStream::<JsonWriter>::new(query_result.stream, payload.query)?
                .map_ok(Frame::data)
                .left_stream(),
            "application/json",
        ),
        _ => (
            WriteRecordBatchStream::<StreamWriter<Vec<u8>>>::new(
                query_result.stream,
                payload.query,
            )?
            .map_ok(Frame::data)
            .right_stream(),
            "application/vnd.apache.arrow.stream",
        ),
    };

    let mut result_stream = result_stream.peekable();

    // return an error (instead of just closing the stream) if there is a error getting the first record batch (eg, out of memory)
    if let Some(Err(_)) = futures::stream::Peekable::peek(Pin::new(&mut result_stream)).await {
        let err = result_stream.next().await.unwrap().unwrap_err();
        return Err(err.into());
    }

    Ok(Response::builder()
        .header(http::header::CONTENT_TYPE, content_type)
        .body(StreamBody::new(result_stream))
        .expect("content-type header is correct")
        .into_response())
}

fn to_param_values(params: QueryParams) -> Result<ParamValues, QueryError> {
    Ok(match params {
        QueryParams::Positional(values) => values
            .into_iter()
            .enumerate()
            .map(|(i, value)| to_scalar_value(value).ok_or_else(|| invalid_param(i + 1)))
            .collect::<Result<Vec<_>, _>>()?
            .into(),
        QueryParams::Named(values) => values
            .into_iter()
            .map(|(name, value)| match to_scalar_value(value) {
                Some(value) => Ok((name, value)),
                None => Err(invalid_param(name)),
            })
            .collect::<Result<HashMap<_, _>, _>>()?
            .into(),
    })
}

fn invalid_param(id: impl std::fmt::Display) -> QueryError {
    QueryError::InvalidParam(format!("${id}"))
}

/// Returns `None` for arrays and objects.
fn to_scalar_value(value: serde_json::Value) -> Option<ScalarValue> {
    use serde_json::Value;

    Some(match value {
        Value::Null => ScalarValue::Null,
        Value::Bool(v) => ScalarValue::Boolean(Some(v)),
        Value::Number(n) => {
            if let Some(v) = n.as_i64() {
                ScalarValue::Int64(Some(v))
            } else if let Some(v) = n.as_u64() {
                ScalarValue::UInt64(Some(v))
            } else {
                ScalarValue::Float64(Some(n.as_f64()?))
            }
        }
        Value::String(v) => ScalarValue::Utf8(Some(v)),
        Value::Array(_) | Value::Object(_) => return None,
    })
}

#[derive(Clone)]
// unfortunately the json writer doesnt give a way to get a mutable reference to the underlying writer, so we need another pointer in to its buffer
// we use a lock here to help make the writer send/sync, despite it being totally uncontended :(
struct LockWriter(Arc<Mutex<Vec<u8>>>);

impl LockWriter {
    fn new() -> Self {
        Self(Arc::new(Mutex::new(Vec::new())))
    }

    fn take(&self) -> Vec<u8> {
        let mut vec = self.0.lock();
        let new_vec = Vec::with_capacity(vec.capacity());
        std::mem::replace(&mut vec, new_vec)
    }
}

impl Write for LockWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.0.lock().flush()
    }
}

pub(crate) struct JsonWriter {
    json_writer: datafusion::arrow::json::Writer<LockWriter, JsonArray>,
    lock_writer: LockWriter,
    finished: bool,
}

impl RecordBatchWriter for JsonWriter {
    fn new(_schema: &Schema) -> Result<Self, DataFusionError> {
        let mut lock_writer = LockWriter::new();
        // we write out under 'rows' key so that we may add extra keys later (eg 'schema')
        lock_writer.write_all(br#"{"rows":"#)?;
        Ok(Self {
            json_writer: datafusion::arrow::json::Writer::new(lock_writer.clone()),
            lock_writer,
            finished: false,
        })
    }

    fn write(&mut self, batch: &RecordBatch) -> Result<Bytes, DataFusionError> {
        self.json_writer.write(batch)?;
        Ok(Bytes::from(self.lock_writer.take()))
    }

    fn finish(&mut self) -> Result<Bytes, DataFusionError> {
        if !self.finished {
            self.finished = true;

            self.json_writer.finish()?;
            self.lock_writer.write_all(b"}")?;
        }
        Ok(Bytes::from(self.lock_writer.take()))
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn param_values_conversion() {
        let params = serde_json::from_value(json!([null, true, -1, u64::MAX, 1.5, "a"])).unwrap();
        let ParamValues::List(values) = to_param_values(params).unwrap() else {
            panic!("expected positional params");
        };
        assert_eq!(
            values.into_iter().map(|v| v.value).collect::<Vec<_>>(),
            vec![
                ScalarValue::Null,
                ScalarValue::Boolean(Some(true)),
                ScalarValue::Int64(Some(-1)),
                ScalarValue::UInt64(Some(u64::MAX)),
                ScalarValue::Float64(Some(1.5)),
                ScalarValue::new_utf8("a"),
            ]
        );

        let params = serde_json::from_value(json!({"id": "inv_1"})).unwrap();
        let ParamValues::Map(values) = to_param_values(params).unwrap() else {
            panic!("expected named params");
        };
        assert_eq!(values["id"].value, ScalarValue::new_utf8("inv_1"));

        let params = serde_json::from_value(json!(["a", [1]])).unwrap();
        let err = to_param_values(params).unwrap_err();
        assert!(
            matches!(&err, QueryError::InvalidParam(id) if id == "$2"),
            "{err}"
        );

        let params = serde_json::from_value(json!({"id": {"a": 1}})).unwrap();
        let err = to_param_values(params).unwrap_err();
        assert!(
            matches!(&err, QueryError::InvalidParam(id) if id == "$id"),
            "{err}"
        );
    }
}
