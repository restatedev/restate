// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

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
use datafusion::common::DataFusionError;
use futures::{StreamExt, TryStreamExt};
use http::{HeaderMap, HeaderValue};
use http_body::Frame;
use http_body_util::StreamBody;
use parking_lot::Mutex;
use serde::Serialize;

use restate_admin_rest_model::query::{QUERY_WARNINGS_HEADER, QueryRequest};
use restate_core::network::TransportConnect;
use restate_storage_query_datafusion::query_warnings::QueryWarnings;
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
            | QueryError::Datafusion(datafusion::error::DataFusionError::SQL(_, _)) => {
                StatusCode::BAD_REQUEST
            }
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

    let query_result = query_context.execute(&payload.query).await?;
    let warnings = Arc::clone(query_result.warnings());

    let (result_stream, content_type) = match headers.get(http::header::ACCEPT) {
        Some(v) if v == HeaderValue::from_static("application/json") => (
            WriteRecordBatchStream::<JsonWriter>::new(
                query_result.stream,
                payload.query,
                Arc::clone(&warnings),
            )?
            .map_ok(Frame::data)
            .left_stream(),
            "application/json",
        ),
        _ => (
            WriteRecordBatchStream::<StreamWriter<Vec<u8>>>::new(
                query_result.stream,
                payload.query,
                (),
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

    // Response headers are sent before the body, so this reports the partitions that were
    // already known to be unavailable when the query was planned - which is every one of
    // them in practice, since routing is resolved at plan time. A partition that becomes
    // unroutable mid-query is recorded in the server log only. The JSON representation has
    // no such limitation because it is written at the end of the body.
    let mut response = Response::builder().header(http::header::CONTENT_TYPE, content_type);
    if let Some(header) = warnings_header(&warnings) {
        response = response.header(QUERY_WARNINGS_HEADER, header);
    }

    Ok(response
        .body(StreamBody::new(result_stream))
        .expect("content-type header is correct")
        .into_response())
}

/// Header values must be visible ASCII, and the full message is available in the server log
/// and (for JSON responses) in the body, so lossily sanitizing here is acceptable.
fn sanitize_for_header(value: &str) -> String {
    value
        .chars()
        .map(|c| {
            if c.is_ascii_graphic() || c == ' ' {
                c
            } else {
                '?'
            }
        })
        .collect()
}

fn warnings_json(warnings: &QueryWarnings) -> Option<serde_json::Value> {
    let collected = warnings.collect();
    if collected.is_empty() {
        return None;
    }

    Some(serde_json::Value::Array(
        collected
            .into_iter()
            .map(|warning| {
                serde_json::json!({
                    "origin": warning.origin.to_string(),
                    "message": warning.message,
                })
            })
            .collect(),
    ))
}

fn warnings_header(warnings: &QueryWarnings) -> Option<HeaderValue> {
    let collected = warnings.collect();
    if collected.is_empty() {
        return None;
    }

    let json = serde_json::Value::Array(
        collected
            .into_iter()
            .map(|warning| {
                serde_json::json!({
                    "origin": sanitize_for_header(&warning.origin.to_string()),
                    "message": sanitize_for_header(&warning.message),
                })
            })
            .collect(),
    );

    HeaderValue::from_str(&json.to_string()).ok()
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
    warnings: Arc<QueryWarnings>,
}

impl RecordBatchWriter for JsonWriter {
    type Context = Arc<QueryWarnings>;

    fn new(_schema: &Schema, warnings: Self::Context) -> Result<Self, DataFusionError> {
        let mut lock_writer = LockWriter::new();
        // we write out under 'rows' key so that we may add extra keys later (eg 'schema')
        lock_writer.write_all(br#"{"rows":"#)?;
        Ok(Self {
            json_writer: datafusion::arrow::json::Writer::new(lock_writer.clone()),
            lock_writer,
            finished: false,
            warnings,
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
            // Written here rather than up front because the set of skipped scans is only
            // complete once the last batch has been produced.
            if let Some(warnings) = warnings_json(&self.warnings) {
                self.lock_writer.write_all(br#","warnings":"#)?;
                self.lock_writer
                    .write_all(warnings.to_string().as_bytes())?;
            }
            self.lock_writer.write_all(b"}")?;
        }
        Ok(Bytes::from(self.lock_writer.take()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_values_are_reduced_to_visible_ascii() {
        // header values must be visible ASCII, so anything else is replaced rather than
        // silently producing a value `HeaderValue::from_str` would reject
        assert_eq!("partition 22", sanitize_for_header("partition 22"));
        assert_eq!("a?b", sanitize_for_header("a\nb"));
        assert_eq!("q???", sanitize_for_header("q\u{00e9}\u{00e8}\t"));

        let json = serde_json::json!([{
            "origin": sanitize_for_header("partition 22"),
            "message": sanitize_for_header("unavailable:\nno leader"),
        }]);
        // the encoded form must always be acceptable as a header value
        assert!(HeaderValue::from_str(&json.to_string()).is_ok());
    }
}
