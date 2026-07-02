// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An HTTP server exposing the admin-compatible SQL query API (`POST /query`) over the opened
//! snapshots, so existing tooling (e.g. `restate sql`) can be pointed at a snapshot instead of a
//! live cluster. Mirrors the request/response contract of the worker's `/admin/query` endpoint.

use std::io::Write;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use anyhow::Context as _;
use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use arrow::ipc::writer::StreamWriter;
use arrow::json::writer::JsonArray;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router, http};
use bytes::Bytes;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use futures::{Stream, StreamExt, TryStreamExt, ready};
use http::{HeaderMap, HeaderValue};
use http_body::Frame;
use http_body_util::StreamBody;
use serde::{Deserialize, Serialize};
use tracing::{Level, enabled, warn};

use restate_cli_util::c_println;
use restate_storage_query_datafusion::context::QueryContext;

/// SQL query request body, matching the admin `/query` endpoint.
#[derive(Debug, Deserialize)]
struct QueryRequest {
    /// SQL query to run against the snapshot.
    query: String,
}

/// Error response body for the query endpoint.
#[derive(Debug, Serialize)]
struct QueryErrorBody {
    message: String,
}

/// Errors that can occur when executing a query.
struct QueryError(restate_storage_query_datafusion::context::QueryError);

impl From<datafusion::error::DataFusionError> for QueryError {
    fn from(err: datafusion::error::DataFusionError) -> Self {
        Self(restate_storage_query_datafusion::context::QueryError::DataFusion(err))
    }
}

impl IntoResponse for QueryError {
    fn into_response(self) -> Response {
        let status_code = match &self.0 {
            restate_storage_query_datafusion::context::QueryError::DataFusion(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
            restate_storage_query_datafusion::context::QueryError::RateLimited(_) => {
                StatusCode::TOO_MANY_REQUESTS
            }
        };
        (
            status_code,
            Json(QueryErrorBody {
                message: self.0.to_string(),
            }),
        )
            .into_response()
    }
}

/// Builds the router, binds the listener, and serves the query API until shutdown.
pub(crate) async fn run_server(ctx: QueryContext, addr: SocketAddr) -> anyhow::Result<()> {
    let router = Router::new()
        .route("/query", post(query))
        .with_state(Arc::new(ctx));

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("failed to bind {addr}"))?;
    let local_addr = listener.local_addr()?;
    c_println!("Query API listening on http://{local_addr}/query");
    c_println!(
        "Try: curl -s http://{local_addr}/query -H 'content-type: application/json' \\\n  -H 'accept: application/json' -d '{{\"query\":\"SELECT table_name FROM information_schema.tables\"}}'"
    );

    axum::serve(listener, router)
        .await
        .context("query API server failed")
}

/// Query the snapshot state by using SQL. Negotiates the response format via the `Accept` header:
/// JSON (`application/json`) or, by default, Arrow IPC stream
/// (`application/vnd.apache.arrow.stream`).
async fn query(
    State(ctx): State<Arc<QueryContext>>,
    headers: HeaderMap,
    Json(payload): Json<QueryRequest>,
) -> Result<Response, QueryError> {
    let query_result = ctx.execute(&payload.query).await.map_err(QueryError)?;

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

    // Return an error (instead of just closing the stream) if getting the first record batch fails
    // (e.g. out of memory).
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

trait RecordBatchWriter
where
    Self: Sized,
{
    /// Create the writer.
    fn new(schema: &Schema) -> Result<Self, DataFusionError>;

    /// Write a single batch to the writer.
    fn write(&mut self, batch: &RecordBatch) -> Result<Bytes, DataFusionError>;

    /// Write footer or termination data, then mark the writer as done.
    fn finish(&mut self) -> Result<Bytes, DataFusionError>;
}

impl RecordBatchWriter for StreamWriter<Vec<u8>> {
    fn new(schema: &Schema) -> Result<Self, DataFusionError> {
        Ok(Self::try_new(Vec::new(), schema)?)
    }

    fn write(&mut self, batch: &RecordBatch) -> Result<Bytes, DataFusionError> {
        self.write(batch)?;
        let bytes = Bytes::copy_from_slice(self.get_ref());
        self.get_mut().clear();
        Ok(bytes)
    }

    fn finish(&mut self) -> Result<Bytes, DataFusionError> {
        self.finish()?;
        let bytes = Bytes::copy_from_slice(self.get_ref());
        self.get_mut().clear();
        Ok(bytes)
    }
}

struct WriteRecordBatchStream<W> {
    done: bool,
    record_batch_stream: SendableRecordBatchStream,
    stream_writer: W,
    query: String,
}

impl<W: RecordBatchWriter> WriteRecordBatchStream<W> {
    fn new(
        record_batch_stream: SendableRecordBatchStream,
        query: String,
    ) -> Result<Self, DataFusionError> {
        Ok(WriteRecordBatchStream {
            done: false,
            stream_writer: W::new(&record_batch_stream.schema())?,
            record_batch_stream,
            query,
        })
    }
}

impl<W: RecordBatchWriter + Unpin> Stream for WriteRecordBatchStream<W> {
    type Item = Result<Bytes, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }

        let record_batch = ready!(self.record_batch_stream.poll_next_unpin(cx));

        if let Some(record_batch) = record_batch {
            match record_batch.and_then(|record_batch| self.stream_writer.write(&record_batch)) {
                Ok(bytes) => Poll::Ready(Some(Ok(bytes))),
                Err(err) => {
                    if enabled!(Level::DEBUG) {
                        warn!(query = %self.query, %err, "Query failed");
                    } else {
                        warn!(%err, "Query failed");
                    }

                    self.done = true;
                    Poll::Ready(Some(Err(err)))
                }
            }
        } else {
            self.done = true;
            match self.stream_writer.finish() {
                Err(err) => Poll::Ready(Some(Err(err))),
                Ok(bytes) => Poll::Ready(Some(Ok(bytes))),
            }
        }
    }
}

/// Shared buffer that gives us read-back access to the bytes the JSON writer produces (the arrow
/// JSON writer owns its sink and exposes no mutable accessor). Uncontended in practice.
#[derive(Clone)]
struct LockWriter(Arc<Mutex<Vec<u8>>>);

impl LockWriter {
    fn new() -> Self {
        Self(Arc::new(Mutex::new(Vec::new())))
    }

    fn take(&self) -> Vec<u8> {
        let mut vec = self.0.lock().expect("lock is not poisoned");
        let new_vec = Vec::with_capacity(vec.capacity());
        std::mem::replace(&mut vec, new_vec)
    }
}

impl Write for LockWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("lock is not poisoned").write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.0.lock().expect("lock is not poisoned").flush()
    }
}

struct JsonWriter {
    json_writer: arrow::json::Writer<LockWriter, JsonArray>,
    lock_writer: LockWriter,
    finished: bool,
}

impl RecordBatchWriter for JsonWriter {
    fn new(_schema: &Schema) -> Result<Self, DataFusionError> {
        let mut lock_writer = LockWriter::new();
        // We write out under the 'rows' key so that we may add extra keys later (e.g. 'schema').
        lock_writer.write_all(br#"{"rows":"#)?;
        Ok(Self {
            json_writer: arrow::json::Writer::new(lock_writer.clone()),
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
