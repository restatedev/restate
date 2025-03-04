// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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
use std::task::{Context, Poll};

use super::QueryServiceState;
use super::convert::{ConvertRecordBatchStream, V1_CONVERTER};
use super::error::StorageQueryError;
use axum::extract::State;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json, http};
use bytes::Bytes;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::json::writer::JsonArray;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use futures::{Stream, StreamExt, TryStreamExt, ready};
use http::{HeaderMap, HeaderValue};
use http_body::Frame;
use http_body_util::StreamBody;
use okapi_operation::*;
use parking_lot::Mutex;
use restate_admin_rest_model::version::AdminApiVersion;
use schemars::JsonSchema;
use serde::Deserialize;
use serde_with::serde_as;
use tracing::{Level, enabled, warn};

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
    Extension(version): Extension<AdminApiVersion>,
    headers: HeaderMap,
    #[request_body(required = true)] Json(payload): Json<QueryRequest>,
) -> Result<impl IntoResponse, StorageQueryError> {
    let record_batch_stream = state.query_context.execute(&payload.query).await?;

    let record_batch_stream: SendableRecordBatchStream = match version {
        AdminApiVersion::V1 => Box::pin(ConvertRecordBatchStream::new(
            V1_CONVERTER,
            record_batch_stream,
        )),
        // treat 'unknown' as latest, users can specify 1 if they want to maintain old behaviour
        AdminApiVersion::Unknown | AdminApiVersion::V2 => record_batch_stream,
    };

    let (result_stream, content_type) = match headers.get(http::header::ACCEPT) {
        Some(v) if v == HeaderValue::from_static("application/json") => (
            WriteRecordBatchStream::<JsonWriter>::new(record_batch_stream, payload.query)?
                .map_ok(Frame::data)
                .left_stream(),
            "application/json",
        ),
        _ => (
            WriteRecordBatchStream::<StreamWriter<Vec<u8>>>::new(
                record_batch_stream,
                payload.query,
            )?
            .map_ok(Frame::data)
            .right_stream(),
            "application/vnd.apache.arrow.stream",
        ),
    };

    Ok(Response::builder()
        .header(http::header::CONTENT_TYPE, content_type)
        .body(StreamBody::new(result_stream))
        .expect("content-type header is correct"))
}

trait RecordBatchWriter
where
    Self: Sized,
{
    // Create the writer
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

struct JsonWriter {
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
