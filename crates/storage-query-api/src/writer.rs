// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use futures::{Stream, StreamExt, ready};
use tracing::warn;

use crate::QueryMetadata;

pub trait RecordBatchWriter
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

pub struct WriteRecordBatchStream<W> {
    done: bool,
    record_batch_stream: SendableRecordBatchStream,
    stream_writer: W,
    metadata: QueryMetadata,
}

impl<W: RecordBatchWriter> WriteRecordBatchStream<W> {
    pub fn new(
        record_batch_stream: SendableRecordBatchStream,
        metadata: QueryMetadata,
    ) -> Result<Self, DataFusionError> {
        Ok(WriteRecordBatchStream {
            done: false,
            stream_writer: W::new(&record_batch_stream.schema())?,
            record_batch_stream,
            metadata,
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
                    // Error messages can embed literal values. Return the original
                    // error to the caller, but log only the redacted diagnostic SQL.
                    warn!(target: "query_engine", session = %self.metadata.session_id,
                        query = %self.metadata.redacted_sql, "Query failed");

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
