// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::task::{Poll, ready};

use futures::{FutureExt, Stream, StreamExt};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use restate_core::TaskHandle;
use restate_types::logs::LogletOffset;

use crate::LogEntry;
use crate::loglet::{LogletReadStream, OperationError};

pub(crate) struct ReplicatedLogletReadStream {
    // the next record this stream will attempt to return when polled
    pub(super) read_pointer: LogletOffset,
    pub(super) rx_stream: ReceiverStream<Result<LogEntry<LogletOffset>, OperationError>>,
    pub(super) reader_task: TaskHandle<Result<(), OperationError>>,
    pub(super) terminated: bool,
}

impl Drop for ReplicatedLogletReadStream {
    fn drop(&mut self) {
        // make sure we abort the reader task if the read stream is disposed
        self.reader_task.abort();
    }
}

impl ReplicatedLogletReadStream {
    pub fn new(
        from_offset: LogletOffset,
        rx_stream: mpsc::Receiver<Result<LogEntry<LogletOffset>, OperationError>>,
        reader_task: TaskHandle<Result<(), OperationError>>,
    ) -> Self {
        Self {
            read_pointer: from_offset,
            rx_stream: ReceiverStream::new(rx_stream),
            reader_task,
            terminated: false,
        }
    }
}

impl LogletReadStream for ReplicatedLogletReadStream {
    /// Current read pointer. This points to the next offset to be read.
    fn read_pointer(&self) -> LogletOffset {
        self.read_pointer
    }
    /// Returns true if the stream is terminated.
    fn is_terminated(&self) -> bool {
        self.terminated
    }
}

impl Stream for ReplicatedLogletReadStream {
    type Item = Result<LogEntry<LogletOffset>, OperationError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.terminated {
            Poll::Ready(None)
        } else {
            match ready!(self.rx_stream.poll_next_unpin(cx)) {
                Some(maybe_record) => match maybe_record {
                    Ok(record) => {
                        self.read_pointer = record.next_sequence_number();
                        Poll::Ready(Some(Ok(record)))
                    }
                    Err(err) => {
                        self.terminated = true;
                        self.rx_stream.close();
                        self.reader_task.abort();
                        Poll::Ready(Some(Err(err)))
                    }
                },
                None => {
                    // stream terminated, but did the task fail?
                    let task_err = ready!(self.reader_task.poll_unpin(cx));
                    self.terminated = true;
                    self.rx_stream.close();
                    match task_err {
                        Ok(Ok(_)) => Poll::Ready(None),
                        Ok(Err(e)) => Poll::Ready(Some(Err(e))),
                        Err(shutdown_err) => Poll::Ready(Some(Err(shutdown_err.into()))),
                    }
                }
            }
        }
    }
}
