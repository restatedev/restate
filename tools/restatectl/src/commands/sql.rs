// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{
    pin::Pin,
    task::{Context, Poll},
    time::Instant,
};

use anyhow::Context as AnyhowContext;
use arrow::ipc::reader::StreamDecoder;
use arrow::{
    array::RecordBatch,
    buffer::Buffer,
    error::ArrowError,
    util::display::{ArrayFormatter, FormatOptions},
};
use cling::prelude::*;
use futures::{Stream, StreamExt, ready};
use restate_cli_util::{
    _comfy_table::{Cell, Table},
    CliContext, c_eprintln, c_println,
    ui::{
        console::{Styled, StyledTable},
        stylesheet::Style,
    },
};
use restate_core::protobuf::cluster_ctrl_svc::{
    QueryRequest, QueryResponse, new_cluster_ctrl_client,
};
use tonic::{Status, Streaming};

use crate::connection::ConnectionInfo;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[cling(run = "query")]
pub struct SqlOpts {
    /// The SQL query to run
    query: String,

    /// Print result as line delimited json instead of using the tabular format
    #[arg(long, alias = "ldjson")]
    pub jsonl: bool,

    /// Print result as json array instead of using the tabular format
    #[arg(long)]
    pub json: bool,
}

async fn query(connection: &ConnectionInfo, args: &SqlOpts) -> anyhow::Result<()> {
    let channel = connection
        .connect(&connection.address[0])
        .await
        .context("Failed to connect to node")?;

    let start_time = Instant::now();
    let mut client = new_cluster_ctrl_client(channel, &CliContext::get().network);

    let response = client
        .query(QueryRequest {
            query: args.query.clone(),
        })
        .await
        .context("Failed to run query")?
        .into_inner();

    let mut stream = RecordBatchStream::from(response);

    let mut table = Table::new_styled();

    let mut row_count: usize = 0;
    if args.json {
        let mut writer = arrow::json::ArrayWriter::new(std::io::stdout());
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            row_count += batch.num_rows();
            writer.write_batches(&[&batch])?;
        }
        writer.finish()?;
    } else if args.jsonl {
        let mut writer = arrow::json::LineDelimitedWriter::new(std::io::stdout());
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            row_count += batch.num_rows();
            writer.write_batches(&[&batch])?;
        }
        writer.finish()?;
    } else {
        let format_options = FormatOptions::default().with_display_error(true);
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            if table.header().is_none() {
                // add headers.
                let mut headers = vec![];
                for col in batch.schema().fields() {
                    headers.push(col.name().clone().to_uppercase());
                }
                table.set_styled_header(headers);
            }

            let formatters = batch
                .columns()
                .iter()
                .map(|c| ArrayFormatter::try_new(c.as_ref(), &format_options))
                .collect::<Result<Vec<_>, ArrowError>>()?;

            for row in 0..batch.num_rows() {
                let mut cells = Vec::new();
                for formatter in &formatters {
                    cells.push(Cell::new(formatter.value(row)));
                }
                table.add_row(cells);
            }
        }

        // Only print if there are actual results.
        if table.row_count() > 0 {
            c_println!("{}", table);
            c_println!();
        }
    }

    c_eprintln!(
        "{} rows. Query took {:?}",
        row_count,
        Styled(Style::Notice, start_time.elapsed())
    );

    Ok(())
}

pub struct RecordBatchStream {
    inner: Streaming<QueryResponse>,
    decoder: StreamDecoder,
    buffer: Option<Buffer>,
    done: bool,
}

impl RecordBatchStream {
    fn new(inner: Streaming<QueryResponse>) -> Self {
        Self {
            inner,
            decoder: StreamDecoder::new(),
            buffer: None,
            done: false,
        }
    }
}

impl From<Streaming<QueryResponse>> for RecordBatchStream {
    fn from(value: Streaming<QueryResponse>) -> Self {
        Self::new(value)
    }
}

impl Stream for RecordBatchStream {
    type Item = Result<RecordBatch, RecordBatchStreamError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }

        if self.buffer.as_ref().is_none_or(|b| b.is_empty()) {
            let item = ready!(self.inner.poll_next_unpin(cx));
            match item {
                None => {
                    self.done = true;
                    return Poll::Ready(None);
                }
                Some(Err(err)) => {
                    self.done = true;
                    return Poll::Ready(Some(Err(err.into())));
                }
                Some(Ok(batch)) => {
                    self.buffer = Some(Buffer::from(&*batch.encoded));
                }
            }
        }

        let mut buffer = self.buffer.take().unwrap();
        match self.decoder.decode(&mut buffer) {
            Err(err) => {
                self.done = true;
                Poll::Ready(Some(Err(err.into())))
            }
            Ok(batch) => {
                if !buffer.is_empty() {
                    self.buffer = Some(buffer);
                }
                Poll::Ready(Ok(batch).transpose())
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RecordBatchStreamError {
    #[error(transparent)]
    Remote(#[from] Status),
    #[error(transparent)]
    Arrow(#[from] ArrowError),
}
