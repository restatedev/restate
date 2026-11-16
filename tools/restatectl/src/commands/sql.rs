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

use anyhow::{Context as AnyhowContext, bail};
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
    QueryRequest, QueryResponse, QueryWarning, new_cluster_ctrl_client,
};
use tonic::metadata::MetadataValue;
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

    /// Query engine to run the query on. Defaults to the server's default engine; the
    /// engine that served the query is reported with the row count. Fails if the
    /// server doesn't confirm that it ran the query on the requested engine.
    #[arg(long, value_enum)]
    pub engine: Option<QueryEngine>,
}

/// A query engine of the server.
#[derive(ValueEnum, Collect, Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueryEngine {
    /// The default engine.
    V1,
    /// The experimental distributed engine.
    V2,
}

impl QueryEngine {
    fn as_str(self) -> &'static str {
        match self {
            Self::V1 => "v1",
            Self::V2 => "v2",
        }
    }
}

/// Selects the engine for one query, and names the engine that served it in the
/// response.
const QUERY_ENGINE_HEADER: &str = "x-restate-query-engine";

/// Fails unless the server reports that it ran the query on the `requested`
/// engine. A server older than v1.8.2 ignores the selection and runs the query
/// on V1; an A/B comparison mustn't silently compare V1 with V1.
fn ensure_engine(requested: Option<QueryEngine>, reported: Option<&str>) -> anyhow::Result<()> {
    let Some(requested) = requested else {
        return Ok(());
    };
    if reported != Some(requested.as_str()) {
        bail!(
            "the server didn't run this query on query engine {}: it reported {}. Selecting the engine needs a server on v1.8.2 or newer, with that engine enabled",
            requested.as_str(),
            reported.unwrap_or("no engine")
        );
    }
    Ok(())
}

async fn query(connection: &ConnectionInfo, args: &SqlOpts) -> anyhow::Result<()> {
    let channel = connection
        .connect(&connection.address[0])
        .await
        .context("Failed to connect to node")?;

    let start_time = Instant::now();
    let mut client = new_cluster_ctrl_client(channel, &CliContext::get().network);

    let mut request = tonic::Request::new(QueryRequest {
        query: args.query.clone(),
    });
    if let Some(engine) = args.engine {
        request.metadata_mut().insert(
            QUERY_ENGINE_HEADER,
            MetadataValue::from_static(engine.as_str()),
        );
    }
    let response = client.query(request).await.context("Failed to run query")?;
    let served_engine = response
        .metadata()
        .get(QUERY_ENGINE_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    ensure_engine(args.engine, served_engine.as_deref())?;
    let response = response.into_inner();

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
            row_count += batch.num_rows();
        }

        // Only print if there are actual results.
        if table.row_count() > 0 {
            c_println!("{}", table);
            c_println!();
        }
    }

    // Display per-node warnings collected during query execution
    for warning in stream.warnings() {
        c_eprintln!("WARNING (partial results):");
        c_eprintln!(
            "  {}: {}",
            Styled(Style::Warn, &warning.node_id),
            Styled(Style::Warn, &warning.message),
        );
    }

    let engine = served_engine
        .map(|engine| format!(" on query engine {engine}"))
        .unwrap_or_default();
    c_eprintln!(
        "{} rows. Query took {:?}{}",
        row_count,
        Styled(Style::Notice, start_time.elapsed()),
        Styled(Style::Notice, engine)
    );

    Ok(())
}

pub struct RecordBatchStream {
    inner: Streaming<QueryResponse>,
    decoder: StreamDecoder,
    buffer: Option<Buffer>,
    done: bool,
    warnings: Vec<QueryWarning>,
}

impl RecordBatchStream {
    fn new(inner: Streaming<QueryResponse>) -> Self {
        Self {
            inner,
            decoder: StreamDecoder::new(),
            buffer: None,
            done: false,
            warnings: Vec::new(),
        }
    }

    /// Returns warnings collected during query execution.
    /// Should be called after the stream is fully consumed.
    pub fn warnings(&self) -> &[QueryWarning] {
        &self.warnings
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
        loop {
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
                    Some(Ok(response)) => {
                        // Collect any per-node warnings from this response
                        if !response.warnings.is_empty() {
                            self.warnings.extend(response.warnings);
                        }
                        // Skip responses with empty encoded data (warnings-only messages)
                        if response.encoded.is_empty() {
                            continue;
                        }
                        self.buffer = Some(Buffer::from(&*response.encoded));
                    }
                }
            }

            let mut buffer = self.buffer.take().unwrap();
            return match self.decoder.decode(&mut buffer) {
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
            };
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

#[cfg(test)]
mod engine_tests {
    use super::*;

    #[test]
    fn a_selected_engine_must_be_confirmed() {
        assert!(
            ensure_engine(None, None).is_ok(),
            "no selection, nothing to check"
        );
        assert!(ensure_engine(Some(QueryEngine::V2), Some("v2")).is_ok());
        // An older server doesn't report an engine; another one ran it on V1.
        for reported in [None, Some("v1")] {
            let error = ensure_engine(Some(QueryEngine::V2), reported).unwrap_err();
            assert!(error.to_string().contains("v1.8.2"), "{error}");
        }
    }
}
