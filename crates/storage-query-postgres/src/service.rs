// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::pgwire_server::HandlerFactory;
use codederror::CodedError;
use restate_core::cancellation_watcher;
use restate_storage_query_datafusion::context::QueryContext;

use restate_types::config::QueryEngineOptions;
use restate_types::errors::GenericError;
use std::io::ErrorKind;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tokio::select;
use tracing::warn;

#[derive(Debug, thiserror::Error, CodedError)]
pub enum Error {
    #[error(
        "failed binding to address '{0}' specified in 'worker.storage_query_postgres.bind_address'"
    )]
    #[code(unknown)]
    AddrInUse(SocketAddr),
    #[error("error: {0:?}")]
    #[code(unknown)]
    Other(#[from] GenericError),
}

pub struct PostgresQueryService {
    pub bind_address: SocketAddr,
    pub query_context: QueryContext,
}

impl PostgresQueryService {
    pub fn from_options(options: &QueryEngineOptions, query_context: QueryContext) -> Self {
        Self {
            bind_address: options.pgsql_bind_address,
            query_context,
        }
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let PostgresQueryService {
            bind_address,
            query_context,
        } = self;

        let listener = TcpListener::bind(&bind_address).await.map_err(|e| {
            if e.kind() == ErrorKind::AddrInUse {
                Error::AddrInUse(bind_address)
            } else {
                Error::Other(e.into())
            }
        })?;

        let shutdown = cancellation_watcher();
        tokio::pin!(shutdown);

        let factory = HandlerFactory::new(query_context);
        loop {
            select! {
                incoming_socket = listener.accept() => {
                    match incoming_socket {
                        Ok((stream, addr)) => factory.spawn_connection(stream, addr),
                        Err(err) => {
                            warn!("Failed to accept storage query connection: {err}");
                        }
                    }
                },
                _ = &mut shutdown => {
                    break;
                },
            }
        }

        Ok(())
    }
}
