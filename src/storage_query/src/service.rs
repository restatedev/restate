// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::context::QueryContext;
use crate::pgwire_server::HandlerFactory;
use codederror::CodedError;
use restate_storage_rocksdb::RocksDBStorage;
use std::io::ErrorKind;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tokio::select;

pub type GenericError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug, thiserror::Error, CodedError)]
pub enum Error {
    #[error("failed binding to address '{0}' specified in 'worker.storage_query.bind_address'")]
    #[code(unknown)]
    AddrInUse(SocketAddr),
    #[error("error: {0:?}")]
    #[code(unknown)]
    Other(#[from] GenericError),
}

pub struct PostgresQueryService {
    pub bind_address: SocketAddr,
    pub rocksdb: RocksDBStorage,
    pub memory_limit: Option<usize>,
    pub temp_folder: Option<String>,
    pub query_parallelism: Option<usize>,
}

impl PostgresQueryService {
    pub async fn run(self, drain: drain::Watch) -> Result<(), Error> {
        let PostgresQueryService {
            bind_address,
            rocksdb,
            memory_limit,
            temp_folder,
            query_parallelism,
        } = self;

        let listener = TcpListener::bind(&bind_address).await.map_err(|e| {
            if e.kind() == ErrorKind::AddrInUse {
                Error::AddrInUse(bind_address)
            } else {
                Error::Other(e.into())
            }
        })?;

        let shutdown = drain.signaled();
        tokio::pin!(shutdown);

        let ctx = QueryContext::new(memory_limit, temp_folder, query_parallelism);
        crate::status::register_self(&ctx, rocksdb.clone()).map_err(|e| Error::Other(e.into()))?;
        crate::state::register_self(&ctx, rocksdb).map_err(|e| Error::Other(e.into()))?;

        let factory = HandlerFactory::new(ctx);
        loop {
            select! {
                incoming_socket = listener.accept() => {
                    if let Ok((stream, _addr)) = incoming_socket {
                        factory.spawn_connection(stream);
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
