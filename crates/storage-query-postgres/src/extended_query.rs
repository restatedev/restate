// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_trait::async_trait;
use pgwire::api::portal::Portal;
use pgwire::api::query::ExtendedQueryHandler;
use pgwire::api::results::{DescribePortalResponse, DescribeStatementResponse, Response};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::ClientInfo;
use pgwire::error::{PgWireError, PgWireResult};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct NoopExtendedQueryHandler {
    query_parser: Arc<NoopQueryParser>,
}

impl NoopExtendedQueryHandler {
    pub(crate) fn new() -> Self {
        NoopExtendedQueryHandler {
            query_parser: Arc::new(Default::default()),
        }
    }
}

#[async_trait]
impl ExtendedQueryHandler for NoopExtendedQueryHandler {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.query_parser.clone()
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        _statement: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        Err(PgWireError::ApiError(
            "Extended Query is not implemented on this server.".into(),
        ))
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        _portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        Err(PgWireError::ApiError(
            "Extended Query is not implemented on this server.".into(),
        ))
    }

    async fn do_query<'a, 'b: 'a, C>(
        &'b self,
        _client: &mut C,
        _portal: &'a Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response<'a>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        Err(PgWireError::ApiError(
            "Extended Query is not implemented on this server.".into(),
        ))
    }
}
