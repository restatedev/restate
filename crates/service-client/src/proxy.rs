// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use hyper::Uri;
use restate_types::config::ProxyUri;
use std::task::{Context, Poll};
use tower_service::Service;

#[derive(Clone, Debug)]
pub struct ProxyConnector<C> {
    proxy: Option<ProxyUri>,
    connector: C,
}

impl<C> ProxyConnector<C> {
    pub fn new(proxy: Option<ProxyUri>, connector: C) -> Self {
        Self { proxy, connector }
    }
}

impl<C> Service<Uri> for ProxyConnector<C>
where
    C: Service<Uri>,
{
    type Response = C::Response;
    type Error = C::Error;
    type Future = C::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.connector.poll_ready(cx)
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        self.connector.call(match &self.proxy {
            Some(proxy) => proxy.dst(uri),
            None => uri,
        })
    }
}
