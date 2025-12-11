// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use hyper::Uri;
use hyper_util::client::proxy::matcher;
use restate_types::config::NoProxy;
use std::{
    sync::Arc,
    task::{Context, Poll},
};
use tower_service::Service;

#[derive(Clone, Debug)]
pub struct ProxyConnector<C> {
    matcher: Arc<matcher::Matcher>,
    connector: C,
}

impl<C> ProxyConnector<C> {
    pub fn new(proxy: Option<String>, no_proxy: Option<NoProxy>, connector: C) -> Self {
        let builder = matcher::Builder::default();
        let builder = if let Some(proxy) = proxy {
            builder.http(proxy)
        } else {
            builder
        };
        let builder = match no_proxy {
            Some(NoProxy::CommaSeparated(no_proxy)) => builder.no(no_proxy),
            Some(NoProxy::List(no_proxy)) => builder.no(no_proxy.join(",")),
            None => builder,
        };
        let matcher = builder.build();
        Self {
            matcher: Arc::new(matcher),
            connector,
        }
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
        if let Some(intercepted) = self.matcher.intercept(&uri) {
            self.connector.call(intercepted.uri().clone())
        } else {
            self.connector.call(uri)
        }
    }
}
