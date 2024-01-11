// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use hyper::http::uri::{InvalidUri, Parts, Scheme};
use hyper::Uri;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::str::FromStr;
use std::task::{Context, Poll};

#[derive(Clone, Debug, thiserror::Error)]
#[error("invalid proxy Uri (must have scheme, authority, and path): {0}")]
pub struct InvalidProxyUri(Uri);

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Proxy {
    uri: Uri,
}

impl Display for Proxy {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.uri.fmt(f)
    }
}

impl TryFrom<String> for Proxy {
    type Error = ProxyFromStrError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Proxy::from_str(&value)
    }
}

impl From<Proxy> for String {
    fn from(value: Proxy) -> Self {
        value.to_string()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ProxyFromStrError {
    #[error(transparent)]
    InvalidUri(#[from] InvalidUri),
    #[error(transparent)]
    InvalidProxyUri(#[from] InvalidProxyUri),
}

impl FromStr for Proxy {
    type Err = ProxyFromStrError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::new(Uri::from_str(s)?)?)
    }
}

impl Proxy {
    fn new(proxy_uri: Uri) -> Result<Self, InvalidProxyUri> {
        match proxy_uri.clone().into_parts() {
            // all three must be present
            Parts {
                scheme: Some(_),
                authority: Some(_),
                path_and_query: Some(_),
                ..
            } => Ok(Self { uri: proxy_uri }),
            _ => Err(InvalidProxyUri(proxy_uri)),
        }
    }

    fn dst(&self, dst: Uri) -> Uri {
        // only proxy non TLS traffic, otherwise just pass through directly to underlying connector
        if dst.scheme() != Some(&Scheme::HTTPS) {
            let mut parts = self.clone().uri.into_parts();
            parts.path_and_query = dst.path_and_query().cloned();

            Uri::from_parts(parts).unwrap()
        } else {
            dst
        }
    }
}

#[derive(Clone, Debug)]
pub struct ProxyConnector<C> {
    proxy: Option<Proxy>,
    connector: C,
}

impl<C> ProxyConnector<C> {
    pub fn new(proxy: Option<Proxy>, connector: C) -> Self {
        Self { proxy, connector }
    }
}

impl<C> tower::Service<Uri> for ProxyConnector<C>
where
    C: tower::Service<Uri>,
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
