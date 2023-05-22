use std::fmt;
use std::fmt::{Display, Formatter};
use std::str::FromStr;
use std::task::{Context, Poll};

use hyper::http::uri::{InvalidUri, Parts, Scheme};
use hyper::Uri;
use tower::Service;

#[derive(Clone, Debug, thiserror::Error)]
#[error("invalid proxy Uri (must have scheme, authority, and path): {0}")]
pub struct InvalidProxyUri(Uri);

#[derive(Clone, Debug)]
pub struct Proxy {
    uri: Uri,
}

impl Display for Proxy {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.uri.fmt(f)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ProxyFromStrErr {
    #[error(transparent)]
    InvalidUri(#[from] InvalidUri),
    #[error(transparent)]
    InvalidProxyUri(#[from] InvalidProxyUri),
}

impl FromStr for Proxy {
    type Err = ProxyFromStrErr;
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

#[derive(Clone)]
pub struct ProxyConnector<C> {
    proxy: Option<Proxy>,
    connector: C,
}

impl<C> ProxyConnector<C> {
    pub fn new(proxy: Option<Proxy>, connector: C) -> Self {
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
