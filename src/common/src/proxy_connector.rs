use std::future::Future;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use futures::TryFutureExt;
use hyper::http::uri::{InvalidUriParts, Scheme};
use hyper::Uri;
use tokio::io::{AsyncRead, AsyncWrite};
use tower::Service;

use crate::utils::GenericError;

#[derive(Clone)]
pub struct ProxyConnector<C> {
    proxy_uri: Option<Uri>,
    connector: C,
}

impl<C> ProxyConnector<C> {
    pub fn new(proxy_uri: Option<Uri>, connector: C) -> Self {
        Self {
            proxy_uri,
            connector,
        }
    }
}

impl<C> Service<Uri> for ProxyConnector<C>
where
    C: Service<Uri>,
    C::Response: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    C::Future: Send + 'static,
    C::Error: Into<GenericError> + 'static,
{
    type Response = C::Response;
    type Error = GenericError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match ready!(self.connector.poll_ready(cx)) {
            Ok(()) => Poll::Ready(Ok(())),
            Err(e) => Poll::Ready(Err(e.into())),
        }
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        // only proxy non TLS traffic, otherwise just pass through directly to underlying connector
        if uri.scheme() != Some(&Scheme::HTTPS) {
            if let Some(proxy_uri) = &self.proxy_uri {
                return match proxy_dst(&uri, proxy_uri) {
                    Ok(proxy_uri) => Box::pin(self.connector.call(proxy_uri).map_err(Into::into)),
                    Err(err) => Box::pin(futures::future::err(err.into())),
                };
            }
        }

        Box::pin(self.connector.call(uri).map_err(|err| err.into()))
    }
}

fn proxy_dst(dst: &Uri, proxy: &Uri) -> Result<Uri, InvalidUriParts> {
    let mut parts = proxy.clone().into_parts();
    parts.path_and_query = dst.path_and_query().cloned();
    Uri::from_parts(parts)
}
