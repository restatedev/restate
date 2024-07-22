// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Copied from https://github.com/smithy-lang/smithy-rs/blob/release-2024-07-16/rust-runtime/aws-smithy-experimental/src/hyper_1_0.rs
//! necessary to set nodelay(true) on http connector, which has a significant Lambda speedup
//! https://github.com/smithy-lang/smithy-rs/issues/3769 is the issue which will enable removing
//! this file.
//! License Apache-2.0

use aws_smithy_async::future::timeout::TimedOutError;
use aws_smithy_async::rt::sleep::{default_async_sleep, SharedAsyncSleep};
use aws_smithy_runtime_api::box_error::BoxError;
use aws_smithy_runtime_api::client::connector_metadata::ConnectorMetadata;
use aws_smithy_runtime_api::client::http::{
    HttpClient, HttpConnector, HttpConnectorFuture, HttpConnectorSettings, SharedHttpClient,
    SharedHttpConnector,
};
use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
use aws_smithy_runtime_api::client::result::ConnectorError;
use aws_smithy_runtime_api::client::runtime_components::{
    RuntimeComponents, RuntimeComponentsBuilder,
};
use aws_smithy_types::body::SdkBody;
use aws_smithy_types::config_bag::ConfigBag;
use aws_smithy_types::error::display::DisplayErrorContext;
use aws_smithy_types::retry::ErrorKind;
use client::connect::Connection;
use h2::Reason;
use http::Uri;
use hyper::rt::{Read, Write};
use hyper_util::client::legacy as client;
use hyper_util::client::legacy::connect::Connect;
use hyper_util::rt::TokioExecutor;
use rustls::crypto::CryptoProvider;
use std::borrow::Cow;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::sync::RwLock;
use std::time::Duration;

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
#[non_exhaustive]
pub enum CryptoMode {
    Ring,
}

impl CryptoMode {
    fn provider(self) -> CryptoProvider {
        match self {
            CryptoMode::Ring => rustls::crypto::ring::default_provider(),
        }
    }
}

#[allow(unused_imports)]
mod cached_connectors {
    use client::connect::HttpConnector;
    use hyper_util::client::legacy as client;
    use hyper_util::client::legacy::connect::dns::GaiResolver;

    use crate::aws_hyper_client::build_connector::make_tls;
    use crate::aws_hyper_client::{CryptoMode, Inner};

    pub(crate) static HTTPS_NATIVE_ROOTS_RING: once_cell::sync::Lazy<
        hyper_rustls::HttpsConnector<HttpConnector>,
    > = once_cell::sync::Lazy::new(|| make_tls(GaiResolver::new(), CryptoMode::Ring.provider()));

    pub(super) fn cached_https(mode: Inner) -> hyper_rustls::HttpsConnector<HttpConnector> {
        match mode {
            Inner::Standard(CryptoMode::Ring) => HTTPS_NATIVE_ROOTS_RING.clone(),
        }
    }
}

mod build_connector {
    use client::connect::HttpConnector;
    use hyper_util::client::legacy as client;
    use rustls::crypto::CryptoProvider;
    use std::sync::Arc;

    fn restrict_ciphers(base: CryptoProvider) -> CryptoProvider {
        let suites = &[
            rustls::CipherSuite::TLS13_AES_256_GCM_SHA384,
            rustls::CipherSuite::TLS13_AES_128_GCM_SHA256,
            // TLS1.2 suites
            rustls::CipherSuite::TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
            rustls::CipherSuite::TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
            rustls::CipherSuite::TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
            rustls::CipherSuite::TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
            rustls::CipherSuite::TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
        ];
        let supported_suites = suites
            .iter()
            .flat_map(|suite| {
                base.cipher_suites
                    .iter()
                    .find(|s| &s.suite() == suite)
                    .cloned()
            })
            .collect::<Vec<_>>();
        CryptoProvider {
            cipher_suites: supported_suites,
            ..base
        }
    }

    pub(crate) fn make_tls<R>(
        resolver: R,
        crypto_provider: CryptoProvider,
    ) -> hyper_rustls::HttpsConnector<HttpConnector<R>> {
        use hyper_rustls::ConfigBuilderExt;
        let mut base_connector = HttpConnector::new_with_resolver(resolver);
        base_connector.enforce_http(false);
        // RESTATE ADDITION
        base_connector.set_nodelay(true);
        // END RESTATE ADDITION
        hyper_rustls::HttpsConnectorBuilder::new()
               .with_tls_config(
                rustls::ClientConfig::builder_with_provider(Arc::new(restrict_ciphers(crypto_provider)))
                    .with_safe_default_protocol_versions()
                    .expect("Error with the TLS configuration. Please file a bug report under https://github.com/smithy-lang/smithy-rs/issues.")
                    .with_native_roots().expect("error with TLS configuration.")
                    .with_no_client_auth()
            )
            .https_or_http()
            .enable_http1()
            .enable_http2()
            .wrap_connector(base_connector)
    }
}

/// [`HttpConnector`] that uses [`hyper`] to make HTTP requests.
///
/// This connector also implements socket connect and read timeouts.
///
/// This shouldn't be used directly in most cases.
/// See the docs on [`HyperClientBuilder`] for examples of how
/// to customize the Hyper client.
#[derive(Debug)]
pub struct HyperConnector {
    adapter: Box<dyn HttpConnector>,
}

impl HyperConnector {
    /// Builder for a Hyper connector.
    pub fn builder() -> HyperConnectorBuilder {
        Default::default()
    }
}

impl HttpConnector for HyperConnector {
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        self.adapter.call(request)
    }
}

/// Builder for [`HyperConnector`].
#[derive(Default, Debug)]
pub struct HyperConnectorBuilder<Crypto = CryptoUnset> {
    connector_settings: Option<HttpConnectorSettings>,
    sleep_impl: Option<SharedAsyncSleep>,
    client_builder: Option<hyper_util::client::legacy::Builder>,
    #[allow(unused)]
    crypto: Crypto,
}

#[derive(Default)]
#[non_exhaustive]
pub struct CryptoUnset {}

pub struct CryptoProviderSelected {
    crypto_provider: Inner,
}

#[derive(Clone)]
enum Inner {
    Standard(CryptoMode),
}

impl<Any> HyperConnectorBuilder<Any> {
    /// Create a [`HyperConnector`] from this builder and a given connector.
    pub(crate) fn build<C>(self, tcp_connector: C) -> HyperConnector
    where
        C: Send + Sync + 'static,
        C: Clone,
        C: tower::Service<Uri>,
        C::Response: Read + Write + Connection + Send + Sync + Unpin,
        C: Connect,
        C::Future: Unpin + Send + 'static,
        C::Error: Into<BoxError>,
    {
        let client_builder =
            self.client_builder
                .unwrap_or(hyper_util::client::legacy::Builder::new(
                    TokioExecutor::new(),
                ));
        let sleep_impl = self.sleep_impl.or_else(default_async_sleep);
        let (connect_timeout, read_timeout) = self
            .connector_settings
            .map(|c| (c.connect_timeout(), c.read_timeout()))
            .unwrap_or((None, None));

        let connector = match connect_timeout {
            Some(duration) => timeout_middleware::ConnectTimeout::new(
                tcp_connector,
                sleep_impl
                    .clone()
                    .expect("a sleep impl must be provided in order to have a connect timeout"),
                duration,
            ),
            None => timeout_middleware::ConnectTimeout::no_timeout(tcp_connector),
        };
        let base = client_builder.build(connector);
        let read_timeout = match read_timeout {
            Some(duration) => timeout_middleware::HttpReadTimeout::new(
                base,
                sleep_impl.expect("a sleep impl must be provided in order to have a read timeout"),
                duration,
            ),
            None => timeout_middleware::HttpReadTimeout::no_timeout(base),
        };
        HyperConnector {
            adapter: Box::new(Adapter {
                client: read_timeout,
            }),
        }
    }

    /// Set the async sleep implementation used for timeouts
    ///
    /// Calling this is only necessary for testing or to use something other than
    /// [`default_async_sleep`].
    pub fn set_sleep_impl(&mut self, sleep_impl: Option<SharedAsyncSleep>) -> &mut Self {
        self.sleep_impl = sleep_impl;
        self
    }

    /// Configure the HTTP settings for the `HyperAdapter`
    pub fn connector_settings(mut self, connector_settings: HttpConnectorSettings) -> Self {
        self.connector_settings = Some(connector_settings);
        self
    }

    /// Override the Hyper client [`Builder`](hyper_util::client::legacy::Builder) used to construct this client.
    ///
    /// This enables changing settings like forcing HTTP2 and modifying other default client behavior.
    pub(crate) fn hyper_builder(
        mut self,
        hyper_builder: hyper_util::client::legacy::Builder,
    ) -> Self {
        self.set_hyper_builder(Some(hyper_builder));
        self
    }

    /// Override the Hyper client [`Builder`](hyper_util::client::legacy::Builder) used to construct this client.
    ///
    /// This enables changing settings like forcing HTTP2 and modifying other default client behavior.
    pub(crate) fn set_hyper_builder(
        &mut self,
        hyper_builder: Option<hyper_util::client::legacy::Builder>,
    ) -> &mut Self {
        self.client_builder = hyper_builder;
        self
    }
}

/// Adapter to use a Hyper 1.0-based Client as an `HttpConnector`
///
/// This adapter also enables TCP `CONNECT` and HTTP `READ` timeouts via [`HyperConnector::builder`].
struct Adapter<C> {
    client: timeout_middleware::HttpReadTimeout<
        hyper_util::client::legacy::Client<timeout_middleware::ConnectTimeout<C>, SdkBody>,
    >,
}

impl<C> fmt::Debug for Adapter<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Adapter")
            .field("client", &"** hyper client **")
            .finish()
    }
}

impl<C> HttpConnector for Adapter<C>
where
    C: Clone + Send + Sync + 'static,
    C: tower::Service<Uri>,
    C::Response: Connection + Read + Write + Unpin + 'static,
    timeout_middleware::ConnectTimeout<C>: Connect,
    C::Future: Unpin + Send + 'static,
    C::Error: Into<BoxError>,
{
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        let request = match request.try_into_http1x() {
            Ok(request) => request,
            Err(err) => {
                return HttpConnectorFuture::ready(Err(ConnectorError::user(err.into())));
            }
        };
        /*let capture_connection = capture_connection(&mut request);
        if let Some(capture_smithy_connection) =
            request.extensions().get::<CaptureSmithyConnection>()
        {
            capture_smithy_connection
                .set_connection_retriever(move || extract_smithy_connection(&capture_connection));
        }*/
        let mut client = self.client.clone();
        use tower::Service;
        let fut = client.call(request);
        HttpConnectorFuture::new(async move {
            let response = fut
                .await
                .map_err(downcast_error)?
                .map(SdkBody::from_body_1_x);
            match HttpResponse::try_from(response) {
                Ok(response) => Ok(response),
                Err(err) => Err(ConnectorError::other(err.into(), None)),
            }
        })
    }
}

/// Downcast errors coming out of hyper into an appropriate `ConnectorError`
fn downcast_error(err: BoxError) -> ConnectorError {
    // is a `TimedOutError` (from aws_smithy_async::timeout) in the chain? if it is, this is a timeout
    if find_source::<TimedOutError>(err.as_ref()).is_some() {
        return ConnectorError::timeout(err);
    }
    // is the top of chain error actually already a `ConnectorError`? return that directly
    let err = match err.downcast::<ConnectorError>() {
        Ok(connector_error) => return *connector_error,
        Err(box_error) => box_error,
    };
    // generally, the top of chain will probably be a hyper error. Go through a set of hyper specific
    // error classifications
    let err = match find_source::<hyper::Error>(err.as_ref()) {
        Some(hyper_error) => return to_connector_error(hyper_error)(err),
        None => err,
    };

    // otherwise, we have no idea!
    ConnectorError::other(err, None)
}

/// Convert a [`hyper::Error`] into a [`ConnectorError`]
fn to_connector_error(err: &hyper::Error) -> fn(BoxError) -> ConnectorError {
    if err.is_timeout() || find_source::<timeout_middleware::HttpTimeoutError>(err).is_some() {
        return ConnectorError::timeout;
    }
    if err.is_user() {
        return ConnectorError::user;
    }
    if err.is_closed() || err.is_canceled() || find_source::<std::io::Error>(err).is_some() {
        return ConnectorError::io;
    }
    // We sometimes receive this from S3: hyper::Error(IncompleteMessage)
    if err.is_incomplete_message() {
        return |err: BoxError| ConnectorError::other(err, Some(ErrorKind::TransientError));
    }

    if let Some(h2_err) = find_source::<h2::Error>(err) {
        if h2_err.is_go_away()
            || (h2_err.is_reset() && h2_err.reason() == Some(Reason::REFUSED_STREAM))
        {
            return ConnectorError::io;
        }
    }

    tracing::warn!(err = %DisplayErrorContext(&err), "unrecognized error from Hyper. If this error should be retried, please file an issue.");
    |err: BoxError| ConnectorError::other(err, None)
}

fn find_source<'a, E: Error + 'static>(err: &'a (dyn Error + 'static)) -> Option<&'a E> {
    let mut next = Some(err);
    while let Some(err) = next {
        if let Some(matching_err) = err.downcast_ref::<E>() {
            return Some(matching_err);
        }
        next = err.source();
    }
    None
}

// TODO(https://github.com/awslabs/aws-sdk-rust/issues/1090): CacheKey must also include ptr equality to any
// runtime components that are used—sleep_impl as a base (unless we prohibit overriding sleep impl)
// If we decide to put a DnsResolver in RuntimeComponents, then we'll need to handle that as well.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct CacheKey {
    connect_timeout: Option<Duration>,
    read_timeout: Option<Duration>,
}

impl From<&HttpConnectorSettings> for CacheKey {
    fn from(value: &HttpConnectorSettings) -> Self {
        Self {
            connect_timeout: value.connect_timeout(),
            read_timeout: value.read_timeout(),
        }
    }
}

struct HyperClient<F> {
    connector_cache: RwLock<HashMap<CacheKey, SharedHttpConnector>>,
    client_builder: hyper_util::client::legacy::Builder,
    tcp_connector_fn: F,
}

impl<F> fmt::Debug for HyperClient<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HyperClient")
            .field("connector_cache", &self.connector_cache)
            .field("client_builder", &self.client_builder)
            .finish()
    }
}

impl<C, F> HttpClient for HyperClient<F>
where
    F: Fn() -> C + Send + Sync,
    C: Clone + Send + Sync + 'static,
    C: tower::Service<Uri>,
    C::Response: Connection + Read + Write + Send + Sync + Unpin + 'static,
    C::Future: Unpin + Send + 'static,
    C::Error: Into<BoxError>,
{
    fn http_connector(
        &self,
        settings: &HttpConnectorSettings,
        components: &RuntimeComponents,
    ) -> SharedHttpConnector {
        let key = CacheKey::from(settings);
        let mut connector = self.connector_cache.read().unwrap().get(&key).cloned();
        if connector.is_none() {
            let mut cache = self.connector_cache.write().unwrap();
            // Short-circuit if another thread already wrote a connector to the cache for this key
            if !cache.contains_key(&key) {
                let mut builder = HyperConnector::builder()
                    .hyper_builder(self.client_builder.clone())
                    .connector_settings(settings.clone());
                builder.set_sleep_impl(components.sleep_impl());

                let start = components.time_source().map(|ts| ts.now());
                let tcp_connector = (self.tcp_connector_fn)();
                let end = components.time_source().map(|ts| ts.now());
                if let (Some(start), Some(end)) = (start, end) {
                    if let Ok(elapsed) = end.duration_since(start) {
                        tracing::debug!("new TCP connector created in {:?}", elapsed);
                    }
                }
                let connector = SharedHttpConnector::new(builder.build(tcp_connector));
                cache.insert(key.clone(), connector);
            }
            connector = cache.get(&key).cloned();
        }

        connector.expect("cache populated above")
    }

    fn validate_base_client_config(
        &self,
        _: &RuntimeComponentsBuilder,
        _: &ConfigBag,
    ) -> Result<(), BoxError> {
        // Initialize the TCP connector at this point so that native certs load
        // at client initialization time instead of upon first request. We do it
        // here rather than at construction so that it won't run if this is not
        // the selected HTTP client for the base config (for example, if this was
        // the default HTTP client, and it was overridden by a later plugin).
        let _ = (self.tcp_connector_fn)();
        Ok(())
    }

    fn connector_metadata(&self) -> Option<ConnectorMetadata> {
        Some(ConnectorMetadata::new("hyper", Some(Cow::Borrowed("1.x"))))
    }
}

/// Builder for a hyper-backed [`HttpClient`] implementation.
///
/// This builder can be used to customize the underlying TCP connector used, as well as
/// hyper client configuration.
///
/// # Examples
///
/// Construct a Hyper client with the RusTLS TLS implementation.
/// This can be useful when you want to share a Hyper connector between multiple
/// generated Smithy clients.
#[derive(Clone, Default, Debug)]
pub struct HyperClientBuilder<Crypto = CryptoUnset> {
    client_builder: Option<hyper_util::client::legacy::Builder>,
    crypto_provider: Crypto,
}

impl HyperClientBuilder<CryptoProviderSelected> {
    /// Create a hyper client using RusTLS for TLS
    ///
    /// The trusted certificates will be loaded later when this becomes the selected
    /// HTTP client for a Smithy client.
    pub fn build_https(self) -> SharedHttpClient {
        let crypto = self.crypto_provider.crypto_provider;
        build_with_fn(self.client_builder, move || {
            cached_connectors::cached_https(crypto.clone())
        })
    }
}

impl HyperClientBuilder<CryptoUnset> {
    /// Creates a new builder.
    pub fn new() -> Self {
        Self::default()
    }

    pub fn crypto_mode(self, provider: CryptoMode) -> HyperClientBuilder<CryptoProviderSelected> {
        HyperClientBuilder {
            client_builder: self.client_builder,
            crypto_provider: CryptoProviderSelected {
                crypto_provider: Inner::Standard(provider),
            },
        }
    }
}

fn build_with_fn<C, F>(
    client_builder: Option<hyper_util::client::legacy::Builder>,
    tcp_connector_fn: F,
) -> SharedHttpClient
where
    F: Fn() -> C + Send + Sync + 'static,
    C: Clone + Send + Sync + 'static,
    C: tower::Service<Uri>,
    C::Response: Connection + Read + Write + Send + Sync + Unpin + 'static,
    C::Future: Unpin + Send + 'static,
    C::Error: Into<BoxError>,
    C: Connect,
{
    SharedHttpClient::new(HyperClient {
        connector_cache: RwLock::new(HashMap::new()),
        client_builder: client_builder
            .unwrap_or_else(|| hyper_util::client::legacy::Builder::new(TokioExecutor::new())),
        tcp_connector_fn,
    })
}

mod timeout_middleware {
    use std::error::Error;
    use std::fmt::Formatter;
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use std::time::Duration;

    use http::Uri;
    use pin_project_lite::pin_project;

    use aws_smithy_async::future::timeout::{TimedOutError, Timeout};
    use aws_smithy_async::rt::sleep::Sleep;
    use aws_smithy_async::rt::sleep::{AsyncSleep, SharedAsyncSleep};
    use aws_smithy_runtime_api::box_error::BoxError;

    #[derive(Debug)]
    pub(crate) struct HttpTimeoutError {
        kind: &'static str,
        duration: Duration,
    }

    impl std::fmt::Display for HttpTimeoutError {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(
                f,
                "{} timeout occurred after {:?}",
                self.kind, self.duration
            )
        }
    }

    impl Error for HttpTimeoutError {
        // We implement the `source` function as returning a `TimedOutError` because when `downcast_error`
        // or `find_source` is called with an `HttpTimeoutError` (or another error wrapping an `HttpTimeoutError`)
        // this method will be checked to determine if it's a timeout-related error.
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            Some(&TimedOutError)
        }
    }

    /// Timeout wrapper that will timeout on the initial TCP connection
    ///
    /// # Stability
    /// This interface is unstable.
    #[derive(Clone, Debug)]
    pub(super) struct ConnectTimeout<I> {
        inner: I,
        timeout: Option<(SharedAsyncSleep, Duration)>,
    }

    impl<I> ConnectTimeout<I> {
        /// Create a new `ConnectTimeout` around `inner`.
        ///
        /// Typically, `I` will implement [`hyper_util::client::legacy::connect::Connect`].
        pub(crate) fn new(inner: I, sleep: SharedAsyncSleep, timeout: Duration) -> Self {
            Self {
                inner,
                timeout: Some((sleep, timeout)),
            }
        }

        pub(crate) fn no_timeout(inner: I) -> Self {
            Self {
                inner,
                timeout: None,
            }
        }
    }

    #[derive(Clone, Debug)]
    pub(crate) struct HttpReadTimeout<I> {
        inner: I,
        timeout: Option<(SharedAsyncSleep, Duration)>,
    }

    impl<I> HttpReadTimeout<I> {
        /// Create a new `HttpReadTimeout` around `inner`.
        ///
        /// Typically, `I` will implement [`tower::Service<http::Request<SdkBody>>`].
        pub(crate) fn new(inner: I, sleep: SharedAsyncSleep, timeout: Duration) -> Self {
            Self {
                inner,
                timeout: Some((sleep, timeout)),
            }
        }

        pub(crate) fn no_timeout(inner: I) -> Self {
            Self {
                inner,
                timeout: None,
            }
        }
    }

    pin_project! {
        /// Timeout future for Tower services
        ///
        /// Timeout future to handle timing out, mapping errors, and the possibility of not timing out
        /// without incurring an additional allocation for each timeout layer.
        #[project = MaybeTimeoutFutureProj]
        pub enum MaybeTimeoutFuture<F> {
            Timeout {
                #[pin]
                timeout: Timeout<F, Sleep>,
                error_type: &'static str,
                duration: Duration,
            },
            NoTimeout {
                #[pin]
                future: F
            }
        }
    }

    impl<F, T, E> Future for MaybeTimeoutFuture<F>
    where
        F: Future<Output = Result<T, E>>,
        E: Into<BoxError>,
    {
        type Output = Result<T, BoxError>;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let (timeout_future, kind, &mut duration) = match self.project() {
                MaybeTimeoutFutureProj::NoTimeout { future } => {
                    return future.poll(cx).map_err(|err| err.into());
                }
                MaybeTimeoutFutureProj::Timeout {
                    timeout,
                    error_type,
                    duration,
                } => (timeout, error_type, duration),
            };
            match timeout_future.poll(cx) {
                Poll::Ready(Ok(response)) => Poll::Ready(response.map_err(|err| err.into())),
                Poll::Ready(Err(_timeout)) => {
                    Poll::Ready(Err(HttpTimeoutError { kind, duration }.into()))
                }
                Poll::Pending => Poll::Pending,
            }
        }
    }

    impl<I> tower::Service<Uri> for ConnectTimeout<I>
    where
        I: tower::Service<Uri>,
        I::Error: Into<BoxError>,
    {
        type Response = I::Response;
        type Error = BoxError;
        type Future = MaybeTimeoutFuture<I::Future>;

        fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            self.inner.poll_ready(cx).map_err(|err| err.into())
        }

        fn call(&mut self, req: Uri) -> Self::Future {
            match &self.timeout {
                Some((sleep, duration)) => {
                    let sleep = sleep.sleep(*duration);
                    MaybeTimeoutFuture::Timeout {
                        timeout: Timeout::new(self.inner.call(req), sleep),
                        error_type: "HTTP connect",
                        duration: *duration,
                    }
                }
                None => MaybeTimeoutFuture::NoTimeout {
                    future: self.inner.call(req),
                },
            }
        }
    }

    impl<I, B> tower::Service<http::Request<B>> for HttpReadTimeout<I>
    where
        I: tower::Service<http::Request<B>>,
        I::Error: Send + Sync + Error + 'static,
    {
        type Response = I::Response;
        type Error = BoxError;
        type Future = MaybeTimeoutFuture<I::Future>;

        fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            self.inner.poll_ready(cx).map_err(|err| err.into())
        }

        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            match &self.timeout {
                Some((sleep, duration)) => {
                    let sleep = sleep.sleep(*duration);
                    MaybeTimeoutFuture::Timeout {
                        timeout: Timeout::new(self.inner.call(req), sleep),
                        error_type: "HTTP read",
                        duration: *duration,
                    }
                }
                None => MaybeTimeoutFuture::NoTimeout {
                    future: self.inner.call(req),
                },
            }
        }
    }
}
