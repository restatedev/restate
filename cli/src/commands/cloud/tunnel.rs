// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::io::Write;
use std::pin::Pin;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};
use std::time::Duration;

use anyhow::Result;
use arc_swap::ArcSwapOption;
use cling::prelude::*;
use comfy_table::Table;
use crossterm::cursor::{MoveTo, MoveToNextLine};
use crossterm::queue;
use crossterm::terminal::{BeginSynchronizedUpdate, Clear, ClearType};
use futures::{FutureExt, StreamExt};
use http::{Request, StatusCode, Uri};
use http_body::Body;
use hyper::client::HttpConnector;
use hyper::service::Service;
use hyper::Response;
use hyper_rustls::HttpsConnector;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use url::Url;

use restate_types::retries::RetryPolicy;

use crate::app::UiConfig;
use crate::ui::console::StyledTable;
use crate::ui::output::Console;
use crate::{build_info, c_indent_table, c_println, c_tip, c_warn, cli_env::CliEnv};

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_tunnel")]
#[clap(visible_alias = "expose", visible_alias = "tun")]
pub struct Tunnel {
    /// The port to forward to the Cloud environment
    #[clap(default_value_t = 9080)]
    port: u16,

    /// If reattaching to a previous tunnel session, the tunnel server to connect to
    #[clap(long = "tunnel-url")]
    tunnel_url: Option<String>,

    /// A name for the tunnel; a random name will be generated if not provided
    #[clap(long = "tunnel-name")]
    tunnel_name: Option<String>,
}

pub async fn run_tunnel(State(env): State<CliEnv>, opts: &Tunnel) -> Result<()> {
    let bearer_token = if let Some(bearer_token) = &env.config.bearer_token {
        // the user may have specifically set an api token
        bearer_token.clone()
    } else if let Some(cloud_credentials) = &env.config.cloud.credentials {
        cloud_credentials.access_token()?.to_string()
    } else {
        return Err(anyhow::anyhow!(
            "Restate Cloud credentials have not been provided; first run `restate cloud login`"
        ));
    };

    let client = reqwest::Client::builder()
        .user_agent(format!(
            "{}/{} {}-{}",
            env!("CARGO_PKG_NAME"),
            build_info::RESTATE_CLI_VERSION,
            std::env::consts::OS,
            std::env::consts::ARCH,
        ))
        .connect_timeout(env.connect_timeout)
        .http2_prior_knowledge()
        .build()?;

    let mut http_connector = HttpConnector::new();
    http_connector.set_nodelay(true);
    http_connector.set_connect_timeout(Some(env.connect_timeout));
    http_connector.enforce_http(false);

    let https_connector = hyper_rustls::HttpsConnectorBuilder::new()
        .with_native_roots()
        .https_or_http()
        .enable_http2()
        .wrap_connector(http_connector);

    let tunnel_name = opts
        .tunnel_name
        .clone()
        .unwrap_or(base62::encode(ulid::Ulid::new()));

    let url = Url::parse(&format!("http://localhost:{}", opts.port)).unwrap();

    let cancellation = CancellationToken::new();

    // prevent ctrl-c from clearing the screen, instead just cancel
    {
        let cancellation = cancellation.clone();
        let boxed: Box<dyn Fn() + Send> = Box::new(move || cancellation.cancel());
        *crate::EXIT_HANDLER.lock().unwrap() = Some(boxed);
    }

    let retry_policy = RetryPolicy::exponential(
        Duration::from_millis(10),
        2.0,
        None,
        Some(Duration::from_secs(12)),
    );

    // use a closure as impl trait associated types are not yet allowed
    let proxy = |inner, request| async move { proxy(inner, request).await };

    let handler = Handler {
        status: HandlerStatus::AwaitingStart,
        https_connector,
        inner: Arc::new(HandlerInner {
            tunnel_name,
            tunnel_renderer: OnceLock::new(),
            bearer_token,
            client,
            url,
            port: opts.port,
            ui_config: env.ui_config.clone(),
        }),
        proxy,
    };

    let default_tunnel_url = opts
        .tunnel_url
        .as_deref()
        .unwrap_or(env.config.cloud.tunnel_base_url.as_str());

    let retry_fut = retry_policy.retry_if(
        || {
            let tunnel_url = handler
                .inner
                .tunnel_renderer
                .get()
                .map(|r: &TunnelRenderer| r.tunnel_url.as_str())
                .unwrap_or(default_tunnel_url)
                .parse()
                .unwrap();

            let handler = handler.clone();

            async move { handler.serve(tunnel_url).await }
        },
        |err: &ServeError| {
            if !err.is_retryable() {
                return false;
            }

            let tunnel_renderer = if let Some(tunnel_renderer) = handler.inner.tunnel_renderer.get()
            {
                tunnel_renderer
            } else {
                // no point retrying if we've never had a success; leave that up to the user
                return false;
            };

            let err = match err.source() {
                Some(source) => format!("{err}, retrying\n  Caused by: {source}"),
                None => format!("{err}, retrying"),
            };
            tunnel_renderer.last_error.store(Some(Arc::new(err)));

            true
        },
    );

    let mut rerender = tokio::time::interval(Duration::from_millis(250));

    let res = {
        tokio::pin!(retry_fut);

        loop {
            tokio::select! {
                res = &mut retry_fut => break res,
                _ = cancellation.cancelled() => break Err(ServeError::ControlC),
                _ = rerender.tick() => {
                    if let Some(tunnel_renderer) = handler.inner.tunnel_renderer.get() {
                        tunnel_renderer.render()
                    }
                },
            }
        }
    };

    if let Some(renderer) =
        Arc::into_inner(handler.into_inner()).and_then(|r| r.tunnel_renderer.into_inner())
    {
        // dropping the renderer will exit the alt screen
        let (tunnel_url, tunnel_name) = renderer.into_tunnel_details();
        eprintln!("To retry with the same endpoint: restate cloud tunnel --tunnel-url {tunnel_url} --tunnel-name {tunnel_name}");
    };

    match res {
        Err(ServeError::ControlC) => {
            eprintln!("Exiting as Ctrl-C pressed");
            Ok(())
        }
        Ok(()) => Ok(()),
        Err(err) => Err(err.into()),
    }
}

struct Handler<Proxy> {
    status: HandlerStatus,
    https_connector: HttpsConnector<HttpConnector>,
    inner: Arc<HandlerInner>,
    proxy: Proxy,
}

impl<Proxy> Handler<Proxy> {
    fn into_inner(self) -> Arc<HandlerInner> {
        self.inner
    }
}

impl<Proxy: Clone> Clone for Handler<Proxy> {
    fn clone(&self) -> Self {
        Self {
            status: HandlerStatus::AwaitingStart,
            https_connector: self.https_connector.clone(),
            inner: self.inner.clone(),
            proxy: self.proxy.clone(),
        }
    }
}

struct HandlerInner {
    ui_config: UiConfig,
    tunnel_name: String,
    tunnel_renderer: OnceLock<TunnelRenderer>,
    bearer_token: String,
    client: reqwest::Client,
    url: Url,
    port: u16,
}

#[derive(Clone, Debug, thiserror::Error)]
enum StartError {
    #[error("Missing trailers")]
    MissingTrailers,
    #[error("Problem reading http2 stream")]
    Read,
    #[error("Bad status: {0}")]
    BadStatus(String),
    #[error("Missing status")]
    MissingStatus,
    #[error("Missing proxy URL")]
    MissingProxyURL,
    #[error("Missing tunnel URL")]
    MissingTunnelURL,
}

#[derive(Debug, thiserror::Error)]
enum ServeError {
    #[error("Failed to initialise tunnel")]
    StartError(#[from] StartError),
    #[error("Failed to serve over tunnel")]
    Hyper(#[source] hyper::Error),
    #[error("Ctrl-C pressed")]
    ControlC,
    #[error("Failed to connect to tunnel server")]
    Connection(#[source] Box<dyn Error + Send + Sync>),
    #[error("Server closed connection while {0}")]
    ServerClosed(String),
}

impl ServeError {
    fn is_retryable(&self) -> bool {
        match self {
            Self::StartError(_) => false,
            Self::Hyper(_) => false,
            Self::ControlC => false,
            Self::Connection(_) => true,
            Self::ServerClosed(_) => true,
        }
    }
}

impl From<hyper::Error> for ServeError {
    fn from(err: hyper::Error) -> Self {
        if let Some(err) = err
            .source()
            .and_then(|err| err.downcast_ref::<StartError>())
        {
            // this can happen when ProcessingStart future returns an error to poll_ready
            Self::StartError(err.clone())
        } else {
            // generic hyper serving error; not sure how to hit this
            Self::Hyper(err)
        }
    }
}

enum HandlerStatus {
    AwaitingStart,
    ProcessingStart(Pin<Box<dyn Future<Output = Result<(), StartError>> + Send + Sync>>),
    Proxying,
}

impl Display for HandlerStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            HandlerStatus::AwaitingStart => write!(f, "awaiting start"),
            HandlerStatus::ProcessingStart(_) => write!(f, "processing start"),
            HandlerStatus::Proxying => write!(f, "proxying"),
        }
    }
}

impl<Proxy, ProxyFut> Handler<Proxy>
where
    Proxy: FnMut(Arc<HandlerInner>, reqwest::Request) -> ProxyFut,
    ProxyFut: Future<Output = Result<Response<hyper::Body>, StartError>> + Send + 'static,
{
    async fn serve(mut self, tunnel_url: Uri) -> Result<(), ServeError> {
        let io = self
            .https_connector
            .call(tunnel_url)
            .await
            .map_err(ServeError::Connection)?;

        hyper::server::conn::Http::new()
            .serve_connection(io, &mut self)
            .await?;

        // there is a race where the server closes the connection before we process the trailers, leading
        // us to not observe a permanent StartError error like unauthorized. so, we should here process the future
        // to completion
        if let HandlerStatus::ProcessingStart(fut) = &mut self.status {
            fut.await?;
        }

        Err(ServeError::ServerClosed(self.status.to_string()))
    }
}

impl<Proxy, ProxyFut> Service<Request<hyper::Body>> for Handler<Proxy>
where
    Proxy: FnMut(Arc<HandlerInner>, reqwest::Request) -> ProxyFut,
    ProxyFut: Future<Output = Result<Response<hyper::Body>, StartError>>,
{
    type Response = Response<hyper::Body>;
    type Error = StartError;
    type Future = futures::future::Either<
        futures::future::Ready<Result<Self::Response, Self::Error>>,
        ProxyFut,
    >;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match &mut self.status {
            HandlerStatus::AwaitingStart | HandlerStatus::Proxying { .. } => Poll::Ready(Ok(())),
            HandlerStatus::ProcessingStart(fut) => match fut.poll_unpin(cx) {
                Poll::Ready(Ok(())) => {
                    self.status = HandlerStatus::Proxying;
                    Poll::Ready(Ok(()))
                }
                Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
                Poll::Pending => Poll::Pending,
            },
        }
    }

    fn call(&mut self, req: Request<hyper::Body>) -> Self::Future {
        match &self.status {
            HandlerStatus::AwaitingStart => {
                let body: hyper::Body = req.into_body();

                self.status = HandlerStatus::ProcessingStart(Box::pin(process_start(
                    self.inner.clone(),
                    body,
                )));

                futures::future::ready(Ok(Response::builder()
                    .header(
                        "authorization",
                        format!("Bearer {}", self.inner.bearer_token),
                    )
                    .header("tunnel-name", &self.inner.tunnel_name)
                    .body(hyper::Body::empty())
                    .unwrap()))
                .left_future()
            }
            HandlerStatus::ProcessingStart(_) => {
                // 'Implementations are permitted to panic if call is invoked without obtaining Poll::Ready(Ok(())) from poll_ready.'
                panic!("Called when not ready")
            }
            HandlerStatus::Proxying => {
                let url = if let Some(path) = req.uri().path_and_query() {
                    self.inner.url.join(path.as_str()).unwrap()
                } else {
                    self.inner.url.clone()
                };

                info!("Proxying request to {}", url);

                let (head, body) = req.into_parts();

                let request = self
                    .inner
                    .client
                    .request(head.method, url)
                    .body(body)
                    .headers(head.headers)
                    .build()
                    .expect("Failed to build request");

                (self.proxy)(self.inner.clone(), request).right_future()
            }
        }
    }
}

async fn process_start(inner: Arc<HandlerInner>, body: hyper::Body) -> Result<(), StartError> {
    let collected = Body::collect(body).await;
    let trailers = match collected {
        Ok(ref collected) if collected.trailers().is_some() => collected.trailers().unwrap(),
        Ok(_) => {
            return Err(StartError::MissingTrailers);
        }
        Err(_) => {
            return Err(StartError::Read);
        }
    };

    match trailers.get("tunnel-status").and_then(|s| s.to_str().ok()) {
        Some("ok") => {}
        Some(other) => {
            return Err(StartError::BadStatus(other.into()));
        }
        None => {
            return Err(StartError::MissingStatus);
        }
    }

    let proxy_url = match trailers.get("proxy-url").and_then(|s| s.to_str().ok()) {
        Some(url) => url,
        None => {
            return Err(StartError::MissingProxyURL);
        }
    };

    let tunnel_url = match trailers.get("tunnel-url").and_then(|s| s.to_str().ok()) {
        Some(url) => url,
        None => {
            return Err(StartError::MissingTunnelURL);
        }
    };

    let tunnel_renderer = inner.tunnel_renderer.get_or_init(|| {
        TunnelRenderer::new(
            inner.ui_config.clone(),
            proxy_url.into(),
            tunnel_url.into(),
            inner.tunnel_name.clone(),
            inner.port,
        )
        .unwrap()
    });

    tunnel_renderer.last_error.store(None);
    tunnel_renderer.render();

    Ok(())
}

pub struct TunnelRenderer {
    ui_config: UiConfig,
    tunnel_url: String,
    proxy_url: String,
    tunnel_name: String,
    port: u16,
    in_flight_requests: AtomicU16,
    total_requests: AtomicU16,
    last_error: ArcSwapOption<String>,
}

impl TunnelRenderer {
    fn new(
        ui_config: UiConfig,
        proxy_url: String,
        tunnel_url: String,
        tunnel_name: String,
        port: u16,
    ) -> std::io::Result<Self> {
        // Redirect console output to in-memory buffer
        let console = Console::in_memory();
        crate::ui::output::set_stdout(console.clone());
        crate::ui::output::set_stderr(console);

        queue!(
            std::io::stdout(),
            crossterm::cursor::SavePosition,
            crossterm::terminal::EnterAlternateScreen,
            crossterm::terminal::DisableLineWrap,
            crossterm::cursor::Hide
        )?;
        Ok(Self {
            ui_config,
            proxy_url,
            tunnel_url,
            tunnel_name,
            port,
            in_flight_requests: AtomicU16::new(0),
            total_requests: AtomicU16::new(0),
            last_error: ArcSwapOption::empty(),
        })
    }

    fn decrement_requests(&self) {
        self.in_flight_requests.fetch_sub(1, Ordering::Relaxed);
    }

    fn into_tunnel_details(mut self) -> (String, String) {
        (
            std::mem::take(&mut self.tunnel_url),
            std::mem::take(&mut self.tunnel_name),
        )
    }

    fn render(&self) {
        let mut stdout = std::io::stdout();

        let (cols, rows) = if let Ok(size) = crossterm::terminal::size() {
            size
        } else {
            return;
        };

        if rows > 37 {
            c_println!(
                "{}",
                restate_types::art::render_restate_logo(crate::console::colors_enabled())
            );
        }

        let mut tunnel_table = Table::new_styled(&self.ui_config);
        tunnel_table.set_width(cols - 4);
        tunnel_table.add_kv_row("Endpoint", &self.proxy_url);
        tunnel_table.add_kv_row("Local Port", self.port);
        tunnel_table.add_kv_row(
            "In Flight Requests",
            self.in_flight_requests.load(Ordering::Relaxed),
        );
        tunnel_table.add_kv_row(
            "Total Requests",
            self.total_requests.load(Ordering::Relaxed),
        );

        c_indent_table!(2, tunnel_table);
        c_println!();

        if let Some(last_error) = self.last_error.load().as_deref() {
            c_warn!("Error: {last_error}")
        }

        c_tip!(
            "To discover:\nrestate dp add {}\nThe endpoint is only reachable from Restate Cloud.",
            self.proxy_url
        );

        let mut lock = std::io::stdout().lock();

        let _ = queue!(lock, MoveTo(0, 0), BeginSynchronizedUpdate,);

        let _ = write!(lock, "restate cloud tunnel - Press Ctrl-C to exit");
        let _ = queue!(lock, Clear(ClearType::UntilNewLine), MoveToNextLine(1));
        let _ = queue!(lock, Clear(ClearType::UntilNewLine), MoveToNextLine(1));

        if let Some(b) = crate::ui::output::stdout().take_buffer() {
            // truncate to fit the screen
            for line in b.lines() {
                if let Ok((_, cur_row)) = crossterm::cursor::position() {
                    if cur_row >= rows - 2 {
                        // enough printing...
                        let _ = write!(lock, "(output truncated to fit screen)");
                        let _ = queue!(lock, Clear(ClearType::UntilNewLine), MoveToNextLine(1));
                        break;
                    }
                }
                let _ = write!(lock, "{}", line);
                let _ = queue!(lock, Clear(ClearType::UntilNewLine), MoveToNextLine(1));
            }
        }

        let _ = queue!(
            stdout,
            crossterm::terminal::EndSynchronizedUpdate,
            Clear(ClearType::FromCursorDown)
        );
    }
}

impl Drop for TunnelRenderer {
    fn drop(&mut self) {
        queue!(
            std::io::stdout(),
            crossterm::terminal::LeaveAlternateScreen,
            crossterm::cursor::Show,
            crossterm::terminal::EnableLineWrap,
            crossterm::cursor::RestorePosition,
            crossterm::cursor::MoveToPreviousLine(1),
        )
        .unwrap();
        crate::ui::output::set_stdout(Console::stdout());
        crate::ui::output::set_stderr(Console::stderr());
        println!();
    }
}

impl HandlerInner {
    fn increment_requests<E: Error>(self: &Arc<Self>) -> RequestGuard<E> {
        if let Some(r) = self.tunnel_renderer.get() {
            r.in_flight_requests.fetch_add(1, Ordering::Relaxed);
            r.total_requests.fetch_add(1, Ordering::Relaxed);
            RequestGuard(Some(Arc::downgrade(self)), None)
        } else {
            RequestGuard(None, None)
        }
    }
}

#[must_use]
struct RequestGuard<E: Error>(Option<std::sync::Weak<HandlerInner>>, Option<E>);

impl<E: Error> RequestGuard<E> {
    fn with_error(mut self, error: E) -> Self {
        let _ = self.1.insert(error);
        self
    }
}

impl<E: Error> Drop for RequestGuard<E> {
    fn drop(&mut self) {
        let RequestGuard(inner, error) = self;
        if let Some(inner) = &inner {
            if let Some(inner) = inner.upgrade() {
                if let Some(r) = inner.tunnel_renderer.get() {
                    r.decrement_requests();
                    r.last_error
                        .store(error.take().map(|error| error.to_string()).map(Arc::new))
                }
            }
        }
    }
}

async fn proxy(
    inner: Arc<HandlerInner>,
    request: reqwest::Request,
) -> Result<Response<hyper::Body>, StartError> {
    let guard = inner.increment_requests();
    let mut result = match inner.client.execute(request).await {
        Ok(result) => result,
        Err(err) => {
            error!("Failed to proxy request: {}", err);

            let _guard = guard.with_error(err);

            return Ok(Response::builder()
                .status(StatusCode::BAD_GATEWAY)
                .body(hyper::Body::empty())
                .unwrap());
        }
    };
    info!("Request proxied with status {}", result.status());

    let mut response = Response::builder().status(result.status());
    if let Some(headers) = response.headers_mut() {
        std::mem::swap(headers, result.headers_mut())
    };

    Ok(response
        .body(hyper::Body::wrap_stream(
            result.bytes_stream().chain(
                futures::stream::once(async move {
                    // decrement once the response body is fully read
                    drop(guard);
                    futures::stream::empty()
                })
                .flatten(),
            ),
        ))
        .unwrap())
}
