// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::pin::Pin;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use futures::channel::oneshot;
use futures::future::{BoxFuture, poll_fn};
use futures::never::Never;
use futures::{FutureExt, ready};
use h2::client::{ResponseFuture as H2ResponseFuture, SendRequest};
use h2::{Reason, RecvStream, SendStream};
use http::{HeaderMap, Request, Response, Uri};
use http_body::{Body, Frame};
use http_body_util::BodyExt;
use parking_lot::Mutex;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::sync::{CancellationToken, DropGuard};
use tower::Service;
use tracing::debug;

use restate_types::errors::GenericError;

use super::Error;
use crate::pool::conn::concurrency::{Concurrency, Permit, PermitFuture};

mod concurrency;

fn next_connection_id() -> usize {
    static CONNECTION_ID: AtomicUsize = AtomicUsize::new(0);
    CONNECTION_ID.fetch_add(1, Ordering::Relaxed)
}

#[derive(thiserror::Error, Debug)]
pub enum ConnectionError<R> {
    #[error(transparent)]
    Error(#[from] Error),
    /// The connection's concurrency limit was reduced and this request's permit
    /// was reclaimed before the request could be sent. The caller should retry
    /// on a different connection. The original request is returned inside.
    #[error("permit to use the connection was reclaimed")]
    PermitReclaimed(R),
}

const STATE_NEW: u8 = 0;
const STATE_CONNECTING: u8 = 1;
const STATE_CONNECTED: u8 = 2;
const STATE_CLOSED: u8 = 3;

/// The H2 handle obtained after a successful handshake. Set exactly once.
struct H2Handle {
    send_request: SendRequest<Bytes>,
    cancel: CancellationToken,
}

/// Lock-free shared state for an H2 connection.
///
/// State transitions: `New → Connecting → Connected → Closed`.
/// The `state` field tracks the discriminant atomically. The `h2` handle is set
/// once via `OnceLock` when transitioning to `Connected`. Only the waiter list
/// requires a brief lock during the `Connecting` phase.
struct ConnectionShared {
    id: usize,
    config: ConnectionConfig,
    concurrency: Concurrency,
    state: AtomicU8,
    h2: OnceLock<H2Handle>,
    /// Waiters registered during the Connecting phase. Narrowly-scoped lock.
    /// This is an Option<T> to mark waiters list as invalid
    /// (not in CONNECTING state anymore) and it's not possible
    /// to add more waiters to the list
    waiters: Mutex<Option<Vec<oneshot::Sender<()>>>>,
}

impl ConnectionShared {
    fn new(config: ConnectionConfig) -> Self {
        let concurrency = Concurrency::new(
            config
                .streams_per_connection_limit
                .min(config.initial_max_send_streams as usize),
        );

        Self {
            id: next_connection_id(),
            config,
            concurrency,
            state: AtomicU8::new(STATE_NEW),
            h2: OnceLock::new(),
            waiters: Mutex::new(Some(Vec::new())),
        }
    }

    /// Mark the connection as closed and wake any pending waiters.
    fn close(&self) {
        self.state.store(STATE_CLOSED, Ordering::Relaxed);
        if let Some(h2) = self.h2.get() {
            h2.cancel.cancel();
        }
        // Drop all waiter senders so receivers get Err (Cancelled)
        self.waiters.lock().take();
    }
}

impl Drop for ConnectionShared {
    fn drop(&mut self) {
        self.close();
    }
}

#[derive(Clone, Copy, derive_builder::Builder)]
#[builder(pattern = "owned", default)]
pub struct ConnectionConfig {
    initial_max_send_streams: u32,
    // upper bound applied to the peer's advertised max-concurrent-streams
    streams_per_connection_limit: usize,
    keep_alive_timeout: Duration,
    keep_alive_interval: Option<Duration>,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            initial_max_send_streams: 50,
            streams_per_connection_limit: 128,
            keep_alive_timeout: Duration::from_secs(20),
            keep_alive_interval: None,
        }
    }
}

/// A lazily-initialized, multiplexed HTTP/2 connection.
///
/// `Connection` wraps a connector `C` (a Tower [`Service`] that produces an async I/O stream)
/// and lazily performs the H2 handshake on the first request. Subsequent requests reuse the
/// same underlying H2 connection.
///
/// Concurrency is bounded by a semaphore that limits the number of in-flight H2 streams
/// (configured via `init_max_streams`, which sets both the semaphore and
/// `h2::client::Builder::initial_max_send_streams`). Callers must call
/// [`poll_ready`](Self::poll_ready) (or [`ready`](Self::ready)) before each
/// [`request`](Self::request) to acquire a stream permit.
///
/// Cloning a `Connection` shares the underlying H2 session; the clone starts without a
/// permit or in-progress acquire future.
pub struct Connection<C> {
    connector: C,
    /// Lock-free shared connection state.
    shared: Arc<ConnectionShared>,
    /// Permit acquired via [`poll_ready`](Self::poll_ready), consumed by [`request`](Self::request).
    permit: Option<Permit>,
    /// In-progress semaphore acquire, if any.
    acquire: Option<Pin<Box<PermitFuture>>>,
}

impl<C> Clone for Connection<C>
where
    C: Clone,
{
    fn clone(&self) -> Self {
        Self {
            connector: self.connector.clone(),
            shared: Arc::clone(&self.shared),
            permit: None,
            acquire: None,
        }
    }
}

impl<C> Connection<C> {
    /// Returns the number of currently in-flight H2 streams on this connection.
    pub(crate) fn inflight(&self) -> usize {
        self.shared.concurrency.acquired()
    }
}

impl<C> Connection<C>
where
    C: Service<Uri>,
    C::Response: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
    C::Future: Send + 'static,
    C::Error: Into<Error>,
{
    pub fn new(connector: C, config: ConnectionConfig) -> Self {
        Self {
            connector,
            shared: Arc::new(ConnectionShared::new(config)),
            permit: None,
            acquire: None,
        }
    }

    /// Returns a unique connection id.
    pub fn id(&self) -> usize {
        self.shared.id
    }

    pub async fn ready(&mut self) -> Result<(), Error> {
        poll_fn(|cx| self.poll_ready(cx)).await
    }

    #[cfg(test)]
    pub fn try_ready(&mut self) -> Option<Result<(), Error>> {
        use std::task::Waker;
        match self.poll_ready(&mut Context::from_waker(Waker::noop())) {
            Poll::Pending => {
                // drop the acquire future since it will never be polled again
                // to clear up Semaphore resources
                self.acquire = None;
                self.permit = None;
                None
            }
            Poll::Ready(result) => Some(result),
        }
    }

    /// Return the number of the available streams on this connection.
    ///
    /// This does not guarantee that poll_ready(), try_ready(), or ready()
    /// will succeed. It can only be used to get an estimate of how many
    /// h2 streams are available
    pub fn available_streams(&self) -> usize {
        self.shared.concurrency.available()
    }

    pub fn max_concurrent_streams(&self) -> usize {
        self.shared.concurrency.size()
    }

    /// Returns `true` if the connection has been closed or encountered a fatal error.
    pub fn is_closed(&self) -> bool {
        self.shared.state.load(Ordering::Relaxed) == STATE_CLOSED
    }

    /// Must be polled before each request. This makes sure we acquire the permit
    /// to open a new h2 stream.
    /// This should return immediately if connection has enough permits. Otherwise
    /// it will return Pending.
    ///
    /// If you want to wait on the connection to be ready, use `ready()` instead.
    pub fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        match self.shared.state.load(Ordering::Acquire) {
            STATE_NEW => {
                if let Err(err) = ready!(self.connector.poll_ready(cx)).map_err(Into::into) {
                    // immediately switch to closed state
                    self.shared.close();
                    return Poll::Ready(Err(err));
                }
            }
            STATE_CLOSED => {
                return Poll::Ready(Err(Error::Closed));
            }
            STATE_CONNECTED => {
                let h2 = self.shared.h2.get().expect("h2 must be set in Connected");
                if h2.cancel.is_cancelled() {
                    // make the closed state explicit
                    self.shared.close();
                    return Poll::Ready(Err(Error::Closed));
                }

                // this is a good synchronization point to update the permits
                // to the last known size known by the send_request object.
                self.shared.concurrency.resize(
                    self.shared
                        .config
                        .streams_per_connection_limit
                        .min(h2.send_request.current_max_send_streams()),
                );
            }
            STATE_CONNECTING => {}
            _ => unreachable!(),
        }

        if self.permit.is_some() {
            return Poll::Ready(Ok(()));
        }

        if self.acquire.is_none() {
            self.acquire = Some(Box::pin(self.shared.concurrency.acquire()));
        }

        let acquire = self.acquire.as_mut().unwrap();

        self.permit = Some(ready!(acquire.poll_unpin(cx)));
        self.acquire = None;

        Poll::Ready(Ok(()))
    }

    /// Sends an HTTP request over the shared H2 connection.
    ///
    /// # Panics
    /// Panics if called without a prior successful [`poll_ready`](Self::poll_ready) call.
    pub fn request<B>(&mut self, request: http::Request<B>) -> ResponseFuture<B>
    where
        B: Body<Data = Bytes> + Send + 'static,
        B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        // we should already have a permit.
        let permit = self.permit.take().expect("poll_ready() was called before");

        let state = match self.shared.state.load(Ordering::Acquire) {
            STATE_CLOSED => ResponseFutureState::error(Error::Closed),
            STATE_NEW => {
                // CAS New → Connecting. Only one request wins and drives the handshake.
                match self.shared.state.compare_exchange(
                    STATE_NEW,
                    STATE_CONNECTING,
                    Ordering::Relaxed,
                    Ordering::Acquire,
                ) {
                    Ok(_) => self.drive_handshake(&request),
                    Err(current) => self.handle_state(current),
                }
            }
            other => self.handle_state(other),
        };

        ResponseFuture {
            permit: Some(permit),
            shared: Arc::clone(&self.shared),
            request: Some(request),
            state,
        }
    }

    /// Build the state for the request that lost the New → Connecting race
    /// or arrived after the transition.
    fn handle_state(&self, mut state: u8) -> ResponseFutureState {
        loop {
            match state {
                STATE_CONNECTING => {
                    let (tx, rx) = oneshot::channel();
                    match &mut *self.shared.waiters.lock() {
                        // If `waiters` is None, it means that the connection
                        // state has changed to either connected, or closed.
                        // In that case we need to re-load the state and check again
                        None => {
                            state = self.shared.state.load(Ordering::Acquire);
                            assert!(state != STATE_CONNECTING);
                        }
                        Some(waiters) => {
                            waiters.push(tx);
                            return ResponseFutureState::WaitingConnection { rx };
                        }
                    }
                }
                STATE_CONNECTED => {
                    return ResponseFutureState::PreFlight {
                        send_request: self
                            .shared
                            .h2
                            .get()
                            .expect("h2 must be set in Connected")
                            .send_request
                            .clone(),
                    };
                }
                STATE_CLOSED => return ResponseFutureState::error(Error::Closed),
                _ => unreachable!(),
            }
        }
    }

    /// Create the driving future that performs the H2 handshake.
    fn drive_handshake<B>(&mut self, request: &http::Request<B>) -> ResponseFutureState {
        let shared = Arc::clone(&self.shared);
        let connect = self.connector.call(request.uri().clone());
        ResponseFutureState::drive(async move {
            let stream = connect.await.map_err(Into::into)?;

            let (send_request, mut connection) = h2::client::Builder::new()
                .initial_max_send_streams(shared.config.initial_max_send_streams as usize)
                .handshake::<_, Bytes>(stream)
                .await?;

            let ping_pong = connection.ping_pong().expect("to succeed on first call");
            let cancel = CancellationToken::new();
            let cancellation = cancel.clone().drop_guard();

            tokio::task::Builder::new()
                .name("h2:connection")
                .spawn(async move {
                    let mut connection = std::pin::pin!(connection);
                    let mut keep_alive = std::pin::pin!(Self::keep_alive(ping_pong, shared.config));

                    let shared_weak = Arc::downgrade(&shared);
                    drop(shared);

                    tokio::select! {
                        result = &mut connection => match result {
                            Ok(_) => {
                                debug!("h2 connection shutdown");
                            },
                            Err(err) => {
                                debug!("h2 connection shutdown with error: {err}");
                            }
                        },
                        Err(err) = &mut keep_alive => {
                            debug!("h2 connection keep-alive error: {err}");
                        }
                        _ = cancel.cancelled() => {
                            debug!("h2 connection cancelled");
                        }
                    };

                    // set state to closed
                    if let Some(shared) = shared_weak.upgrade() {
                        shared.close();
                    }
                })
                .unwrap();

            Ok((send_request, cancellation))
        })
    }

    async fn keep_alive(
        mut ping_pong: h2::PingPong,
        config: ConnectionConfig,
    ) -> Result<Never, Error> {
        let keep_alive_interval = match config.keep_alive_interval {
            None => {
                return futures::future::pending().await;
            }
            Some(interval) => interval,
        };

        let mut interval = tokio::time::interval(keep_alive_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            interval.tick().await;

            match tokio::time::timeout(
                config.keep_alive_timeout,
                ping_pong.ping(h2::Ping::opaque()),
            )
            .await
            {
                Ok(Ok(_)) => {}
                Ok(Err(err)) => return Err(err.into()),
                Err(_) => {
                    return Err(Error::KeepAliveTimeout);
                }
            }
        }
    }
}

/// Internal state machine for a single in-flight request on a [`Connection`].
///
/// Each variant represents a phase of the request lifecycle:
/// - **Driving** – this request is driving the initial H2 handshake.
/// - **WaitingConnection** – another request is driving the handshake; we wait for notification.
/// - **PreFlight** – we have a `SendRequest` handle and are waiting for H2 stream capacity.
/// - **InFlight** – the request has been sent; we are waiting for the response.
/// - **Error** – a terminal error was captured for the caller to consume.
enum ResponseFutureState {
    Driving {
        fut: BoxFuture<'static, Result<(SendRequest<Bytes>, DropGuard), Error>>,
    },
    WaitingConnection {
        rx: oneshot::Receiver<()>,
    },
    PreFlight {
        send_request: SendRequest<Bytes>,
    },
    InFlight {
        fut: H2ResponseFuture,
    },
    Error {
        err: Option<Error>,
    },
}

impl ResponseFutureState {
    fn error(err: impl Into<Error>) -> Self {
        Self::Error {
            err: Some(err.into()),
        }
    }

    fn drive<F>(fut: F) -> Self
    where
        F: Future<Output = Result<(SendRequest<Bytes>, DropGuard), Error>> + Send + 'static,
    {
        Self::Driving { fut: Box::pin(fut) }
    }
}

/// Future returned by [`Connection::request`].
///
/// Drives the request through its [`RequestFutureState`] state machine until a response
/// is received. Holds a semaphore permit for the duration of the request to bound
/// concurrent H2 streams.
///
/// On drop, if this future was responsible for driving the H2 handshake (i.e. the
/// connection is still in `Connecting` state), the connection is moved to `Closed` to
/// prevent waiters from hanging indefinitely.
pub struct ResponseFuture<B> {
    permit: Option<Permit>,
    shared: Arc<ConnectionShared>,
    request: Option<http::Request<B>>,
    state: ResponseFutureState,
}

impl<B> Drop for ResponseFuture<B> {
    fn drop(&mut self) {
        // if the driving future was dropped (while in Connecting state), we need to
        // immediately switch connection to closed to make sure
        // waiters are immediately notified otherwise they will be stuck forever
        if let ResponseFutureState::Driving { .. } = &self.state {
            self.shared.close();
        }
    }
}

impl<B> Future for ResponseFuture<B>
where
    B: Body<Data = Bytes> + Unpin + Send + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send,
{
    type Output = Result<Response<PermittedRecvStream>, ConnectionError<http::Request<B>>>;

    // Error handling strategy for h2 errors:
    //
    // Errors are classified as either "connection-level" or "stream-level" based on
    // when they occur in the request lifecycle:
    //
    // **Connection-level** (close the entire connection via `shared.close()`):
    //   - Errors during `Driving` (handshake failures).
    //   - Errors from `send_request.poll_ready()` in `PreFlight`.
    //   These go through the `Error` state which cancels the h2 handle and marks
    //   the connection as closed.
    //
    // **Stream-level** (returned only to the individual caller):
    //   - Errors from `send_request.send_request()` in `PreFlight`.
    //   - Errors from the response future in `InFlight`.
    //   These are returned directly without closing the connection. If the
    //   underlying cause was actually a connection-level h2 error, it will
    //   surface on subsequent requests either via `send_request.poll_ready()`
    //   (triggering a connection close in PreFlight), or via the background
    //   connection task detecting the h2 shutdown and calling `shared.close()`.
    //
    // This simplifies error handling here: we don't need to distinguish h2
    // connection errors from stream errors ourselves — we let the phase of
    // the lifecycle determine the behavior.
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        loop {
            match this.state {
                ResponseFutureState::Error { ref mut err } => {
                    this.shared.close();
                    return Poll::Ready(Err(err
                        .take()
                        .expect("future not polled after finish")
                        .into()));
                }
                ResponseFutureState::Driving { ref mut fut } => {
                    let (send_request, cancel) = match ready!(fut.poll_unpin(cx)) {
                        Ok(stream) => stream,
                        Err(err) => {
                            this.state = ResponseFutureState::error(err);
                            continue;
                        }
                    };

                    // Sync the semaphore to the peer's advertised limit, clamped
                    // by the configured cap.
                    //
                    // If the peer's SETTINGS frame hasn't arrived yet,
                    // `current_max_send_streams()` still reflects the initial
                    // value which is fine, every later `poll_ready()` re-runs
                    // this resize.
                    //
                    // If the new size is smaller than what's currently in use,
                    // permits are reclaimed: waiters drop their permits and retry again
                    // (can land on different connection). That's wasted work, but preferable
                    // to blocking them waiting on streams on this connection to free
                    // up.
                    this.shared.concurrency.resize(
                        this.shared
                            .config
                            .streams_per_connection_limit
                            .min(send_request.current_max_send_streams()),
                    );

                    this.state = ResponseFutureState::PreFlight {
                        send_request: send_request.clone(),
                    };

                    // Store h2 handle (set once, visible to all threads via OnceLock)
                    let _ = this.shared.h2.set(H2Handle {
                        send_request,
                        cancel: cancel.disarm(),
                    });
                    this.shared.state.store(STATE_CONNECTED, Ordering::Release);

                    // Drain and notify waiters
                    if let Some(waiters) = this.shared.waiters.lock().take() {
                        for waiter in waiters {
                            let _ = waiter.send(());
                        }
                    }
                }
                ResponseFutureState::WaitingConnection { ref mut rx } => {
                    match ready!(rx.poll_unpin(cx)) {
                        Ok(_) => {
                            let send_request = this
                                .shared
                                .h2
                                .get()
                                .expect("h2 must be set after notification")
                                .send_request
                                .clone();
                            this.state = ResponseFutureState::PreFlight { send_request };
                        }
                        Err(_) => {
                            this.state = ResponseFutureState::error(Error::Closed);
                            continue;
                        }
                    }
                }
                ResponseFutureState::PreFlight {
                    ref mut send_request,
                } => {
                    match send_request.poll_ready(cx) {
                        Poll::Ready(Ok(_)) => {}
                        Poll::Ready(Err(err)) => {
                            this.state = ResponseFutureState::error(err);
                            continue;
                        }
                        Poll::Pending => {
                            // The H2 stream isn't ready yet. Check whether the
                            // concurrency limit was lowered and our permit reclaimed.
                            let mut permit = this.permit.take().expect("available permit");
                            match permit.poll_reclaimed(cx) {
                                Poll::Ready(()) => {
                                    // Permit reclaimed. Return the request so the
                                    // caller can retry on a different connection.
                                    return Poll::Ready(Err(ConnectionError::PermitReclaimed(
                                        this.request.take().unwrap(),
                                    )));
                                }
                                Poll::Pending => {
                                    // Not reclaimed. Put the permit
                                    // back and wait for either signal.
                                    this.permit = Some(permit);
                                    return Poll::Pending;
                                }
                            }
                        }
                    }

                    // we finally can forward the request now
                    let (parts, body) = this.request.take().unwrap().into_parts();

                    let req = Request::from_parts(parts, ());
                    let end_stream = body.is_end_stream();
                    let (fut, send_stream) = send_request
                        .send_request(req, end_stream)
                        .map_err(Error::from)?;

                    if !end_stream {
                        tokio::task::Builder::new()
                            .name("h2:request-pump")
                            .spawn(RequestPumpTask::new(send_stream, body).run())
                            .unwrap();
                    }
                    this.state = ResponseFutureState::InFlight { fut };
                }
                ResponseFutureState::InFlight { ref mut fut } => {
                    let resp = ready!(fut.poll_unpin(cx)).map_err(Error::from)?;
                    let permit = this.permit.take().expect("available permit");
                    let resp = resp.map(|recv| PermittedRecvStream::new(recv, permit));
                    return Poll::Ready(Ok(resp));
                }
            }
        }
    }
}

/// Background task that streams the request body into an H2 `SendStream`.
///
/// Spawned by [`RequestFuture`] once the H2 stream is established. Reads frames from
/// the body, respects H2 flow-control by reserving and polling capacity before each
/// write, and sends trailers (or empty trailers) once the body is exhausted.
struct RequestPumpTask<B> {
    send_stream: SendStream<Bytes>,
    body: B,
}

impl<B> RequestPumpTask<B>
where
    B: http_body::Body<Data = Bytes> + Unpin,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send,
{
    fn new(send_stream: SendStream<Bytes>, body: B) -> Self {
        Self { send_stream, body }
    }

    async fn run(mut self) {
        if let Err(err) = self.run_inner().await {
            debug!("h2 request pump error: {err}");

            let reason = match err {
                RequestPumpError::Input(_) => Reason::CANCEL,
                RequestPumpError::Pump(err) => err.reason().unwrap_or(Reason::INTERNAL_ERROR),
            };

            self.send_stream.send_reset(reason);
        }
    }

    async fn run_inner(&mut self) -> Result<(), RequestPumpError> {
        while let Some(frame) = self.body.frame().await {
            match frame {
                Ok(frame) => {
                    if self.handle_frame(frame, self.body.is_end_stream()).await? {
                        // end stream already sent!
                        return Ok(());
                    }
                }
                Err(err) => {
                    return Err(RequestPumpError::Input(err.into()));
                }
            }
        }

        // Send an explicit end stream
        self.send_stream.send_trailers(HeaderMap::default())?;

        Ok(())
    }

    /// handle a frame, returns true if it's last frame or trailers. It's illegal to
    /// send more data frames after handle_frame returns true
    async fn handle_frame(
        &mut self,
        frame: Frame<Bytes>,
        end_of_stream: bool,
    ) -> Result<bool, h2::Error> {
        if frame.is_data() {
            let mut data = frame.into_data().unwrap();

            while !data.is_empty() {
                self.send_stream.reserve_capacity(data.len());
                let size = poll_fn(|cx| self.send_stream.poll_capacity(cx))
                    .await
                    .ok_or(Reason::INTERNAL_ERROR)??;

                let chunk = data.split_to(size.min(data.len()));
                self.send_stream
                    .send_data(chunk, end_of_stream && data.is_empty())?;
            }
            Ok(end_of_stream)
        } else if frame.is_trailers() {
            let trailers = frame.into_trailers().unwrap();
            self.send_stream.send_trailers(trailers)?;
            Ok(true)
        } else {
            Err(Reason::PROTOCOL_ERROR.into())
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum RequestPumpError {
    #[error("request stream failed with an error: {0}")]
    Input(#[from] GenericError),
    #[error("sending data to remote peer failed with an error: {0}")]
    Pump(#[from] h2::Error),
}

/// Response body stream that holds an H2 stream permit for its lifetime.
///
/// Implements [`http_body::Body`] by delegating to the inner [`RecvStream`],
/// automatically releasing H2 flow-control capacity after each data frame.
/// The semaphore permit is held until this stream is dropped, ensuring the
/// concurrency slot remains occupied while the response body is being consumed.
#[derive(Debug)]
pub struct PermittedRecvStream {
    stream: RecvStream,
    /// Tracks whether all data frames have been consumed and we should poll trailers next.
    data_done: bool,
    _permit: Permit,
}

impl PermittedRecvStream {
    fn new(stream: RecvStream, permit: Permit) -> Self {
        Self {
            stream,
            data_done: false,
            _permit: permit,
        }
    }
}

impl Body for PermittedRecvStream {
    type Data = Bytes;
    type Error = h2::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        if !self.data_done {
            match ready!(self.stream.poll_data(cx)) {
                Some(Ok(data)) => {
                    let len = data.len();
                    let _ = self.stream.flow_control().release_capacity(len);
                    return Poll::Ready(Some(Ok(Frame::data(data))));
                }
                Some(Err(err)) => return Poll::Ready(Some(Err(err))),
                None => {
                    self.data_done = true;
                }
            }
        }

        // Data is exhausted, poll for trailers
        match ready!(self.stream.poll_trailers(cx)) {
            Ok(Some(trailers)) => Poll::Ready(Some(Ok(Frame::trailers(trailers)))),
            Ok(None) => Poll::Ready(None),
            Err(err) => Poll::Ready(Some(Err(err))),
        }
    }

    fn is_end_stream(&self) -> bool {
        self.stream.is_end_stream()
    }
}

#[cfg(test)]
mod test {
    use bytes::Bytes;
    use http::Request;
    use http_body::Frame;
    use http_body_util::BodyExt;
    use tokio::sync::mpsc;
    use tokio::task::JoinSet;

    use crate::pool::conn::{ConnectionConfigBuilder, next_connection_id};
    use crate::pool::test_util::{ControlledConnector, TestConnector};

    use super::Connection;

    /// Sends a request with an empty body and returns the response body stream.
    /// The response body must be consumed or dropped by the caller.
    async fn send_request(conn: &mut Connection<TestConnector>) -> super::PermittedRecvStream {
        conn.ready().await.unwrap();
        let resp = conn
            .request(
                Request::builder()
                    .uri("http://test-host:80")
                    .body(http_body_util::Empty::<Bytes>::new())
                    .unwrap(),
            )
            .await
            .unwrap();
        resp.into_body()
    }

    #[test]
    fn test_connection_id() {
        assert_ne!(next_connection_id(), next_connection_id());
    }

    /// Client starts with init_max_streams=100 but server advertises
    /// max_concurrent_streams=5. After the first request round-trip triggers
    /// the handshake and a subsequent poll_ready reads the updated setting,
    /// the semaphore must shrink to 5.
    #[tokio::test]
    async fn permits_sync_with_server_max_concurrent_streams() {
        let mut connection = Connection::new(
            TestConnector::new(5),
            ConnectionConfigBuilder::default()
                .initial_max_send_streams(100)
                .build()
                .unwrap(),
        );

        // First request triggers the handshake. Drop the body to release permit.
        drop(send_request(&mut connection).await);

        // Second ready() hits the Connected arm which calls semaphore_updater.update(),
        // syncing the semaphore down to 5. Send a request to consume that permit too.
        drop(send_request(&mut connection).await);

        // Now hold exactly 5 response bodies to exhaust the synced semaphore.
        let mut held_bodies = Vec::new();
        for _ in 0..5 {
            let mut c = connection.clone();
            held_bodies.push(send_request(&mut c).await);
        }

        // 6th try_ready must fail (no permits left)
        let mut c6 = connection.clone();
        assert!(
            c6.try_ready().is_none(),
            "expected try_ready to return None at capacity"
        );

        // Drop all held bodies, permits are released
        drop(held_bodies);
    }

    /// Server advertises max_concurrent_streams=50, but the connection is
    /// configured with cap_max_send_streams=3. After the handshake, the
    /// semaphore must be clamped to the cap (not the server-advertised value)
    /// so the pool keeps opening new connections instead of funneling every
    /// request through this one.
    #[tokio::test]
    async fn permits_clamped_to_cap_max_send_streams() {
        let connection = Connection::new(
            TestConnector::new(50),
            ConnectionConfigBuilder::default()
                .initial_max_send_streams(10)
                .streams_per_connection_limit(3)
                .build()
                .unwrap(),
        );

        // Hold exactly 3 response bodies to exhaust the cap.
        let mut held_bodies = Vec::new();
        for _ in 0..3 {
            let mut c = connection.clone();
            held_bodies.push(send_request(&mut c).await);
        }

        // 4th try_ready must fail — the cap is the active limit even though
        // the server would happily accept up to 50 concurrent streams.
        let mut c4 = connection.clone();
        assert!(
            c4.try_ready().is_none(),
            "expected try_ready to return None at cap"
        );

        // Releasing one held body frees a permit under the cap.
        drop(held_bodies.pop());
        c4.ready().await.unwrap();

        drop(held_bodies);
    }

    /// With max_concurrent_streams=2, holding two response bodies should
    /// exhaust permits. Dropping one should free a slot.
    #[tokio::test]
    async fn try_ready_fails_at_capacity() {
        let mut connection = Connection::new(
            TestConnector::new(2),
            ConnectionConfigBuilder::default()
                .initial_max_send_streams(2)
                .build()
                .unwrap(),
        );

        // Open two streams, hold both response bodies
        let body1 = send_request(&mut connection).await;
        let body2 = send_request(&mut connection).await;

        // A third try_ready must fail
        let mut c3 = connection.clone();
        assert!(
            c3.try_ready().is_none(),
            "expected try_ready to return None when at capacity"
        );

        // Drop one body, freeing a permit
        drop(body1);

        // Now ready should succeed
        c3.ready().await.unwrap();

        drop(body2);
    }

    /// Multiple tasks sharing a single Connection can send concurrent
    /// requests. The first request triggers the handshake; subsequent
    /// requests wait for it and then reuse the same H2 session.
    #[tokio::test]
    async fn concurrent_requests_on_shared_connection() {
        let connection = Connection::new(
            TestConnector::new(10),
            ConnectionConfigBuilder::default()
                .initial_max_send_streams(10)
                .build()
                .unwrap(),
        );

        let mut handles = JoinSet::default();
        for i in 0u8..50 {
            let mut c = connection.clone();
            handles.spawn(async move {
                c.ready().await.unwrap();
                let resp = c
                    .request(
                        Request::builder()
                            .uri("http://test-host:80")
                            .body(http_body_util::Full::new(Bytes::from(vec![i; 4])))
                            .unwrap(),
                    )
                    .await
                    .unwrap();

                let collected = resp.into_body().collect().await.unwrap().to_bytes();
                assert_eq!(
                    collected.as_ref(),
                    &[i; 4],
                    "response should echo request body"
                );
            });
        }

        handles.join_all().await;
    }

    /// Sends multiple data frames over a streaming request body and reads
    /// back each echoed frame from the response, verifying bidirectional
    /// streaming over an open H2 stream.
    #[tokio::test]
    async fn streaming_request_and_response() {
        let mut connection = Connection::new(
            TestConnector::new(10),
            ConnectionConfigBuilder::default()
                .initial_max_send_streams(10)
                .build()
                .unwrap(),
        );
        connection.ready().await.unwrap();

        let (tx, rx) = mpsc::channel::<Result<Frame<Bytes>, std::convert::Infallible>>(10);
        let resp = connection
            .request(
                Request::builder()
                    .uri("http://test-host:80")
                    .body(http_body_util::StreamBody::new(
                        tokio_stream::wrappers::ReceiverStream::new(rx),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        let mut body = resp.into_body();

        // Send 3 messages, reading the echo after each one
        for i in 0u8..3 {
            let msg = Bytes::from(vec![i; 8]);
            tx.send(Ok(Frame::data(msg.clone()))).await.unwrap();

            let frame = body.frame().await.unwrap().unwrap();
            assert_eq!(
                frame.data_ref().unwrap().as_ref(),
                msg.as_ref(),
                "echo for message {i} should match"
            );
        }

        // Close the request body stream, then expect trailers from the server
        drop(tx);
        let trailer_frame = body.frame().await.unwrap().unwrap();
        assert!(trailer_frame.is_trailers(), "expected trailers frame");

        // Stream should be done
        assert!(body.frame().await.is_none());
    }

    fn waiter_count<C>(conn: &Connection<C>) -> usize {
        conn.shared.waiters.lock().as_ref().map_or(0, |v| v.len())
    }

    /// Spins until the expected number of waiters are registered on the connection.
    async fn wait_for_waiters<C>(conn: &Connection<C>, expected: usize) {
        for _ in 0..10_000 {
            if waiter_count(conn) >= expected {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!(
            "timed out waiting for {expected} waiters, got {}",
            waiter_count(conn)
        );
    }

    #[tokio::test]
    async fn waiters_notified_on_successful_connection() {
        let (connector, gate) = ControlledConnector::new(10);
        let connection = Connection::new(
            connector,
            ConnectionConfigBuilder::default()
                .initial_max_send_streams(10)
                .build()
                .unwrap(),
        );

        let mut handles = JoinSet::default();
        for i in 0u8..5 {
            let mut c = connection.clone();
            handles.spawn(async move {
                c.ready().await.unwrap();
                c.request(
                    Request::builder()
                        .uri("http://test-host:80")
                        .body(http_body_util::Full::new(Bytes::from(vec![i; 4])))
                        .unwrap(),
                )
                .await
            });
        }

        // 1 task drives the handshake, the other 4 become waiters
        wait_for_waiters(&connection, 4).await;
        gate.notify_waiters();

        let results = handles.join_all().await;
        for result in &results {
            assert!(result.is_ok(), "all requests should succeed: {result:?}");
        }
        assert!(!connection.is_closed());
    }

    #[tokio::test]
    async fn waiters_notified_on_connection_failure() {
        let (connector, gate) = ControlledConnector::with_error(10);
        let connection = Connection::new(
            connector,
            ConnectionConfigBuilder::default()
                .initial_max_send_streams(10)
                .build()
                .unwrap(),
        );

        let mut handles = JoinSet::default();
        for _ in 0..5 {
            let mut c = connection.clone();
            handles.spawn(async move {
                c.ready().await.unwrap();
                c.request(
                    Request::builder()
                        .uri("http://test-host:80")
                        .body(http_body_util::Empty::<Bytes>::new())
                        .unwrap(),
                )
                .await
            });
        }

        wait_for_waiters(&connection, 4).await;
        gate.notify_waiters();

        let results = handles.join_all().await;
        for result in &results {
            assert!(result.is_err(), "all requests should fail: {result:?}");
        }
        assert!(connection.is_closed());
    }

    #[tokio::test]
    async fn waiters_notified_on_driver_drop() {
        let (connector, _gate) = ControlledConnector::new(10);
        let connection = Connection::new(
            connector,
            ConnectionConfigBuilder::default()
                .initial_max_send_streams(10)
                .build()
                .unwrap(),
        );

        // First request wins the CAS and becomes the driver
        let mut driver = connection.clone();
        driver.ready().await.unwrap();
        let driving_fut = driver.request(
            Request::builder()
                .uri("http://test-host:80")
                .body(http_body_util::Empty::<Bytes>::new())
                .unwrap(),
        );

        // Spawn waiter tasks
        let mut handles = JoinSet::default();
        for _ in 0..4 {
            let mut c = connection.clone();
            handles.spawn(async move {
                c.ready().await.unwrap();
                c.request(
                    Request::builder()
                        .uri("http://test-host:80")
                        .body(http_body_util::Empty::<Bytes>::new())
                        .unwrap(),
                )
                .await
            });
        }

        wait_for_waiters(&connection, 4).await;

        // Drop the driver: Drop impl CAS CONNECTING→CLOSED, calls close()
        drop(driving_fut);

        let results = handles.join_all().await;
        for result in &results {
            let err = result.as_ref().unwrap_err();
            assert!(
                matches!(err, super::ConnectionError::Error(super::Error::Closed)),
                "expected ConnectionError::Closed, got {err:?}"
            );
        }
        assert!(connection.is_closed());
    }
}
