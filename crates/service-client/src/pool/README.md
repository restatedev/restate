# HTTP/2 Connection Pool

A hierarchical, multiplexed HTTP/2 connection pool designed for high-throughput
service-to-service communication.

## Architecture

The pool is organized into three layers:

```
Pool
 └── per (scheme, authority) ──► AuthorityPool
                                   └── grows on demand ──► Connection
                                                             └── up to N H2 streams
```

### `Pool<C>`

Top-level entry point. Routes each request to an `AuthorityPool` keyed by
`(scheme, authority)` using a concurrent `DashMap`. Authority pools are created
lazily on first request. The connector `C` is a Tower `Service<Uri>` that
produces an async I/O stream (e.g. `TcpConnector`, or `TlsConnector` for HTTPS).

`Pool::request` loops over `poll_ready` / `call`: if a connection hands the
request back unsent (`ConnectionError::Retry`), it is retried on another
connection.

#### Eviction

When `idle_connection_timeout` or `max_connection_age` is set, a background
task periodically removes connections from each authority pool:

- **Idle**: no in-flight streams and unused for longer than
  `idle_connection_timeout`.
- **Aged**: past their `retire_at` deadline (`max_connection_age` plus a
  per-connection jitter drawn at creation), regardless of in-flight streams.
  Removing them stops new streams; in-flight streams finish on the old
  connection. The next request opens a fresh one, letting an L4 load balancer
  spread connections across newly added backends.

The task runs every quarter of the shortest configured timeout (at least every
10s), drops authority pools that become empty, and exits once all `Pool`
clones are dropped.

### `AuthorityPool<C>`

Manages a set of `Connection` instances for a single HTTP authority. Implements
the Tower `poll_ready` / `call` contract:

1. **Try existing connections** -- polls candidate connections; the first to
   yield a stream permit wins. Retired (draining/closed) and aged connections
   are skipped.
2. **Scale up** -- a new `Connection` is created when there are no live
   connections, when no streams are available, or when the ratio of available
   to total streams falls below `1.0 - connection_saturation_threshold`
   (proactive expansion, disabled when the threshold is `None`). At most one
   connection is created per pass. There is no upper bound on the number of
   connections.
3. **Back-pressure** -- if no candidate is ready, all polled connections have
   registered wakers and the caller receives `Pending` until a stream frees up.

A still-connecting connection counts its `initial_max_send_streams` as
available, which suppresses further expansion while concurrent cold-start
callers pile onto it.

Retired connections are reaped when the pool expands. Cloning an
`AuthorityPool` shares the underlying connection set via
`Arc<RwLock<...>>`; each clone keeps its own `poll_ready` / `call` state.

### `Connection<C>`

A single lazily-initialized HTTP/2 session. The H2 handshake is deferred until
the first request. State transitions are lock-free via an `AtomicU8`:

```
New ──► Connecting ──► Connected ──► Draining ──► Closed
```

Fatal errors can move to `Closed` from any state.

- **New**: No TCP connection yet. The first `request()` call wins a CAS race
  and becomes the *driver* that performs the handshake.
- **Connecting**: The driver is performing the H2 handshake. Other requests
  register as waiters (via oneshot channels) and are notified once the
  handshake completes (or fails).
- **Connected**: The H2 session is established. The `SendRequest<Bytes>` handle
  is stored in a `OnceLock` and shared across all clones. A background task
  drives the H2 connection (and keep-alive) and closes the connection when it
  ends.
- **Draining**: The peer sent a graceful GOAWAY (`NO_ERROR`). The connection
  accepts no new streams; already-accepted streams continue.
- **Closed**: Terminal state. All waiters are drained and future requests
  receive `Error::Closed`.

If the driving future is dropped while in the `Connecting` state, the
connection is immediately moved to `Closed` to prevent waiters from hanging.

#### Concurrency control

Each `Connection` has a `Concurrency` limiter (a custom async semaphore) that
bounds the number of in-flight H2 streams. The initial limit is
`initial_max_send_streams` (default 50) capped by
`streams_per_connection_limit` (default 128). It is resized to the remote
peer's advertised `MAX_CONCURRENT_STREAMS`, again capped by
`streams_per_connection_limit`, after the handshake and on every `poll_ready`.
If the limit shrinks, waiting permits may be reclaimed; the affected requests
are returned unsent and retried on another connection.

Callers must acquire a `Permit` via `poll_ready()` before sending a request.
The permit is held for the lifetime of the response body (`PermittedRecvStream`),
ensuring stream slots remain occupied while the caller consumes the response.

#### Keep-alive

An optional HTTP/2 PING-based keep-alive mechanism detects dead connections.
Configured via `keep_alive_interval` (how often to send PINGs, with
`keep_alive_interval_jitter` added) and `keep_alive_timeout` (max wait for a
PONG). If the timeout fires, the connection is closed.

### `ResponseFuture<B>`

State machine that drives a single request through its lifecycle:

```
   Driving ──► PreFlight ──► InFlight ──► Done
                   ▲  │
WaitingConnection ─┘  └──► Draining (request returned for retry)
```

- **Driving**: This request is performing the H2 handshake.
- **WaitingConnection**: Another request is driving; waiting for notification.
- **PreFlight**: H2 handle acquired; waiting for `send_request.poll_ready()`.
- **InFlight**: Request sent; waiting for response headers.
- **Draining**: The connection received a graceful GOAWAY; the unsent request
  is returned as `ConnectionError::Retry`.

Errors during `Driving` or `PreFlight/poll_ready` are connection-level (close
the connection), except graceful GOAWAY (`NO_ERROR`) during `PreFlight`.
That retires the connection from admission without cancelling active streams,
and the pool retries the unsent request on another connection. Callers that
reserved capacity before draining began also return their unsent requests.
Errors during `send_request()` or `InFlight` remain stream-level (only the
individual request fails); the pool does not replay these requests.

## TLS

The `tls` module provides a Tower `Layer` / `Service` that wraps a TCP
connector with optional TLS. For HTTPS URIs it performs a rustls handshake
(bounded by a handshake timeout) with ALPN restricted to `h2`; for HTTP URIs
the connection passes through as plain text. The resulting `MaybeTlsStream<S>`
enum implements `AsyncRead + AsyncWrite`.

## Configuration

| Parameter                         | Default | Description                                                                   |
|-----------------------------------|---------|-------------------------------------------------------------------------------|
| `connection_saturation_threshold` | 0.7     | Stream utilization that triggers proactive expansion (`None` = disabled)      |
| `initial_max_send_streams`        | 50      | Initial H2 stream concurrency per connection                                  |
| `streams_per_connection_limit`    | 128     | Upper bound on per-connection streams, capping the peer's advertised limit    |
| `initial_stream_window_size`      | 2 MiB   | Initial per-stream receive flow-control window                                |
| `initial_connection_window_size`  | 5 MiB   | Initial connection-level receive flow-control window                          |
| `max_frame_size`                  | 16 KiB  | Largest H2 DATA frame payload accepted                                        |
| `data_frame_budget`               | 1 MiB   | DATA framing overhead budget; exhausting it closes with `ENHANCE_YOUR_CALM`   |
| `keep_alive_interval`             | None    | Interval between HTTP/2 PING frames (`None` = disabled)                       |
| `keep_alive_interval_jitter`      | 0.2     | Fractional jitter added to `keep_alive_interval`                              |
| `keep_alive_timeout`              | 20s     | Max wait for a PING response                                                  |
| `idle_connection_timeout`         | 5 min   | Evict connections idle for this long (`None` = disabled)                      |
| `max_connection_age`              | None    | Retire connections older than this (`None` = disabled)                        |
| `max_connection_age_jitter`       | 0.1     | Fractional jitter added to `max_connection_age`, drawn once per connection    |

Use `PoolBuilder` to construct a `Pool` with custom settings.
