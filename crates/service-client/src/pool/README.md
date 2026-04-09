# HTTP/2 Connection Pool

A hierarchical, multiplexed HTTP/2 connection pool designed for high-throughput
service-to-service communication.

## Architecture

The pool is organized into three layers:

```
Pool
 └── per (scheme, authority) ──► AuthorityPool
                                   └── up to max_connections ──► Connection
                                                                   └── up to N H2 streams
```

### `Pool<C>`

Top-level entry point. Routes each request to an `AuthorityPool` keyed by
`(scheme, authority)` using a concurrent `DashMap`. Authority pools are created
lazily on first request. The connector `C` is a Tower `Service<Uri>` that
produces an async I/O stream (e.g. `TcpConnector`, or `TlsConnector` for HTTPS).

### `AuthorityPool<C>`

Manages a set of `Connection` instances for a single HTTP authority. Implements
the Tower `poll_ready` / `call` contract:

1. **Try existing connections** -- polls candidates that report available H2
   streams. The first to become ready wins.
2. **Scale up** -- if all connections are at capacity and the pool is below
   `max_connections`, a new `Connection` is created and polled.
3. **Back-pressure** -- if already at `max_connections` with no available
   streams, all live connections are polled so wakers are registered; the caller
   receives `Pending` until a stream frees up.

Closed connections are evicted during each `poll_ready` pass. Cloning an
`AuthorityPool` shares the underlying connection set via `Arc<Mutex<...>>`.

### `Connection<C>`

A single lazily-initialized HTTP/2 session. The H2 handshake is deferred until
the first request. State transitions are lock-free via an `AtomicU8`:

```
New ──► Connecting ──► Connected ──► Closed
```

- **New**: No TCP connection yet. The first `request()` call wins a CAS race
  and becomes the *driver* that performs the handshake.
- **Connecting**: The driver is performing the H2 handshake. Other requests
  register as waiters (via oneshot channels) and are notified once the
  handshake completes (or fails).
- **Connected**: The H2 session is established. A `SendRequest<Bytes>` handle
  is stored in a `OnceLock` and shared across all clones.
- **Closed**: Terminal state. All waiters are drained and future requests
  receive `ConnectionError::Closed`.

If the driving future is dropped while in the `Connecting` state, the
connection is immediately moved to `Closed` to prevent waiters from hanging.

#### Concurrency control

Each `Connection` has a `Concurrency` limiter (a custom async semaphore) that
bounds the number of in-flight H2 streams. The initial limit is set by
`initial_max_send_streams` (default 50) and is dynamically resized to match
the remote peer's advertised `MAX_CONCURRENT_STREAMS` after the handshake.

Callers must acquire a `Permit` via `poll_ready()` before sending a request.
The permit is held for the lifetime of the response body (`PermittedRecvStream`),
ensuring stream slots remain occupied while the caller consumes the response.

#### Keep-alive

An optional HTTP/2 PING-based keep-alive mechanism detects dead connections.
Configured via `keep_alive_interval` (how often to send PINGs) and
`keep_alive_timeout` (max wait for a PONG). If the timeout fires, the
connection is closed with `ConnectionError::KeepAliveTimeout`.

### `ResponseFuture<B>`

State machine that drives a single request through its lifecycle:

```
   Driving ──► PreFlight ──► InFlight ──► Done
                   ▲
WaitingConnection ─┘
```

- **Driving**: This request is performing the H2 handshake.
- **WaitingConnection**: Another request is driving; waiting for notification.
- **PreFlight**: H2 handle acquired; waiting for `send_request.poll_ready()`.
- **InFlight**: Request sent; waiting for response headers.

Errors during `Driving` or `PreFlight/poll_ready` are connection-level (close
the connection). Errors during `send_request()` or `InFlight` are stream-level
(only the individual request fails).

## TLS

The `tls` module provides a Tower `Layer` / `Service` that wraps a TCP
connector with optional TLS. For HTTPS URIs it performs a rustls handshake with
ALPN negotiated to `h2`; for HTTP URIs the connection passes through as plain
text. The resulting `MaybeTlsStream<S>` enum implements `AsyncRead + AsyncWrite`.

## Configuration

| Parameter                  | Default | Description                                         |
|----------------------------|---------|-----------------------------------------------------|
| `max_connections`          | 1       | Max connections per authority                        |
| `initial_max_send_streams` | 50      | Initial H2 stream concurrency per connection        |
| `keep_alive_interval`      | None    | Interval between HTTP/2 PING frames (None=disabled) |
| `keep_alive_timeout`       | 20s     | Max wait for a PING response                        |

Use `PoolBuilder` to construct a `Pool` with custom settings.
