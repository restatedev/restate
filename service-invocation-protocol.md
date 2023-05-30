# Restate Service Invocation Protocol

The following specification describes the protocol used by Restate to invoke remote Restate services.

## Architecture

The system is composed of two actors:

- Restate Runtime
- Service endpoint, which is split into:
  - SDK, which contains the implementation of the Restate Protocol
  - User business logic, which interacts with the SDK to access Restate system calls (or syscalls)

Each service method invocation is modeled by the protocol as a state machine, where state transitions can be caused
either by user code or by _Runtime events_.

Every state transition is logged in the _Invocation journal_, used to implement the Restate durable execution model. The
journal is also used to suspend an invocation and resume it at a later point in time. The _Invocation journal_ is
tracked both by Restate runtime and service endpoint.

Runtime and service endpoint exchange _Messages_ containing the invocation journal and runtime events through an HTTP
message stream.

## State machine and journal

Every invocation state machine begins when the stream is opened and ends when the stream is closed. In the middle,
arbitrary interaction can be performed from the Service endpoint to the Runtime and vice versa via well-defined
messages.

### Syscalls

Most Restate features, such as interaction with other services, accessing service instance state, and so on, are defined
as _Restate syscalls_ and exposed through the service protocol. The user interacts with these syscalls using the SDK
APIs, which generate _Journal Entry_ messages that will be handled by the invocation state machine.

Depending on the specific syscall, the Restate runtime generates as response either:

- A completion, that is the response to the syscall
- An ack, that is a confirmation the syscall has been persisted and **will** be executed
- Nothing

Each syscall defines a priori whether it replies with an ack or a completion, or doesn't reply at all.

There are a couple of special message streams for initializing and closing the invocation.

### Replaying and Processing

Both runtime and SDKs transition the message stream through 2 states:

- _Replaying_, that is when there are journal entries to replay before continuing the execution. Described in
  [Suspension behavior](#suspension-behavior).
- _Processing_, that is after the _replaying_ state is over.

There are a couple of properties that we enforce through the design of the protocol:

- Runtime and service endpoint both have their view of the journal
- The source of truth of the journal is:
  - The runtime, when the invocation is not in _processing_ state
  - The service endpoint, when the invocation is in _processing_ state
- When in _replaying_ state, the service endpoint cannot create new journal entries.
- When in _processing_ state, only the service endpoint can create new journal entries, picking their order.
  Consequently, it might have newer entries that the runtime is not aware of. It’s also the responsibility of the
  service endpoint to make sure the runtime has the same ordered view of the journal it has.

## Messages

The protocol is composed by messages that are sent back and forth between runtime and service Endpoint. The protocol
mandates the following messages:

- `StartMessage`
- `[..]EntryMessage`
- `CompletionMessage`
- `SuspensionMessage`

### Message stream

In order to execute a service method invocation, service endpoint and restate Runtime open a single stream between the
runtime and the service endpoint. Given 10 concurrent service method invocations to a service endpoint, there are 10
concurrent streams, each of them mapping to a specific invocation.

This stream is implemented using HTTP, and depending on the deployment environment and the HTTP version it can operate
in two modes:

- Full duplex (bidirectional) stream: Messages are sent back and forth on the same stream at the same time. This option
  is supported only when using HTTP/2.
- Request/Response stream: Messages are sent from runtime to service endpoint, and later from service endpoint to
  runtime. Once the service endpoint starts sending messages to the runtime, the runtime cannot send messages anymore
  back to the service endpoint.

When opening the stream, the request method MUST be `POST` and the request path MUST have the following format:

```
/invoke/{fullyQualifiedServiceName}/{methodName}
```

For example:

```
/invoke/counter.Counter/Add
```

In case the `fullyQualifiedServiceName` or `methodName` is unknown, the SDK MUST close the stream replying back with a
`404` status code.

A message stream MUST start with `StartMessage` and MUST end with either `OutputStreamEntry` or `SuspensionMessage`.

### Message header

Each message is sent together with a message header prepending the serialized message bytes.

The message header is a fixed 64-bit number containing:

- (MSB) Message type: 16 bit. The type of the message. Used to deserialize the message. The first 6 bits are used as the
  message namespace, to categorize the different message types.
- Message reserved bits: 16 bit. These bits can be used to send flags and other information, and are defined per message
  type/namespace.
- Message length: 32 bit. Length of serialized message bytes, excluding header length.

<table>
<thead>
  <tr>
    <th colspan="32">32 bit</th>
  </tr>
</thead>
<tbody>
  <tr>
    <td colspan="16">Type</td>
    <td colspan="16">Flags</td>
  </tr>
  <tr>
    <td colspan="32">Length</td>
  </tr>
</tbody>
</table>

### StartMessage

The `StartMessage` carries the metadata required to bootstrap the invocation state machine, including:

- `known_entries`: The known journal length
- `state_map`: The eager state map (see [Eager state](#eager-state))

**Header**

<table>
<thead>
  <tr>
    <th colspan="32">32 bit</th>
  </tr>
</thead>
<tbody>
  <tr>
    <td colspan="16">0x0000</td>
    <td colspan="5">Reserved</td>
    <td colspan="1">PS</td>
    <td colspan="10">PV</td>
  </tr>
  <tr>
    <td colspan="32">StartMessage length</td>
  </tr>
</tbody>
</table>

**Flags**

- 5 bits (MSB): Reserved
- 1 bit `PS`: `PARTIAL_STATE` flag (see [Eager state](#eager-state))
- 10 bits `PV`: Protocol version

### Entries and Completions

<!-- TODO -->

### Messages reference

<!-- TODO -->

## Suspension

<!-- TODO -->

## Failures

<!-- TODO -->

## Optional features

The following section describes optional features SDK developers MAY implement to improve the experience and provide
additional features to the users.

### Custom entry messages

The protocol allows the SDK to register an arbitrary entry type within the journal. The type MUST be `>= 0xFC00`. The
runtime will treat this entry as any other entry, persisting it and sending it during replay in the correct order.

If the SDK needs an acknowledgment that the entry has been persisted, it can set the `REQUIRES_ACK` flag in the header.
When set, the runtime will send back a completion, as described in
[Entries and Completions section](#entries-and-completions), as soon as the entry is persisted.

**Header**

<table>
<thead>
  <tr>
    <th colspan="32">32 bit</th>
  </tr>
</thead>
<tbody>
  <tr>
    <td colspan="16">Type</td>
    <td colspan="15">Reserved</td>
    <td colspan="1">RA</td>
  </tr>
  <tr>
    <td colspan="32">StartMessage length</td>
  </tr>
</tbody>
</table>

- Type MUST be `>= 0xFC00`

**Flags**

- 15 bits (MSB): Reserved
- 1 bit `RA`: `REQUIRES_ACK` flag

### Eager state

As described in [Message reference](#messages-reference), to get a service instance state entry, the SDK creates a
`GetStateEntryMessage` without a result, and waits for a `Completion` with the result, or alternatively suspends and
expects the `GetStateEntryMessage.result` is filled when replaying.

SDKs MAY optimize the state access operations by reading the flag `PARTIAL_STATE` and `state_map` within the
[`StartMessage`](#startmessage). The `state_map` field contains key-value pairs of the current state of the service
instance. When `PARTIAL_STATE` is set, the `state_map` is partial/incomplete, meaning there might be entries stored in
the Runtime that are not part of `state_map`. When `PARTIAL_STATE` is unset, the `state_map` is complete, thus if an
entry is not within the map, the SDK can assume it's not stored in the runtime either.

A possible implementation could be the following. Given a user requests a state entry with key `my-key`:

- If `my-key` is available in `state_map`, generate a `GetStateEntryMessage` with filled `result`, and return the value
  to the user
- If `my-key` is not available in `state_map`
  - If `PARTIAL_STATE` is unset, generate a `GetStateEntryMessage` with empty `result`, and return empty to the user
  - If `PARTIAL_STATE` is set, generate a `GetStateEntryMessage` without a `result`, and wait for the runtime to send a
    `Completion` back (same logic as without eager state)

In order for the aforementioned algorithm to work, set and clear state operations must be reflected on the local
`state_map` as well.
