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

Every state transition is logged in the _Invocation journal_, used to implement Restate's durable execution model. The
journal is also used to suspend an invocation and resume it at a later point in time. The _Invocation journal_ is
tracked both by Restate's runtime and the service endpoint.

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
- The source of truth of the journal and its ordering is:
  - The runtime, when the invocation is not in _processing_ state
  - The service endpoint, when the invocation is in _processing_ state
- When in _replaying_ state, the service endpoint cannot create new journal entries.
- When in _processing_ state, only the service endpoint can create new journal entries, picking their order.
  Consequently, it might have newer entries that the runtime is not aware of. It’s also the responsibility of the
  service endpoint to make sure the runtime has the same ordered view of the journal it has.
- Only in processing state the runtime can send
  [`CompletionMessage`](#completable-journal-entries-and-completionmessage)

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

Every unit of the stream contains a Message serialized using the
[Protobuf encoding](https://protobuf.dev/programming-guides/encoding/), using the definitions in
[`protocol.proto`](dev/restate/service/protocol.proto), prepended by a [message header](#message-header).

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

An arbitrary path MAY prepend the aforementioned path format.

In case the path format is not respected, or `fullyQualifiedServiceName` or `methodName` is unknown, the SDK MUST close
the stream replying back with a `404` status code.

A message stream MUST start with `StartMessage` and MUST end with either:

- One `OutputStreamEntry`
- One [`SuspensionMessage`](#suspension)
- One [`ErrorMessage`](#failures).
- None of the above, which is equivalent to sending an empty [`ErrorMessage`](#failures).

### Message header

Each message is sent together with a message header prepending the serialized message bytes.

    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    |              Type             |            Reserved           |
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    |                             Length                            |
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

The message header is a fixed 64-bit number containing:

- (MSB) Message type: 16 bit. The type of the message. Used to deserialize the message. The first 6 bits are used as the
  message namespace, to categorize the different message types.
- Message reserved bits: 16 bit. These bits can be used to send flags and other information, and are defined per message
  type/namespace.
- Message length: 32 bit. Length of serialized message bytes, excluding header length.

### StartMessage

The `StartMessage` carries the metadata required to bootstrap the invocation state machine, including:

- `known_entries`: The known journal length
- `state_map`: The eager state map (see [Eager state](#eager-state))

**Header**

    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    |             0x0000            | Reserved|S|         PV        |
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    |                             Length                            |
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

Flags:

- 5 bits (MSB): Reserved
- 1 bit `S`: `PARTIAL_STATE` flag (see [Eager state](#eager-state)). Mask: `0x0000_0400_0000_0000`
- 10 bits `PV`: Protocol version. Mask: `0x0000_03FF_0000_0000`

### Entries and Completions

We distinguish among two types of journal entries:

- Completable journal entries. These represent actions the runtime will perform, and for which consequently provide a
  completion value. All these entries have a `result` field defined in the message descriptor, defining the different
  variants of the completion value, and have a `COMPLETED` flag in the header.
- Non-completable journal entries. These represent actions the runtime will perform, but won't provide any completion
  value to it.

Whether a journal entry is completable or not is intrinsic in the definition of the journal action itself.

The header format for journal entries applies both when the runtime is sending entries to the SDK during a replay, and
when the SDK sends entries to the runtime during processing.

**Headers**

Completable journal entries:

    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    |              Type             |           Reserved          |C|
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    |                             Length                            |
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

Flags:

- 15 bits (MSB): Reserved
- 1 bit `C`: `COMPLETED` flag. Mask: `0x0000_0001_0000_0000`

Non-Completable journal entries:

    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    |              Type             |            Reserved           |
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    |                             Length                            |
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

Flags:

- 16 bits (MSB): Reserved

#### Completable journal entries and `CompletionMessage`

A completable journal entry at any point in time is either completed or not. After a completable journal entry is
completed, it cannot change its state back to not completed.

There are three situations where a completable journal entry can be completed:

- At creation time: when the SDK creates a completable journal entry, it can fill its `result` field and set the
  `COMPLETED` flag before sending the entry to the runtime. When replaying, the same `result` will be used.
- At suspension time: when the invocation is suspended, meaning there is no in-flight message stream, the runtime might
  internally complete a journal entry filling its `result` field.
- During the invocation processing: when the message stream is active and in [Full duplex mode](#message-stream), the
  runtime can notify a completion by sending a `CompletionMessage`.

A `CompletionMessage` holds the `result` of the JournalEntry and its `entry_index`. A `CompletionMessage` can hold all
the possible variants of a `result` field, and the SDK MUST be able to correlate the `result` field of the entry with
the `result` field of `CompletionMessage` through the `entry_index`. After the completion is notified, the SDK MUST NOT
send any additional messages related to this specific entry. On subsequent replays, the runtime automatically fills the
`result` field of this entry, without sending a subsequent `CompletionMessage`.

The runtime can send `CompletionMessage` in a different order than the one used to store journal entries. The SDK might
also not be interested in the `result` of completable journal entries, or it might be interested in the `results` in a
different order used to create the related journal entries. Usually it's the service business logic that dictates in
which `result`s the SDK is interested, and in which order.

**`CompletionMessage` Header**

    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    |             0x0001            |            Reserved           |
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    |                             Length                            |
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

### Journal entries reference

The following tables describe the currently available journal entries. For more details, check the protobuf message
descriptions in [`protocol.proto`](dev/restate/service/protocol.proto).

**Completable journal entries**

| Message                       | Type     | Description                                                                                                                                                  |
| ----------------------------- | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `PollInputStreamEntryMessage` | `0x0400` | Carries the service method input message(s) of the invocation. Note: currently the runtime always sends this entry completed, but this may change in future. |
| `GetStateEntryMessage`        | `0x0800` | Get the value of a service instance state key.                                                                                                               |
| `SleepEntryMessage`           | `0x0C00` | Initiate a timer that completes after the given time.                                                                                                        |
| `InvokeEntryMessage`          | `0x0C01` | Invoke another Restate service.                                                                                                                              |
| `AwakeableEntryMessage`       | `0x0C03` | Arbitrary result container which can be completed from another service, given a specific id.                                                                 |

**Non-Completable journal entries**

| Message                         | Type     | Description                                                                                                                                                                         |
| ------------------------------- | -------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `OutputStreamEntryMessage`      | `0x0401` | Carries the service method output message(s) or terminal failure of the invocation. Note: currently the runtime accepts only one entry of this type, but this may change in future. |
| `SetStateEntryMessage`          | `0x0800` | Set the value of a service instance state key.                                                                                                                                      |
| `ClearStateEntryMessage`        | `0x0801` | Clear the value of a service instance state key.                                                                                                                                    |
| `BackgroundInvokeEntryMessage`  | `0x0C02` | Invoke another Restate service at the given time, without waiting for the response.                                                                                                 |
| `CompleteAwakeableEntryMessage` | `0x0C04` | Complete an `Awakeable`, given its id (see `AwakeableEntryMessage`).                                                                                                                |

## Suspension

As mentioned in [Replaying and processing](#replaying-and-processing), an invocation can be suspended while waiting for
some journal entries to complete. When suspended, no message stream is in-flight for the given invocation.

To suspend an invocation, the SDK MUST send a `SuspensionMessage` containing entry indexes of the journal entry results
required to continue the computation. This set MUST contain only indexes of completable journal entries that are not
completed and that have been sent to the runtime. After sending the `SuspensionMessage`, the stream MUST be closed.

The runtime will resume the invocation as soon as at least one of the given indexes is completed.

## Failures

There are a number of failures that can incur during a service invocation, including:

- Transient network failures that interrupt the message stream
- SDK bugs
- Protocol violations
- Business logic bugs
- User thrown retryable errors

To notify a failure, the SDK can either:

- Close the stream with `ErrorMessage` as last message. This message is used by the runtime for accurate reporting to
  the user.
- Close the stream without `OutputStreamEntry` or `SuspensionMessage` or `ErrorMessage`. This is equivalent to sending
  an `ErrorMessage` with unknown reason.

The runtime takes care of retrying to execute the invocation after such failures occur, following a defined set of
policies. When retrying, the previous stored journal will be reused. Moreover, the SDK MUST NOT assume that every
journal entry previously sent on the same message stream has been correctly stored.

The SDK can allow users to end/terminate invocations with an exceptional return value. This is done in a similar fashion
to the successful return value case, by generating a `OutputStreamEntry` with the `failure` variant set, sending it and
closing the stream afterward.

**`ErrorMessage` Header**

    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    |             0x0003            |            Reserved           |
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    |                             Length                            |
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

## Optional features

The following section describes optional features SDK developers MAY implement to improve the experience and provide
additional features to the users.

### Custom entry messages

The protocol allows the SDK to register an arbitrary entry type within the journal. The type MUST be `>= 0xFC00`. The
runtime will treat this entry as any other entry, persisting it and sending it during replay in the correct order.

If the SDK needs an acknowledgment that the entry has been persisted, it can set the `REQUIRES_ACK` flag in the header.
When set, as soon as the entry is persisted, the runtime will send back a `CompletionMessage` with the `result.empty`
field set, as described in [Entries and Completions section](#entries-and-completions).

**Header**

    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    |              Type             |           Reserved          |A|
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    |                             Length                            |
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

- Type MUST be `>= 0xFC00`

Flags:

- 15 bits (MSB): Reserved
- 1 bit `A`: `REQUIRES_ACK` flag. Mask: `0x0000_0001_0000_0000`

### Eager state

As described in [Journal entries reference](#journal-entries-reference), to get a service instance state entry, the SDK
creates a `GetStateEntryMessage` without a result, and waits for a `Completion` with the result, or alternatively
suspends and expects the `GetStateEntryMessage.result` is filled when replaying.

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
