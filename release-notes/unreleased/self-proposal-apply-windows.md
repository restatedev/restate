# Bound partition proposal backlogs

## Improvement

Partition leaders now limit how far each source of work can get ahead of local
application. Invoker traffic at its limit pauses admission to the log while RPCs
and other sources remain eligible. This helps keep new RPCs from queueing behind
a large backlog of invocation effects.

### Configuration

The limits are per partition, in log records, and apply until the records have
been applied locally—not just committed to Bifrost. Defaults are:

```toml
[worker.self-proposal-max-in-flight]
invoker = 256
network-service = 256
timer = 256
scheduler = 256
shuffle = 16
partition-maintenance = 16
upsert-schema = 1
upsert-rule-book = 1
```

The cleaner continues to use `worker.cleanup-max-in-flight-purges` (default 32).
All limits must be positive. Changes to the new settings are picked up by running
leaders when configuration is reloaded. Larger windows improve throughput when
log latency is high; smaller windows leave less work ahead of newly arriving RPCs.

Existing ingestion and scheduling batches are admitted as a unit and can exceed
a window by one batch. Application acknowledgements can cover a whole append
batch, so credits may return conservatively. RPC proposals and forwarded ingestion
share the `network-service` window. Already-appended backlogs still need to drain.

### Upstream invoker backpressure

Invocation tasks now await space in a bounded output queue instead of continuously
draining SDK messages into an unbounded queue. This propagates sustained downstream
pressure back to protocol processing. The per-invoker queue defaults to 256 messages:

```toml
[worker.invoker]
task-output-queue-length = 256
```

This setting takes effect when the invoker is recreated, such as after a node
restart or leadership change. It limits queued message count, not total memory:
payload sizes, decoder/transport buffers, and an output retained by each task
waiting to send still contribute to memory use. Receive-to-propose latency includes
time spent waiting for this queue.

### Pipeline metrics

The following metrics have `partition` and `flow` labels:

- `restate.partition.self_proposer.inflight`: admitted log records awaiting local
  application acknowledgement.
- `restate.partition.self_proposer.window_blocked_ms.total`: cumulative milliseconds
  a flow's admission window is full. It continues updating while the window is full,
  even if the source has no additional queued work.
- `restate.partition.self_proposer.pending`: messages waiting in the invoker-effect
  or network-event input queue. This counts messages/batches, not log records, and
  does not include earlier queues inside the invoker or networking layer.
- `restate.partition.self_proposer.receive_to_propose.seconds`: a latency distribution
  for successfully proposed invoker effects and network messages. For the invoker,
  timing starts when the protocol runner emits its output, before both invoker
  queues. For network RPCs and ingestion, it starts at local RPC receipt before
  service-memory admission and partition-mailbox waits. Timing ends at successful
  enqueue into the Bifrost appender. Discarded work and internal invoker control
  effects without a receive timestamp are excluded. A network batch contributes
  one latency sample.

Under overload, rising invoker receive-to-propose latency and blocked time show
that waiting is moving upstream of the log. Compare those signals with RPC
receive-to-propose latency and the existing write-to-read latency metric.
