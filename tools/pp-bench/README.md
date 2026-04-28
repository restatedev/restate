# PP-Bench

A benchmark tool for the partition processor state machine replay path. It drives `StateMachine::apply()` directly (bypassing Bifrost) against a real RocksDB-backed `PartitionStore`, measuring throughput, latency, CPU, and memory in isolation.

## Architecture

The tool exercises the state machine apply + RocksDB commit path in **follower mode** (`is_leader: false`), which means the state machine processes commands and updates storage but does not emit actions to actuators. This isolates the pure state-machine + storage performance from networking, invoker, and scheduler overhead.

Everything — the apply loop, the interval reporter, and the metrics HTTP server — runs on a **single-threaded tokio `LocalRuntime`**. Tasks interleave at `.await` points (mostly RocksDB I/O in the apply path, timer ticks for the reporter).

**Note: this does NOT exactly match production scheduling.** In production, each partition processor runs on its own dedicated `current_thread` runtime in a separate OS thread, while infrastructure tasks (metrics server, tracing, network I/O) run on a separate multi-thread runtime. Here we collapse everything onto one thread to keep the tool simple. The trade-off is that background tasks (interval reporter ticks, metrics scrape handling) compete with the apply loop for thread time, which can perturb measurements slightly compared to the isolated production runtime. For most benchmarking purposes this is acceptable; if you need the most accurate apply-loop measurements, set `--metrics-port 0` and use a long `--report-interval`.

Data is stored under `./restate-data/<node-name>/`, following the standard Restate server layout. Each run generates a unique node name (`pp-bench-<timestamp>`) by default, so runs don't interfere with each other or with a local development server.

## Subcommands

### `generate`

Pre-generate a synthetic command file offline. No TaskCenter or RocksDB needed. Commands are generated and written one at a time with constant memory overhead.

### `extract`

Extract a **snapshot bundle** from a stopped Restate server's data directory. The bundle contains a RocksDB checkpoint of the partition store and the log records that follow it. This captures a real workload for repeatable replay benchmarking.

### `run`

Run the benchmark. Loads commands from a file (via `--command-file`) or generates them in-memory before measurement. Applies commands in batched transactions, measures per-batch latency (HDR histogram), and reports throughput.

### `inspect`

Print metadata and a sample of decoded commands from a command file. Works with both generated and extracted files.

## Workloads

### Synthetic

| Workload | Command type | What it exercises |
|----------|-------------|-------------------|
| `patch-state` | `PatchState(ExternalStateMutation)` | State table reads/writes, virtual object status lookups. Simplest workload, no pre-existing state needed. |
| `invoke` | `Invoke(ServiceInvocation)` | Invocation status table, journal table, virtual object locking, inbox, outbox. Exercises more storage tables. |

### Extracted (real workloads)

The `extract` subcommand captures a real workload from a stopped Restate server. It produces a snapshot bundle directory containing:

- `partition-store/` -- RocksDB checkpoint (hardlinked SSTs, near-instant to create)
- `commands.bin` -- log records extracted from Bifrost, with original LSNs
- `metadata.json` -- bundle metadata (applied_lsn, partition_id, command count)

Extracted command files carry real LSNs and preserve the full `Envelope` (header + command) from the Bifrost log.

## How to run

### Synthetic workloads

```sh
# Quick one-shot benchmark (generates commands in-memory, runs, reports)
RUST_LOG=warn cargo run --release -p pp-bench -- \
    run --workload patch-state --num-commands 1000000 --batch-size 10

# Separated workflow: generate once, benchmark many times
cargo run --release -p pp-bench -- \
    generate -o commands.bin --workload invoke --num-commands 1000000

# Inspect the generated file
cargo run --release -p pp-bench -- inspect commands.bin

# Run from file
RUST_LOG=warn cargo run --release -p pp-bench -- \
    run --command-file commands.bin --batch-size 10

# JSON output for CI
RUST_LOG=warn cargo run --release -p pp-bench -- --json \
    run --command-file commands.bin --batch-size 10 > results.json

# A/B comparison across code changes
RUST_LOG=warn cargo run --release -p pp-bench -- --json \
    run --command-file commands.bin --batch-size 10 > before.json
# ... make code changes, rebuild ...
RUST_LOG=warn cargo run --release -p pp-bench -- --json \
    run --command-file commands.bin --batch-size 10 > after.json
```

### Extracting a real workload

```sh
# 1. Run a Restate server with your workload and log trimming disabled
# 2. Stop the server
# 3. Extract
cargo run --release -p pp-bench -- \
    extract --data-dir ./restate-data/<node-name> -o ./my-snapshot

# Inspect the extracted commands
cargo run --release -p pp-bench -- inspect ./my-snapshot/commands.bin

# Replay the extracted commands
RUST_LOG=warn cargo run --release -p pp-bench -- \
    run --command-file ./my-snapshot/commands.bin --batch-size 10
```

**Note:** The `extract` command requires exclusive access to the server's RocksDB databases, so the server must be stopped. Log trimming must be disabled so that records are available for extraction.

**Limitation:** The current implementation assumes single-segment log chains (the common case for single-partition setups). Multi-segment chains are not yet supported.

### Key options for `run`

| Flag | Description | Default |
|------|-------------|---------|
| `--command-file <PATH>` | Load commands from a file (generated or extracted) | (generate in-memory) |
| `--batch-size <N>` | Commands per RocksDB transaction commit | 1 |
| `--num-commands <N>` | Commands to apply during measurement | 1,000,000 |
| `--warmup <N>` | Warmup commands before measurement | 50,000 |
| `--workload <TYPE>` | `patch-state` or `invoke` (for in-memory generation) | `patch-state` |
| `--num-keys <N>` | Distinct virtual-object keys (key-space cardinality) | 10,000 |
| `--seed <N>` | RNG seed for deterministic generation | 42 |
| `--base-dir <PATH>` | Override the data directory (default: `./restate-data`) | |
| `--node-name <NAME>` | Override the unique node name | `pp-bench-<timestamp>` |
| `--config-file <PATH>` | Restate TOML config file (for RocksDB tuning, etc.) | |

### Key options for `generate`

| Flag | Description | Default |
|------|-------------|---------|
| `-o, --output <PATH>` | Output file path (required) | |
| `--num-commands <N>` | Number of commands to generate | 1,000,000 |
| `--workload <TYPE>` | `patch-state` or `invoke` | `patch-state` |
| `--num-keys <N>` | Distinct virtual-object keys | 10,000 |
| `--state-entries <N>` | State entries per PatchState mutation | 1 |
| `--value-size <BYTES>` | Size of each state value | 128 |
| `--seed <N>` | RNG seed | 42 |

### Key options for `extract`

| Flag | Description | Default |
|------|-------------|---------|
| `--data-dir <PATH>` | Node data directory (required) | |
| `-o, --output <PATH>` | Output snapshot bundle directory (required) | |
| `--partition-id <N>` | Partition to extract | 0 |
| `--from-lsn <N>` | Start LSN (default: partition store's applied_lsn + 1) | |
| `--to-lsn <N>` | End LSN (inclusive, default: end of log) | |
| `--limit <N>` | Maximum commands to extract | |

## CPU profiling

The benchmark is a plain binary with jemalloc as the global allocator (matching production). Profile it with any standard tool:

```sh
# samply (recommended on macOS)
samply record cargo run --release -p pp-bench -- \
    run --command-file commands.bin --batch-size 10 --metrics-port 0

# perf (Linux)
perf record -g cargo run --release -p pp-bench -- \
    run --command-file commands.bin --batch-size 10 --metrics-port 0

# cargo-flamegraph
cargo flamegraph --release -p pp-bench -- \
    run --command-file commands.bin --batch-size 10 --metrics-port 0
```

## Observability

### Prometheus metrics endpoint

By default, an HTTP metrics server listens on port 9090. Point Prometheus/Grafana at `http://localhost:9090/metrics` to observe RocksDB internals in real time during a run. Use `--metrics-port 0` to disable.

### End-of-run summary

At completion, the tool prints styled tables covering:
- **Config & throughput** - commands/s, batches/s
- **Latency** - HDR histogram percentiles (p50 through max) for batch commit time
- **CPU** - user/system time, wall time, average utilisation
- **Memory** - jemalloc allocated/active/resident/mapped/retained
- **RocksDB** - write stalls, WAL syncs/bytes, flush/compaction I/O, block cache hit rate

Use `--raw-rocksdb-stats` to additionally dump the full raw RocksDB statistics string.

### JSON output

Use `--json` to emit a machine-readable JSON summary to stdout (all other output goes to stderr). This is designed for CI regression detection: compare `commands_per_sec` and latency percentiles across runs.

## Storage

Data is stored under `./restate-data/<node-name>/db/`, following the standard Restate server layout. Each run gets a unique node name by default so runs don't clobber each other. To inspect RocksDB after a run, pass `--node-name my-run` and look in `./restate-data/my-run/db/`.

To tune RocksDB settings, use `--config-file` with a standard Restate TOML config.

## CI integration

For regression detection in CI:

1. Check a command file into the repo (or generate in CI setup): `pp-bench generate -o bench-input.bin --workload invoke --num-commands 500000`
2. Run with `--json`: `pp-bench --json run --command-file bench-input.bin --batch-size 10 > results.json`
3. Compare `commands_per_sec` and latency percentiles against a stored baseline
4. Fixed seed + fixed command file = identical workload every time
5. Warmup phase (default 50,000 commands) lets RocksDB caches stabilize before measurement

## Planned features

The following features are designed but not yet implemented:

- **`--snapshot` replay** -- Clone an extracted partition-store checkpoint into the benchmark's working directory and replay commands on top of real accumulated state. This enables realistic benchmarking where the state machine starts from a non-empty partition store.
- **`--restart-every N`** -- Simulate state machine stop/restart cycles during replay. Flushes RocksDB, re-reads FSM state, and re-creates the StateMachine every N commands. Measures recovery cost and performance degradation under accumulating state.
