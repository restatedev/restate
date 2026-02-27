# Log-Server Bench

A load-generation tool for benchmarking the Restate log-server's RocksDB storage layer in isolation. It drives configurable workloads (writes, reads, trims) through the `LogletWorker` pipeline using `ServiceMessage::fake_rpc()`, bypassing all networking.

## Architecture

The tool exercises the full write path: **LogletWorker -> LogStoreWriter batching -> WriteBatch -> RocksDB commit**, without spinning up a full Restate node or any networking stack. This makes it suitable for profiling and tuning RocksDB column family configurations.

## Benchmarks

### `write-throughput`

Sequential write benchmark. Sends `Store` batches into one or more loglet workers and measures per-batch latency (HDR histogram) and aggregate throughput.

### `mixed-workload`

Duration-based concurrent workload. Spawns per-loglet writer, reader, and trimmer tasks that run simultaneously for a configurable duration. Writers send `Store` batches, readers issue `GetRecords` trailing behind the writer, and trimmers issue periodic `Trim` operations.

### `generate-payload`

Offline subcommand that pre-generates a deterministic payload file. The file can then be loaded via `--payload-file` on `write-throughput` or `mixed-workload` runs to:

- **Eliminate RNG overhead** during the measured phase
- **Ensure identical data** across repeated runs for fair A/B comparisons

## How to run

```sh
# Basic write throughput test
RUST_LOG=info cargo run --profile=bench --bin logserver-bench -- \
    write-throughput --num-records 5000000 --records-per-batch 10 --payload-size 512

# Mixed workload with reads and trims for 60 seconds
RUST_LOG=info cargo run --profile=bench --bin logserver-bench -- \
    mixed-workload --duration 60s --num-loglets 10 --enable-reads --enable-trims

# With a custom RocksDB config
RUST_LOG=info cargo run --profile=bench --bin logserver-bench -- \
    --config-file=restate.toml --retain-test-dir \
    write-throughput --num-records 10000000

# Pre-generate a payload file, then use it
cargo run --profile=bench --bin logserver-bench -- \
    generate-payload --output payloads.bin \
    --records-per-batch 10 --num-batches 2048 \
    --payload-size 1KB --entropy 0.8 --seed 42

RUST_LOG=info cargo run --profile=bench --bin logserver-bench -- \
    write-throughput --num-records 5000000 --records-per-batch 10 \
    --payload-file payloads.bin
```

## Observability

### Prometheus metrics endpoint

By default, an HTTP metrics server listens on port 9090. Point Prometheus/Grafana at `http://localhost:9090/metrics` to observe RocksDB internals in real time during a run:

- Tickers: block cache hit/miss, compaction bytes, stall micros, WAL syncs
- Histograms: db.write, db.get, wal.file.sync, sst.read/write latencies
- CF properties: num-files-at-levelN, estimate-pending-compaction-bytes, is-write-stopped, live-sst-files-size, mem-table sizes
- Write buffer manager usage and capacity

Use `--metrics-port 0` to disable.

### End-of-run summary

At completion, the tool prints styled KV tables covering:
- **Config & throughput** — records/s, batches/s, MB/s
- **Latency** — HDR histogram percentiles (P50 through max)
- **CPU** — user/system time, wall time, average utilisation
- **Memory** — jemalloc allocated/active/resident/mapped/retained
- **RocksDB** — write stalls, WAL syncs/bytes, flush/compaction I/O, block cache hit rate, LSM level file counts, live SST size, pending compaction, write buffer usage

Use `--raw-rocksdb-stats` to additionally dump the full raw RocksDB statistics string.

## Payload generation

Payloads are pre-generated into a pool of batches and cycled through during the benchmark. This avoids allocation overhead in the hot path. There are two modes:

### In-memory (default)

When no `--payload-file` is specified, batches are generated in-memory from an RNG at startup:

| Flag | Description | Default |
|------|-------------|---------|
| `--payload-size` | Body size per record (e.g. "512", "4KB", "1MiB") | 512 |
| `--entropy` | 0.0 = all zeros (compressible), 1.0 = random (incompressible) | 1.0 |
| `--key-style` | Record key type: `none`, `single`, `pair`, `range` | none |
| `--seed` | PRNG seed for reproducible payloads | random |

### From file (`--payload-file`)

Pass `--payload-file path/to/payloads.bin` to load pre-generated batches from a file created by `generate-payload`. The tool validates that the file's `batch_size` matches `--records-per-batch` and errors out on mismatch. When loading from file, the `--payload-size`, `--entropy`, `--key-style`, and `--seed` flags on the benchmark subcommand are ignored (the file's parameters take precedence).

The payload file uses a compact binary format with a 40-byte self-describing header (magic, version, payload size, key style, batch size, count, seed) followed by raw record data.

## Tuning RocksDB

RocksDB configuration is controlled through the standard Restate TOML config file (`--config-file`). The log-server uses two column families:

- **`data`** — 85% of memory budget, 7 LSM levels, Zstd compression, prefix extractor (9 bytes)
- **`metadata`** — 15% of memory budget, 3 LSM levels, merge operator for trim points

Use `--retain-test-dir` (or `--base-dir`) to persist the RocksDB directory between runs for inspecting SST files or running follow-up experiments on existing data.
