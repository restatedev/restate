# Service Client Benchmarks

Compares the custom H2 connection pool against hyper_util's legacy client using in-memory duplex streams.

## Benchmark groups

| Group | What it measures |
|-------|-----------------|
| `sequential` | Single request latency |
| `concurrent/{10,50}` | Throughput under H2 multiplexing |
| `body-{1KB,64KB}` | Data echo throughput |

## Running benchmarks

Run all benchmarks:

```bash
cargo bench -p restate-service-client --bench h2_pool_benchmark
```

Dry-run (verify they execute without measuring):

```bash
cargo bench -p restate-service-client --bench h2_pool_benchmark -- --test
```

Run a single benchmark by name filter:

```bash
cargo bench -p restate-service-client --bench h2_pool_benchmark -- "sequential/custom-pool"
```

## CPU profiling with pprof (built-in)

The benchmarks include [pprof](https://github.com/tikv/pprof-rs) integration that generates flamegraph SVGs.

### Prerequisites

On Linux, allow perf events:

```bash
sudo sysctl kernel.perf_event_paranoid=-1
```

### Profile a benchmark

Pass `--profile-time=<seconds>` to activate profiling:

```bash
cargo bench -p restate-service-client --bench h2_pool_benchmark -- "sequential/custom-pool" --profile-time=30
```

The flamegraph is written to:

```
target/criterion/sequential/custom-pool/profile/flamegraph.svg
```

Open it in a browser for an interactive view.

## CPU profiling with samply (external)

[samply](https://github.com/mstange/samply) can profile the benchmark binary without any code changes.

```bash
# Build the benchmark binary (release mode)
cargo bench -p restate-service-client --bench h2_pool_benchmark --no-run

# Find and run with samply
samply record target/release/deps/h2_pool_benchmark-* --bench "sequential/custom-pool" --profile-time=30
```

This opens the Firefox Profiler UI automatically.
