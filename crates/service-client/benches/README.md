# Service Client Benchmarks

Compares the custom H2 connection pool against hyper_util's legacy client and the raw h2 client using in-memory duplex streams.

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

## CPU profiling with samply (external)

[samply](https://github.com/mstange/samply) can profile the benchmark binary without any code changes.

```bash
# Build and select the benchmark executable (requires jq)
BENCHMARK=$(cargo bench -p restate-service-client --bench h2_pool_benchmark --no-run --message-format=json |
    jq -r 'select(.reason == "compiler-artifact" and .target.name == "h2_pool_benchmark" and .executable != null) | .executable')
samply record "$BENCHMARK" --bench "sequential/custom-pool" --profile-time=30
```

This opens the Firefox Profiler UI automatically.

## CPU profiling with cargo-flamegraph

```bash
cargo flamegraph -p restate-service-client --bench h2_pool_benchmark -- --bench "sequential/custom-pool" --profile-time=30
```

This writes `flamegraph.svg` in the current directory. The embedded `pprof`
integration has been removed; `--profile-time` runs the workload but requires an
external profiler to capture a profile. See the [profiling guide](../../../benchmarks/README.md#profiling-the-benchmarks)
for installation and platform requirements.
