# Benchmarks

The benchmarks crate contains currently the following benchmarks:

* [sequential_throughput](benches/throughput_sequential.rs): Runs the Restate runtime and ingest counter.Counter/GetAndAdd requests sequentially (same key)
* [parallel_throughput](benches/throughput_parallel.rs): Runs the Restate runtime and ingest counter.Counter/GetAndAdd requests concurrently (random key)

## Running the benchmarks

All benchmarks can be run via:

```shell
cargo bench --package restate-benchmarks 
```

To run a single benchmark run it via:

```shell
cargo bench --package restate-benchmarks --bench throughput_parallel
```

## Profiling the benchmarks

Use an external profiler: [samply](https://github.com/mstange/samply) for an interactive
profile, or [cargo-flamegraph](https://github.com/flamegraph-rs/flamegraph) for an SVG.
Install the chosen tool with `cargo install --locked samply` or `cargo install --locked flamegraph`.
On Linux, configure perf-event permissions as described in the profiler's documentation.
The workspace's bench profile already enables debug symbols.

### Samply

Build first, then profile the benchmark executable rather than Cargo. This example
uses `jq` to select the executable from Cargo's JSON output:

```shell
BENCHMARK=$(cargo bench -p restate-benchmarks --bench throughput_parallel --no-run --message-format=json |
    jq -r 'select(.reason == "compiler-artifact" and .target.name == "throughput_parallel" and .executable != null) | .executable')
samply record "$BENCHMARK" --bench 'throughput/parallel' --profile-time=30
```

### Cargo-flamegraph

```shell
cargo flamegraph -p restate-benchmarks --bench throughput_parallel -- --bench 'throughput/parallel' --profile-time=30
```

This writes `flamegraph.svg` in the current directory. Criterion's `--profile-time`
runs the selected workload for profiling without collecting benchmark statistics.
It no longer generates an SVG by itself; the embedded `pprof` integration and its
`frame-pointer` Cargo feature have been removed.

## Changing Restate's configuration

The benchmarks spawn Restate with a default configuration.
You can [overwrite this configuration by specifying environment variables](https://docs.restate.dev/server/configuration#overrides) of the form `RESTATE_WORKER__PARTITIONS=1337`.

## Changing the benchmark parameters

The parallel benchmark can be configured via environment variables:

* `BENCHMARK_REQUESTS`: Number of requests to send to the Restate runtime (default: 4000)
* `BENCHMARK_PARALLEL_REQUESTS`: Number of parallel requests (default: 1000)
* `BENCHMARK_SAMPLE_SIZE`: Number of samples to take (default: 20)
