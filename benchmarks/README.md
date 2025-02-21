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

Prerequisites:

* (Linux only) You need to set `kernel.perf_event_paranoid` to `-1` to allow perf collect all the events: `sudo sysctl kernel./perf_event_paranoid=-1`

In order to profile the benchmarks select a benchmark and pass the `--profile-time=<time_to_run>` option:

```shell
cargo bench --package restate-benchmarks --bench throughput_parallel -- --profile-time=30
```

On MacOS you need to enable the frame-pointer feature:

```shell
cargo bench --package restate-benchmarks --features frame-pointer --bench throughput_parallel -- --profile-time=30
```

This will profile the *throughput_parallel* benchmark for *30 s*.
The profiler will generate a flamegraph under `target/criterion/<name_of_benchmark>/profile/flamegraph.svg`.

## Changing Restate's configuration

The benchmarks spawn Restate with a default configuration.
You can [overwrite this configuration by specifying environment variables](https://docs.restate.dev/operate/configuration) of the form `RESTATE_WORKER__PARTITIONS=1337`.

## Changing the benchmark parameters

The parallel benchmark can be configured via environment variables:

* `BENCHMARK_REQUESTS`: Number of requests to send to the Restate runtime (default: 4000)
* `BENCHMARK_PARALLEL_REQUESTS`: Number of parallel requests (default: 1000)
* `BENCHMARK_SAMPLE_SIZE`: Number of samples to take (default: 20)