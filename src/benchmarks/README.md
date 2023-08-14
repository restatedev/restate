# Benchmarks

The benchmarks crate contains currently the following benchmarks:

* [sequential_throughput](benches/throughput_sequential.rs): Runs the Restate runtime and ingest counter.Counter/GetAndAdd requests sequentially (same key)
* [parallel_throughput](benches/throughput_parallel.rs): Runs the Restate runtime and ingest counter.Counter/GetAndAdd requests concurrently (random key)

## Prerequisites

The above-mentioned benchmarks require the [counter.Counter service](https://github.com/restatedev/e2e/blob/a500164a31d58c0ee65ae77a7f99a8a2ef1825cb/services/node-services/src/counter.ts) running on `localhost:8080`. 
You can use both the Java or the Node service.

To start the Java service:

```shell
SERVICES=counter.Counter LOG4J_CONFIGURATION_FILE=unknown gradle :services:java-services:run
```

To start the Node service:

```shell
SERVICES=counter.Counter gradle :services:node-services:npm_run_app
```

See the [node services' readme](https://github.com/restatedev/e2e/blob/a500164a31d58c0ee65ae77a7f99a8a2ef1825cb/services/node-services/README.md) for more details.

## Running the benchmarks

All benchmarks can be run via:

```shell
cargo bench 
```

To run a single benchmark run it via:

```shell
cargo bench --bench throughput_parallel
```

## Profiling the benchmarks

Prerequisites:

* (Linux only) You need to set `kernel.perf_event_paranoid` to `-1` to allow perf collect all the events: `sudo sysctl kernel.perf_event_paranoid=-1`

In order to profile the benchmarks select a benchmark and pass the `--profile-time=<time_to_run>` option:

```shell
cargo bench --bench throughput_parallel -- --profile-time=30
```

On MacOS you need to enable the frame-pointer feature:

```shell
cargo bench --features frame-pointer --bench throughput_parallel -- --profile-time=30
```

This will profile the *throughput_parallel* benchmark for *30 s*.
The profiler will generate a flamegraph under `target/criterion/<name_of_benchmark>/profile/flamegraph.svg`.

## Changing Restate's configuration

The benchmarks spawn Restate with a default configuration.
You can [overwrite this configuration by specifying environment variables](https://docs.restate.dev/restate/configuration) of the form `RESTATE_WORKER__PARTITIONS=1337`.
