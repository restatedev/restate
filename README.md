[![Documentation](https://img.shields.io/badge/doc-reference-blue)](https://docs.restate.dev)
[![Discord](https://img.shields.io/badge/join-discord-purple)](https://discord.gg/skW3AZ6uGd)
[![Twitter](https://img.shields.io/twitter/follow/restatedev.svg?style=social&label=Follow)](https://twitter.com/intent/follow?screen_name=restatedev)

# Restate - Building resilient applications made easy!

[Restate](https://restate.dev) is a system for easily building resilient applications using distributed durable async/await.
This repository contains the Restate server and CLI.

The basic primitives Restate offers to simplify application development are the following:

* **reliable execution**: user code will always run to completion. Intermediate failures result in re-tries that use the durable execution mechanism to recover partial progress and not duplicate already executed steps.
* **suspending user code**: long-running user code suspends when awaiting on a promise and resume when that promise is resolved.
* **reliable communication**: user code communicates with exactly-once semantics. Restate reliably delivers messages and anchors both sender and receiver in the durable execution to ensure no losses or duplicates can happen.
* **durable timers**: user code can sleep (and suspend) or schedule calls for later.
* **isolation**: user code can be keyed, which makes Restate scheduled them to obey single-writer-per-key semantics.
* **consistent state**: keyed user code can attach key/value state, which is eagerly pushed into handlers during invocation, and written back upon completion. This is particularly efficient for FaaS deployments (stateful serverless, yay!).
* **observability & introspection**: Restate automatically generates Open Telemetry traces for the interactions between handlers and gives you a SQL shell to query the distributed state of the application.

Check [our GitHub org](https://github.com/restatedev) or the [docs](https://docs.restate.dev) for further details.

## Building Restate

In order to build Restate locally [follow the build instructions](https://github.com/restatedev/restate/blob/main/docs/dev/local-development.md#building-restate).

## Running Restate

You can start the runtime via:

```shell
just run --bin restate-server --release
```

or the latest release via docker:

```shell
docker run --name restate --rm --network=host docker.io/restatedev/restate
```

In order to change the log level, configure the [`RUST_LOG` env variable](https://rust-lang-nursery.github.io/rust-cookbook/development_tools/debugging/config_log.html#enable-log-levels-per-module) respectively.
For example, to enable debug mode for Restate crates:

```shell
RUST_LOG=info,restate=debug just run --bin restate-server --release
```

### Registering Restate services

After the runtime is running on `localhost:9070`, you can register a service running on `localhost:9080` via `curl`:

```shell
curl -X POST localhost:9070/endpoints -H 'content-type: application/json' -d '{"uri": "http://localhost:9080"}'
```

For more information check [how to register services](https://docs.restate.dev/services/registration).

### Invoking a Restate service

After registering a service you can invoke a service via via HTTP/JSON:

```shell
curl -X POST localhost:8080/counter.Counter/GetAndAdd -H 'content-type: application/json' -d '{"counter_name": "foobar", "value": 10 }'
```

For more information check [how to invoke services](https://docs.restate.dev/services/invocation).

