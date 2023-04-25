# Restate - Building distributed applications made simple

The Restate runtime together with its SDKs simplifies the development of distributed applications by:

* providing reliable messaging
* supporting durable method execution
* storing state consistently
* handling timers

and much more.

## SDKs

Restate supports the following SDKs:

* [Java and Kotlin](https://github.com/restatedev/sdk-java)
* [JavaScript and Typescript](https://github.com/restatedev/typescript-sdk-expirment)

## Running the runtime

You can start the runtime via:

```shell
just run --release
```

In order to change the log level, configure the [`RUST_LOG` env variable](https://rust-lang-nursery.github.io/rust-cookbook/development_tools/debugging/config_log.html#enable-log-levels-per-module) respectively.

For example, to enable debug mode for Restate crates:

```shell
RUST_LOG=info,restate=debug just run --release
```

### Registering Restate services

After the runtime is running, you can register a service running on `localhost:8080` via `curl`:

```shell
curl -X POST localhost:8081/services/discover -H 'content-type: application/json' -d '{"uri": "http://localhost:8080"}'
```

This assumes that the runtime is running on `localhost:8081`.

To check the OpenAPI documentation of the available operational APIs:

```shell
curl localhost:8081/openapi
```

### Invoking a Restate service

After registering a service you can invoke a service via [grpcurl](https://github.com/fullstorydev/grpcurl):

```shell
grpcurl -plaintext -d '{"counter_name": "foobar", "value": 10}' localhost:9090 counter.Counter/GetAndAdd
```

or using [grpcui](https://github.com/fullstorydev/grpcui):

```shell
grpcui -plaintext localhost:9090
```

or via HTTP/JSON that gets transcoded to gRPC:

```shell
curl -X POST localhost:9090/counter.Counter/GetAndAdd -H 'content-type: application/json' -d '{"counter_name": "foobar", "value": 10 }'
```
