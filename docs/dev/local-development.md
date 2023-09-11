# Local development environment

This file contains recommendations for how to set up your local development environment.

## Required dependencies

To build the project, you need the Rust toolchain. Follow the guide here https://rustup.rs/ to setup Rust, Cargo, etc.

The project contains some Rust libraries binding to native libraries/build tools, which you'll require during the development, notably:

* [Clang](https://clang.llvm.org/)
* [CMake](https://cmake.org/)
* [OpenSSL](https://www.openssl.org/)
* [RocksDB](http://rocksdb.org/)
* [Protobuf compiler](https://grpc.io/docs/protoc-installation/) version >= 3.15

To setup these on Fedora, run:

```
sudo dnf install clang lld lldb libcxx cmake openssl-devel rocksdb-devel protobuf-compiler
```

## Speeding builds up via sccache

In order to speed up the build process, one can install the [sccache](https://github.com/mozilla/sccache) which caches build artifacts of `rustc`.

## Building on MacOS (M1)

In order to build the runtime on MacOS (M1) you need to export `CMAKE_OSX_ARCHITECTURES="arm64"`.
This will prevent `librdkafka` from building a [fat binary that fails to be linked on MacOS (M1)](https://github.com/rust-lang/cargo/issues/8875).

You can also add the following section to your `~/.cargo/config.toml` to make cargo always run with this environment variable:

```toml
[env]
# Fix architecture to arm64 to make rdkafka build. Without this env var, the
# librdkafka build script will generate a fat binary that fails to be linked
# on MacOS. See https://github.com/rust-lang/cargo/issues/8875 and
# https://github.com/rust-lang/rust/issues/50220#issuecomment-433070637 for
# more details.
CMAKE_OSX_ARCHITECTURES = "arm64"
```

## Tracing

It is useful to enable tracing when testing the runtime. 
To set up the runtime to publish traces to Jaeger, refer to the observability documentation in the Restate official documentation.

#### Starting Jaeger on Linux and MacOS

To start Jaeger, run:

```shell
docker run --rm -d -p16686:16686 -p4317:4317 -e COLLECTOR_OTLP_ENABLED=true --name jaeger jaegertracing/all-in-one:latest
```

## Setting up Knative

This section contains guides for how to set up [Knative](https://knative.dev/) in different local setups.
If your setup is missing, then please add a description for it.

### Knative on Kind

If you are running [Kind](https://kind.sigs.k8s.io/docs/user/quick-start/) as your local K8s environment, then please follow [this quickstart](https://knative.dev/docs/install/quickstart-install/) in order to install Knative.

### Knative on Rancher desktop

If you are running [Rancher desktop](https://rancherdesktop.io/) as your local K8s environment, then you have to disable Traefik first. 
This can be done in the Rancher desktop UI under 'Kubernetes Settings' and requires a restart of Rancher.
Next you can install Knative via:

```shell
KNATIVE_VERSION=1.8.0 curl -sL https://raw.githubusercontent.com/csantanapr/knative-minikube/master/install.sh | bash
```

## Runtime documentation

This section explains how to generate the configuration documentation and REST api documentation.

### Workaround for stuck _Analysis..._ in CLion

Due to https://github.com/intellij-rust/intellij-rust/issues/10847, disable Rust macro expansion feature of the Rust IntelliJ plugin, 
as described here https://plugins.jetbrains.com/plugin/8182-rust/docs/rust-project-settings.html#general-settings.

### Build the configuration documentation

Requirements:

* [`generate-schema-doc`](https://github.com/coveooss/json-schema-for-humans#installation)

To generate the JSON schema:

```shell
$ cargo xtask generate-config-schema > restate_config_schema.json 
```

To generate the HTML documentation:

```shell
$ generate-schema-doc --minify restate_config_schema.json restate_config_doc.html 
```

The schema can be associated to `restate.yaml` in Jetbrains IDEs to enable autocompletion: https://www.jetbrains.com/help/idea/json.html#ws_json_using_schemas

### Build the REST API documentation

Requirements:

* [`npx`](https://www.npmjs.com/package/npx)

To generate the OpenAPI file:

```shell
$ cargo xtask generate-rest-api-doc > openapi.json
```

To generate the HTML documentation:

```shell
$ npx @redocly/cli build-docs openapi.json
```

## Performance analysis

Requirements:

* [flamegraph](https://github.com/flamegraph-rs/flamegraph#installation)

For performance analysis you can generate a flamegraph of the runtime binary via:

```shell
$ just flamegraph --bin restate
```

This command will produce a `flamegraph.svg` in the current working directory when the process is stopped.

See the [flamegraph documentation](https://github.com/flamegraph-rs/flamegraph#usage) for more details.
