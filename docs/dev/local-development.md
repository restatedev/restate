# Local development environment

This file contains recommendations for how to set up your local development environment.

## Building Restate

### Required dependencies

To build the project, you need the Rust toolchain. Follow the guide here <https://rustup.rs/> to set up Rust, Cargo, etc.

The project contains some Rust libraries binding to native libraries/build tools, which you'll require during the development, notably:

* [Clang](https://clang.llvm.org/)
* [CMake](https://cmake.org/)
* [OpenSSL](https://www.openssl.org/)
* [RocksDB](http://rocksdb.org/) if you plan to dynamically link RocksDB you must use [the following fork](https://github.com/restatedev/rocksdb/tree/restate) until [this PR](https://github.com/facebook/rocksdb/pull/12968) is merged into the upstream
* [Protobuf compiler](https://grpc.io/docs/protoc-installation/) version >= 3.15

Optionally, you can install [just](https://github.com/casey/just) to make use of our [justfile](https://github.com/restatedev/restate/blob/main/justfile).

To set up these on Fedora, run:

```shell
sudo dnf install clang lld lldb libcxx cmake openssl-devel rocksdb-devel protobuf-compiler protobuf-devel just liburing-devel perl
```

On MacOS, you can use [homebrew](https://brew.sh)

```shell
brew install cmake protobuf just
```

Optionally, you can install node and Java for supporting tools and examples:

```shell
brew install node openjdk
```

If you choose to install OpenJDK via homebrew, you'll also need to link it so that it's available through the system-level wrappers.

```shell
sudo ln -sfn /opt/homebrew/opt/openjdk/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk.jdk
```

### Building on MacOS (M1)

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

### Building the binaries

In order to build the `restate-server` binary run:

```shell
just build --bin restate-server [--release]
```

In order to build the `restate-cli` binary run:

```shell
just build --bin restate [--release]
```

### Running the unit tests

We recommend cargo-nextest <https://nexte.st/> to run our unit tests

```shell
just test
```

or directly with cargo:

```shell
cargo nextest run --workspace
```

## Speeding builds up via sccache

In order to speed up the build process, one can install the [sccache](https://github.com/mozilla/sccache) which caches build artifacts of `rustc`.

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

Due to <https://github.com/intellij-rust/intellij-rust/issues/10847>, disable Rust macro expansion feature of the Rust IntelliJ plugin,
as described here <https://plugins.jetbrains.com/plugin/8182-rust/docs/rust-project-settings.html#general-settings>.

### Build the configuration documentation

Requirements:

* [`generate-schema-doc`](https://github.com/coveooss/json-schema-for-humans#installation)

To generate the JSON schema:

```shell
cargo xtask generate-config-schema > restate_config_schema.json 
```

To generate the HTML documentation:

```shell
generate-schema-doc --minify restate_config_schema.json restate_config_doc.html 
```

The schema can be associated to `restate.yaml` in Jetbrains IDEs to enable autocompletion: <https://www.jetbrains.com/help/idea/json.html#ws_json_using_schemas>

### Build the REST API documentation

Requirements:

* [`npx`](https://www.npmjs.com/package/npx)

To generate the OpenAPI file:

```shell
cargo xtask generate-rest-api-doc > openapi.json
```

To generate the HTML documentation:

```shell
npx @redocly/cli build-docs openapi.json
```

## Performance analysis

Requirements:

* [flamegraph](https://github.com/flamegraph-rs/flamegraph#installation)

For performance analysis you can generate a flamegraph of the runtime binary via:

```shell
just flamegraph --bin restate-server
```

This command will produce a `flamegraph.svg` in the current working directory when the process is stopped.

See the [flamegraph documentation](https://github.com/flamegraph-rs/flamegraph#usage) for more details.

## Troubleshooting
On many systems `cargo build` invokes `gcc` by default. Some `gcc` versions fail to compile certain dependencies, leading to errors like:
```
.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/krb5-src-0.3.4/krb5/src/lib/rpc/auth_none.c: In function 'authnone_wrap':
  /home/slinkydeveloper/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/krb5-src-0.3.4/krb5/src/lib/rpc/auth_none.c:146:18: error: too many arguments to function 'xfunc'; expected 0, have 2
    146 |         return ((*xfunc)(xdrs, xwhere));
        |                 ~^~~~~~~ ~~~~
  make[1]: *** [Makefile:500: auth_none.o] Error 1
  make: *** [Makefile:1005: all-recurse] Error 1
```
If you see this type of failure, rebuild with `clang`:
```
CC=clang cargo build
```
When that resolves the issue, you can make `clang` the default compiler by adding the following to `~/.cargo/config.toml`:
```
# cat ~/.cargo/config.toml
 
[env]
CC = "clang"
CXX = "clang++"
```
