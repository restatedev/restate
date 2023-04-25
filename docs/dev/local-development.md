# Local development environment

This file contains recommendations for how to set up your local development environment.

## Required dependencies

To build the project, you need the Rust toolchain. Follow the guide here https://rustup.rs/ to setup Rust, Cargo, etc.

The project contains some Rust libraries binding to native libraries/build tools, which you'll require during the development, notably:

* [Clang](https://clang.llvm.org/)
* [CMake](https://cmake.org/)
* [OpenSSL](https://www.openssl.org/)
* [RocksDB](http://rocksdb.org/)

To setup these on Fedora, run:

```
sudo dnf install clang lld lldb libcxx cmake openssl-devel rocksdb-devel
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

Restate supports exposing tracing information via opentelemetry.

### Using Jaeger

In order publish tracing information to [Jaeger](https://www.jaegertracing.io/), you have to enable the feature `jaeger`.
Moreover, you have to start Jaeger.

#### Starting Jaeger on Linux

To start Jaeger, run:

```shell
docker run -d -p16686:16686 -p6831:6831/udp --name jaeger jaegertracing/all-in-one:latest
```

#### Starting Jaeger on MacOS

When using the [LimaVM](https://github.com/lima-vm/lima) for running Docker containers on MacOS, there is a [problem with forwarding UDP ports](https://github.com/lima-vm/lima/issues/366).
As a workaround, you can [start the jaeger-agent locally](https://www.jaegertracing.io/docs/1.6/getting-started/#running-individual-jaeger-components) via

```shell
git clone git@github.com:jaegertracing/jaeger.git jaeger
cd jaeger
go run ./cmd/agent/main.go --reporter.grpc.host-port localhost:14250
```

Note that you have to forward the grpc port `14250` for the Jaeger container:

```shell
docker run -d -p16686:16686 -p14250:14250 jaegertracing/all-in-one:latest
```

Moreover, you have to set the maximum udp package size to 65536 via

```shell
sudo sysctl net.inet.udp.maxdgram=65536
```

### Using Zipkin

In order to publish tracing information to [Zipkin](https://zipkin.io/), you hae to enable the feature `zipkin`.

Moreover, you have to start Zipkin via

```shell
docker run -d -p 9411:9411 --name zipkin openzipkin/zipkin
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
