[![Documentation](https://img.shields.io/badge/doc-reference-blue)](https://docs.restate.dev)
[![Examples](https://img.shields.io/badge/view-examples-blue)](https://github.com/restatedev/examples)
[![Discord](https://img.shields.io/discord/1128210118216007792?logo=discord)](https://discord.gg/skW3AZ6uGd)
[![Twitter](https://img.shields.io/twitter/follow/restatedev.svg?style=social&label=Follow)](https://twitter.com/intent/follow?screen_name=restatedev)

# Restate - Building resilient applications made easy!

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://restate.dev/poster_intro_dark.svg">
    <source media="(prefers-color-scheme: light)" srcset="https://restate.dev/poster_intro_2.svg">
    <img alt="Restate overview" src="https://restate.dev/poster_intro_2.svg" width="500">
  </picture>
</p>

Easily build workflows, event-driven applications, and distributed services in a fault-tolerant manner with durable async/await.

[Restate](https://restate.dev) is great at building:

* [Lambda Workflows as Code](https://restate.dev/blog/we-replaced-400-lines-of-stepfunctions-asl-with-40-lines-of-typescript-by-making-lambdas-suspendable/)
* [Transactional RPC Handlers](https://github.com/restatedev/examples/tree/main/typescript/ecommerce-store)
* [Event Processing with Kafka](https://restate.dev/blog/restate--kafka-event-driven-apps-where-event-driven-is-an-implementation-detail/)
* [Much more](https://github.com/restatedev/examples)

## Get started with Restate

1. 🏎 [Check out our quickstart](https://docs.restate.dev/quickstart) to get up and running with Restate in 2 minutes!
1. 💡 [The tour of Restate](https://docs.restate.dev/tour) walks you through all features of Restate.

## SDKs

Restate supports the following SDKs:

* [Typescript](https://github.com/restatedev/sdk-typescript)
* [Java and Kotlin](https://github.com/restatedev/sdk-java)

## Install

We offer pre-built binaries of the CLI and the server for MacOS and Linux.

### Install the server

Install via Homebrew:
```bash
brew install restatedev/tap/restate-server
```

Run via npx:
```bash
npx @restatedev/restate-server
```

Run via docker:
```bash
docker run --rm -it --network=host docker.io/restatedev/restate:latest
```

### Install the CLI

Install via Homebrew:
```bash
brew install restatedev/tap/restate
```

Install via npm:
```bash
npm install --global @restatedev/restate
```

You can also download the binaries from the [release page](https://github.com/restatedev/restate/releases) or our [download page](https://restate.dev/get-restate/).

## Community

* 🤗️ [Join our online community](https://discord.gg/skW3AZ6uGd) for help, sharing feedback and talking to the community.
* 📖 [Check out our documentation](https://docs.restate.dev) to get quickly started!
* 📣 [Follow us on Twitter](https://twitter.com/restatedev) for staying up to date.
* 🙋 [Create a GitHub issue](https://github.com/restatedev/restate/issues) for requesting a new feature or reporting a problem.
* 🏠 [Visit our GitHub org](https://github.com/restatedev) for exploring other repositories.

## Core primitives

The basic primitives Restate offers to simplify application development are the following:

* **Reliable Execution**: user code will always run to completion. Intermediate failures result in re-tries that use the durable execution mechanism to recover partial progress and not duplicate already executed steps.
* **Suspending User Code**: long-running user code suspends when awaiting on a promise and resume when that promise is resolved.
* **Reliable Communication**: user code communicates with exactly-once semantics. Restate reliably delivers messages and anchors both sender and receiver in the durable execution to ensure no losses or duplicates can happen.
* **Durable Timers**: user code can sleep (and suspend) or schedule calls for later.
* **Isolation**: user code can be keyed, which makes Restate scheduled them to obey single-writer-per-key semantics.
* **Consistent State**: keyed user code can attach key/value state, which is eagerly pushed into handlers during invocation, and written back upon completion. This is particularly efficient for FaaS deployments (stateful serverless, yay!).
* **Observability & Introspection**: Restate automatically generates Open Telemetry traces for the interactions between handlers and gives you a SQL shell to query the distributed state of the application.

## Contributing

We’re excited if you join the Restate community and start contributing!
Whether it is feature requests, bug reports, ideas & feedback or PRs, we appreciate any and all contributions.
We know that your time is precious and, therefore, deeply value any effort to contribute!

Check out our [development guidelines](/docs/dev/development-guidelines.md) and [tips for local development](/docs/dev/local-development.md) to get started.

### Building Restate locally

In order to build Restate locally [follow the build instructions](https://github.com/restatedev/restate/blob/main/docs/dev/local-development.md#building-restate).

