[![Documentation](https://img.shields.io/badge/doc-reference-blue)](https://docs.restate.dev)
[![Examples](https://img.shields.io/badge/view-examples-blue)](https://github.com/restatedev/examples)
[![Discord](https://img.shields.io/discord/1128210118216007792?logo=discord)](https://discord.gg/skW3AZ6uGd)
[![Slack](https://img.shields.io/badge/Slack-4A154B?logo=slack&logoColor=fff)](https://join.slack.com/t/restatecommunity/shared_invite/zt-2v9gl005c-WBpr167o5XJZI1l7HWKImA)
[![Twitter](https://img.shields.io/twitter/follow/restatedev.svg?style=social&label=Follow)](https://x.com/intent/follow?screen_name=restatedev)

# Restate - Building resilient applications made easy!

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://restate.dev/blog/announcing-restate-1.0-restate-cloud-and-our-seed-funding-round/title_figure_hudf3f5cd4c2c091de8198c7c4b273b831_2636815_6620x0_resize_q75_h2_box_3.webp">
    <source media="(prefers-color-scheme: light)" srcset="https://restate.dev/blog/announcing-restate-1.0-restate-cloud-and-our-seed-funding-round/title_figure_hudf3f5cd4c2c091de8198c7c4b273b831_2636815_6620x0_resize_q75_h2_box_3.webp">
    <img alt="Restate overview" src="https://restate.dev/blog/announcing-restate-1.0-restate-cloud-and-our-seed-funding-round/title_figure_hudf3f5cd4c2c091de8198c7c4b273b831_2636815_6620x0_resize_q75_h2_box_3.webp" width="100%">
  </picture>
</p>

[Restate](https://restate.dev) is the simplest way to build resilient applications.

Restate provides a distributed durable version of your everyday building blocks, letting you build a wide range of use cases:

* [Durable AI Agents](https://github.com/restatedev/ai-examples)
* [Workflows-as-Code](https://docs.restate.dev/use-cases/workflows)
* [Microservice Orchestration](https://docs.restate.dev/use-cases/microservice-orchestration)
* [Event Processing](https://docs.restate.dev/use-cases/event-processing)
* [Async Tasks](https://docs.restate.dev/use-cases/async-tasks)
* [Agents, Stateful Actors, state machines, and much more](https://github.com/restatedev/examples)

## Get started with Restate

1. 🏎 [Follow the Quickstart](https://docs.restate.dev/get_started/quickstart) to get Restate up and running within 2 minutes!
1. 💡 [The Tour of Restate](https://docs.restate.dev/get_started/tour) walks you through the most important features of Restate.

## SDKs

Restate supports the following SDKs:

* [Typescript](https://github.com/restatedev/sdk-typescript)
* [Java and Kotlin](https://github.com/restatedev/sdk-java)
* [Python](https://github.com/restatedev/sdk-python)
* [Go](https://github.com/restatedev/sdk-go)
* [Rust](https://github.com/restatedev/sdk-rust)

## Install

We offer pre-built binaries of the CLI and the server for MacOS and Linux.

Have a look at the [Quickstart](https://docs.restate.dev/get_started/quickstart) or [installation instructions in the docs](https://docs.restate.dev/develop/local_dev). 

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
docker run --rm -p 8080:8080 -p 9070:9070 -p 9071:9071 \
    --add-host=host.docker.internal:host-gateway docker.restate.dev/restatedev/restate:latest
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

Run via npx:
```bash
npx @restatedev/restate
```

You can also download the binaries from the [release page](https://github.com/restatedev/restate/releases) or our [download page](https://restate.dev/get-restate/).

## Community

* 🤗️ Join our online community on [Discord](https://discord.gg/skW3AZ6uGd) or [Slack](https://join.slack.com/t/restatecommunity/shared_invite/zt-2v9gl005c-WBpr167o5XJZI1l7HWKImA) for help, sharing feedback and talking to the community.
* 📖 [Check out our documentation](https://docs.restate.dev) to get started quickly!
* 📣 [Follow us on Twitter](https://twitter.com/restatedev) for staying up to date.
* 🙋 [Create a GitHub issue](https://github.com/restatedev/restate/issues) for requesting a new feature or reporting a problem.
* 🏠 [Visit our GitHub org](https://github.com/restatedev) for exploring other repositories.

## Core primitives

The basic primitives Restate offers to simplify application development are the following:

* **Reliable Execution**: Restate guarantees code runs to completion. Failures result in retries that use the [Durable Execution mechanism](https://docs.restate.dev/concepts/durable_execution) to recover partial progress and prevent re-executing completed steps.
* **Reliable Communication**: Services communicate with exactly-once semantics: whether it's [request-response, one-way messages, or scheduled tasks](https://docs.restate.dev/concepts/invocations). Restate reliably delivers messages and uses Durable Execution to ensure no losses or duplicates can happen.
* **Durable Promises and Timers**: Register Promises/Futures and timers in Restate to make them resilient to failures (e.g. sleep, webhooks, timers). Restate can recover them across failures, processes, and time.
* **Consistent State**: Implement [stateful entities](https://docs.restate.dev/concepts/services) with isolated K/V state per entity. Restate persists the K/V state updates together with the execution progress to ensure consistent state. Restate attaches the K/V state to the request on invocation, and writes it back upon completion. This is particularly efficient for FaaS deployments (stateful serverless, yay!).
* **Suspending User Code**: long-running code suspends when awaiting on a Promise/Future and resumes when that promise is resolved. This is particularly useful in combination with serverless deployments.
* **Observability & Introspection**: Restate includes a UI and CLI to inspect the [state of your application](https://docs.restate.dev/operate/introspection) across services and invocations. Restate automatically generates Open Telemetry traces for the interactions between handlers.

## Contributing

We’re excited if you join the Restate community and start contributing!
Whether it is feature requests, bug reports, ideas & feedback or PRs, we appreciate any and all contributions.
We know that your time is precious and, therefore, deeply value any effort to contribute!

Check out our [development guidelines](/docs/dev/development-guidelines.md) and [tips for local development](/docs/dev/local-development.md) to get started.

## Versions

Restate follows [Semantic Versioning](https://semver.org/).

You can safely upgrade from a Restate `x.y` to `x.(y+1)` release without performing any manual data migration, as Restate performs an automatic data migration for you.

For SDK compatibility, refer to the supported version matrix in the respective READMEs:

* [Restate Java/Kotlin SDK](https://github.com/restatedev/sdk-java#versions)
* [Restate TypeScript SDK](https://github.com/restatedev/sdk-typescript#versions)
* [Restate Go SDK](https://github.com/restatedev/sdk-go#versions)
* [Restate Python SDK](https://github.com/restatedev/sdk-python#versions)
* [Restate Rust SDK](https://github.com/restatedev/sdk-rust#versions)

### Building Restate locally

In order to build Restate locally [follow the build instructions](https://github.com/restatedev/restate/blob/main/docs/dev/local-development.md#building-restate).
