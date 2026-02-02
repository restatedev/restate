# Releasing Restate

Restate artifacts to release:

* Runtime (this repo)
* [Documentation](https://github.com/restatedev/docs-restate)
* [Examples](https://github.com/restatedev/examples)
* [Operator](https://github.com/restatedev/restate-operator)
* [CDK Constructs](https://github.com/restatedev/cdk)

Check the respective documentation of the single artifacts to perform a release.

## Versioning policy

We follow [SemVer](https://semver.org/):

> Given a version number MAJOR.MINOR.PATCH, increment the:

> * MAJOR version when you make incompatible API changes
> * MINOR version when you add functionality in a backward compatible manner
> * PATCH version when you make backward compatible bug fixes

Runtime and SDKs follow independent artifact versioning. Restate server and SDK compatibility is defined by the intersection of supported service protocol versions.

## Pre-release

Before releasing, make sure all the issues tagged with release-blocker have either been solved, or PRs are ready to solve them:
https://github.com/issues?q=is%3Aopen+org%3Arestatedev+label%3Arelease-blocker

Confirm if any SDK releases are needed to keep up with the runtime and/or service protocol releases.

Check that the e2e tests are passing:

* [Jepsen tests](https://github.com/restatedev/jepsen/actions)
* [E2e verification runner](https://github.com/restatedev/e2e-verification-runner/actions)
* [E2e tests](https://github.com/restatedev/e2e/actions/workflows/ci.yml)

## Releasing the Restate runtime

1. Make sure that the version is set to the new release version `X.Y.Z`. fields in: 
  - [/Cargo.toml](/Cargo.toml) 
  - [charts/restate-helm/Chart.yaml](/charts/restate-helm/Chart.yaml) 
1. Make sure that [COMPATIBILITY_INFORMATION](/crates/node/src/cluster_marker.rs) is updated if `X.Y.Z` changes the requirements for backward/forward compatible Restate versions.
1. [Publish the unreleased release notes](/release-notes/README.md#release-process).
1. Create a tag of the form `vX.Y.Z` and push it to the repository. The tag will trigger the [release.yml](/.github/workflows/release.yml) workflow which runs the unit tests, the e2e tests, creates the docker image of the runtime, builds the CLI/runtime binaries, and prepares a Github draft release.
1. Manually publish the draft release created by the release automation [here](https://github.com/restatedev/restate/releases).
1. Bump the version in the [Cargo.toml](/Cargo.toml) to the next patch version with a `-dev` suffix after the release. The `-dev` suffix is helpful for distinguishing between versions that are under development and those that are released.

**Note:** 
Don't immediately create a release branch after a MAJOR/MINOR release.
A release branch `release-MAJOR.MINOR` should only be created once a change to the storage formats, APIs or a new feature gets merged that should be shipped with the next MAJOR/MINOR release.

## Post-release

If you are releasing a new major/minor version of the runtime, please also create a new release of the [documentation](https://github.com/restatedev/docs-restate).
