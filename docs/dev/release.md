# Releasing Restate

Restate artifacts to release:

* Runtime (this repo)
* [SDK-Typescript](https://github.com/restatedev/sdk-typescript/?tab=readme-ov-file#releasing-the-package) and [Node template](https://github.com/restatedev/node-template-generator)
* [Java SDK](https://github.com/restatedev/sdk-java/blob/main/development/release.md)
* [Documentation](https://github.com/restatedev/documentation/)
* [Examples](https://github.com/restatedev/examples)
* [Service protocol](https://github.com/restatedev/service-protocol)
* [CDK](https://github.com/restatedev/cdk)
* [Operator](https://github.com/restatedev/restate-operator)

Check the respective documentation of the single artifacts to perform a release.

## Versioning policy

We follow [SemVer](https://semver.org/):

> Given a version number MAJOR.MINOR.PATCH, increment the:

> * MAJOR version when you make incompatible API changes
> * MINOR version when you add functionality in a backward compatible manner
> * PATCH version when you make backward compatible bug fixes

Runtime and SDKs follow independent versioning. SDKs should declare their compatibility matrix with respective runtime versions.

## Pre-release

Before releasing, make sure all the issues tagged with release-blocker have either been solved, or PRs are ready to solve them:
https://github.com/issues?q=is%3Aopen+org%3Arestatedev+label%3Arelease-blocker

## Releasing the Restate runtime

1. Make sure that the version field in the [Cargo.toml](/Cargo.toml) and [Chart.yaml](/charts/restate-helm/Chart.yaml) is set to the new release version `X.Y.Z`. 
1. Create a tag of the form `vX.Y.Z` and push it to the repository. The tag will trigger the [release.yml](/.github/workflows/release.yml) workflow which runs the unit tests, the e2e tests, creates the docker image of the runtime, builds the CLI/runtime binaries, and prepares a Github draft release.
1. Manually publish the draft release created by the release automation [here](https://github.com/restatedev/restate/releases).
1. In case you're creating a MAJOR or MINOR release, create the branch with the name `release-MAJOR.MINOR` as well.
1. Bump the version in the [Cargo.toml](/Cargo.toml) to the next patch version after the release.

## Post-release

If you are releasing a new major/minor version of the runtime, please also create a new release of the [documentation](https://github.com/restatedev/restate).
