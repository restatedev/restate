# Releasing Restate

Restate artifacts:

* Runtime (this repo)
* [SDK-Typescript](https://github.com/restatedev/sdk-typescript) and [Node template](https://github.com/restatedev/node-template-generator)
* [Proto](https://github.com/restatedev/proto/)
* [Documentation](https://github.com/restatedev/documentation/) and [Tour of Restate - Typescript](https://github.com/restatedev/tour-of-restate-typescript)
* [Examples](https://github.com/restatedev/examples)

## Versioning policy

In order to keep the versioning of our artifacts simple, we're currently following this release policy:

* We treat minor as major: every minor can break in a non-backward compatible way. E.g. 0.5.x doesn't have to be compatible with 0.4.x. Neverthless, we should clearly state it in the breaking changes section of the release notes.
* We keep in sync minor releases between artifacts. E.g. when releasing runtime 0.5.0, we release also 0.5.0 for every SDK, documentation, examples and proto.
* Patch releases of the same minor release must be compatible with each other. E.g. every SDK 0.5.x must be compatible with every runtime 0.5.x release, and viceversa.

This release policy applies to all the aforementioned artifacts.

## Pre-release

Before releasing, make sure all the issues tagged with release-blocker have either been solved, or PRs are ready to solve them:
https://github.com/issues?q=is%3Aopen+org%3Arestatedev+label%3Arelease-blocker

## Release order

When performing a full release of all the artifacts, this order should be followed:

1. [Runtime](#releasing-the-restate-runtime)
1. [Proto](https://github.com/restatedev/proto/)
1. After releasing proto you might need to update the descriptors in the SDKs and examples (`buf.lock` files or `git subtree` updates, refer to the specific release docs of repos)
1. [Java SDK](https://github.com/restatedev/sdk-java/blob/main/development/release.md)
1. [Typescript SDK](https://github.com/restatedev/sdk-typescript#releasing-the-package) and [Node template generator](https://github.com/restatedev/node-template-generator#releasing)
1. Execute a manual run of the [e2e tests](https://github.com/restatedev/e2e/actions/workflows/e2e.yaml) to check everything works fine.
1. [CDK](https://github.com/restatedev/cdk) (might require a release if the server's API changed)
1. [Operator](https://github.com/restatedev/restate-operator) (might require a release if the server's API changed)
1. [Examples](https://github.com/restatedev/examples#releasing-for-restate-developers)
1. [Documentation](https://github.com/restatedev/documentation#releasing-the-documentation)

## Post release

1. Update the sdk-java version the e2e tests is using to the new snapshot: https://github.com/restatedev/e2e/blob/main/gradle/libs.versions.toml

## Releasing the Restate runtime

In order to release the Restate runtime, you first have to make sure that the version field in the [Cargo.toml](/Cargo.toml) is set to the new release version `X.Y.Z`. 
Then you have to create a tag of the form `vX.Y.Z` and push it to the repository.
The tag will trigger the [release.yml](/.github/workflows/release.yml) workflow which does the following:

* Running the local tests
* Creating and pushing the Docker image with the runtime
* Creating a draft release

In order to finish the release, you have to publish it [here](https://github.com/restatedev/restate/releases).

Please also bump the version in the [Cargo.toml](/Cargo.toml) to the next patch version after the release.

If you are releasing a new major/minor version of the runtime, please also create a new release of the [documentation](https://github.com/restatedev/restate).
