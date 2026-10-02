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

Prepare the release first, optionally cut release candidates for testing, then create the final release. Release candidates must be built from the prepared state so that they test what the final release ships.

### Preparing the release

1. Update [COMPATIBILITY_INFORMATION](/crates/types/src/cluster_marker.rs) if `X.Y.Z` changes the backward/forward compatible Restate versions.
1. [Publish the unreleased release notes](/release-notes/README.md#release-process) as `release-notes/vX.Y.Z.md`.

### Creating release candidates (optional)

Release candidates use the version `X.Y.Z-rc.N`, starting at `N = 1`.

1. Set the version to `X.Y.Z-rc.N` in [/Cargo.toml](/Cargo.toml) and [charts/restate-helm/Chart.yaml](/charts/restate-helm/Chart.yaml), run `cargo check` to update [Cargo.lock](/Cargo.lock), and commit.
1. Tag the commit as `vX.Y.Z-rc.N` and push the tag. The [release.yml](/.github/workflows/release.yml) workflow creates a Github pre-release and publishes the docker images and npm packages under `X.Y.Z-rc.N` (npm dist-tag `next`, docker `latest` is not moved). Homebrew is skipped for pre-releases.
1. Set the version back to `X.Y.Z-dev`, or keep the rc version if the next candidate follows shortly.

Repeat with an incremented `N` for every fix that needs validation. Update the release notes and compatibility information before the next candidate if a fix requires it.

### Creating the final release

1. Set the version to `X.Y.Z` in [/Cargo.toml](/Cargo.toml) and [charts/restate-helm/Chart.yaml](/charts/restate-helm/Chart.yaml).
1. Tag the commit as `vX.Y.Z` and push the tag. The [release.yml](/.github/workflows/release.yml) workflow runs the unit and e2e tests, builds the binaries and docker images, creates the [Github release](https://github.com/restatedev/restate/releases), and publishes the docker images, npm packages and Homebrew formulae.
1. Verify that the workflow succeeded and the artifacts are available.
1. Bump the version to the next patch version with a `-dev` suffix to distinguish development builds from releases.
1. Upload the Grafana dashboards to the marketplace: log into grafana.com, go to My Account -> Org Settings -> My Dashboards, open `Details` for the dashboard, and click `Upload Revision` at the bottom of the page.

**Note:**
Don't immediately create a release branch after a MAJOR/MINOR release.
A release branch `release-MAJOR.MINOR` should only be created once a change to the storage formats, APIs or a new feature gets merged that should be shipped with the next MAJOR/MINOR release.

## Post-release

If you are releasing a new major/minor version of the runtime, please also create a new release of the [documentation](https://github.com/restatedev/docs-restate).
