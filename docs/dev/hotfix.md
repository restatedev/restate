# Hotfix Releases

Prefer a regular patch release that follows the [release process](release.md). Use a hotfix when users need fixes on top of a released version `vX.Y.Z` before the next patch release is available.

A hotfix is released as the SemVer pre-release `X.Y.Z-hotfix.N`, so it never becomes the latest release. Don't use `X.Y.Z+hotfix.N`: npm and Docker can't publish build metadata as a separate version, and the release tooling would treat the hotfix as a stable release. As a pre-release, a hotfix still passes the `X.Y.Z-dev` feature gates, so it can enable every feature of `X.Y.Z`.

## Naming

| Item | Name |
| --- | --- |
| Branch | `hotfix/X.Y.Z-hotfix.N` |
| Cargo and Helm version | `X.Y.Z-hotfix.N` |
| Git tag | `vX.Y.Z-hotfix.N` |

Use the next unused `N` for every hotfix of the same release.

## Releasing a hotfix

1. Create `hotfix/X.Y.Z-hotfix.N` from `vX.Y.Z` and cherry-pick the fixes with `git cherry-pick -x`.
1. Set the version to `X.Y.Z-hotfix.N` in [/Cargo.toml](/Cargo.toml) and [charts/restate-helm/Chart.yaml](/charts/restate-helm/Chart.yaml), run `cargo check` to update [Cargo.lock](/Cargo.lock), and commit.
1. Push the branch. CI runs on the push.
1. Tag the tested commit as `vX.Y.Z-hotfix.N` and push the tag. The [release.yml](/.github/workflows/release.yml) workflow publishes the hotfix like a release candidate: a GitHub pre-release with binaries, Docker images, npm packages (dist-tag `next`), and the Helm chart. Homebrew is skipped.
1. Verify that the artifacts are available and add the list of fixes to the description of the GitHub release.
1. Include the fixes in the next regular patch release.

## Image-only hotfix

To ship only a GHCR image, prepare the branch the same way but don't push a tag, since any tag containing `X.Y.Z` starts the full release. Instead, run the `Build Docker image` workflow on the branch:

```sh
gh workflow run docker.yml --repo restatedev/restate --ref hotfix/X.Y.Z-hotfix.N
```

The image is pushed as `ghcr.io/restatedev/restate:hotfix-X.Y.Z-hotfix.N`. Give users its digest and keep the branch, which is the only reference to the image's source.
