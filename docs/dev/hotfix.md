# Hotfix Releases

Prefer a regular patch release for fixes that can follow the normal [release process](release.md). Use a hotfix when users need fixes on top of a released version before the next patch release is available.

A hotfix of `vX.Y.Z` is the SemVer pre-release `X.Y.Z-hotfix.N`. There are two kinds:

* **Published hotfix:** released by [release.yml](/.github/workflows/release.yml) like a release candidate: a GitHub pre-release with binaries, Docker images, npm packages, and the Helm chart.
* **Image-only hotfix:** a single GHCR image built from the hotfix branch. Nothing else is published.

Neither kind becomes the latest release anywhere.

## Naming

For the `N`th hotfix of `v1.8.1`:

| Item | Name |
| --- | --- |
| Branch | `hotfix/1.8.1-hotfix.N` |
| Cargo and Helm version | `1.8.1-hotfix.N` |
| Git tag (published only) | `v1.8.1-hotfix.N` |
| Docker images (published) | `restatedev/restate:1.8.1-hotfix.N` and `restatedev/restate-cli:1.8.1-hotfix.N` on GHCR and Docker Hub |
| npm packages (published) | `@restatedev/restate@1.8.1-hotfix.N`, `@restatedev/restate-server@1.8.1-hotfix.N`, `@restatedev/restatectl@1.8.1-hotfix.N` |
| Docker image (image-only) | `ghcr.io/restatedev/restate:hotfix-1.8.1-hotfix.N` |

As for `release/X.Y` branches, branch names don't carry the `v`; tags do.

Use the next unused `N` for every hotfix of the same release, including image-only ones. A hotfix is built from the release tag, not from an earlier hotfix, so check that a new hotfix still contains the fixes of earlier hotfixes that users rely on.

### Why `-hotfix.N` and not `+hotfix.N`

SemVer has no post-release versions. Build metadata (`X.Y.Z+hotfix.N`) has the same precedence as `X.Y.Z`: npm treats it as `X.Y.Z`, which is already published, Docker tags cannot contain `+`, and the release tooling treats it as a stable release that becomes the latest. The pre-release label is what keeps a hotfix off `latest` everywhere: the GitHub release is a pre-release, npm uses the `next` dist-tag, Docker moves neither `latest` nor `X.Y`, and Homebrew is skipped.

A pre-release ranks below `X.Y.Z`, so a hotfix server logs a notice that `vX.Y.Z` is available. It ranks above `X.Y.Z-dev`, which is the version that feature gates use (the `RESTATE_VERSION_*` constants in [restate_version.rs](/crates/types/src/restate_version.rs)), so a hotfix can enable every feature of `X.Y.Z`. Always base the version on the patched release: `X.Y.(Z+1)-hotfix.N` would pass the gates of `X.Y.(Z+1)` features that the hotfix does not contain.

## Preparing the hotfix branch

1. Choose the release tag `vX.Y.Z` to patch and the next unused hotfix number `N`. Check the existing `vX.Y.Z-hotfix.*` tags, `hotfix/X.Y.Z-hotfix.*` branches, and GHCR images.
1. Create `hotfix/X.Y.Z-hotfix.N` from `vX.Y.Z`.
1. Cherry-pick the fixes with `git cherry-pick -x`. Prefer fixes that are merged into `main`. If a fix is still an open PR, pick its squashed head and check again before tagging that the PR has not changed since.
1. Bring the workflows up to date. Pushes and tags run the workflows of the hotfix branch, not those of `main`, so fixes to workflows and permissions merged since `vX.Y.Z` are missing. List them with `git log --reverse vX.Y.Z..main -- .github dist-workspace.toml` and cherry-pick the ones that are needed, including dependency or lockfile commits they build on. The branch needs at least:
   * `hotfix/*` in the push triggers of [ci.yml](/.github/workflows/ci.yml), so that pushing the branch runs CI.
   * `packages: write` for the [docker.yml](/.github/workflows/docker.yml) job and its callers, so that images can be pushed to GHCR.
   * For published hotfixes: pre-release versions must not move the `latest` tag of the `restate-cli` image in [docker-push-release.yml](/.github/workflows/docker-push-release.yml).
1. Set the version to `X.Y.Z-hotfix.N` in [/Cargo.toml](/Cargo.toml) and [charts/restate-helm/Chart.yaml](/charts/restate-helm/Chart.yaml), run `cargo check` to update [Cargo.lock](/Cargo.lock), and commit.
1. For published hotfixes, check that cargo-dist accepts the branch: `dist plan --tag=vX.Y.Z-hotfix.N --output-format=json` must report `"announcement_is_prerelease": true`, and `dist generate --mode=ci --check` must pass.
1. Push the branch. CI runs on the push. Run any additional tests relevant to the fixes.

## Publishing a hotfix release

1. Tag the tested commit as `vX.Y.Z-hotfix.N` and push the tag. The [release.yml](/.github/workflows/release.yml) workflow runs the unit and e2e tests, builds the binaries and Docker images, creates the GitHub pre-release, and publishes the Docker images, npm packages, and Helm chart under `X.Y.Z-hotfix.N`.
1. Verify that the workflow succeeded and that the artifacts are available. The GitHub release must be marked as a pre-release, the Docker `latest` and `X.Y` tags must not have moved, and npm must have published the packages with the `next` dist-tag.
1. Add the list of fixes to the description of the GitHub release. The release automation only writes installation instructions. Its Homebrew instructions install the latest stable release, not the hotfix.
1. Homebrew does not get the hotfix: the tap only has unversioned formulae and only follows the latest stable release. Point users to the GitHub release, npm, or Docker instead.

## Building an image-only hotfix

1. Prepare the hotfix branch as described above.
1. Do not push a `vX.Y.Z-hotfix.N` tag. Any tag containing `X.Y.Z` starts the full release workflow.
1. Run the `Build Docker image` workflow on the hotfix branch: `gh workflow run docker.yml --repo restatedev/restate --ref hotfix/X.Y.Z-hotfix.N`. Keep `profile` set to `release` and leave `pushToDockerHub` disabled.
1. The image is pushed as `ghcr.io/restatedev/restate:hotfix-X.Y.Z-hotfix.N`. Branch builds never move `latest` or the version tags. Record the image digest (`docker buildx imagetools inspect ghcr.io/restatedev/restate:hotfix-X.Y.Z-hotfix.N`) and give users the digest, which is the immutable reference to the delivered image.
1. Keep the hotfix branch. Without a tag, it is the only reference to the source of the image.

## After the hotfix

1. Include the fixes in the next regular patch release and move users to that release when it is available.
1. Delete the branch of a published hotfix when it is no longer needed. The tag and the published artifacts remain.
